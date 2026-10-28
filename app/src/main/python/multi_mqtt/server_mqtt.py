#!/usr/bin/env python3
import copy
import json
import logging
import os
import tempfile
import threading
import time
import sys
from pathlib import Path
from urllib.parse import urlsplit

from ecdsa import VerifyingKey
from multi_mqtt import (
    BROKER_LIST,
    ConnectionQualityStats,
    MultiMQTTManager,
    _describe_public_key,
    get_standard_public_pem_bytes,
)
from rpc_executor import utc_ms,stime
from rpc_executor import PythonExecutor#, format_result

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"
DEFAULT_REPLY_TOPIC = "sys/device/response"
DEFAULT_PUB_KEY='ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBOf+GndQauvHHEICc3HTrEeV9bv5S/FTiI64tnMAUPCAaofEbRDmXa8d5R54k8tYOPjt+zdnZJaoSMmJ2pnp4ys='

RPC_CONFIG_PATH = Path(__file__).with_name("rpc.json")
_RPC_CONFIG_LOCK = globals().setdefault('_RPC_CONFIG_LOCK', threading.RLock())  # reload 会重造新锁对象：正攥旧锁写 rpc.json 的线程与拿新锁的线程同时进临界区，setdefault 保住同一把锁跨 reload
gms = globals().setdefault('gms', None)  # 当前活着的服务端实例（构造/启动时自注册 gms = self），reload 重执行把名字重绑回 None——rpc_config()/apply 热配置路径（324-352 行）静默失效直到下次实例化；setdefault 让 reload 后仍指向活着的实例

# --- [global_topic_list 监听 topic 定期报告] 默认配置 ---
# 命名说明：stats.get_report 是网络连接质量报告；本组是"监听 topic 列表"报告，
# 定期发布到 global_topic_list，供新订阅者经 Retain 消息发现服务端监听 topic
# 全部参数均可在 rpc.json 中按同名键覆盖，运行期经 rpc_config(...) 热更新
DEFAULT_GLOBAL_TOPIC_LIST = "global_topic_list"          # 报告发布到的 topic
DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL = 3600.0              # 发布间隔：空闲期每小时一次
DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD = 86400.0       # 距上次下发指令超过 24 小时视为空闲（可配置）
DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL = ConnectionQualityStats.PING_INTERVAL  # 默认复用连接质量 ping 间隔（600s），与 ping 同节拍统一时间驱动避免交错唤醒；可配置为更小值如 15 便于测试
# 发布参数（经 **ka 一路透传到 paho-mqtt）：QoS1+Retain，
# broker 会连同 QoS 一起保存保留消息，新订阅者上线即收到最新 topic 列表
DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS = {"qos": 1, "retain": True}
DEFAULT_GLOBAL_TOPIC_URL = "https://qmm.pythonanywhere.com/"


def _default_rpc_config():
    return {
        "broker_list": copy.deepcopy(BROKER_LIST),
        "broker_list_add": [],
        "broker_list_del": [],
        "request_topic": REQUEST_TOPIC,
        "pub_key": DEFAULT_PUB_KEY,
        "global_topic_url": DEFAULT_GLOBAL_TOPIC_URL,
        # [global_topic_list 监听 topic 报告] 参数（同名键可在 rpc.json 覆盖）
        "enable_global_topic_list": True,
        "global_topic_list": DEFAULT_GLOBAL_TOPIC_LIST,
        "global_topic_list_interval": DEFAULT_GLOBAL_TOPIC_LIST_INTERVAL,
        "global_topic_list_idle_threshold": DEFAULT_GLOBAL_TOPIC_LIST_IDLE_THRESHOLD,
        "global_topic_list_check_interval": DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL,
        "global_topic_list_publish_kwargs": dict(DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS),
    }


def _normalize_broker_list(brokers, name, allow_empty=False):
    if not isinstance(brokers, list) or (not brokers and not allow_empty):
        suffix = "列表" if allow_empty else "非空列表"
        raise ValueError(f"{name} 必须是{suffix}")
    normalized_brokers = []
    for index, entry in enumerate(brokers):
        if not isinstance(entry, (list, tuple)) or len(entry) not in (2, 3):
            raise ValueError(f"{name}[{index}] 必须包含 host、port 和可选认证信息")
        host, port = entry[:2]
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"{name}[{index}] 的 host 必须是非空字符串")
        if isinstance(port, bool) or not isinstance(port, (int, str)):
            raise ValueError(f"{name}[{index}] 的 port 必须是整数")
        if isinstance(port, str) and not port.isdecimal():
            raise ValueError(f"{name}[{index}] 的 port 必须是整数")
        try:
            port = int(port)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{name}[{index}] 的 port 必须是整数") from error
        if not 1 <= port <= 65535:
            raise ValueError(f"{name}[{index}] 的 port 超出有效范围")
        broker = [host.strip(), port]
        if len(entry) == 3:
            auth = entry[2]
            if not (
                isinstance(auth, str)
                or isinstance(auth, (list, tuple)) and len(auth) >= 2
            ):
                raise ValueError(f"{name}[{index}] 的认证信息格式无效")
            broker.append(list(auth) if isinstance(auth, tuple) else auth)
        normalized_brokers.append(tuple(broker))
    return normalized_brokers


def _broker_identity(broker):
    return broker[0].casefold(), broker[1]


def _normalize_positive_seconds(value, name):
    """正数秒校验：接受 int/float/纯数字字符串，统一归一为 float。"""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} 必须是正数（秒）")
    try:
        number = float(value.strip() if isinstance(value, str) else value)
    except ValueError as error:
        raise ValueError(f"{name} 必须是正数（秒）") from error
    if not (number > 0.0) or number == float("inf"):   # 同时挡住 0/负数/NaN/inf
        raise ValueError(f"{name} 必须是正数（秒）")
    return number


def _normalize_rpc_config(config):
    if not isinstance(config, dict):
        raise TypeError("RPC 配置必须是 JSON object")
    result = _default_rpc_config()
    known_keys = set(result)
    result.update({key: value for key, value in config.items() if key in known_keys})

    broker_base = _normalize_broker_list(
        config.get("_broker_list_base", result["broker_list"]), "broker_list"
    )
    brokers_add = _normalize_broker_list(
        result["broker_list_add"], "broker_list_add", allow_empty=True
    )
    brokers_del = _normalize_broker_list(
        result["broker_list_del"], "broker_list_del", allow_empty=True
    )
    deleted = {_broker_identity(broker) for broker in brokers_del}
    effective_brokers = [
        broker for broker in broker_base
        if _broker_identity(broker) not in deleted
    ]
    for broker in brokers_add:
        identity = _broker_identity(broker)
        for index, existing in enumerate(effective_brokers):
            if _broker_identity(existing) == identity:
                effective_brokers[index] = broker
                break
        else:
            effective_brokers.append(broker)
    result["_broker_list_base"] = broker_base
    result["broker_list"] = effective_brokers
    result["broker_list_add"] = brokers_add
    result["broker_list_del"] = brokers_del

    topic = result["request_topic"]
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("request_topic 必须是非空字符串")
    result["request_topic"] = topic.strip()

    pub_key = result["pub_key"]
    if isinstance(pub_key, bytes):
        try:
            pub_key = pub_key.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("pub_key 必须是 UTF-8 公钥字符串") from error
    if pub_key is not None and not isinstance(pub_key, str):
        raise ValueError("pub_key 必须是字符串或 null")
    if pub_key:
        try:
            normalized_key = get_standard_public_pem_bytes(pub_key)
            VerifyingKey.from_pem(normalized_key)
        except Exception as error:
            raise ValueError(f"pub_key 无法解析: {error}") from error
    result["pub_key"] = pub_key or None
    global_topic_url = result["global_topic_url"]
    if not isinstance(global_topic_url, str) or not global_topic_url.strip():
        raise ValueError("global_topic_url 必须是非空 HTTP(S) URL")
    global_topic_url = global_topic_url.strip()
    parsed_url = urlsplit(global_topic_url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
        raise ValueError("global_topic_url 必须是非空 HTTP(S) URL")
    result["global_topic_url"] = global_topic_url

    # [global_topic_list 监听 topic 报告] 参数校验（qos 钉死规则在 MQTTServer.__init__）
    report_topic = result["global_topic_list"]
    if not isinstance(report_topic, str) or not report_topic.strip():
        raise ValueError("global_topic_list 必须是非空字符串")
    result["global_topic_list"] = report_topic.strip()
    result["global_topic_list_interval"] = _normalize_positive_seconds(
        result["global_topic_list_interval"], "global_topic_list_interval")
    result["global_topic_list_idle_threshold"] = _normalize_positive_seconds(
        result["global_topic_list_idle_threshold"], "global_topic_list_idle_threshold")
    result["global_topic_list_check_interval"] = _normalize_positive_seconds(
        result["global_topic_list_check_interval"], "global_topic_list_check_interval")
    enable_report = result["enable_global_topic_list"]
    if isinstance(enable_report, bool):
        pass
    elif isinstance(enable_report, str) and enable_report.strip().lower() in ("true", "false"):
        enable_report = enable_report.strip().lower() == "true"
    elif isinstance(enable_report, int) and enable_report in (0, 1):
        enable_report = bool(enable_report)
    else:
        raise ValueError("enable_global_topic_list 必须是布尔值（true/false）")
    result["enable_global_topic_list"] = enable_report
    publish_kwargs = result["global_topic_list_publish_kwargs"]
    if publish_kwargs is None:
        publish_kwargs = dict(DEFAULT_GLOBAL_TOPIC_LIST_PUBLISH_KWARGS)
    elif not isinstance(publish_kwargs, dict):
        raise ValueError("global_topic_list_publish_kwargs 必须是 JSON object 或 null")
    result["global_topic_list_publish_kwargs"] = dict(publish_kwargs)
    return result


def _json_rpc_config(config):
    return {
        "broker_list": _json_brokers(config["broker_list"]),
        "broker_list_add": _json_brokers(config["broker_list_add"]),
        "broker_list_del": _json_brokers(config["broker_list_del"]),
        "request_topic": config["request_topic"],
        "pub_key": config["pub_key"],
        "global_topic_url": config["global_topic_url"],
        "enable_global_topic_list": config["enable_global_topic_list"],
        "global_topic_list": config["global_topic_list"],
        "global_topic_list_interval": config["global_topic_list_interval"],
        "global_topic_list_idle_threshold": config["global_topic_list_idle_threshold"],
        "global_topic_list_check_interval": config["global_topic_list_check_interval"],
        "global_topic_list_publish_kwargs": config["global_topic_list_publish_kwargs"],
    }


def _json_brokers(brokers):
    return [
        [entry[0], entry[1], list(entry[2]) if isinstance(entry[2], tuple) else entry[2]]
        if len(entry) == 3 else [entry[0], entry[1]]
        for entry in brokers
    ]


def _read_rpc_config_document(path=None):
    config_path = Path(path) if path is not None else RPC_CONFIG_PATH
    with _RPC_CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                stored = json.load(config_file)
        except FileNotFoundError:
            stored = {}
        except json.JSONDecodeError as error:
            raise ValueError(f"RPC 配置文件 JSON 格式错误: {config_path}: {error}") from error
        if not isinstance(stored, dict):
            raise ValueError(f"RPC 配置文件必须包含 JSON object: {config_path}")
        return stored


def load_rpc_config(path=None):
    """读取 rpc.json；未显式设置的项使用内置默认值。"""
    return _normalize_rpc_config(_read_rpc_config_document(path))


def _write_rpc_config_updates(updates, path=None):
    """只持久化用户显式修改的项，避免把内置 broker 凭据复制进配置文件。"""
    config_path = Path(path) if path is not None else RPC_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with _RPC_CONFIG_LOCK:
        try:
            with config_path.open("r", encoding="utf-8") as config_file:
                stored = json.load(config_file)
        except FileNotFoundError:
            stored = {}
        except json.JSONDecodeError as error:
            raise ValueError(f"RPC 配置文件 JSON 格式错误: {config_path}: {error}") from error
        if not isinstance(stored, dict):
            raise ValueError(f"RPC 配置文件必须包含 JSON object: {config_path}")
        source = _read_rpc_config_document(config_path)
        source.update(updates)
        normalized = _normalize_rpc_config(source)
        serialized = _json_rpc_config(normalized)
        for key in updates:
            if key == "broker_list":
                stored[key] = _json_brokers(normalized["_broker_list_base"])
            elif key == "pub_key" and updates[key] == "":
                stored[key] = ""
            elif key in serialized:
                stored[key] = serialized[key]
        fd, temp_path = tempfile.mkstemp(prefix=".rpc.", suffix=".json", dir=config_path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as config_file:
                json.dump(stored, config_file, ensure_ascii=False, indent=2)
                config_file.write("\n")
                config_file.flush()
                os.fsync(config_file.fileno())
            os.replace(temp_path, config_path)
        except Exception:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
            raise


def _resolve_request_topic(request_topic=None, stored=None):
    if request_topic is not None:
        return request_topic

    if stored is None:
        stored = _read_rpc_config_document()
    if "request_topic" in stored:
        return stored["request_topic"]

    try:
        import U
        machine_topic = U.get_machine_id()
        if not isinstance(machine_topic, str) or not machine_topic.strip():
            raise ValueError("get_machine_id() 未返回有效 topic")
        machine_topic = machine_topic.strip()
    except Exception as error:
        logger.warning(
            "获取设备 request_topic 失败，回退到内置默认值 %s: %s",
            REQUEST_TOPIC,
            error,
        )
        return REQUEST_TOPIC

    _write_rpc_config_updates({"request_topic": machine_topic})
    return machine_topic


def rpc_config(**updates):
    """持久化并立即应用设置，例如 rpc_config(request_topic='sys/new/request')。"""
    global gms
    if not updates:
        return rpc_config_get()
    with _RPC_CONFIG_LOCK:
        if gms is not None:
            current = gms.get_rpc_config()
        else:
            current = _normalize_rpc_config(_read_rpc_config_document())
        current["broker_list"] = current["_broker_list_base"]
        if "broker_list" in updates:
            current["_broker_list_base"] = updates["broker_list"]
        candidate = _normalize_rpc_config({**current, **updates})
        previous = _normalize_rpc_config(current)
        if gms is not None:
            gms.apply_rpc_config(candidate)
        try:
            _write_rpc_config_updates(updates)
        except Exception:
            if gms is not None:
                gms.apply_rpc_config(previous)
            raise
        return rpc_config_get()


def rpc_config_get():
    """返回服务当前实际生效的设置；服务未启动时返回 rpc.json 合并默认值。"""
    with _RPC_CONFIG_LOCK:
        if gms is not None:
            return _json_rpc_config(gms.get_rpc_config())
        return _json_rpc_config(load_rpc_config())
get_config=get_rpc=get_rpc_config=rpc_get_config=rpc_config_get  

def _rpc_config_with_cli_overrides(
    request_topic=None, pub=None, brokers=None, global_topic_url=None
):
    """按命令行参数覆盖配置文件，并校验最终 MQTT 配置。"""
    stored = _read_rpc_config_document()
    config = _normalize_rpc_config(stored)
    config["broker_list"] = config["_broker_list_base"]
    config["request_topic"] = _resolve_request_topic(request_topic, stored)
    if pub is not None:
        config["pub_key"] = " ".join(pub) if isinstance(pub, (list, tuple)) else pub
    if brokers is not None:
        config["broker_list"] = []
        config["broker_list_add"] = []
        config["broker_list_del"] = []
        for broker in brokers:
            host, separator, port = broker.rpartition(":")
            if not separator:
                raise ValueError(f"broker 格式必须是 host:port: {broker!r}")
            try:
                config["broker_list"].append((host, int(port)))
            except ValueError as error:
                raise ValueError(f"broker 端口必须是整数: {broker!r}") from error
        config["_broker_list_base"] = config["broker_list"]
    if global_topic_url is not None:
        config["global_topic_url"] = global_topic_url
    return _normalize_rpc_config(config)


def _is_verify_enabled(mqtt_net):
    """
    判断当前 MQTT 网络层是否真的处于"验签"状态。

    MultiMQTTManager 里的关键属性：
      - server_public_key_bytes: 已解析成 PEM bytes（None 表示未配置）
      - server_vk:              ecdsa.VerifyingKey 对象（None 表示未启用）

    公钥配置但解析失败的情况在 MultiMQTTManager.__init__ 里就会让
    ``VerifyingKey.from_pem`` 直接抛异常（构造即失败、fail-closed），
    因此不存在"invalid 标志"这条路径。

    返回 (enabled: bool, reason: str)
    """
    if getattr(mqtt_net, "server_vk", None) is not None:
        return True, "启用"
    if getattr(mqtt_net, "server_public_key_bytes", None):
        # 有 PEM bytes 但没有 vk，说明构造时被异常吞掉过，需要显式暴露
        return False, "公钥已配置但 VerifyingKey 未初始化！"
    return False, "关闭（接收所有消息）"


class MQTTServer:
    def __init__(
        self,
        globals=None,
        server_public_key_bytes=None,
        brokers=BROKER_LIST,
        request_topic=None,
        reply_topic=DEFAULT_REPLY_TOPIC,
        keepalive=None,
        max_reconnect_delay=None,
        enable_global_topic_list=None,
        global_topic_list=None,
        global_topic_list_interval=None,
        global_topic_list_idle_threshold=None,
        global_topic_list_check_interval=None,
        global_topic_list_publish_kwargs=None,
        global_topic_list_payload_fn=None,
        global_topic_url=DEFAULT_GLOBAL_TOPIC_URL,
        rpc_config_source=None,
    ):
        # 实例化网络层管理器（enable_crypto 默认为 False）
        if request_topic is None:
            if rpc_config_source is not None and "request_topic" in rpc_config_source:
                request_topic = rpc_config_source["request_topic"]
            else:
                request_topic = _resolve_request_topic()
        self.request_topic = request_topic
        self.reply_topic = reply_topic
        self._net_options = {}

        # 公钥解析统一由 MultiMQTTManager 内部完成（含 OpenSSH -> PEM 转换）。
        # 这里只把原始输入透传进去，不再重复解析，避免结果不一致。
        net_kwargs = {}
        if keepalive is not None:
            net_kwargs["keepalive"] = keepalive
        if max_reconnect_delay is not None:
            net_kwargs["max_reconnect_delay"] = max_reconnect_delay
        self._net_options = dict(net_kwargs)
        self._rpc_config = _normalize_rpc_config(
            rpc_config_source
            if rpc_config_source is not None
            else {
                "broker_list": brokers,
                "request_topic": request_topic,
                "pub_key": server_public_key_bytes,
                "global_topic_url": global_topic_url,
            }
        )
        self._rpc_config_lock = threading.RLock()
        self.mqtt_net = MultiMQTTManager(
            brokers=brokers,
            log_messages=False,  # 每个 broker 不打印原始消息
            server_public_key_bytes=server_public_key_bytes,
            enable_stats=True,
            **net_kwargs,
        )
        
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor(globals=globals)

        # --- [global_topic_list 定期报告] 状态（tick 由 _dispatch_loop 周期驱动，不另开线程） ---
        # 生效优先级：显式构造参数 > rpc.json（rpc_config_source）> 模块默认值；
        # 解析结果回写 _rpc_config，get_rpc_config()/apply_rpc_config() 看到的即生效值
        self.enable_global_topic_list = (
            self._rpc_config["enable_global_topic_list"]
            if enable_global_topic_list is None
            else bool(enable_global_topic_list)
        )
        self.global_topic_list = (
            self._rpc_config["global_topic_list"]
            if global_topic_list is None
            else str(global_topic_list)
        )
        self.global_topic_list_interval = (
            self._rpc_config["global_topic_list_interval"]
            if global_topic_list_interval is None
            else float(global_topic_list_interval)
        )
        # 距上次下发指令超过该秒数视为空闲（默认 24 小时，可配置），恢复每小时发布
        self.global_topic_list_idle_threshold = (
            self._rpc_config["global_topic_list_idle_threshold"]
            if global_topic_list_idle_threshold is None
            else float(global_topic_list_idle_threshold)
        )
        self.global_topic_list_check_interval = (
            self._rpc_config["global_topic_list_check_interval"]
            if global_topic_list_check_interval is None
            else float(global_topic_list_check_interval)
        )
        # 发布参数缺省 QoS1+Retain，经 **ka 一路透传到 paho-mqtt
        self.global_topic_list_publish_kwargs = (
            dict(self._rpc_config["global_topic_list_publish_kwargs"])
            if global_topic_list_publish_kwargs is None
            else dict(global_topic_list_publish_kwargs)
        )
        # 报告依赖 QoS1+Retain 的 broker 侧保留语义（新订阅者上线即收最新
        # 列表）；自定义发布参数无论传什么，qos 恒为 1，不允许静默跌回 QoS0。
        self.global_topic_list_publish_kwargs["qos"] = 1
        # 自定义报告函数: fn(server) -> dict；None 则用默认的监听 topic 列表
        self.global_topic_list_payload_fn = global_topic_list_payload_fn
        self.global_topic_url = self._rpc_config["global_topic_url"]
        self._rpc_config.update({
            "enable_global_topic_list": self.enable_global_topic_list,
            "global_topic_list": self.global_topic_list,
            "global_topic_list_interval": self.global_topic_list_interval,
            "global_topic_list_idle_threshold": self.global_topic_list_idle_threshold,
            "global_topic_list_check_interval": self.global_topic_list_check_interval,
            "global_topic_list_publish_kwargs": dict(self.global_topic_list_publish_kwargs),
        })
        self._last_gtl_publish_ts = 0.0           # 上次发布报告的时间，0=尚未发布过
        self.last_command_ts = 0.0                # 最近一次收到控制指令的时间，0=从未收到

        verify_enabled, reason = _is_verify_enabled(self.mqtt_net)
        if verify_enabled:
            logger.debug(
                "MQTTServer 初始化完成: 验签已启用, mqtt_pub_key=%s",
                _describe_public_key(self.mqtt_net.server_public_key_bytes),
            )
        else:
            logger.debug("MQTTServer 初始化完成: 验签未启用, 原因=%s", reason)

    def get_rpc_config(self):
        with self._rpc_config_lock:
            return copy.deepcopy(self._rpc_config)

    def apply_rpc_config(self, config):
        """更新服务设置；broker 或公钥变更时以新网络管理器平滑替换旧实例。

        global_topic_list 报告参数（间隔/阈值/tick 粒度/开关）随之热生效：
        check_interval 变更或开关翻转时在当前 manager 上重注册/注销周期任务。
        """
        global gms
        candidate = _normalize_rpc_config(config)
        with self._rpc_config_lock:
            previous = self._rpc_config
            network_changed = (
                candidate["broker_list"] != previous["broker_list"]
                or candidate["pub_key"] != previous["pub_key"]
            )
            topic_changed = candidate["request_topic"] != previous["request_topic"]
            gtl_was_enabled = self.enable_global_topic_list
            gtl_old_check_interval = self.global_topic_list_check_interval
            if network_changed:
                replacement = MultiMQTTManager(
                    brokers=candidate["broker_list"],
                    log_messages=False,
                    server_public_key_bytes=candidate["pub_key"],
                    enable_stats=True,
                    **self._net_options,
                )
                replacement.set_on_message(self.handle_message)
                try:
                    replacement.start()
                    if not replacement.clients:
                        raise RuntimeError("新 MQTT 网络管理器未能创建任何 broker 连接")
                    replacement.subscribe(candidate["request_topic"])
                except Exception:
                    replacement.stop()
                    raise
                previous_net = self.mqtt_net
                self.mqtt_net = replacement
                self.request_topic = candidate["request_topic"]
                self.global_topic_url = candidate["global_topic_url"]
                self._apply_global_topic_list_settings(candidate)
                self._rpc_config = candidate
                if self.enable_global_topic_list:
                    replacement.set_periodic_task(
                        self.global_topic_list_tick,
                        interval=self.global_topic_list_check_interval,
                    )
                previous_net.stop()
            else:
                if topic_changed:
                    self.mqtt_net.subscribe(candidate["request_topic"])
                    self.mqtt_net.unsubscribe(previous["request_topic"])
                self.request_topic = candidate["request_topic"]
                self.global_topic_url = candidate["global_topic_url"]
                self._apply_global_topic_list_settings(candidate)
                self._rpc_config = candidate
                if self.enable_global_topic_list and (
                    not gtl_was_enabled
                    or self.global_topic_list_check_interval != gtl_old_check_interval
                ):
                    self._register_global_topic_list_tick()   # set_periodic_task 覆盖式注册
                elif gtl_was_enabled and not self.enable_global_topic_list:
                    self._unregister_global_topic_list_tick()
            gms = self

    def _apply_global_topic_list_settings(self, candidate):
        """把候选配置的 global_topic_list 报告参数热更新到实例字段。"""
        self.enable_global_topic_list = candidate["enable_global_topic_list"]
        self.global_topic_list = candidate["global_topic_list"]
        self.global_topic_list_interval = candidate["global_topic_list_interval"]
        self.global_topic_list_idle_threshold = candidate["global_topic_list_idle_threshold"]
        self.global_topic_list_check_interval = candidate["global_topic_list_check_interval"]
        self.global_topic_list_publish_kwargs = dict(candidate["global_topic_list_publish_kwargs"])
        # 与 __init__ 同规则：报告的 broker 侧保留语义依赖 QoS1，qos 恒钉在 1
        self.global_topic_list_publish_kwargs["qos"] = 1

    def _reply_publish(self, net, reply_topic, payload):
        """[#17 回包防丢] 回包统一经"收到本请求的 manager"快照发出。

        执行期间 apply_rpc_config 可能换掉 self.mqtt_net（rpc_config 改
        broker/公钥）：旧 manager 在请求返回前就 stop、新 manager 冷启动
        还没连上任何 broker——直接经 self.mqtt_net 发布会 published=0 静默
        丢回包（配置实际已生效但调用方超时）。这里先经快照发；0 成功且
        网络层确已被换时，等新 manager 连上后重发一次；仍 0 记 error。

        publish_broadcast 返回成功入队的 broker 数；None 视为已发出
        （测试桩/不计数实现），不触发回退。
        """
        try:
            published = net.publish_broadcast(reply_topic, payload)
        except Exception:
            logger.exception("回包发布异常 reply=%s req_id=%s",
                             reply_topic, payload.get("req_id"))
            return
        if published is None or published:
            return
        new_net = self.mqtt_net
        if new_net is not net:
            try:
                new_net.wait_connected(min_count=1, timeout=5.0)
                published = new_net.publish_broadcast(reply_topic, payload)
            except Exception:
                logger.exception("回包经新 manager 重发失败 reply=%s req_id=%s",
                                 reply_topic, payload.get("req_id"))
        if not published:
            logger.error(
                "⚠️ 回包发布 0 成功（全部 broker 不可达或换网未就绪）: "
                "reply=%s req_id=%s", reply_topic, payload.get("req_id"))

    # ------------------------------------------------------------------
    # 消息处理：运行在 MultiMQTTManager 的 "MQTTMsgDispatch" 后台线程中。
    # 注意：
    #   1) 该线程是所有 broker 共享的单线程，回调阻塞会拖慢全部 broker 消息。
    #   2) 本函数必须捕获全部异常，否则 MultiMQTTManager._dispatch_loop 会记
    #      错误日志，但消息会丢。
    #   3) 日志要克制，避免海量请求把日志系统打爆。
    # ------------------------------------------------------------------
    def handle_message(self, topic, data, rx_broker):
        try:
            if not isinstance(data, dict):
                logger.warning(
                    "收到非 dict 消息，忽略: topic=%s type=%s",
                    topic,
                    type(data),
                )
                return

            req_id = data.get("req_id")
            reply_topic = data.get("reply_topic") or self.reply_topic
            code = data.get("code",None)
            # [#17] 快照"收到本请求的 manager"，全部回包经 _reply_publish
            # 由它发出（执行期间 rpc_config 可能换掉 self.mqtt_net）。
            net = self.mqtt_net

            # 网络层重放窗口拒收（验签通过但 timestamp 超窗）：不静默丢帧。
            # 按标准回包格式向 reply_topic 发布明确错误，客户端立即拿到
            # 诊断（时钟偏差/重放帧），而不是死等到自身超时摸不清状况。
            _ts_reject = data.pop("_cmq_timestamp_reject", None)
            if _ts_reject is not None:
                if isinstance(req_id, str) and "|" in req_id:
                    req_id = req_id.split("|", 1)[0]
                _err = str(_ts_reject.get("error")
                           or "rejected: timestamp outside replay window")
                logger.warning(
                    "⛔ [重放窗口拒收] 已向 %s 回包错误: req_id=%s reason=%s "
                    "window=%s frame_ts=%s server_now_ms=%s",
                    reply_topic, req_id, _ts_reject.get("reason"),
                    _ts_reject.get("window"), _ts_reject.get("frame_ts"),
                    _ts_reject.get("server_now_ms"),
                )
                if reply_topic:
                    self._reply_publish(net, reply_topic, {
                        "req_id": req_id,
                        "r": None,
                        "stdout": "",
                        "ok": False,
                        "error": _err,
                        "rejected": _ts_reject.get("reason", "timestamp_window"),
                        "server_time": utc_ms(),
                        "server_from": rx_broker,
                    })
                return

            if code is not None:
                # 收到下发指令：刷新指令时间戳（距上次指令超过 command_idle_threshold
                # 视为空闲，报告线程据此恢复每小时上报）
                self.last_command_ts = time.time()

            verify_enabled, _ = _is_verify_enabled(self.mqtt_net)

            logger.info(f"⚡req_id={req_id} 首发:{rx_broker}),code={bool(code)},verify={verify_enabled}"    )

            if code is None:
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "missing code",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self._reply_publish(net, reply_topic, response_data)
                return

            if not isinstance(code, str):
                logger.warning("code 非字符串，忽略: req_id=%s type=%s",
                               req_id, type(code))
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "code must be str",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self._reply_publish(net, reply_topic, response_data)
                return

            execution = self.executor.execute(code, format_result=True)
            server_time = utc_ms()

            # MultiMQTTManager 在收到签名消息并验签通过后，会在回调里把 req_id
            # 恢复成真正的原始 req_id（去掉 "|<sig>" 部分）；如果仍带 "|"，
            # 说明是未验签路径下的原始消息，此处只做兼容性剥离。
            if (
                verify_enabled
                and isinstance(req_id, str)
                and "|" in req_id
            ):
                req_id = req_id.split("|", 1)[0]

            # ok = bool(execution.get("ok"))
            execution.update({
                "req_id": req_id,
                "server_time": server_time,
                "server_from": rx_broker,})
                # "r": format_result(execution.get("r")),
                # "stdout": execution.get("stdout", ""),
                # "ok": ok,
                # "latency_send": server_time - data.get("timestamp")
                # client/server 时间不同步，测量值不是真实延迟
            # }
            # if not ok:
                # response_data["error"] = execution.get("error", "unknown error")

            if reply_topic:
                self._reply_publish(net, reply_topic, execution)

        except Exception as handle_message_error:# 兜底：绝不让异常冒到 MultiMQTTManager 的分发线程 logger.exception(自动追加完整 traceback
            logger.exception(f"处理MQTT请求失败:topic={topic} rx_broker={rx_broker} {handle_message_error}")

    # ------------------------------------------------------------------
    # global_topic_list 定期报告（可配置：rpc.json / rpc_config() / start(config)）
    # 与 stats.get_report 网络质量报告不同，本功能发布"服务端监听 topic 列表"。
    # tick 经 mqtt_net.set_periodic_task 并入 _dispatch_loop，与 Ping/统计报告
    # 同级别调度，不额外创建线程。
    # 触发条件（全部经 _last_gtl_publish_ts 去重，同 tick 多次调用不会连发）：
    #   距上次收到控制指令超过 global_topic_list_idle_threshold 秒（默认 24
    #   小时，可配置）且距上次发布超过 global_topic_list_interval 秒（默认 1
    #   小时）且有在线 broker -> 把监听 topic 列表发布到 global_topic_list，
    #   发布参数默认 qos=1 + retain=True（经 **ka 透传 paho-mqtt）；
    #   broker 连同 QoS 一起保存保留消息，新订阅者上线即可收到最新列表；
    #   断网恢复后同样因"距上次发布已超 interval"自然续上，无需专门分支。
    #   global_topic_list_payload_fn 可自定义报告内容: fn(server) -> dict。
    # ------------------------------------------------------------------
    def _build_global_topic_list_payload(self, reason: str) -> dict:
        """默认报告内容：当前监听的全部 topic。"""
        return {
            # "type": "global_topic_list",
            "reason": reason,
            "listen_topics": sorted(getattr(self.mqtt_net, "subscribed_topics", set()) or set()),
            "request_topic": self.request_topic,
            "reply_topic": self.reply_topic,
            # "server_time": utc_ms(),
            "stime": stime(),
        }

    def _resolve_global_topic_list_payload(self, reason: str):
        """自定义报告函数优先；返回 None 表示本次放弃发布。"""
        if self.global_topic_list_payload_fn is not None:
            payload = self.global_topic_list_payload_fn(self)
            if not isinstance(payload, dict):
                logger.warning("global_topic_list_payload_fn 返回非 dict(%s)，忽略本次发布", type(payload))
                return None
            return payload
        return self._build_global_topic_list_payload(reason)

    def _append_global_topic_list_remote(self, payload: dict):
        """把本次报告追加到远端 topics 文件（标准库，超时 9s）；失败只记日志。"""
        try:  # 远端上报：仅用标准库，失败不影响 MQTT 发布
            import urllib.parse, urllib.request,json  # 局部导入，避免顶层强依赖
            body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))  # 紧凑 JSON，保留中文
            body = body.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "%0A")  # 换行折叠为字面量 %0A
            body_q = urllib.parse.quote(body, safe="")  # 完整百分号编码，防止引号/反斜杠破坏远端 r'''...'''
            code_path = "with%20open('topics','a')%20as%20f:" f"f.write(r'''{body_q}%0A''');r=f.tell()"  # 按远端约定拼路径，仅空格写 %20
            url = self.global_topic_url.rstrip("/") + "/" + code_path
            req = urllib.request.Request(url, method="GET",)
            with urllib.request.urlopen(req, timeout=9) as resp:  # 超时=9s
                resp.read()  # 读空响应体，及时释放连接
        except Exception as qmm_e:
            # 证书过期/网络抖动属预期失败，只记一行警告，避免每轮 tick 刷整段 traceback
            logger.warning("远端 topics 追加失败（不影响 MQTT 发布）：%s", qmm_e)

    def _publish_global_topic_list(self, reason: str, confirm_timeout=None, remote=True) -> dict:
        """发布监听 topic 报告并返回发布状态。

        confirm_timeout>0 时等待 QoS1 PUBACK，阻塞至多该秒数，仅限后台线程；
        remote=False 跳过远端 topics 追加，供交互场景避免最长 9 秒阻塞。
        返回 ok/reason/topic/queued/acked/sent/timeout/payload，可直接作为
        RPC 响应交给调用方判断是否真的发成功。
        """
        payload = self._resolve_global_topic_list_payload(reason)
        if payload is None:
            return {
                "ok": False, "reason": reason, "sent": False,
                "queued": 0, "acked": 0, "timeout": False,
                "error": "payload_not_dict",
            }
        if confirm_timeout and float(confirm_timeout) > 0:
            stats = self.mqtt_net.publish_broadcast_confirmed(
                self.global_topic_list,
                payload,
                confirm_timeout=float(confirm_timeout),
                **self.global_topic_list_publish_kwargs,
            )
        else:
            queued = self.mqtt_net.publish_broadcast(
                self.global_topic_list, payload, **self.global_topic_list_publish_kwargs
            )
            stats = {"queued": queued, "acked": None, "timeout": False}
        self._last_gtl_publish_ts = time.time()  # 时间戳防抖，断网恢复自然续上
        if remote:
            self._append_global_topic_list_remote(payload)
        acked = stats.get("acked")
        status = {
            "ok": True,
            "reason": reason,
            "topic": self.global_topic_list,
            "queued": stats.get("queued"),
            "acked": acked,
            "sent": bool(acked) if acked is not None else bool(stats.get("queued")),
            "timeout": bool(stats.get("timeout")),
            "payload": payload,
        }
        logger.info(
            "📨 [%s] 已发布监听 topic 报告 -> %s (reason=%s, queued=%s, acked=%s, topics=%s)",
            stime(), self.global_topic_list, reason,
            status["queued"], status["acked"], payload.get("listen_topics"),
        )  # 本地日志
        return status

    def publish_global_topic_list_now(self, reason="manual", confirm_timeout=2.0, remote=False) -> dict:
        """立即发布一次监听 topic 报告，可指定与周期报告不同的 reason。

        与 tick 共用同一份 payload 与发布参数；默认等待 QoS1 PUBACK 并跳过
        远端 topics 追加，适合设备端交互立即触发。阻塞至多 confirm_timeout
        秒，必须在后台线程调用。
        """
        return self._publish_global_topic_list(
            reason, confirm_timeout=confirm_timeout, remote=remote
        )
            
    def global_topic_list_tick(self, now=None):
        """周期 tick：由 _dispatch_loop 驱动（也可手动调用）。

        判定：距上次收到控制指令超过 idle_threshold（默认 24 小时）且距上次
        发布超过 interval 且有在线 broker -> 发布；发布即刷新时间戳，
        天然防抖，断网恢复后因距上次发布已超 interval 自然续上。
        """
        if now is None:
            now = time.time()

        online_count = sum(
            1 for online in self.mqtt_net.get_connection_snapshot().values() if online
        )
        if (
            online_count
            and (now - self.last_command_ts) >= self.global_topic_list_idle_threshold
            and (now - self._last_gtl_publish_ts) >= self.global_topic_list_interval
        ):
            reason = "startup" if self._last_gtl_publish_ts == 0.0 else "idle_no_command"
            self._publish_global_topic_list(reason)

    def _register_global_topic_list_tick(self):
        """把 tick 注册进 _dispatch_loop 周期任务槽（与 stats Ping/报告同级，不另开线程）。"""
        if not self.enable_global_topic_list:
            return
        self.mqtt_net.set_periodic_task(
            self.global_topic_list_tick, interval=self.global_topic_list_check_interval
        )

    def _unregister_global_topic_list_tick(self):
        self.mqtt_net.set_periodic_task(None, interval=1.0)

    def start(self, block=True):
        """
        启动 MQTT 服务端。

        block=True:  CLI 使用，启动后阻塞主线程，直到 KeyboardInterrupt。
        block=False: Chaquopy/全局 start 使用，启动并订阅后立即返回 self。

        返回值:
            成功: self
            失败: None（例如 MultiMQTTManager 检测到旧分发线程未退出而拒绝启动）
        """
        self.mqtt_net.start()

        # MultiMQTTManager.start() 在旧分发线程未退出时会直接 return，
        # 此时 self.mqtt_net.clients 保持为空。这里做一个显式判断，
        # 避免调用方以为启动成功。
        if not getattr(self.mqtt_net, "clients", None):
            logger.error("❌ MQTT 启动失败: 底层 MultiMQTTManager 未建立任何连接"
                         "（可能是旧分发线程未退出）。")
            return None

        global gms
        gms = self
        self.mqtt_net.subscribe(self.request_topic)
        # 不写死 sleep：首个 broker 一连上就宣布就绪；迟到的 broker 会在
        # on_connect 时按 subscribed_topics 自动补订阅，不会漏消息。
        online = self.mqtt_net.wait_connected(min_count=1, timeout=5.0)

        verify_enabled, verify_reason = _is_verify_enabled(self.mqtt_net)
        logger.info(
            "🚀服务端就绪（在线%d/%d），监听:%s,reply_topic=%s ,pub=%s , 验签=%s",
            online, len(getattr(self.mqtt_net, "clients", {}) or {}),
            self.request_topic,
            self.reply_topic,
            _describe_public_key(self.mqtt_net.server_public_key_bytes),
            verify_reason if verify_enabled else f"{verify_reason}",
        )

        logger.info("[连接质量统计报告]%s", self.mqtt_net.stats.get_report(is_windows_cmd=(sys.platform=="win32")))

        self._register_global_topic_list_tick()
        if not block:
            return self
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，正在停止 MQTT 服务端...")
        finally:
            self._unregister_global_topic_list_tick()
            try:
                self.mqtt_net.stop()
            except Exception:
                logger.exception("停止 MultiMQTTManager 时发生异常")

        return self


def start(config=None,globals=None):
    """
    为了导出给 Chaquopy 调用。
    非阻塞启动，返回 server 实例；调用方应保存返回值，避免对象被回收。
    启动失败时返回 None。
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config 必须是 dict")

    rpc_settings = load_rpc_config()
    explicit_rpc_settings = {}
    for key in (
        "broker_list",
        "broker_list_add",
        "broker_list_del",
        "request_topic",
        "pub_key",
        "global_topic_url",
        "global_topic_list",
        "global_topic_list_interval",
        "global_topic_list_idle_threshold",
        "global_topic_list_check_interval",
        "global_topic_list_publish_kwargs",
        "enable_global_topic_list",
    ):
        if key in config:
            explicit_rpc_settings[key] = config[key]
    if "brokers" in config:
        explicit_rpc_settings["broker_list"] = config["brokers"]
    if "mqtt_request_topic" in config:
        explicit_rpc_settings["request_topic"] = config["mqtt_request_topic"]
    if "mqtt_pub_key" in config:
        explicit_rpc_settings["pub_key"] = config["mqtt_pub_key"]
    # 兼容既有 mqtt_ 前缀的 global_topic_list_* 键（Chaquopy start(config) 约定）
    for key in (
        "mqtt_enable_global_topic_list",
        "mqtt_global_topic_list",
        "mqtt_global_topic_list_interval",
        "mqtt_global_topic_list_idle_threshold",
        "mqtt_global_topic_list_check_interval",
        "mqtt_global_topic_list_publish_kwargs",
    ):
        if key in config:
            explicit_rpc_settings[key[len("mqtt_"):]] = config[key]
    if "broker_list" in explicit_rpc_settings:
        rpc_settings["_broker_list_base"] = explicit_rpc_settings["broker_list"]
    rpc_settings = _normalize_rpc_config({**rpc_settings, **explicit_rpc_settings})
    request_topic = _resolve_request_topic(explicit_rpc_settings.get("request_topic"))
    rpc_settings["request_topic"] = request_topic
    reply_topic = str(config.get("mqtt_reply_topic") or DEFAULT_REPLY_TOPIC)

    # 直接把原始值交给 MultiMQTTManager，由它统一走 get_standard_public_pem_bytes
    server = MQTTServer(
        server_public_key_bytes=rpc_settings["pub_key"],
        brokers=rpc_settings["broker_list"],
        request_topic=request_topic,
        global_topic_url=rpc_settings["global_topic_url"],
        rpc_config_source=rpc_settings,
        reply_topic=reply_topic,
        globals=globals,
        enable_global_topic_list=rpc_settings["enable_global_topic_list"],
        global_topic_list=rpc_settings["global_topic_list"],
        global_topic_list_interval=rpc_settings["global_topic_list_interval"],
        global_topic_list_idle_threshold=rpc_settings["global_topic_list_idle_threshold"],
        global_topic_list_check_interval=rpc_settings["global_topic_list_check_interval"],
        global_topic_list_publish_kwargs=rpc_settings["global_topic_list_publish_kwargs"],
        global_topic_list_payload_fn=config.get("mqtt_global_topic_list_payload_fn"),
    )

    # logger.info(
        # "🚀 启动 MQTT RPC，使用网络层 BROKER_LIST，共 %d 个 Broker",
        # len(BROKER_LIST),
    # )

    return server.start(block=False)


def main(port=1177, host="0.0.0.0", globals=None, locals=None, block=False,
         request_topic=None, pub=None, brokers=None, global_topic_url=None):
    """启动 MQTT + HTTP 双通道 RPC，返回 (http_server, mqtt_server)。

    globals/locals 原样透传给两个通道的执行器：HTTP 侧零拷贝持有 globals
    字典引用（启动后才赋值的名字对 RPC 实时可见），MQTT 侧 PythonExecutor
    使用同一 globals。ipy 里调用传 globals=globals(), locals=locals() 即可
    把 IPython 命名空间同时暴露给 HTTP/MQTT RPC；缺省暴露本模块 globals。
    pub/brokers/request_topic/global_topic_url 与 CLI 参数同名，None 时全部
    走 rpc.json。block=True 阻塞到 KeyboardInterrupt（脚本入口用），默认
    False 启动即返回。
    """
    if globals is None:
        globals = sys.modules[__name__].__dict__
    rpc_settings = _rpc_config_with_cli_overrides(
        request_topic=request_topic,
        pub=pub,
        brokers=brokers,
        global_topic_url=global_topic_url,
    )
    gms = MQTTServer(
        globals=globals,
        brokers=rpc_settings["broker_list"],
        server_public_key_bytes=rpc_settings["pub_key"],
        request_topic=rpc_settings["request_topic"],
        global_topic_url=rpc_settings["global_topic_url"],
        rpc_config_source=rpc_settings,
    )
    gms.mqtt_net.is_windows_cmd = (sys.platform == "win32")
    import server_http
    ghs = server_http.start_rpc_server(
        port=port,
        ip=host,
        globals=globals,
        locals=locals,
    )

    gms.start(block=block)  # block=True 阻塞主线程直到 KeyboardInterrupt；ipy 等交互调用用默认 False
    print(ghs, gms)
    return ghs, gms


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="mqtt http rpc")
    parser.add_argument("--port", "-port", "-p", type=int, default=1177)
    parser.add_argument("--host", "-host", default="0.0.0.0")
    parser.add_argument("--request_topic", "--topic", "-t", default=None)
    parser.add_argument("--pub", "--pubkey", "-pub", nargs="*", default=None)
    parser.add_argument("--broker", action="append", default=None,
                        help="追加 broker，格式为 host:port")
    parser.add_argument("--global_topic_url", default=None)
    args = parser.parse_args()
    try:
        main(
            port=args.port,
            host=args.host,
            request_topic=args.request_topic,
            pub=args.pub,
            brokers=args.broker,
            global_topic_url=args.global_topic_url,
            block=True,
        )
    except ValueError as error:
        parser.error(str(error))