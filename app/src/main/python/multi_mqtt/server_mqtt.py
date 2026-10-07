#!/usr/bin/env python3
from multi_mqtt import BROKER_LIST, MultiMQTTManager, stime, utc_ms, _describe_public_key
import time,logging,sys
from rpc_executor import PythonExecutor, format_result

logger = logging.getLogger("Server")
REQUEST_TOPIC = "sys/device/request"
DEFAULT_REPLY_TOPIC = "sys/device/response"


def _is_verify_enabled(mqtt_net):
    """
    判断当前 MQTT 网络层是否真的处于"验签"状态。

    MultiMQTTManager 里的关键属性：
      - server_public_key_bytes: 已解析成 PEM bytes（None 表示未配置）
      - server_vk:              ecdsa.VerifyingKey 对象（None 表示未启用）
      - _server_vk_invalid:     配置了公钥但解析失败时为 True

    返回 (enabled: bool, reason: str)
    """
    if getattr(mqtt_net, "_server_vk_invalid", False):
        return False, "公钥解析失败，验签未启用！"
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
        request_topic=REQUEST_TOPIC,
        reply_topic=DEFAULT_REPLY_TOPIC,
        keepalive=None,
        max_reconnect_delay=None,
    ):
        # 实例化网络层管理器（enable_crypto 默认为 False）
        self.request_topic = request_topic
        self.reply_topic = reply_topic

        # 公钥解析统一由 MultiMQTTManager 内部完成（含 OpenSSH -> PEM 转换）。
        # 这里只把原始输入透传进去，不再重复解析，避免结果不一致。
        net_kwargs = {}
        if keepalive is not None:
            net_kwargs["keepalive"] = keepalive
        if max_reconnect_delay is not None:
            net_kwargs["max_reconnect_delay"] = max_reconnect_delay
        self.mqtt_net = MultiMQTTManager(
            brokers=brokers,
            log_messages=False,  # 每个 broker 不打印原始消息
            server_public_key_bytes=server_public_key_bytes,
            enable_stats=True,
            **net_kwargs,
        )
        
        self.mqtt_net.set_on_message(self.handle_message)
        self.executor = PythonExecutor(globals=globals)

        verify_enabled, reason = _is_verify_enabled(self.mqtt_net)
        if verify_enabled:
            logger.info(
                "MQTTServer 初始化完成: 验签已启用, mqtt_pub_key=%s",
                _describe_public_key(self.mqtt_net.server_public_key_bytes),
            )
        else:
            logger.warning("MQTTServer 初始化完成: 验签未启用, 原因=%s", reason)

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
            code = data.get("code", data.get("payload"))

            verify_enabled, _ = _is_verify_enabled(self.mqtt_net)

            logger.info(
                "⚡ [%s] [服务端处理请求] req_id=%s (首发节点: %s) , has_code=%s , verify=%s",
                stime(),
                req_id,
                rx_broker,
                bool(code),
                verify_enabled,
            )

            if code is None:
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "missing code/payload",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self.mqtt_net.publish_broadcast(reply_topic, response_data)
                return

            if not isinstance(code, str):
                logger.warning("code/payload 非字符串，忽略: req_id=%s type=%s",
                               req_id, type(code))
                response_data = {
                    "req_id": req_id,
                    "r": None,
                    "stdout": "",
                    "ok": False,
                    "error": "code/payload must be str",
                    "server_time": utc_ms(),
                    "server_from": rx_broker,
                }
                if reply_topic:
                    self.mqtt_net.publish_broadcast(reply_topic, response_data)
                return

            execution = self.executor.execute(code)
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

            ok = bool(execution.get("ok"))
            response_data = {
                "req_id": req_id,
                "r": format_result(execution.get("r")) if ok else None,
                "stdout": execution.get("stdout", ""),
                "ok": ok,
                "server_time": server_time,
                "server_from": rx_broker,
                # "latency_send": server_time - data.get("timestamp")
                # client/server 时间不同步，测量值不是真实延迟
            }
            if not ok:
                response_data["error"] = execution.get("error", "unknown error")

            if reply_topic:
                self.mqtt_net.publish_broadcast(reply_topic, response_data)

        except Exception:
            # 兜底：绝不让异常冒到 MultiMQTTManager 的分发线程
            logger.exception(
                "处理 MQTT 请求失败: topic=%s rx_broker=%s",
                topic,
                rx_broker,
            )

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

        self.mqtt_net.subscribe(self.request_topic)
        # 不写死 sleep：首个 broker 一连上就宣布就绪；迟到的 broker 会在
        # on_connect 时按 subscribed_topics 自动补订阅，不会漏消息。
        online = self.mqtt_net.wait_connected(min_count=1, timeout=5.0)

        verify_enabled, verify_reason = _is_verify_enabled(self.mqtt_net)
        logger.info(
            "🚀 [%s] 服务端已就绪（在线 broker %d/%d），正在监听: %s , reply_topic=%s , mqtt_pub_key=%s , 验签=%s",
            stime(),
            online, len(getattr(self.mqtt_net, "clients", {}) or {}),
            self.request_topic,
            self.reply_topic,
            _describe_public_key(self.mqtt_net.server_public_key_bytes),
            verify_reason if verify_enabled else f"{verify_reason}",
        )

        logger.info("[连接质量统计报告]%s", self.mqtt_net.stats.get_report(is_windows_cmd=(sys.platform=="win32")))
        if not block:
            return self
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到 KeyboardInterrupt，正在停止 MQTT 服务端...")
        finally:
            try:
                self.mqtt_net.stop()
            except Exception:
                logger.exception("停止 MultiMQTTManager 时发生异常")

        return self


def start(config,globals=None):
    """
    为了导出给 Chaquopy 调用。
    非阻塞启动，返回 server 实例；调用方应保存返回值，避免对象被回收。
    启动失败时返回 None。
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config 必须是 dict")

    request_topic = str(config.get("mqtt_request_topic") or REQUEST_TOPIC)
    reply_topic = str(config.get("mqtt_reply_topic") or DEFAULT_REPLY_TOPIC)

    # 直接把原始值交给 MultiMQTTManager，由它统一走 get_standard_public_pem_bytes
    server = MQTTServer(
        server_public_key_bytes=config.get("mqtt_pub_key"),
        request_topic=request_topic,
        reply_topic=reply_topic,
        globals=globals,
    )

    # logger.info(
        # "🚀 启动 MQTT RPC，使用网络层 BROKER_LIST，共 %d 个 Broker",
        # len(BROKER_LIST),
    # )

    return server.start(block=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="mqtt http rpc")
    parser.add_argument("--port", "-port", "-p", type=int, default=1177)
    parser.add_argument("--host", "-host", default="0.0.0.0")
    parser.add_argument("--request_topic", "--topic", "-t", default=REQUEST_TOPIC) # 这里不使用 alias_request_topic 是不想引入client依赖

    _PUB = b"ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBER9c5vu215n+5gv1YjGdm78Nf99wpfqw1fIT8nXib2FLUglq4NBMe7hLp2VOkqv9z00m5Wn+uUADH4zyXLiWzI="
    # _PUB = b''

    # nargs='*' ：--pub 放到最后，其后所有空格分隔的片段会被拼回一个参数
    # --pub（后面什么都不给） 或 --pub ""  ->  b''
    parser.add_argument("--pub", "--pubkey", "-pub", nargs="*", default=None)
    args = parser.parse_args()
    args.pub = _PUB if args.pub is None else " ".join(args.pub).encode("utf-8")

    gms = MQTTServer(
        globals=globals(),
        server_public_key_bytes=args.pub,
        request_topic=args.request_topic,
    )# 为什么放到 ghs后面定义 dir找不到变量？
    gms.mqtt_net.is_windows_cmd=(sys.platform=="win32")
    import server_http
    ghs = server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
        # websocket_handler=editor.websocket,
        # websocket_path='/ws',
        # redirect_root='/preview_html(p)',
    )

    gms.start()  # 默认 block=True，CLI 下阻塞运行
    print(ghs, gms)