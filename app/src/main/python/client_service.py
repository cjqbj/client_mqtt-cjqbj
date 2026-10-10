"""Client-side bridge for MQTT RPC and remote file operations."""

import ast
import json
import base64
import copy
import importlib
import importlib.util
import os
import re
import shutil
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import PurePosixPath

import time
_STATE = {
    "files_dir": None,
    "lock": threading.RLock(),
    "selected_topic": None,
    "selected_device_id": None,
    "last_config": None,
    "operation_logs": [],
    "rpc_logs": [],
    "rpc_health": {},
}
DEFAULT_REMOTE_ROOT_OPTIONS = ("/sdcard", "/data/user/0/com.qgb.xime/")
_DEFAULT_DEVICE = {
    "name": "Target",
    "request_topic": "sys/device/request",
    "private_key": "",
    "allow_no_server_pubkey_response": True,
    "timeout": 10,
    "remote_root": DEFAULT_REMOTE_ROOT_OPTIONS[0],
    # None/缺省 = 全部 feature 对该目标生效（默认全选）；
    # 显式列表 = 白名单，只有列出的 feature 在该目标显示/可用。
    "enabled_features": None,
}


def _normalize_enabled_features(value):
    """None/缺省 -> None（全选）；序列 -> 去重去 feature_ 前缀的合法标识列表。"""
    if value is None:
        return None
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, list):
        raise ValueError("enabled_features must be a list or null")
    names = []
    for item in value:
        name = str(item).strip()
        if name.startswith("feature_"):
            name = name[8:]
        if name and name.isidentifier() and name not in names:
            names.append(name)
    return names


def enabled_features(device_ref=None):
    """某目标的 feature 白名单 JSON；null 表示全部生效。供 feature/UI 统一查询。"""
    enabled = _device_config(device_ref).get("enabled_features")
    return json.dumps(_normalize_enabled_features(enabled), ensure_ascii=False)


def is_feature_enabled(feature, device_ref=None):
    """目标级开关判定；缺省配置（None）恒为 True。"""
    name = _normalize_feature_name(feature)
    allowed = _device_config(device_ref).get("enabled_features")
    if allowed is None:
        return True
    return name in _normalize_enabled_features(allowed)


_CONFIG_NAME = "client_mqtt.json"
# feature 脚本下载根目录（设置页可改）。默认走 ghfast 代理拉 GitHub main；
# 内置 feature 在首选 URL 失败时再按 _FALLBACK_FEATURE_URL_ROOTS 顺序回退。
DEFAULT_FEATURE_URL_ROOT = (
    "https://ghfast.top/https://raw.githubusercontent.com/"
    "cjqbj/client_mqtt-cjqbj/refs/heads/main/app/src/main/python/"
)
_FALLBACK_FEATURE_URL_ROOTS = (
    "https://raw.githubusercontent.com/cjqbj/client_mqtt-cjqbj/refs/heads/main/app/src/main/python/",
    "https://github.com/cjqbj/client_mqtt-cjqbj/raw/refs/heads/main/app/src/main/python/",
)
# 单次 RPC 允许的超时上下限：设备配置的 timeout 只是默认值，feature 可覆盖。
_RPC_TIMEOUT_MIN = 1.0
_RPC_TIMEOUT_MAX = 600.0

# ---------------------------------------------------------------------------
# 持久化设置存储
#
# 历史教训：配置曾放在 <scriptRoot>/client_mqtt.json，而 scriptRoot 由
# SharedPreferences 的 use_external_scripts 决定——这个开关本身在应用私有目录，
# 卸载/重装即被清掉。重装后 App 回到内部根目录，sdcard 上的旧配置成了孤儿，
# 用户保存的 aliyun 设置"看起来丢了"。
#
# 现在设置固定优先落到外置根 /sdcard/apm/client_mqtt/settings/（与脚本开关无关），
# 按 topic 建子文件夹单独存放；拿不到外置存储权限才回退 App 私有目录。
#   settings/global.json                 全局设置 + topic 文件夹映射
#   settings/topics/<safe-topic>/device.json   单个 topic 的设备设置 + aliyun
# ---------------------------------------------------------------------------
EXTERNAL_STORAGE_ROOT = "/sdcard/apm/client_mqtt"
_SETTINGS_SUBDIR = "settings"
_TOPICS_SUBDIR = "topics"
_GLOBAL_FILE = "global.json"
_DEVICE_FILE = "device.json"
_DURABLE_SCHEMA = 2
# 只放在 global.json、不属于任何 topic 的字段。
_GLOBAL_FIELDS = (
    "feature_url_root",
    "online_probe_enabled",
    "online_probe_interval",
    "remote_root_options",
    "scan_limit",
    "selected_device_id",
    # 以下三项此前漏在白名单外，durable 后端保存时被静默丢弃：
    # feature_settings 各 feature 按目标的持久设置（拨号历史等）、
    # selected_feature 各目标最后选中的标签、known_features 本机特性启用名单
    "feature_settings",
    "selected_feature",
    "known_features",
)


def _durable_storage_root():
    """外置设置根：Android 真机固定 /sdcard；桌面测试需显式给 QGB_SETTINGS_ROOT，
    防止在构建机（root）上误写真实 /sdcard 造成测试串状态。"""
    override = os.environ.get("QGB_SETTINGS_ROOT", "").strip()
    if override:
        return override
    if hasattr(sys, "getandroidapilevel"):
        return EXTERNAL_STORAGE_ROOT
    return None


def _is_dir_writable(path):
    try:
        os.makedirs(path, exist_ok=True)
        probe = os.path.join(path, ".write_probe")
        with open(probe, "w", encoding="utf-8") as handle:
            handle.write("ok")
        os.unlink(probe)
        return True
    except OSError:
        return False


def _empty_config():
    return {"devices": [_DEFAULT_DEVICE.copy()], "scan_limit": 100,
            "remote_root_options": list(DEFAULT_REMOTE_ROOT_OPTIONS)}


def _normalize_remote_root_options(value):
    options = []
    if isinstance(value, (list, tuple)):
        for item in value:
            path = str(item or "").strip()
            if path and path not in options:
                options.append(path)
    for path in DEFAULT_REMOTE_ROOT_OPTIONS:
        if path not in options:
            options.append(path)
    return options


def _read_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            value = json.load(handle)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def _atomic_write_json(path, value):
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    temporary = path + ".tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
    os.replace(temporary, path)


def _safe_topic_name(topic):
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(topic or "")).strip("._-")
    return name or "topic"


def _find_topic_folder(paths, device, mapping):
    """定位设备已有的 topic 文件夹：映射 → 按 id/topic 扫盘 → 新建唯一名。
    topic 改名时旧文件夹里的 request_topic 对不上，会走到新建分支。"""
    topics_dir = paths["topics_dir"]
    device_id = device.get("id")
    target_topic = device.get("request_topic")
    mapped = mapping.get(device_id)
    if mapped:
        doc = _read_json_file(os.path.join(topics_dir, mapped, _DEVICE_FILE))
        if isinstance(doc, dict) and (
            not target_topic or doc.get("request_topic") == target_topic
            or (not doc.get("request_topic") and doc.get("id") == device_id)
        ):
            return mapped
    if os.path.isdir(topics_dir) and target_topic:
        for name in sorted(os.listdir(topics_dir)):
            doc = _read_json_file(os.path.join(topics_dir, name, _DEVICE_FILE))
            # 一个 topic 只对应一个文件夹：topic 对得上就复用。
            # 不能只按 id 匹配——topic 改名时旧文件夹必须让位给新文件夹。
            if isinstance(doc, dict) and doc.get("request_topic") == target_topic:
                return name
    base = _safe_topic_name(target_topic or device_id or "topic")
    name, counter = base, 1
    while os.path.isdir(os.path.join(topics_dir, name)):
        name = "%s__%d" % (base, counter)
        counter += 1
    return name


def _load_durable_config(paths):
    global_doc = _read_json_file(paths["global_path"]) or {}
    combined = {key: global_doc[key] for key in _GLOBAL_FIELDS if key in global_doc}
    combined["devices"] = []
    combined["scan_limit"] = combined.get("scan_limit", 100)
    topics_dir = paths["topics_dir"]
    if os.path.isdir(topics_dir):
        for name in sorted(os.listdir(topics_dir)):
            doc = _read_json_file(os.path.join(topics_dir, name, _DEVICE_FILE))
            if isinstance(doc, dict):
                combined["devices"].append(doc)
    if not combined["devices"]:
        combined["devices"] = [_DEFAULT_DEVICE.copy()]
    combined["aliyun"] = global_doc.get("aliyun")
    if not isinstance(combined["aliyun"], dict):
        combined["aliyun"] = {}
    if "aliyun_json_draft" in global_doc:
        combined["aliyun_json_draft"] = global_doc["aliyun_json_draft"]
    return _apply_effective_aliyun(combined)


def _save_durable_config(config, paths):
    devices = [item for item in config.get("devices", []) if isinstance(item, dict)]
    previous_global = _read_json_file(paths["global_path"]) or {}
    mapping = dict(previous_global.get("topic_dirs") or {})
    new_mapping, used_folders = {}, set()
    for device in devices:
        # 迁移自旧单文件配置的设备可能没有 id（id 过去只在编辑时才分配）。
        device.setdefault("id", uuid.uuid4().hex)
        folder = _find_topic_folder(paths, device, mapping)
        while folder in used_folders:
            # 不同 id 清洗后撞名：加序号拆开。
            folder = "%s__%d" % (_safe_topic_name(device.get("request_topic") or "topic"),
                                 len(used_folders) + 1)
        used_folders.add(folder)
        new_mapping[device["id"]] = folder
        _atomic_write_json(
            os.path.join(paths["topics_dir"], folder, _DEVICE_FILE), device
        )
    # 删除设备或 topic 改名后，旧文件夹不再被任何设备引用就删掉（只删登记过的）。
    active_folders = set(new_mapping.values())
    for folder in mapping.values():
        if folder not in active_folders:
            shutil.rmtree(os.path.join(paths["topics_dir"], folder), ignore_errors=True)
    global_doc = {"schema": _DURABLE_SCHEMA, "topic_dirs": new_mapping}
    for key in _GLOBAL_FIELDS:
        if key in config:
            global_doc[key] = config[key]
    if isinstance(config.get("aliyun"), dict):
        global_doc["aliyun"] = config["aliyun"]
    if config.get("aliyun_json_draft") is not None:
        global_doc["aliyun_json_draft"] = config["aliyun_json_draft"]
    _atomic_write_json(paths["global_path"], global_doc)


def _configure_storage(files_dir):
    """决定本次运行用哪套设置存储，并在首次使用外置存储时迁移旧单文件配置。"""
    legacy_path = os.path.join(files_dir, _CONFIG_NAME)
    _STATE["legacy_config_path"] = legacy_path
    root = _durable_storage_root()
    paths = None
    if root:
        candidate = {
            "root": root,
            "settings_dir": os.path.join(root, _SETTINGS_SUBDIR),
            "topics_dir": os.path.join(root, _SETTINGS_SUBDIR, _TOPICS_SUBDIR),
        }
        candidate["global_path"] = os.path.join(candidate["settings_dir"], _GLOBAL_FILE)
        if _is_dir_writable(candidate["settings_dir"]) and _is_dir_writable(
            candidate["topics_dir"]
        ):
            paths = candidate
            if not os.path.isfile(paths["global_path"]):
                # 一次性迁移：旧单文件（内部或 sdcard 旧位置）拆分成 per-topic 存储。
                legacy = _read_json_file(legacy_path) or _empty_config()
                _save_durable_config(_apply_effective_aliyun(legacy), paths)
    _STATE["durable_paths"] = paths
    if paths is not None:
        _STATE["settings_backend"] = "durable"
        _STATE["config_path"] = paths["global_path"]
    else:
        _STATE["settings_backend"] = "legacy"
        _STATE["config_path"] = legacy_path
        if not os.path.isfile(legacy_path):
            _atomic_write_json(legacy_path, _empty_config())


def settings_storage_info():
    """供设置页显示设置实际存放位置（是否随卸载保留）。"""
    paths = _STATE.get("durable_paths")
    return json.dumps({
        "backend": _STATE.get("settings_backend", "legacy"),
        "external": bool(paths),
        "root": (paths or {}).get("root"),
        "settings_dir": (paths or {}).get("settings_dir"),
        "topics_dir": (paths or {}).get("topics_dir"),
        "legacy_path": _STATE.get("legacy_config_path"),
    }, ensure_ascii=False)


def _apply_effective_aliyun(config):
    """aliyun 生效值：当前选中 topic 私有配置优先，全局配置兜底。"""
    devices = [item for item in config.get("devices", []) if isinstance(item, dict)]
    selected_id = config.get("selected_device_id") or _STATE.get("selected_device_id")
    selected = next(
        (item for item in devices if item.get("id") == selected_id),
        None,
    )
    if selected is None:
        selected_topic = _STATE.get("selected_topic")
        selected = next(
            (item for item in devices if item.get("request_topic") == selected_topic),
            devices[0] if devices else None,
        )
    topic_aliyun = selected.get("aliyun") if isinstance(selected, dict) else None
    if isinstance(topic_aliyun, dict) and topic_aliyun:
        config["aliyun"] = topic_aliyun
    elif not isinstance(config.get("aliyun"), dict):
        config["aliyun"] = {}
    topic_draft = selected.get("aliyun_json_draft") if isinstance(selected, dict) else None
    if topic_draft is not None:
        config["aliyun_json_draft"] = topic_draft
    return config


def _normalize_url_root(value):
    root = str(value or "").strip()
    if not (root.startswith("http://") or root.startswith("https://")):
        raise ValueError("feature URL root must start with http:// or https://")
    return root if root.endswith("/") else root + "/"


def feature_url_root():
    """当前生效的 feature 下载根目录；配置缺失/非法时回默认代理地址。"""
    try:
        return _normalize_url_root(load_config().get("feature_url_root"))
    except ValueError:
        return DEFAULT_FEATURE_URL_ROOT


def feature_download_settings():
    return json.dumps({"feature_url_root": feature_url_root()}, ensure_ascii=False)


def update_feature_download_settings(values):
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("feature download settings must be an object")
    root = _normalize_url_root(values.get("feature_url_root"))
    config = load_config()
    config["feature_url_root"] = root
    save_config(config)
    return feature_download_settings()


def _feature_source_urls(filename, allow_fallback=True):
    """feature_*.py 的下载 URL 列表：首选配置根；内置文件追加官方镜像回退。"""
    primary = feature_url_root() + str(filename)
    urls = [primary]
    if allow_fallback:
        for root in _FALLBACK_FEATURE_URL_ROOTS:
            candidate = root + str(filename)
            if candidate not in urls:
                urls.append(candidate)
    return urls


def call_feature(feature, action="run", *args):
    """Dispatch through the stable bootstrap so feature modules can be replaced."""
    import bootstrap
    try:
        result = bootstrap.call_feature(feature, action, *args)
    except BaseException as error:
        return json.dumps({
            "ok": False,
            "feature": str(feature),
            "action": str(action),
            "error": f"{type(error).__name__}: {error}",
        }, ensure_ascii=False)
    if isinstance(result, str):
        return result
    try:
        return json.dumps(result, ensure_ascii=False)
    except (TypeError, ValueError):
        return json.dumps({"ok": True, "result": str(result)}, ensure_ascii=False)


def build_feature_view(feature, context):
    """宿主桥：让 feature 脚本自己用 Chaquopy 构建原生 View。

    Kotlin 通用宿主（PythonViewPage）拿到 Context 后调用本函数：加载
    feature_<name> 模块并执行其 build_view(context)，返回 View 树。
    任何失败都降级成一个显示错误文本的 TextView，保证 UI 永不白屏/闪退。
    必须在 Android 主线程调用（View 构造约束）。
    """
    import logging
    import traceback
    import bootstrap
    try:
        module = bootstrap.load_feature(feature)
        builder = getattr(module, "build_view", None)
        if not callable(builder):
            raise AttributeError(
                f"feature {feature!r} has no callable build_view(context)"
            )
        view = builder(context)
        if view is None:
            raise RuntimeError("build_view(context) returned None")
        return view
    except Exception:
        logging.getLogger("error").exception(
            "build_feature_view failed feature=%s", feature
        )
        import pyui_kit
        from android.widget import ScrollView, TextView

        error_view = TextView(context)
        # 错误文本必须能长按选择复制，方便把 traceback 发出来排查
        error_view.setTextIsSelectable(True)
        error_view.setPadding(
            pyui_kit.dp(context, 16), pyui_kit.dp(context, 16),
            pyui_kit.dp(context, 16), pyui_kit.dp(context, 16),
        )
        error_view.setText(
            "Python UI failed for feature_%s.py:\n\n%s"
            % (feature, traceback.format_exc()[-1500:])
        )
        # traceback 常超过一屏，套一层可滚动容器
        scroll = ScrollView(context)
        scroll.addView(
            error_view,
            ScrollView.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
        )
        return scroll


# feature 自绘页的返回键拦截登记表：feature 名 -> 无参回调。
# 回调返回 True 表示已消费（例如 Files 回上一层目录）；False/未注册表示
# 交给宿主做"连按两次退出"。回调在 Android 主线程执行，必须轻量、不阻塞。
_BACK_HANDLERS = {}


def set_feature_back_handler(feature, handler):
    """注册/注销某 feature 的全局返回键处理（handler 传 None 注销）。"""
    name = str(feature)
    if handler is None:
        _BACK_HANDLERS.pop(name, None)
    elif callable(handler):
        _BACK_HANDLERS[name] = handler
    return json.dumps({"ok": True, "feature": name}, ensure_ascii=False)


def handle_feature_back(feature):
    """宿主返回键入口：feature 消费返回 True；否则 False（宿主执行两次退出）。"""
    handler = _BACK_HANDLERS.get(str(feature))
    if handler is None:
        return False
    try:
        return bool(handler())
    except BaseException as error:  # noqa: BLE001 - 返回键绝不能因 feature 异常卡死
        import logging
        logging.getLogger("error").exception(
            "feature back handler failed feature=%s: %s", feature, error
        )
        return False


def _mqtt_client_module():
    module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "multi_mqtt")
    client_dir = os.path.join(module_dir, "client")
    package_source = os.path.join(module_dir, "multi_mqtt.py")
    # 同时支持两种打包布局：
    #   旧（扁平）: <module_dir>/client_mqtt.py
    #   新（子包）: <module_dir>/client/client_mqtt.py
    # 关键：不能用 os.path.isfile 探测源码路径。Chaquopy 打包/运行时，Python
    # 模块只以 .pyc 形式存放在 APK 内的 app.imy 中、由 AssetFinder 的导入钩子
    # 按需加载，files/chaquopy/AssetFinder 物理目录里只有 .md/.txt 等数据文件，
    # 既无 .py 也无已解压的 .pyc，isfile 恒为 False，会在 Application.onCreate
    # 阶段直接 ImportError 闪退。这里统一把目录挂上 sys.path 后交给 import 系统：
    # 桌面运行命中真实 .py，APK 内命中 AssetFinder 加载的 .pyc。client/ 置前以
    # 优先匹配新子包布局，不存在时自然回退到扁平布局。
    for path in (client_dir, module_dir):
        if path not in sys.path:
            sys.path.insert(0, path)
    mqtt_module = sys.modules.get("multi_mqtt")
    mqtt_file = os.path.abspath(str(getattr(mqtt_module, "__file__", "")))
    if mqtt_file != os.path.abspath(package_source) or not hasattr(mqtt_module, "MultiMQTTManager"):
        for name in tuple(sys.modules):
            if name == "multi_mqtt" or name.startswith("multi_mqtt."):
                sys.modules.pop(name, None)
        importlib.invalidate_caches()
        mqtt_module = importlib.import_module("multi_mqtt")
        if not hasattr(mqtt_module, "MultiMQTTManager"):
            raise ImportError(f"Unable to load MQTT module from {package_source}")

    client_module = sys.modules.get("client_mqtt")
    if client_module is None or not callable(getattr(client_module, "rpc", None)):
        sys.modules.pop("client_mqtt", None)
        importlib.invalidate_caches()
        client_module = importlib.import_module("client_mqtt")
        if not callable(getattr(client_module, "rpc", None)):
            raise ImportError(f"Unable to find client_mqtt.rpc in {module_dir}")
    return client_module


def feature_catalog():
    import bootstrap
    return json.dumps(bootstrap.describe_features(), ensure_ascii=False)


def builtin_feature_files():
    """Filenames of the statically registered built-in feature modules."""
    import bootstrap
    return json.dumps(
        [f"feature_{name}.py" for name in bootstrap.BUILTIN_FEATURES],
        ensure_ascii=False,
    )


def reload_feature(feature):
    """Drop the cached feature module and re-import it (long-press action)."""
    import bootstrap
    try:
        bootstrap.reload_feature(feature)
        descriptor = next(
            (item for item in bootstrap.describe_features() if item.get("name") == str(feature)),
            None,
        )
        return json.dumps({"ok": True, "feature": str(feature), "descriptor": descriptor}, ensure_ascii=False)
    except BaseException as error:
        return json.dumps({
            "ok": False,
            "feature": str(feature),
            "error": f"{type(error).__name__}: {error}",
        }, ensure_ascii=False)


def install_feature(url, filename, sha256=""):
    import bootstrap
    return bootstrap.install_feature(url, filename, sha256)


def _append_operation_log(message):
    entry = f"{time.strftime('%H:%M:%S')} {message}"
    with _STATE["lock"]:
        logs = _STATE.setdefault("operation_logs", [])
        logs.append(entry)
        del logs[:-200]
    return entry


def operation_logs():
    with _STATE["lock"]:
        return json.dumps(list(_STATE.get("operation_logs", [])), ensure_ascii=False)


def _append_rpc_log(message):
    text = str(message).replace("\r\n", "\n")
    if len(text) > 20000:
        text = text[:20000] + "\n...[diagnostic entry truncated]"
    entry = f"{time.strftime('%H:%M:%S')} {text}"
    with _STATE["lock"]:
        logs = _STATE.setdefault("rpc_logs", [])
        logs.append(entry)
        del logs[:-500]
    return entry


def rpc_logs():
    with _STATE["lock"]:
        return json.dumps(list(_STATE.get("rpc_logs", [])), ensure_ascii=False)


def clear_rpc_logs():
    with _STATE["lock"]:
        _STATE.setdefault("rpc_logs", []).clear()
    return json.dumps({"ok": True})


def standardize_private_key(value):
    """Normalize a private key using the upstream multi_mqtt implementation."""
    mqtt_client = _mqtt_client_module()
    normalized = mqtt_client.get_standard_pem_bytes(value)
    if not normalized:
        raise ValueError("private key is empty")
    try:
        return normalized.decode("utf-8")
    except AttributeError:
        return bytes(normalized).decode("utf-8")


def install_builtin_features(script_root, retries=4, timeout=15, force=False):
    """Install missing bundled feature scripts into a chosen script root.

    force=True 时（设置页"一键全部重新下载"）忽略已存在文件，全部重新拉取，
    并清掉旧模块缓存/pyc，让新脚本在不重启 App 的情况下立即生效。
    """
    import bootstrap

    update_dir = os.path.join(os.path.abspath(str(script_root)), "py_updates")
    os.makedirs(update_dir, exist_ok=True)
    with _STATE["lock"]:
        _STATE.setdefault("operation_logs", []).clear()
    _append_operation_log(
        ("Force reinstalling" if force else "Installing")
        + f" built-in features into {update_dir}"
    )
    results = []

    for filename in _builtin_feature_filenames():
        destination = os.path.join(update_dir, filename)
        if not force and os.path.isfile(destination):
            try:
                with open(destination, "rb") as feature_file:
                    compile(feature_file.read(), filename, "exec")
                _append_operation_log(f"{filename}: already present; skipped")
                results.append({"filename": filename, "ok": True, "skipped": True})
                continue
            except (OSError, SyntaxError):
                _append_operation_log(f"{filename}: existing file is invalid; downloading replacement")

        try:
            result = _install_builtin_feature_file(
                bootstrap, filename, update_dir, retries, timeout
            )
            if force and result.get("ok"):
                # 丢弃旧缓存，下一次动作/打开页面即用新文件。
                feature_name = filename[8:-3]
                try:
                    sys.modules.pop("feature_" + feature_name, None)
                    bootstrap._purge_feature_pyc("feature_" + feature_name)
                    result["reloaded"] = True
                except Exception as reload_error:
                    _append_operation_log(
                        f"{filename}: downloaded but cache purge failed: {reload_error}"
                    )
            results.append(result)
        except Exception as error:
            detail = f"{type(error).__name__}: {error}"[:300]
            _append_operation_log(f"{filename}: failed: {detail}")
            results.append({"filename": filename, "ok": False, "error": detail})

    installed = sum(bool(item.get("ok")) for item in results)
    _append_operation_log(f"Finished: {installed}/{len(results)} feature files ready")
    return json.dumps({
        "ok": installed == len(results),
        "update_dir": update_dir,
        "force": bool(force),
        "results": results,
        "logs": json.loads(operation_logs()),
    }, ensure_ascii=False)


def reinstall_builtin_features(script_root, retries=4, timeout=15):
    """设置页"一键全部重新下载"：无条件重下全部内置 feature 脚本。"""
    return install_builtin_features(script_root, retries, timeout, force=True)


def _builtin_feature_filenames():
    """内置 feature 文件名以 bootstrap.BUILTIN_FEATURES 为唯一来源。"""
    import bootstrap
    return [f"feature_{name}.py" for name in bootstrap.BUILTIN_FEATURES]


def _install_builtin_feature_file(bootstrap, filename, update_dir, retries, timeout):
    filename = str(filename)
    if filename not in _builtin_feature_filenames():
        raise ValueError(f"unknown built-in feature file: {filename}")
    urls = _feature_source_urls(filename, allow_fallback=True)
    return bootstrap.install_feature(
        urls[0],
        filename,
        update_dir=update_dir,
        retries=retries,
        timeout=timeout,
        fallback_urls=urls[1:],
        progress=_append_operation_log,
    )


def install_named_feature(script_root, filename, retries=4, timeout=20):
    """按设置页配置的 URL 根目录下载任意 feature_<name>.py（添加新 feature）。

    接受裸名 "demo" 或 "feature_demo.py"，统一落盘到 <script_root>/py_updates。
    非内置文件名只打配置根 URL（不追加 GitHub 回退，避免自定义仓库必然 404
    的重试）；内置文件名复用 install_builtin_feature 的回退链路。
    """
    import bootstrap

    update_dir = os.path.join(os.path.abspath(str(script_root)), "py_updates")
    os.makedirs(update_dir, exist_ok=True)
    with _STATE["lock"]:
        _STATE.setdefault("operation_logs", []).clear()
    # 同时接受裸名 "demo" 和 "feature_demo.py"，统一成 feature_demo.py。
    name = str(filename or "").strip()
    if not name.endswith(".py"):
        name += ".py"
    if not name.startswith("feature_"):
        name = "feature_" + name
    _append_operation_log(f"Installing {name} from configured feature URL root")
    try:
        if not name[8:-3].isidentifier():
            raise ValueError("filename must be feature_<valid-name>.py")
        builtin = name in _builtin_feature_filenames()
        urls = _feature_source_urls(name, allow_fallback=builtin)
        result = bootstrap.install_feature(
            urls[0],
            name,
            update_dir=update_dir,
            retries=retries,
            timeout=timeout,
            fallback_urls=urls[1:],
            progress=_append_operation_log,
        )
        # 新下载的模块立刻可用：弹缓存 + 清 pyc，无需重启或长按 Reload。
        module_name = name[:-3]
        sys.modules.pop(module_name, None)
        bootstrap._purge_feature_pyc(module_name)
        result["reloaded"] = True
        # 自定义白名单的目标自动收养新 feature，否则"成功却看不到条目"。
        result["adopted_whitelists"] = _adopt_feature_into_whitelists(name[8:-3])
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"[:300]
        _append_operation_log(f"{name}: failed: {detail}")
        result = {"filename": name, "ok": False, "error": detail}
    _append_operation_log(
        f"Finished: {name} {'ready' if result.get('ok') else 'failed'}"
    )
    return json.dumps({
        "ok": bool(result.get("ok")),
        "update_dir": update_dir,
        "result": result,
        "logs": json.loads(operation_logs()),
    }, ensure_ascii=False)


def _adopt_feature_into_whitelists(name):
    """显式安装的 feature 并入所有"自定义白名单"目标（等同重新启用）。

    enabled_features=None 的目标默认全选，无需改动；显式白名单里没有的
    新 feature 否则在 UI 上不可见（用户反馈"添加成功却没有条目"的根因）。
    """
    name = _normalize_feature_name(name)
    config = load_config()
    changed = False
    for device in config.get("devices") or []:
        allowed = _normalize_enabled_features(device.get("enabled_features"))
        if allowed is not None and name not in allowed:
            device["enabled_features"] = allowed + [name]
            changed = True
    if changed:
        save_config(config)
    return changed


def _apply_known_features(config):
    """把"新出现"的 feature 并入 config 内自定义白名单（纯内存，不做 IO）。

    与显式安装不同，只收养相对 config["known_features"] 的差集，
    用户主动勾掉的既有 feature 不会被加回来；首次运行只登记不收容。
    返回是否改动了白名单。
    """
    import bootstrap

    current = list(bootstrap.list_features())
    known = config.get("known_features")
    changed = False
    if isinstance(known, list):
        known_set = set(known)
        new_names = [name for name in current if name not in known_set]
        for device in config.get("devices") or []:
            allowed = _normalize_enabled_features(device.get("enabled_features"))
            if allowed is None:
                continue
            additions = [name for name in new_names if name not in allowed]
            if additions:
                device["enabled_features"] = allowed + additions
                changed = True
    config["known_features"] = current
    return changed


def _sync_known_features():
    """load -> 收养差集 -> save 的磁盘包装（rescan 用）。"""
    config = load_config()
    if _apply_known_features(config):
        save_config(config)
        return True
    # known_features 登记本身也要落盘（即使白名单没动）。
    save_config(config)
    return False


def delete_runtime_feature(script_root, filename):
    """删除 py_updates 里的 feature_<name>.py 热更覆盖文件（设置页列表用）。

    只允许删 py_updates 目录内的文件；APK 内置文件不可删。删同名覆盖后
    模块回退内置版本；内置不存在时该 feature 条目消失。
    """
    import bootstrap

    name = str(filename or "").strip()
    if not name.endswith(".py"):
        name += ".py"
    if not name.startswith("feature_"):
        name = "feature_" + name
    bare_name = name[8:-3]
    if not bare_name.isidentifier():
        return json.dumps({"ok": False, "error": "invalid feature name"}, ensure_ascii=False)

    update_dir = os.path.join(os.path.abspath(str(script_root)), "py_updates")
    target = os.path.abspath(os.path.join(update_dir, name))
    if os.path.commonpath([target, os.path.abspath(update_dir)]) != os.path.abspath(update_dir):
        return json.dumps({"ok": False, "error": "path escapes py_updates"}, ensure_ascii=False)

    removed_file = False
    if os.path.isfile(target):
        os.unlink(target)
        removed_file = True
    module_name = "feature_" + bare_name
    sys.modules.pop(module_name, None)
    bootstrap._purge_feature_pyc(module_name)
    importlib.invalidate_caches()
    builtin = bare_name in bootstrap.BUILTIN_FEATURES
    return json.dumps({
        "ok": True,
        "name": bare_name,
        "removed_file": removed_file,
        # 同名内置还在：条目保留但已回退内置实现；否则条目随扫描消失。
        "falls_back_to_builtin": builtin,
    }, ensure_ascii=False)


def rescan_features():
    """重新扫描 py_updates 发现新增/删除的 feature（设置页"刷新列表"按钮）。

    新文件在下一次动作/建页时按需导入；被删除的运行时（非内置）模块立即清出
    sys.modules 与 pyc。返回最新 catalog JSON 供 UI 直接刷新。
    """
    import importlib
    import bootstrap

    importlib.invalidate_caches()
    builtin_modules = {f"feature_{name}" for name in bootstrap.BUILTIN_FEATURES}
    for module_name in tuple(sys.modules):
        if not module_name.startswith("feature_") or module_name in builtin_modules:
            continue
        runtime_file = bootstrap._runtime_module_path(module_name)
        module = sys.modules.get(module_name)
        if (
            runtime_file
            and not os.path.isfile(runtime_file)
            and module is not None
            and bootstrap._is_runtime_module(module)
        ):
            bootstrap._purge_feature_pyc(module_name)
            sys.modules.pop(module_name, None)
    # 新出现的 feature（含 APK 升级带来的新内置、手动 push 的脚本）
    # 自动并入自定义白名单；用户主动勾掉的既有项不受影响。
    _sync_known_features()
    return feature_catalog()


def install_builtin_feature(script_root, filename, retries=4, timeout=15):
    """Download or refresh one selected built-in feature script."""
    import bootstrap

    update_dir = os.path.join(os.path.abspath(str(script_root)), "py_updates")
    os.makedirs(update_dir, exist_ok=True)
    with _STATE["lock"]:
        _STATE.setdefault("operation_logs", []).clear()
    _append_operation_log(f"Installing {filename} into {update_dir}")
    try:
        result = _install_builtin_feature_file(
            bootstrap, filename, update_dir, retries, timeout
        )
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"[:300]
        _append_operation_log(f"{filename}: failed: {detail}")
        result = {"filename": str(filename), "ok": False, "error": detail}
    _append_operation_log(
        f"Finished: {filename} {'ready' if result.get('ok') else 'failed'}"
    )
    return json.dumps({
        "ok": bool(result.get("ok")),
        "update_dir": update_dir,
        "result": result,
        "logs": json.loads(operation_logs()),
    }, ensure_ascii=False)


def initialize(files_dir):
    _STATE["files_dir"] = str(files_dir)
    # 选择设置后端：外置 /sdcard 优先（卸载不丢），不可用时回退 App 私有单文件；
    # 首次启用外置存储会自动迁移旧 client_mqtt.json。
    _configure_storage(_STATE["files_dir"])
    durable = _STATE.get("settings_backend") == "durable"
    with _STATE["lock"]:
        config = load_config()
        devices = [item for item in config.get("devices", []) if isinstance(item, dict)]
        if not devices:
            devices = [_DEFAULT_DEVICE.copy()]
        if durable:
            # per-topic 存储：aliyun/草稿留在各 device 文档里，不做全局提升。
            for device in devices:
                device.setdefault("id", uuid.uuid4().hex)
                device.setdefault("remote_root", DEFAULT_REMOTE_ROOT_OPTIONS[0])
                if "aliyun" in device and not isinstance(device["aliyun"], dict):
                    device.pop("aliyun", None)
                if "aliyun_json_draft" in device and not isinstance(
                    device["aliyun_json_draft"], str
                ):
                    device.pop("aliyun_json_draft", None)
            config["devices"] = devices
            if not isinstance(config.get("aliyun"), dict):
                config["aliyun"] = {}
        else:
            # 旧单文件后端：历史上把 per-device aliyun 提升为全局共享配置，
            # 保持老版本配置文件与既有单测的语义不变。
            aliyun = config.get("aliyun")
            if not isinstance(aliyun, dict):
                aliyun = next((
                    device.get("aliyun") for device in devices
                    if isinstance(device.get("aliyun"), dict) and device.get("aliyun")
                ), {})
            aliyun_draft = config.get("aliyun_json_draft")
            if not isinstance(aliyun_draft, str):
                aliyun_draft = next((
                    device.get("aliyun_json_draft") for device in devices
                    if isinstance(device.get("aliyun_json_draft"), str)
                ), None)
            for device in devices:
                device.setdefault("id", uuid.uuid4().hex)
                device.setdefault("remote_root", DEFAULT_REMOTE_ROOT_OPTIONS[0])
                device.pop("aliyun", None)
                device.pop("aliyun_json_draft", None)
            config["devices"] = devices
            config["aliyun"] = aliyun
            if aliyun_draft is not None:
                config["aliyun_json_draft"] = aliyun_draft
        config.setdefault("online_probe_enabled", True)
        config.setdefault("online_probe_interval", 30)
        config["remote_root_options"] = _normalize_remote_root_options(
            config.get("remote_root_options")
        )
        config.pop("remote_root", None)
        saved_selected_id = config.get("selected_device_id")
        selected_device = next(
            (device for device in devices if device.get("id") == saved_selected_id),
            None,
        )
        if selected_device is None:
            selected_device = devices[0]
            config["selected_device_id"] = selected_device["id"]
        _STATE["selected_device_id"] = selected_device["id"]
        _STATE["selected_topic"] = selected_device.get(
            "request_topic", _DEFAULT_DEVICE["request_topic"]
        )
        # 选中 topic 决定 aliyun 生效值，必须在 _apply_known_features/save 前算好。
        _apply_effective_aliyun(config)
        # APK 升级带来的新内置 feature（或新 push 的脚本）对自定义白名单
        # 目标默认可见；用户主动勾掉的既有项不会被加回。
        _apply_known_features(config)
        save_config(config)
    return {"ok": True, "files_dir": _STATE["files_dir"]}


def update_settings(values):
    with _STATE["lock"]:
        config = load_config()
        if isinstance(values, str):
            values = json.loads(values)
        config.update(dict(values or {}))
        save_config(config)
        return json.dumps(config, ensure_ascii=False)


def general_settings():
    config = load_config()
    try:
        interval = int(config.get("online_probe_interval", 30))
    except (TypeError, ValueError):
        interval = 30
    return json.dumps({
        "online_probe_enabled": bool(config.get("online_probe_enabled", True)),
        "online_probe_interval": max(5, min(interval, 3600)),
    }, ensure_ascii=False)


def remote_root_options():
    """全局保存的远程根目录下拉选项。"""
    config = load_config()
    options = _normalize_remote_root_options(config.get("remote_root_options"))
    if config.get("remote_root_options") != options:
        config["remote_root_options"] = options
        save_config(config)
    return json.dumps(options, ensure_ascii=False)


def update_general_settings(values):
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("general settings must be an object")
    interval = int(values.get("online_probe_interval", 30))
    config = load_config()
    config["online_probe_enabled"] = bool(values.get("online_probe_enabled", True))
    config["online_probe_interval"] = max(5, min(interval, 3600))
    save_config(config)
    return general_settings()


def aliyun_settings(device=None):
    """aliyun 配置：durable 后端按 topic 私有存放（读当前/指定 topic），
    topic 没存过就回退全局；legacy 单文件后端只有全局一份。"""
    config = load_config()
    aliyun, draft = config.get("aliyun"), config.get("aliyun_json_draft")
    if _STATE.get("settings_backend") == "durable":
        selected = _device_config(device)
        topic_aliyun = selected.get("aliyun")
        if isinstance(topic_aliyun, dict) and topic_aliyun:
            aliyun = topic_aliyun
        if selected.get("aliyun_json_draft") is not None:
            draft = selected.get("aliyun_json_draft")
    return json.dumps({
        "aliyun": aliyun if isinstance(aliyun, dict) else {},
        "aliyun_json_draft": draft,
    }, ensure_ascii=False)


def update_aliyun_settings(values, device=None):
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("Aliyun settings must be a JSON object")
    aliyun = values.get("aliyun")
    draft = values.get("aliyun_json_draft")
    if aliyun is not None and not isinstance(aliyun, dict):
        raise ValueError("aliyun must be a JSON object")
    if draft is not None and not isinstance(draft, str):
        raise ValueError("aliyun_json_draft must be a string")
    config = load_config()
    if _STATE.get("settings_backend") == "durable":
        # 落进当前 topic 的 device.json；不影响其它 topic。
        selected = _device_config(device)
        target = next(
            (item for item in config.get("devices", [])
             if item.get("id") == selected.get("id")
             or item.get("request_topic") == selected.get("request_topic")),
            None,
        )
        if target is None:
            raise ValueError("selected topic not found")
        if aliyun is not None:
            target["aliyun"] = aliyun
        if draft is None:
            target.pop("aliyun_json_draft", None)
        else:
            target["aliyun_json_draft"] = draft
    else:
        if aliyun is not None:
            config["aliyun"] = aliyun
        if draft is None:
            config.pop("aliyun_json_draft", None)
        else:
            config["aliyun_json_draft"] = draft
    save_config(config)
    return aliyun_settings(device)


def device_catalog():
    config = load_config()
    devices = config.get("devices") or [_DEFAULT_DEVICE.copy()]
    changed = False
    for device in devices:
        if not device.get("id"):
            device["id"] = uuid.uuid4().hex
            changed = True
        device.setdefault("remote_root", DEFAULT_REMOTE_ROOT_OPTIONS[0])
    if changed:
        config["devices"] = devices
        save_config(config)
    return json.dumps(devices, ensure_ascii=False)


def device_settings(device_ref=None):
    selected = _device_config(device_ref)
    return json.dumps(selected, ensure_ascii=False)


def select_device(device_ref):
    selected = _device_config(device_ref)
    with _STATE["lock"]:
        _STATE["selected_device_id"] = selected["id"]
        _STATE["selected_topic"] = selected["request_topic"]
        config = load_config()
        if config.get("selected_device_id") != selected["id"]:
            config["selected_device_id"] = selected["id"]
            save_config(config)
    return json.dumps({"ok": True, "device": selected}, ensure_ascii=False)


def _normalize_feature_name(feature):
    name = str(feature or "").strip()
    if name.startswith("feature_"):
        name = name[8:]
    if not name:
        raise ValueError("feature name is required")
    return name


def _memory_device_id(device_ref=None):
    selected = _device_config(device_ref)
    return str(selected.get("id") or selected.get("request_topic") or "")


def selected_feature(device_ref=None):
    """Return the persisted last-selected feature name for a target ("" if none).

    Plain string (not JSON) so the UI can use the call result directly.
    """
    config = load_config()
    mapping = config.get("selected_feature")
    if not isinstance(mapping, dict):
        return ""
    return str(mapping.get(_memory_device_id(device_ref)) or "")


def select_feature(feature, device_ref=None):
    """Persist the last-selected feature name for a target."""
    name = _normalize_feature_name(feature)
    device_id = _memory_device_id(device_ref)
    with _STATE["lock"]:
        config = load_config()
        mapping = config.get("selected_feature")
        mapping = dict(mapping) if isinstance(mapping, dict) else {}
        mapping[device_id] = name
        config["selected_feature"] = mapping
        save_config(config)
    return json.dumps({"ok": True, "device_id": device_id, "feature": name}, ensure_ascii=False)


def feature_settings(feature, device_ref=None):
    """Return the persisted per-target settings JSON object for a feature ("{}" if none)."""
    name = _normalize_feature_name(feature)
    device_id = _memory_device_id(device_ref)
    config = load_config()
    all_settings = config.get("feature_settings")
    if not isinstance(all_settings, dict):
        return "{}"
    per_device = all_settings.get(device_id)
    if not isinstance(per_device, dict):
        return "{}"
    settings = per_device.get(name)
    if not isinstance(settings, dict):
        return "{}"
    return json.dumps(settings, ensure_ascii=False)


def update_feature_settings(feature, values, device_ref=None):
    """Shallow-merge per-target settings for a feature; returns the merged JSON object."""
    name = _normalize_feature_name(feature)
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("feature settings must be a JSON object")
    device_id = _memory_device_id(device_ref)
    with _STATE["lock"]:
        config = load_config()
        all_settings = config.get("feature_settings")
        all_settings = dict(all_settings) if isinstance(all_settings, dict) else {}
        per_device = all_settings.get(device_id)
        per_device = dict(per_device) if isinstance(per_device, dict) else {}
        settings = per_device.get(name)
        settings = dict(settings) if isinstance(settings, dict) else {}
        settings.update(values)
        per_device[name] = settings
        all_settings[device_id] = per_device
        config["feature_settings"] = all_settings
        save_config(config)
    return json.dumps(settings, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 在线探测 / 健康状态
#
# 历史问题：online() 以前走普通 rpc()（吃设备配置里可能很长的超时，还会注入
# aliyun 配置），而任意业务 RPC 无论成败都会覆写 last_probe_*，于是 feature
# 执行超时后仍可能被副作用刷成 online。现拆成两条互相独立的通道：
#
# 1) probe（专用轻探针）——在线状态的【唯一权威来源】。固定短超时（默认 2s），
#    代码只取 sys/platform 四元组，不注入任何业务配置；feature_probe 与设置里
#    的周期探测、手动 online() 全部收敛到 probe_online() 这一个入口。
# 2) rpc（业务/feature 调用）——只登记 last_rpc_* 供诊断；只有在【完全收不到
#    应答】（超时 None / 传输异常）时才把在线状态打成"待重探"：last_probe_at_ms
#    清零，UI 立刻显示 checking 并在下一个轮询周期重新探针。目标有应答但代码
#    报错（traceback）属于"在线但执行失败"，不动在线状态。
# ---------------------------------------------------------------------------
PROBE_CODE = (
    "import platform,sys\n"
    "r = (sys.executable, platform.node(), platform.machine(), platform.release())"
)
PROBE_TIMEOUT = 2.0
_PROBE_TIMEOUT_MIN = 1.0
_PROBE_TIMEOUT_MAX = 15.0


def _health_defaults(selected):
    return {
        "device_id": selected.get("id") or selected.get("request_topic"),
        "topic": selected.get("request_topic", "unknown"),
        "inflight_count": 0,
        # 在飞请求租约绝对时间戳（ms）。并发请求时取最远到期时间；
        # target_health 发现租约已过而计数没归零（线程被杀/异常漏记），
        # 一律按 0 处理 —— UI 永远不会被一次卡死的调用永久钉在 checking。
        "inflight_until_ms": 0,
        # 专用探针（在线判定唯一依据）
        "last_probe_at_ms": 0,
        "last_probe_ok": None,
        "last_probe_error": "",
        "node": "",
        "machine": "",
        "release": "",
        # 业务 RPC（仅诊断用）
        "last_rpc_at_ms": 0,
        "last_rpc_ok": None,
        "last_rpc_error": "",
        "last_success_at_ms": 0,
    }


# 给在飞请求的宽限：声明超时之外再给 12s 传输/收尾余量，
# 超过仍未 end 的调用按泄漏处理（极端 MQTT 卡死也不拖死在线状态）。
_INFLIGHT_GRACE_MS = 12_000


def target_health(device_ref=None):
    selected = _device_config(device_ref)
    key = selected.get("id") or selected["request_topic"]
    now = int(time.time() * 1000)
    with _STATE["lock"]:
        stored = _STATE.get("rpc_health", {}).get(key)
        health = dict(stored) if isinstance(stored, dict) else {}
        # 租约过期自愈：计数若没被 end 归零，这里强制归零并清租约，
        # 避免"探针早超时了，顶栏永远 checking"。
        inflight = int(health.get("inflight_count", 0) or 0)
        lease_until = int(health.get("inflight_until_ms", 0) or 0)
        if inflight > 0 and lease_until and now >= lease_until:
            health["inflight_count"] = 0
            health["inflight_until_ms"] = 0
            if isinstance(stored, dict):
                stored["inflight_count"] = 0
                stored["inflight_until_ms"] = 0
    defaults = _health_defaults(selected)
    defaults.update(health)
    defaults["device_id"] = key
    defaults["topic"] = selected["request_topic"]
    return json.dumps(defaults, ensure_ascii=False)


def _health_entry(selected):
    """取/建目标健康条目；调用方须持有 _STATE["lock"]。"""
    key = selected.get("id") or selected.get("request_topic")
    table = _STATE.setdefault("rpc_health", {})
    health = table.get(key)
    if not isinstance(health, dict):
        health = _health_defaults(selected)
        table[key] = health
    health["topic"] = selected.get("request_topic", health.get("topic", "unknown"))
    return health


def _record_request_start(selected, timeout_seconds=10):
    if not selected:
        return
    key = selected.get("id") or selected.get("request_topic")
    if not key:
        return
    now = int(time.time() * 1000)
    try:
        lease_seconds = max(1.0, float(timeout_seconds or 10))
    except (TypeError, ValueError):
        lease_seconds = 10.0
    lease_until = now + int(lease_seconds * 1000) + _INFLIGHT_GRACE_MS
    with _STATE["lock"]:
        health = _health_entry(selected)
        health["inflight_count"] = health.get("inflight_count", 0) + 1
        # 并发请求取最远到期时间；任一请求泄漏，租约一过整体自愈。
        health["inflight_until_ms"] = max(health.get("inflight_until_ms", 0), lease_until)


def _record_request_end(selected, kind, ok, error=""):
    """登记一次请求结束。kind="probe" 写在线字段；kind="rpc" 只写业务字段。"""
    if not selected:
        return
    key = selected.get("id") or selected.get("request_topic")
    if not key:
        return
    now = int(time.time() * 1000)
    with _STATE["lock"]:
        health = _health_entry(selected)
        remaining = max(0, health.get("inflight_count", 0) - 1)
        health["inflight_count"] = remaining
        if remaining == 0:
            health["inflight_until_ms"] = 0
        health[f"last_{kind}_at_ms"] = now
        health[f"last_{kind}_ok"] = bool(ok)
        health[f"last_{kind}_error"] = str(error or "")[:300]
        if ok:
            health["last_success_at_ms"] = now


def _mark_unreachable(selected, error=""):
    """业务 RPC 完全收不到应答：不直接下离线结论，清零探针时间戳，
    迫使 UI 下一秒用 2s 轻探针重新判定，避免把网络抖动永久刷成离线。"""
    if not selected:
        return
    key = selected.get("id") or selected.get("request_topic")
    if not key:
        return
    with _STATE["lock"]:
        health = _health_entry(selected)
        health["last_probe_at_ms"] = 0
        health["last_probe_ok"] = False
        health["last_probe_error"] = str(error or "rpc unreachable")[:300]


def _parse_probe_tuple(raw):
    """远端回传的是 tuple 的 repr 文本，literal_eval 安全还原。"""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = ast.literal_eval(raw.strip())
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, tuple) else None


def probe_online(device=None, timeout=PROBE_TIMEOUT, topic=None):
    """在线探测的唯一入口（feature_probe / 周期探测 / online() 共用）。

    向目标 request topic 发最轻量的 sys/platform 四元组代码，固定短超时，
    直接走共享 MQTT 客户端，绕过业务 rpc() 的代码注入与设备长超时。
    始终返回 dict（不抛异常），并据此写入 last_probe_* 健康字段。
    device 可传设备 id 或 request_topic（_device_config 两种都认）；
    topic 显式给定时按字面值发送（健康状态仍登记在 device 对应目标上）。
    """
    selected = _device_config(device)
    topic = str(topic or selected["request_topic"])
    try:
        effective_timeout = min(
            max(float(timeout or PROBE_TIMEOUT), _PROBE_TIMEOUT_MIN),
            _PROBE_TIMEOUT_MAX,
        )
    except (TypeError, ValueError):
        effective_timeout = PROBE_TIMEOUT

    result = {
        "ok": False,
        "topic": topic,
        "timeout": effective_timeout,
        "node": "",
        "machine": "",
        "release": "",
        "executable": "",
        "error": "",
        "raw": None,
    }
    _record_request_start(selected, effective_timeout)
    try:
        mqtt_client = _mqtt_client_module()
        response = mqtt_client.rpc(
            PROBE_CODE,
            request_topic=topic,
            timeout=effective_timeout,
            client_private_key_bytes=selected.get("private_key") or None,
            allow_no_server_pubkey_response=bool(
                selected.get("allow_no_server_pubkey_response", False)
            ),
        )
    except BaseException as error:  # noqa: BLE001 - 探测入口必须返回结构化结果
        detail = f"{type(error).__name__}: {error}"
        result["error"] = detail
        _record_request_end(selected, "probe", False, detail)
        return result

    if not isinstance(response, dict) or response.get("r") is None:
        detail = ""
        if isinstance(response, dict):
            detail = str(response.get("error") or "").strip()
        result["error"] = detail or "no response before timeout (target offline?)"
        _record_request_end(selected, "probe", False, result["error"])
        return result

    raw = response.get("r")
    result["raw"] = raw
    values = _parse_probe_tuple(raw)
    if values and len(values) >= 4:
        result["ok"] = True
        result["executable"], result["node"], result["machine"], result["release"] = (
            str(values[0]), str(values[1]), str(values[2]), str(values[3]),
        )
        with _STATE["lock"]:
            health = _health_entry(selected)
            health["node"], health["machine"], health["release"] = (
                result["node"], result["machine"], result["release"],
            )
        _record_request_end(selected, "probe", True)
    else:
        # 有应答但不是约定 4-tuple：契约不符，按失败呈现（raw 保留备查）。
        result["error"] = "target answered but result is not a 4-tuple (see raw)"
        _record_request_end(selected, "probe", False, result["error"])
    return result


def _private_key_kind(value):
    if not value:
        return "none"
    if isinstance(value, bytes):
        return "pem-bytes" if value.startswith(b"-----BEGIN") else "binary"
    text = str(value).strip()
    if text.startswith("-----BEGIN"):
        return "pem-text"
    if os.path.isfile(text):
        return "key-file"
    if any(char in text for char in "**+-/%() "):
        return "integer-expression"
    return "raw-text"


def _collect_config_strings(value):
    if isinstance(value, dict):
        for nested in value.values():
            yield from _collect_config_strings(nested)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            yield from _collect_config_strings(nested)
    elif isinstance(value, str) and value:
        yield value


def _redact_rpc_content(value, selected, limit=16000):
    detail = str(value)
    secrets = list(_collect_config_strings(load_config().get("aliyun", {})))
    private_key = selected.get("private_key") if selected else None
    if private_key:
        secrets.append(str(private_key))
    for secret in sorted(set(secrets), key=len, reverse=True):
        detail = detail.replace(secret, "<redacted>")
    if len(detail) > limit:
        detail = detail[:limit] + "\n...[content truncated]"
    return detail


def _redact_rpc_error(value, selected):
    lines = [line.strip() for line in str(value).splitlines() if line.strip()]
    detail = lines[-1] if lines else str(value)
    return _redact_rpc_content(detail, selected, limit=1200).replace("\n", " ")


def _remote_rpc_error(response, selected):
    error = response.get("error") if isinstance(response, dict) else None
    result_value = response.get("r") if isinstance(response, dict) else None
    if isinstance(result_value, str):
        try:
            result_value = json.loads(result_value)
        except ValueError:
            result_value = None
    if isinstance(result_value, dict) and result_value.get("ok") is False:
        error = result_value.get("error") or error
    if isinstance(response, dict) and response.get("ok") is False and not error:
        error = "server returned ok=false without error details"
    return _redact_rpc_error(error, selected) if error else None


def update_device_settings(existing_device, values):
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("device settings must be a JSON object")
    values = dict(values)
    request_topic = str(values.get("request_topic", "")).strip()
    if not request_topic:
        raise ValueError("request_topic is required")
    # legacy 后端 aliyun 只走全局接口保存；durable 后端允许随 topic 一起存。
    incoming_aliyun = values.pop("aliyun", None)
    incoming_aliyun_draft = values.pop("aliyun_json_draft", None)
    timeout_draft = values.pop("timeout_draft", None)
    if timeout_draft is not None and not isinstance(timeout_draft, str):
        raise ValueError("timeout_draft must be a string")

    with _STATE["lock"]:
        config = load_config()
        devices = config.get("devices") or []
        selected = next((
            device for device in devices
            if device.get("id") == existing_device or device.get("request_topic") == existing_device
        ), None)
        if selected is None:
            selected = _DEFAULT_DEVICE.copy()
            selected["id"] = uuid.uuid4().hex
            devices.append(selected)
        selected.update(values)
        selected["id"] = selected.get("id") or uuid.uuid4().hex
        selected["request_topic"] = request_topic
        # name 规则：显式传入用传入；否则保留已有的自定义名；
        # 老版本内置默认 "Target"（用户没起过名）回退 topic 末段，
        # 避免 request topic 在顶栏永远显示成误导性的 "Target"。
        explicit_name = str(values.get("name") or "").strip()
        if explicit_name:
            selected["name"] = explicit_name
        elif not selected.get("name") or selected.get("name") == "Target":
            selected["name"] = request_topic.rsplit("/", 1)[-1]
        selected["remote_root"] = str(
            values.get("remote_root", DEFAULT_REMOTE_ROOT_OPTIONS[0])
        )
        if "enabled_features" in values:
            selected["enabled_features"] = _normalize_enabled_features(
                values.get("enabled_features")
            )
        if timeout_draft is None:
            selected.pop("timeout_draft", None)
        else:
            selected["timeout_draft"] = timeout_draft
        if _STATE.get("settings_backend") == "durable":
            if incoming_aliyun is not None and isinstance(incoming_aliyun, dict):
                selected["aliyun"] = incoming_aliyun
            if incoming_aliyun_draft is not None:
                selected["aliyun_json_draft"] = str(incoming_aliyun_draft)
        config["devices"] = devices
        config["selected_device_id"] = selected["id"]
        _STATE["selected_device_id"] = selected["id"]
        _STATE["selected_topic"] = request_topic
        _apply_effective_aliyun(config)
        save_config(config)
        return json.dumps(selected, ensure_ascii=False)


def load_config():
    with _STATE["lock"]:
        try:
            if _STATE.get("settings_backend") == "durable" and _STATE.get("durable_paths"):
                value = _load_durable_config(_STATE["durable_paths"])
            else:
                path = _STATE.get("config_path")
                if not path or not os.path.isfile(path):
                    return _empty_config()
                value = _read_json_file(path)
                if not isinstance(value, dict):
                    raise ValueError("bad config")
            _STATE["last_config"] = copy.deepcopy(value)
            return value
        except (OSError, ValueError):
            pass
        previous = _STATE.get("last_config")
        if isinstance(previous, dict):
            return copy.deepcopy(previous)
        return _empty_config()


def save_config(value):
    if not _STATE.get("config_path"):
        return {"ok": False, "error": "service is not initialized"}
    with _STATE["lock"]:
        if _STATE.get("settings_backend") == "durable" and _STATE.get("durable_paths"):
            _save_durable_config(value, _STATE["durable_paths"])
        else:
            _atomic_write_json(_STATE["config_path"], value)
        _STATE["last_config"] = copy.deepcopy(value)
    return {"ok": True}


def _device_config(device=None):
    config = load_config()
    devices = config.get("devices") or [_DEFAULT_DEVICE.copy()]
    device_ref = device or _STATE.get("selected_device_id") or _STATE.get("selected_topic")
    selected = next((
        item for item in devices
        if item.get("id") == device_ref or item.get("request_topic") == device_ref
    ), None)
    selected = selected or devices[0]
    merged = _DEFAULT_DEVICE.copy()
    merged.update(selected)
    return merged


def rpc(code, device=None, timeout=None):
    """Execute short control code through the existing MQTT racing client.

    timeout 缺省用目标设备配置的 timeout（默认值）；feature 可显式传秒数覆盖，
    范围裁剪到 [1, 600]。例如拍照/上传等慢操作传 timeout=45。
    """
    # 只用一个请求 id：与竞速客户端/目标端回包同一个 req_id（预生成后透传），
    # 日志从发送前到回包后都能串起来，不再维护第二套本地 request_id。
    req_id = None
    selected = None
    effective_timeout = None
    try:
        client_mqtt = _mqtt_client_module()
        req_id = client_mqtt.get_req_id(client_mqtt.utc_ms())
        selected = _device_config(device)
        if timeout is None:
            timeout = float(selected.get("timeout", 10))
        effective_timeout = min(
            max(float(timeout), _RPC_TIMEOUT_MIN), _RPC_TIMEOUT_MAX
        )
        timeout = effective_timeout
        _record_request_start(selected, effective_timeout)
        topic = selected["request_topic"]
        private_key = selected.get("private_key") or None
        key_kind = _private_key_kind(private_key)
        reply_topic = "sys/device/response"
        _append_rpc_log(
            f"INFO req_id={req_id} topic={topic} reply_topic={reply_topic} phase=loading-client "
            f"timeout={timeout:g}s private_key_configured={'yes' if private_key else 'no'} key_format={key_kind}"
        )
        if private_key:
            try:
                normalized_key = client_mqtt.get_standard_pem_bytes(private_key)
            except BaseException as error:
                _append_rpc_log(
                    f"ERROR req_id={req_id} topic={topic} phase=key-normalization-failed "
                    f"key_configured=yes format={key_kind} exception_type={type(error).__name__}"
                )
                raise
            _append_rpc_log(
                f"INFO req_id={req_id} topic={topic} phase=key-normalized "
                f"format={key_kind} normalized_bytes={len(normalized_key) if normalized_key else 0}"
            )
        # aliyun 凭据按 topic 私有优先（durable 后端），全局配置兜底。
        aliyun_obj = selected.get("aliyun")
        if not isinstance(aliyun_obj, dict) or not aliyun_obj:
            aliyun_obj = load_config().get("aliyun") or {}
        aliyun = json.dumps(aliyun_obj, ensure_ascii=False)
        code = (
            "import sys\n"
            f"sys.__dict__.setdefault('_qgb_dict', {{}}).setdefault('aliyun_git', {{}}).update({aliyun})\n"
            + code
        )
        safe_code = _redact_rpc_content(code, selected)
        _append_rpc_log(
            f"REQUEST CODE req_id={req_id} topic={topic} chars={len(code)}"
            f"\n--- begin request code ---\n{safe_code}\n--- end request code ---"
        )
        _append_rpc_log(f"INFO req_id={req_id} topic={topic} phase=request-sent")
        response = client_mqtt.rpc(
            code,
            request_topic=topic,
            timeout=timeout,
            client_private_key_bytes=private_key,
            allow_no_server_pubkey_response=bool(selected.get("allow_no_server_pubkey_response", False)),
            req_id=req_id,
        )
    except BaseException as error:
        failure = f"{type(error).__name__}: {error}"
        _record_request_end(selected, "rpc", False, failure)
        # 传输层异常（连不上 broker 等）按不可达处理：立刻进入重探，
        # 不再把在线状态留在上一次成功的旧值上。
        _mark_unreachable(selected, failure)
        topic = selected.get("request_topic", "unknown") if selected else "unknown"
        detail = f"{type(error).__name__}: {error}"[:300]
        _append_rpc_log(
            f"ERROR req_id={req_id} topic={topic} phase=exception "
            f"exception_type={type(error).__name__}"
        )
        return {
            "ok": False,
            "error": detail,
            "req_id": req_id,
            "topic": topic,
        }
    if response is None:
        # 真正的超时（完全无应答）——这是"feature 超时却还显示 online"的根因点：
        # 旧代码此前也记 False，但任意后续业务 RPC 的应答又会把探针状态刷回 True。
        # 现在业务 RPC 不再触碰探针字段，这里清零时间戳强制 2s 轻探针立即复判。
        _record_request_end(selected, "rpc", False, "RPC timeout")
        _mark_unreachable(selected, "RPC timeout")
        node = getattr(client_mqtt, "_default_client", None)
        clients = getattr(getattr(node, "mqtt_net", None), "clients", {})
        states = []
        for host, client in clients.items():
            try:
                state = "connected" if client.is_connected() else "disconnected"
            except Exception:
                state = "unknown"
            states.append(f"{host}:{state}")
        broker_state = ",".join(states) or "no broker clients"
        _append_rpc_log(
            f"WARN req_id={req_id} topic={topic} phase=timeout "
            f"timeout={timeout:g}s brokers=[{broker_state}]"
        )
        _append_rpc_log(f"MQTT RESPONSE req_id={req_id}: <no response before timeout>")
        return {
            "ok": False,
            "error": "RPC timeout",
            "req_id": req_id,
            "topic": topic,
            "broker_states": states,
        }
    # 有应答 = 链路可达；但远程代码可能抛错（ok=False），这属于"在线但执行失败"，
    # 只登记业务结果，绝不据此改写在线状态（在线由 2s 轻探针单独判定）。
    rpc_ok = bool(response.get("ok", True))
    _record_request_end(
        selected, "rpc", rpc_ok,
        "" if rpc_ok else str(response.get("error") or "remote execution error"),
    )
    response_detail = _redact_rpc_content(
        json.dumps(response, ensure_ascii=False, indent=2, default=str),
        selected,
    )
    _append_rpc_log(f"MQTT RESPONSE ENVELOPE req_id={req_id}\n{response_detail}")
    # 目标 Python 的 stdout/stderr 原始打印值，单独成段原样展示，
    # 只做密钥/Aliyun 脱敏，不做任何重排或 repr 加工。
    remote_stdout = response.get("stdout")
    if isinstance(remote_stdout, str) and remote_stdout:
        _append_rpc_log(
            f"REMOTE STDOUT req_id={req_id} topic={topic} chars={len(remote_stdout)}"
            f"\n--- begin remote stdout ---\n{_redact_rpc_content(remote_stdout, selected)}"
            f"\n--- end remote stdout ---"
        )
    remote_stderr = response.get("stderr")
    if isinstance(remote_stderr, str) and remote_stderr:
        _append_rpc_log(
            f"REMOTE STDERR req_id={req_id} topic={topic} chars={len(remote_stderr)}"
            f"\n--- begin remote stderr ---\n{_redact_rpc_content(remote_stderr, selected)}"
            f"\n--- end remote stderr ---"
        )
    if isinstance(response, dict) and not response.get("ok", True) and response.get("error"):
        _append_rpc_log(
            f"REMOTE ERROR RAW req_id={req_id} topic={topic}"
            f"\n--- begin remote error ---\n{_redact_rpc_content(response.get('error'), selected)}"
            f"\n--- end remote error ---"
        )
    remote_error = _remote_rpc_error(response, selected)
    if remote_error:
        _append_rpc_log(
            f"ERROR req_id={req_id} topic={topic} "
            f"phase=target-error server={response.get('server_from', 'unknown')} "
            f"remote_error={remote_error}"
        )
    else:
        _append_rpc_log(
            f"INFO req_id={req_id} topic={topic} "
            f"phase=response server_time={response.get('server_time', 'unknown')} "
            f"server={response.get('server_from', 'unknown')} client={response.get('client_from', 'unknown')} "
            f"remote_latency_ms={response.get('latency_ms', 'unknown')}"
        )
    return response


def online(device=None):
    """周期/手动在线探测入口：复用 Probe 页面按目标保存的超时设置和 probe_online，
    不再走吃设备长超时的业务 rpc()。返回与历史调用方兼容的 {ok, result}。"""
    settings = json.loads(feature_settings("probe", device))
    timeout = settings.get("timeout", PROBE_TIMEOUT) if isinstance(settings, dict) else PROBE_TIMEOUT
    result = probe_online(device, timeout=timeout)
    return json.dumps({"ok": bool(result.get("ok")), "result": result}, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 下载服务器（设置里的 ghfast/raw URL root）feature 文件列表
#
# 设置页的 Refresh 刷的是【下载服务器上有哪些 feature_*.py】，不是本机目录
# （本机由 bootstrap 自动监控），也不是目标端。列表来源按顺序尝试：
# 1) GitHub Contents API（raw URL 自动反推 owner/repo/branch/path）；
# 2) GitHub tree HTML（走 ghfast 代理）；
# 3) 直接 GET root（兼容 nginx/apache/python -m http.server 这类目录索引页）。
# 全部只展示文件名，一行一个。
# ---------------------------------------------------------------------------
_FEATURE_FILE_RE = re.compile(r"feature_[A-Za-z0-9_-]+\.py")
_FEATURE_FILE_FULL_RE = re.compile(r"^feature_[A-Za-z0-9_-]+\.py$")


def _split_proxied_url(root):
    """ghfast 形态 '<proxy-base>/https://real/...' → (proxy_base, real_url)。"""
    match = re.match(r"^(https?://[^/]+)/(https?://.*)$", str(root or "").strip())
    if match:
        return match.group(1), match.group(2)
    return "", str(root or "").strip()


def _github_parts(real_url):
    """从 raw/github 下载 URL 反推 (owner, repo, branch, subpath)，非 GitHub 返回 None。"""
    parsed = urllib.parse.urlsplit(real_url)
    parts = [segment for segment in parsed.path.split("/") if segment]
    if parsed.netloc == "raw.githubusercontent.com":
        if len(parts) >= 5 and parts[2] == "refs" and parts[3] == "heads":
            return parts[0], parts[1], parts[4], "/".join(parts[5:])
        if len(parts) >= 3:
            return parts[0], parts[1], parts[2], "/".join(parts[3:])
    if parsed.netloc == "github.com":
        match = re.match(
            r"^/([^/]+)/([^/]+)/raw/(?:refs/heads/)?([^/]+)/(.*)$", parsed.path
        )
        if match:
            return match.group(1), match.group(2), match.group(3), match.group(4)
    return None


def _server_listing_urls(root):
    proxy, real = _split_proxied_url(root)
    urls = []

    def add(url):
        if url and url not in urls:
            urls.append(url)

    parts = _github_parts(real)
    if parts:
        owner, repo, branch, subpath = parts
        subpath = subpath.rstrip("/")
        api_url = (
            "https://api.github.com/repos/%s/%s/contents/%s?ref=%s"
            % (owner, repo, subpath, branch)
        )
        tree_url = "https://github.com/%s/%s/tree/%s/%s" % (
            owner, repo, branch, subpath
        )
        if proxy:
            # 列表必须拿最新：直连 Contents API 优先；ghfast 对 API/tree 返回
            # 403（只代理 raw 文件下载），代理 URL 只作快速失败的兜底。
            add(api_url)
            add(tree_url)
            add("%s/%s" % (proxy, api_url))
            add("%s/%s" % (proxy, tree_url))
        else:
            add(api_url)
            add(tree_url)
    add(root)
    return urls


def _parse_feature_listing(text, root):
    """GitHub API 返回 JSON 数组；其它任意文本（HTML 索引页等）正则提取文件名。"""
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        data = None
    if isinstance(data, list):
        files = []
        for item in data:
            if not isinstance(item, dict) or item.get("type") != "file":
                continue
            name = str(item.get("name") or "")
            if _FEATURE_FILE_FULL_RE.match(name):
                files.append({
                    "name": name,
                    "size": item.get("size"),
                    "download_url": item.get("download_url") or (root + name),
                })
        if files:
            return files
    if isinstance(data, dict) and data.get("message"):
        raise ValueError(str(data["message"])[:200])
    return [
        {"name": name, "size": None, "download_url": root + name}
        for name in sorted(set(_FEATURE_FILE_RE.findall(text)))
    ]


def download_server_feature_files(timeout=15):
    """一次性列出下载 URL root 下最新的 feature_*.py。

    GitHub（含 ghfast 代理形态的 raw URL）：反推出 owner/repo/branch 后走
    Contents API，直连优先（ghfast 对 API 返回 403，只放行 raw 文件下载）；
    普通 HTTP 文件服务（nginx/apache/python -m http.server 等）：直接 GET root，
    解析目录索引页里的文件名——服务器上新增文件后下次刷新立刻可见。
    """
    try:
        timeout_seconds = min(max(float(timeout or 15), 1.0), 60.0)
    except (TypeError, ValueError):
        timeout_seconds = 15.0
    root = feature_url_root()
    attempts, files, source_url = [], None, None
    for url in _server_listing_urls(root):
        meta = {"url": url}
        try:
            request = urllib.request.Request(url, headers={
                "User-Agent": "ClientMqtt/1.0",
                "Accept": "application/vnd.github+json, text/html;q=0.9, */*;q=0.8",
            })
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                meta["http_status"] = getattr(response, "status", 200)
                body = response.read(5_000_000)
            parsed = _parse_feature_listing(body.decode("utf-8", "replace"), root)
            if parsed:
                files, source_url = parsed, url
                meta["found"] = len(parsed)
                attempts.append(meta)
                break
            meta["error"] = "no feature_*.py entries"
        except urllib.error.HTTPError as error:
            meta["http_status"] = error.code
            meta["error"] = "HTTP %d" % error.code
        except (urllib.error.URLError, OSError, ValueError) as error:
            meta["error"] = "%s: %s" % (type(error).__name__, error)
        attempts.append(meta)
    if files:
        return json.dumps({
            "ok": True,
            "root": root,
            "source_url": source_url,
            "files": files,
            "attempts": attempts,
        }, ensure_ascii=False)
    last_error = next((item["error"] for item in reversed(attempts) if item.get("error")),
                      "no feature files found")
    return json.dumps({
        "ok": False,
        "root": root,
        "error": last_error,
        "attempts": attempts,
    }, ensure_ascii=False)


def _set_aliyun_config(config):
    safe_config = dict(config or {})
    sys.__dict__.setdefault("_qgb_dict", {}).setdefault("aliyun_git", {}).update(safe_config)
    return safe_config


def make_progress(callback, interval=2.5):
    """把高频下载字节回调节流成"隔几秒一次"的进度汇报，返回 (got, total) 回调。

    第一次和最后一次（got >= total）必发，中间至少间隔 interval 秒；
    callback 自身抛错不影响下载。callback 由工作线程调用，更新 UI 请用
    pyui_kit.on_main(...) 投递到主线程。
    """
    if callback is None:
        return None
    state = {"last": 0.0}

    def report(got, total):
        try:
            got = int(got or 0)
            total = int(total or 0)
            now = time.time()
            if now - state["last"] >= interval or (total and got >= total) or got and not total:
                state["last"] = now
                callback(got, total)
        except BaseException:  # noqa: BLE001 - 进度汇报绝不能炸下载
            pass
    return report


# 其他 topic（旧 APK）里可能跑着老版本 aliyun_git，download() 没有 progress 形参，
# 直接传 progress= 会 TypeError 让下载整个失败。按模块缓存能力探测结果：
# 有钩子才传，没有就静默降级（功能正常，只是看不到速度）。
_DOWNLOAD_PROGRESS_CAPABLE = set()


def _download_supports_progress(aliyun_git):
    module_key = getattr(aliyun_git, "__file__", None) or id(aliyun_git)
    if module_key in _DOWNLOAD_PROGRESS_CAPABLE:
        return True
    try:
        import inspect
        params = inspect.signature(aliyun_git.download).parameters
    except (TypeError, ValueError, RuntimeError, OSError):
        # 部分运行时/代理对象取签名会失败：用一次假参试调代价太高，保守按不支持处理。
        return False
    if "progress" in params:
        _DOWNLOAD_PROGRESS_CAPABLE.add(module_key)
        return True
    return False


def _aliyun_download(aliyun_git, url, report, **kwargs):
    """统一下载入口：仅当目标 aliyun_git 版本支持 progress 钩子时才传。"""
    if report is not None and _download_supports_progress(aliyun_git):
        kwargs["progress"] = report
    return aliyun_git.download(url, **kwargs)


def download_transfer(url, config=None, save_to=None, progress=None):
    """Download a short transfer URL outside MQTT, returning bytes or a local path."""
    _set_aliyun_config(config if config is not None else load_config().get("aliyun", {}))
    aliyun_git = importlib.import_module("aliyun_git")
    report = make_progress(progress)
    data = _aliyun_download(
        aliyun_git, url, report, save_to=save_to, max_show_bytes_size=0
    )
    return {"ok": True, "path": data if save_to else None, "size": os.path.getsize(data) if save_to else len(data)}


def download_transfer_base64(url, config=None, progress=None):
    """Download an image/file into memory for the Android bridge, never MQTT."""
    _set_aliyun_config(config if config is not None else load_config().get("aliyun", {}))
    aliyun_git = importlib.import_module("aliyun_git")
    report = make_progress(progress)
    data = _aliyun_download(aliyun_git, url, report, max_show_bytes_size=0)
    return base64.b64encode(bytes(data)).decode("ascii")


def download_remote_to_file(url, name, config=None, progress=None):
    root = _STATE.get("files_dir") or os.getcwd()
    target_dir = os.path.join(root, "downloads")
    os.makedirs(target_dir, exist_ok=True)
    safe_name = os.path.basename(str(name)) or "download.bin"
    target = os.path.join(target_dir, safe_name)
    result = download_transfer(url, config=config, save_to=target, progress=progress)
    return json.dumps(result, ensure_ascii=False)


def parse_json_result(response):
    if not response:
        return {"ok": False, "error": "empty RPC response"}
    if isinstance(response, dict):
        value = response.get("r")
    else:
        value = response
    try:
        if isinstance(response, dict) and response.get("ok") is False:
            result = {
                "ok": False,
                "error": response.get("error") or "RPC server returned ok=false",
            }
        elif isinstance(response, dict) and "r" not in response:
            result = {"ok": False, "error": "RPC response is missing result field"}
        elif value is None:
            result = {
                "ok": False,
                "error": response.get("error") if isinstance(response, dict) and response.get("error")
                else "target RPC returned null result",
            }
        else:
            result = json.loads(value) if isinstance(value, str) else value
        if result is None:
            result = {"ok": False, "error": "target RPC returned JSON null"}
        elif not isinstance(result, dict):
            result = {
                "ok": False,
                "error": f"target RPC result must be a JSON object, got {type(result).__name__}",
                "result": result,
            }
        metadata_fields = (
            "req_id", "server_time", "server_from", "latency_ms", "client_from",
        )
        metadata = {key: response[key] for key in metadata_fields if key in response}
        if metadata and isinstance(result, dict):
            result["_rpc"] = metadata
        if isinstance(result, dict):
            # 原始 Python 输出透传给 UI 单独原样渲染，UI 不要用 Java 再加工 print。
            stdout = response.get("stdout")
            if isinstance(stdout, str) and stdout:
                result.setdefault("_stdout", stdout)
            stderr = response.get("stderr")
            if isinstance(stderr, str) and stderr:
                result.setdefault("_stderr", stderr)
            envelope_error = response.get("error")
            if isinstance(envelope_error, str) and envelope_error and not result.get("error"):
                result["_remote_error"] = envelope_error
        return result
    except (TypeError, ValueError) as exc:
        return {"ok": False, "error": f"invalid RPC JSON: {exc}", "raw": str(value)[:500]}


def normalize_relative_path(root, relative):
    root_path = PurePosixPath(str(root))
    candidate = PurePosixPath(str(relative))
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError("path escapes configured root")
    return str(root_path.joinpath(candidate))
