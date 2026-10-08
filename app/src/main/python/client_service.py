"""Client-side bridge for MQTT RPC and remote file operations."""

import json
import base64
import copy
import importlib
import importlib.util
import os
import sys
import threading
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
_DEFAULT_DEVICE = {
    "name": "Target",
    "request_topic": "sys/device/request",
    "private_key": "",
    "allow_no_server_pubkey_response": True,
    "timeout": 10,
    "remote_root": "/data/data",
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
        from android.widget import TextView
        error_view = TextView(context)
        error_view.setText(
            "Python UI failed for feature_%s.py:\n\n%s"
            % (feature, traceback.format_exc()[-1500:])
        )
        return error_view


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
    path = os.path.join(_STATE["files_dir"], _CONFIG_NAME)
    _STATE["config_path"] = path
    if not os.path.isfile(path):
        save_config({"devices": [_DEFAULT_DEVICE.copy()], "scan_limit": 100, "remote_root": "/data/data"})
    with _STATE["lock"]:
        config = load_config()
        devices = [item for item in config.get("devices", []) if isinstance(item, dict)]
        if not devices:
            devices = [_DEFAULT_DEVICE.copy()]
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
            device.setdefault("remote_root", config.get("remote_root", "/data/data"))
            device.pop("aliyun", None)
            device.pop("aliyun_json_draft", None)
        config["devices"] = devices
        config["aliyun"] = aliyun
        config.setdefault("online_probe_enabled", True)
        config.setdefault("online_probe_interval", 30)
        if aliyun_draft is not None:
            config["aliyun_json_draft"] = aliyun_draft
        config.pop("remote_root", None)
        saved_selected_id = config.get("selected_device_id")
        selected_device = next(
            (device for device in devices if device.get("id") == saved_selected_id),
            None,
        )
        if selected_device is None:
            selected_device = devices[0]
            config["selected_device_id"] = selected_device["id"]
        save_config(config)
        _STATE["selected_device_id"] = selected_device["id"]
        _STATE["selected_topic"] = selected_device.get("request_topic", _DEFAULT_DEVICE["request_topic"])
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


def aliyun_settings():
    config = load_config()
    return json.dumps({
        "aliyun": config.get("aliyun") if isinstance(config.get("aliyun"), dict) else {},
        "aliyun_json_draft": config.get("aliyun_json_draft"),
    }, ensure_ascii=False)


def update_aliyun_settings(values):
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, dict):
        raise ValueError("Aliyun settings must be a JSON object")
    config = load_config()
    aliyun = values.get("aliyun")
    draft = values.get("aliyun_json_draft")
    if aliyun is not None:
        if not isinstance(aliyun, dict):
            raise ValueError("aliyun must be a JSON object")
        config["aliyun"] = aliyun
    if draft is None:
        config.pop("aliyun_json_draft", None)
    elif isinstance(draft, str):
        config["aliyun_json_draft"] = draft
    else:
        raise ValueError("aliyun_json_draft must be a string")
    save_config(config)
    return aliyun_settings()


def device_catalog():
    config = load_config()
    devices = config.get("devices") or [_DEFAULT_DEVICE.copy()]
    changed = False
    for device in devices:
        if not device.get("id"):
            device["id"] = uuid.uuid4().hex
            changed = True
        device.setdefault("remote_root", config.get("remote_root", "/data/data"))
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


def target_health(device_ref=None):
    selected = _device_config(device_ref)
    key = selected.get("id") or selected["request_topic"]
    with _STATE["lock"]:
        health = dict(_STATE.get("rpc_health", {}).get(key, {}))
    health.setdefault("device_id", key)
    health.setdefault("topic", selected["request_topic"])
    health.setdefault("last_probe_at_ms", 0)
    health.setdefault("last_probe_ok", None)
    health.setdefault("last_success_at_ms", 0)
    health.setdefault("inflight_count", 0)
    return json.dumps(health, ensure_ascii=False)


def _record_rpc_start(selected):
    if not selected:
        return
    key = selected.get("id") or selected.get("request_topic")
    if not key:
        return
    with _STATE["lock"]:
        health = _STATE.setdefault("rpc_health", {}).setdefault(key, {
            "device_id": key,
            "topic": selected.get("request_topic", "unknown"),
            "last_success_at_ms": 0,
            "last_probe_at_ms": 0,
            "last_probe_ok": None,
            "inflight_count": 0,
        })
        health["inflight_count"] = health.get("inflight_count", 0) + 1


def _record_rpc_health(selected, succeeded):
    if not selected:
        return
    key = selected.get("id") or selected.get("request_topic")
    if not key:
        return
    now = int(time.time() * 1000)
    with _STATE["lock"]:
        health = _STATE.setdefault("rpc_health", {}).setdefault(key, {
            "device_id": key,
            "topic": selected.get("request_topic", "unknown"),
            "last_success_at_ms": 0,
        })
        health["topic"] = selected.get("request_topic", health.get("topic", "unknown"))
        health["inflight_count"] = max(0, health.get("inflight_count", 0) - 1)
        health["last_probe_at_ms"] = now
        health["last_probe_ok"] = bool(succeeded)
        if succeeded:
            health["last_success_at_ms"] = now


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
    values.pop("aliyun", None)
    values.pop("aliyun_json_draft", None)
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
        selected["remote_root"] = str(values.get("remote_root", "/data/data"))
        if "enabled_features" in values:
            selected["enabled_features"] = _normalize_enabled_features(
                values.get("enabled_features")
            )
        if timeout_draft is None:
            selected.pop("timeout_draft", None)
        else:
            selected["timeout_draft"] = timeout_draft
        config["devices"] = devices
        config["selected_device_id"] = selected["id"]
        save_config(config)
        _STATE["selected_device_id"] = selected["id"]
        _STATE["selected_topic"] = request_topic
        return json.dumps(selected, ensure_ascii=False)


def load_config():
    path = _STATE.get("config_path")
    if not path or not os.path.isfile(path):
        return {"devices": [_DEFAULT_DEVICE.copy()], "scan_limit": 100, "remote_root": "/data/data"}
    with _STATE["lock"]:
        try:
            with open(path, "r", encoding="utf-8") as config_file:
                value = json.load(config_file)
            if isinstance(value, dict):
                _STATE["last_config"] = copy.deepcopy(value)
                return value
        except (OSError, ValueError):
            pass
        previous = _STATE.get("last_config")
        if isinstance(previous, dict):
            return copy.deepcopy(previous)
        return {"devices": [_DEFAULT_DEVICE.copy()], "scan_limit": 100, "remote_root": "/data/data"}


def save_config(value):
    path = _STATE.get("config_path")
    if not path:
        return {"ok": False, "error": "service is not initialized"}
    with _STATE["lock"]:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        temporary = path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as config_file:
            json.dump(value, config_file, ensure_ascii=False, indent=2)
        os.replace(temporary, path)
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
    request_id = uuid.uuid4().hex[:8]
    started = time.perf_counter()
    selected = None
    effective_timeout = None
    try:
        selected = _device_config(device)
        _record_rpc_start(selected)
        if timeout is None:
            timeout = float(selected.get("timeout", 10))
        effective_timeout = min(
            max(float(timeout), _RPC_TIMEOUT_MIN), _RPC_TIMEOUT_MAX
        )
        timeout = effective_timeout
        topic = selected["request_topic"]
        private_key = selected.get("private_key") or None
        key_kind = _private_key_kind(private_key)
        reply_topic = "sys/device/response"
        _append_rpc_log(
            f"INFO id={request_id} topic={topic} reply_topic={reply_topic} phase=loading-client "
            f"timeout={timeout:g}s private_key_configured={'yes' if private_key else 'no'} key_format={key_kind}"
        )
        client_mqtt = _mqtt_client_module()
        if private_key:
            try:
                normalized_key = client_mqtt.get_standard_pem_bytes(private_key)
            except BaseException as error:
                _append_rpc_log(
                    f"ERROR id={request_id} topic={topic} phase=key-normalization-failed "
                    f"key_configured=yes format={key_kind} exception_type={type(error).__name__}"
                )
                raise
            _append_rpc_log(
                f"INFO id={request_id} topic={topic} phase=key-normalized "
                f"format={key_kind} normalized_bytes={len(normalized_key) if normalized_key else 0}"
            )
        aliyun = json.dumps(load_config().get("aliyun") or {}, ensure_ascii=False)
        code = (
            "import sys\n"
            f"sys.__dict__.setdefault('_qgb_dict', {{}}).setdefault('aliyun_git', {{}}).update({aliyun})\n"
            + code
        )
        safe_code = _redact_rpc_content(code, selected)
        _append_rpc_log(
            f"REQUEST CODE id={request_id} topic={topic} chars={len(code)}"
            f"\n--- begin request code ---\n{safe_code}\n--- end request code ---"
        )
        _append_rpc_log(f"INFO id={request_id} topic={topic} phase=request-sent")
        response = client_mqtt.rpc(
            code,
            request_topic=topic,
            timeout=timeout,
            client_private_key_bytes=private_key,
            allow_no_server_pubkey_response=bool(selected.get("allow_no_server_pubkey_response", False)),
        )
    except BaseException as error:
        _record_rpc_health(selected, False)
        topic = selected.get("request_topic", "unknown") if selected else "unknown"
        elapsed = round((time.perf_counter() - started) * 1000, 2)
        detail = f"{type(error).__name__}: {error}"[:300]
        _append_rpc_log(
            f"ERROR id={request_id} topic={topic} phase=exception elapsed_ms={elapsed} "
            f"exception_type={type(error).__name__}"
        )
        return {
            "ok": False,
            "error": detail,
            "request_id": request_id,
            "topic": topic,
            "elapsed_ms": elapsed,
        }
    elapsed = round((time.perf_counter() - started) * 1000, 2)
    if response is None:
        _record_rpc_health(selected, False)
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
            f"WARN id={request_id} topic={topic} phase=timeout elapsed_ms={elapsed} "
            f"timeout={timeout:g}s brokers=[{broker_state}]"
        )
        _append_rpc_log(f"MQTT RESPONSE id={request_id}: <no response before timeout>")
        return {
            "ok": False,
            "error": "RPC timeout",
            "request_id": request_id,
            "topic": topic,
            "elapsed_ms": elapsed,
            "broker_states": states,
        }
    response["elapsed_ms"] = elapsed
    response["request_id"] = request_id
    _record_rpc_health(selected, True)
    response_detail = _redact_rpc_content(
        json.dumps(response, ensure_ascii=False, indent=2, default=str),
        selected,
    )
    _append_rpc_log(f"MQTT RESPONSE ENVELOPE id={request_id}\n{response_detail}")
    # 目标 Python 的 stdout/stderr 原始打印值，单独成段原样展示，
    # 只做密钥/Aliyun 脱敏，不做任何重排或 repr 加工。
    remote_stdout = response.get("stdout")
    if isinstance(remote_stdout, str) and remote_stdout:
        _append_rpc_log(
            f"REMOTE STDOUT id={request_id} topic={topic} chars={len(remote_stdout)}"
            f"\n--- begin remote stdout ---\n{_redact_rpc_content(remote_stdout, selected)}"
            f"\n--- end remote stdout ---"
        )
    remote_stderr = response.get("stderr")
    if isinstance(remote_stderr, str) and remote_stderr:
        _append_rpc_log(
            f"REMOTE STDERR id={request_id} topic={topic} chars={len(remote_stderr)}"
            f"\n--- begin remote stderr ---\n{_redact_rpc_content(remote_stderr, selected)}"
            f"\n--- end remote stderr ---"
        )
    if isinstance(response, dict) and not response.get("ok", True) and response.get("error"):
        _append_rpc_log(
            f"REMOTE ERROR RAW id={request_id} topic={topic}"
            f"\n--- begin remote error ---\n{_redact_rpc_content(response.get('error'), selected)}"
            f"\n--- end remote error ---"
        )
    remote_error = _remote_rpc_error(response, selected)
    if remote_error:
        _append_rpc_log(
            f"ERROR id={request_id} req_id={response.get('req_id', 'unknown')} topic={topic} "
            f"phase=target-error server={response.get('server_from', 'unknown')} "
            f"remote_error={remote_error}"
        )
    else:
        _append_rpc_log(
            f"INFO id={request_id} req_id={response.get('req_id', 'unknown')} topic={topic} "
            f"phase=response elapsed_ms={elapsed} server_time={response.get('server_time', 'unknown')} "
            f"server={response.get('server_from', 'unknown')} client={response.get('client_from', 'unknown')} "
            f"remote_latency_ms={response.get('latency_ms', 'unknown')}"
        )
    return response


def online(device=None):
    result = rpc("r = {'online': True}", device)
    return json.dumps({"ok": bool(result.get("ok")), "result": result}, ensure_ascii=False)


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
            "req_id", "request_id", "server_time", "server_from", "latency_ms", "client_from", "elapsed_ms",
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
