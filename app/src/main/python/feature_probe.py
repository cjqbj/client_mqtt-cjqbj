"""最小在线探针（feature）。

向【当前选中目标】的 request topic 发一段最轻量的代码，2 秒超时，目标在线
时返回：sys.executable / platform.node() / platform.machine() /
platform.release()。

用途：
- 在跑重 RPC 前快速判断目标在不在线、架构对不对；
- 不依赖任何业务模块，目标端是裸 multi_mqtt 也能应答。

topic 不再写死：默认跟随当前选中目标（client_service 里的 request_topic），
切目标后探针自动打到新 topic；仍可显式传 topic 覆盖。超时固定 2s，
不吃设备配置里可能很长的全局超时。
"""
import ast
import json

import client_service

FEATURE = {
    "name": "probe",
    "title": "Probe",
    "actions": ["run"],
    "icon": "bug",
    "version": 1,
    # 界面由本脚本用 Chaquopy 自绘（见 build_view），APK 只有通用宿主。
    "ui": "python",
}

# 注意：末行必须是赋值给 r 的表达式。远端执行器取末尾表达式/ r 变量，
# 经 repr 风格格式化后回传，这里用 ast.literal_eval 还原成 tuple。
PROBE_CODE = (
    "import platform,sys\n"
    "r = (sys.executable, platform.node(), platform.machine(), platform.release())"
)
DEFAULT_TIMEOUT = 2.0


def current_topic():
    """当前选中目标的 request topic。"""
    return str(client_service._device_config(None).get("request_topic") or "")


def _request(topic=None, timeout=DEFAULT_TIMEOUT):
    """直接复用底层共享 MQTT 客户端，绕过全局超时。topic 默认跟选中目标。"""
    selected = client_service._device_config(None)
    topic = str(topic or selected.get("request_topic") or "")
    mqtt_client = client_service._mqtt_client_module()
    return mqtt_client.rpc(
        PROBE_CODE,
        request_topic=topic,
        timeout=float(timeout or DEFAULT_TIMEOUT),
        client_private_key_bytes=selected.get("private_key") or None,
        allow_no_server_pubkey_response=bool(
            selected.get("allow_no_server_pubkey_response", False)
        ),
    )


def _parse_tuple(raw):
    """远端回传的是 tuple 的 repr/pprint 文本，literal_eval 安全还原。"""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = ast.literal_eval(raw.strip())
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, tuple) else None


def run(topic=None, timeout=DEFAULT_TIMEOUT):
    """UI 动作入口；topic 缺省跟随当前选中目标。始终返回 JSON 字符串。"""
    topic = str(topic or current_topic())
    timeout = float(timeout or DEFAULT_TIMEOUT)
    try:
        response = _request(topic, timeout)
    except Exception as error:
        return json.dumps(
            {"ok": False, "topic": topic, "timeout": timeout,
             "error": f"{type(error).__name__}: {error}"},
            ensure_ascii=False,
        )

    if not isinstance(response, dict) or response.get("r") is None:
        detail = ""
        if isinstance(response, dict):
            detail = str(response.get("error") or "").strip()
        return json.dumps(
            {"ok": False, "topic": topic, "timeout": timeout,
             "elapsed_ms": response.get("elapsed_ms") if isinstance(response, dict) else None,
             "error": detail or "no result before timeout (target offline?)"},
            ensure_ascii=False,
        )

    raw = response.get("r")
    result = {
        "ok": bool(response.get("ok", True)),
        "topic": topic,
        "timeout": timeout,
        "elapsed_ms": response.get("elapsed_ms"),
        "raw": raw,
    }
    values = _parse_tuple(raw)
    if values and len(values) >= 4:
        result.update(
            executable=values[0],
            node=values[1],
            machine=values[2],
            release=values[3],
        )
    else:
        # 目标有应答但回的不是约定 4-tuple：契约不符，整体按失败呈现（raw 保留备查）。
        result["ok"] = False
        result["error"] = "target answered but result is not a 4-tuple (see raw)"
    return json.dumps(result, ensure_ascii=False)


def _format(payload):
    if not isinstance(payload, dict):
        return repr(payload)
    if not payload.get("ok"):
        return "probe failed: %s\ntopic=%s timeout=%ss" % (
            payload.get("error", "unknown error"),
            payload.get("topic"), payload.get("timeout"),
        )
    elapsed_ms = payload.get("elapsed_ms")
    return "\n".join([
        "target online  (topic=%s, %.0fs, elapsed=%s)" % (
            payload.get("topic"), float(payload.get("timeout") or 0),
            ("%sms" % elapsed_ms) if elapsed_ms is not None else "n/a",
        ),
        "node       : %s" % payload.get("node"),
        "machine    : %s" % payload.get("machine"),
        "release    : %s" % payload.get("release"),
        "executable : %s" % payload.get("executable"),
    ])


def build_view(context):
    """Chaquopy 自绘：探针结果区 + 底部探针按钮（topic 实时跟选中目标）。"""
    import bootstrap
    import pyui_kit

    page = pyui_kit.Page(context, "Probe", "2s minimal platform probe")
    result = pyui_kit.make_text(
        context,
        "点按钮向【当前选中目标】发送 2 秒最小探针；\n切换目标后按钮上的 topic 会自动跟着变。",
    )
    page.add(result)
    button = pyui_kit.make_button(context, "PROBE", lambda: None)

    def refresh_label(_key=None, topic=None, _cfg=None):
        topic = topic or current_topic()
        page.set_target("target topic: " + topic)
        button.setText("PROBE  topic=%s  timeout=%ss" % (topic, DEFAULT_TIMEOUT))

    def on_click():
        button.setEnabled(False)
        result.setText("probing ...")

        def work():
            # 不传 topic：run() 内部取当前选中目标的 request_topic。
            return bootstrap.call_feature("probe", "run")

        def apply_ok(raw):
            result.setText(_format(json.loads(raw)))

        def apply_error(error):
            result.setText("ERROR %s: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(button,))

    pyui_kit.click(button, on_click)
    page.bottom_add(button, weight=1.0)
    # 页面挂上后持续跟随当前目标；切目标即时更新按钮与提示行。
    pyui_kit.watch_target(page.root, refresh_label)
    return page.root
