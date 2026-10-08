"""最小在线探针（feature）。

在线探测的唯一实现是 client_service.probe_online()：向【当前选中目标】的
request topic 发最轻量代码，2 秒超时，目标在线时返回
sys.executable / platform.node() / platform.machine() / platform.release()。

本 feature 只是 probe_online 的界面封装：
- 设置页的周期在线探测、online()、本页面按钮全部走同一个入口，结果一致；
- 不依赖任何业务模块，目标端是裸 multi_mqtt 也能应答。

topic 不写死：默认跟随当前选中目标（client_service 里的 request_topic），
切目标后探针自动打到新 topic；仍可显式传 topic 覆盖。超时固定 2s，
不吃设备配置里可能很长的全局超时。
"""
import json

import client_service

FEATURE = {
    "actions": ["run"],
    "icon": "bug",
    "version": 1,
    # 界面由本脚本用 Chaquopy 自绘（见 build_view），APK 只有通用宿主。
    "ui": "python",
}

DEFAULT_TIMEOUT = client_service.PROBE_TIMEOUT


def current_topic():
    """当前选中目标的 request topic。"""
    return str(client_service._device_config(None).get("request_topic") or "")


def run(topic=None, timeout=DEFAULT_TIMEOUT):
    """UI 动作入口；topic 缺省跟随当前选中目标。始终返回 JSON 字符串。

    直接委托 client_service.probe_online —— 在线探测只有这一份实现，
    feature 页面、周期探测、手动 online() 不会再出现结论不一致。
    """
    payload = client_service.probe_online(None, timeout, topic=topic or None)
    return json.dumps(payload, ensure_ascii=False)


def _format(payload):
    if not isinstance(payload, dict):
        return repr(payload)
    if not payload.get("ok"):
        return "probe failed: %s\ntopic=%s timeout=%ss" % (
            payload.get("error", "unknown error"),
            payload.get("topic"), payload.get("timeout"),
        )
    return "\n".join([
        "target online  (topic=%s, %.0fs)" % (
            payload.get("topic"), float(payload.get("timeout") or 0),
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
        selectable=True,
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
