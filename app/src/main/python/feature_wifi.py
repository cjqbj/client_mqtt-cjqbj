"""Target Wi-Fi information feature. Replaceable at runtime."""

import json
import client_service

FEATURE = {"name": "wifi", "title": "Wi-Fi", "version": 1, "actions": ["info"],
           "ui": "python", "icon": "wifi"}


def run():
    return FEATURE


def build_wifi_code():
    return '''import json
from com.chaquo.python import Python
from android.content import Context
from android.net.wifi import WifiManager
from android.text.format import Formatter

appctx = Python.getPlatform().getApplication()
wm = appctx.getSystemService(Context.WIFI_SERVICE)
wi = wm.getConnectionInfo()
ssid = wi.getSSID()
if ssid:
    ssid = ssid.strip('"')
r = json.dumps({"ok": True, "wifi": {
    "ssid": ssid,
    "bssid": str(wi.getBSSID()) if wi.getBSSID() else None,
    "rssi": wi.getRssi(),
    "link_speed": wi.getLinkSpeed(),
    "frequency": wi.getFrequency(),
    "ip": Formatter.formatIpAddress(wi.getIpAddress()),
    "mac": str(wi.getMacAddress()) if wi.getMacAddress() else None,
}}, ensure_ascii=False)
'''


def info():
    # 设备配置 timeout 只是默认值；Wi-Fi 查询很快，固定 10s 上限。
    result = client_service.rpc(build_wifi_code(), timeout=10)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘：Wi-Fi 结果区 + 底部 Refresh/Copy。"""
    import bootstrap
    import pyui_kit

    page = pyui_kit.Page(context, "Wi-Fi", "Query target Wi-Fi connection info")
    result = pyui_kit.make_text(context, "Waiting for target RPC", selectable=True)
    page.add(result)
    copy_hint = pyui_kit.make_text(context, "", size=12, color=pyui_kit.MUTED)
    state = {"text": "Waiting for target RPC"}

    refresh = pyui_kit.make_button(context, "Refresh Wi-Fi", lambda: None)
    copy_button = pyui_kit.make_button(context, "Copy", lambda: None)

    def on_refresh():
        result.setText("Querying ...")
        copy_hint.setText("")

        def work():
            return bootstrap.call_feature("wifi", "info")

        def apply_ok(raw):
            state["text"] = pyui_kit.render_result(raw)
            result.setText(state["text"])

        def apply_error(error):
            state["text"] = "Wi-Fi query failed: %s" % error
            result.setText(state["text"])

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(refresh, copy_button))

    def on_copy():
        pyui_kit.copy_text(context, state["text"])
        copy_hint.setText("Copied")

    pyui_kit.click(refresh, on_refresh)
    pyui_kit.click(copy_button, on_copy)
    page.bottom_add(refresh, weight=1.0)
    page.bottom_space()
    page.bottom_add(copy_button)
    page.add(copy_hint, top=6)
    pyui_kit.watch_target(
        page.root,
        lambda key, topic, cfg: page.set_target("target topic: " + topic),
    )
    return page.root
