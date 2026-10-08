"""In-memory remote dialer feature. Replaceable at runtime.

远端动作: dial(number) / hangup() / state()
UI: 号码输入 + Call / Hang up / Refresh；号码按 target 持久化。
"""

import json
import time

import client_service

FEATURE = {
    "name": "dialer",
    "title": "Dialer",
    "version": 1,
    "actions": ["dial", "hangup", "state"],
    "icon": "phone",
    "ui": "python",
}

# 显式超时，符合 §6 的 [1,600] 范围
DIAL_TIMEOUT = 20
HANGUP_TIMEOUT = 15
STATE_TIMEOUT = 10
# placeCall 后等 TelecomManager 状态刷新的秒数
STATE_SETTLE = 2.0


def run():
    return FEATURE


# ---------------------------------------------------------------------------
# 目标设备执行代码（字符串形式，由 client_service.rpc 下发）
# ---------------------------------------------------------------------------

def build_dial_code(number):
    number_literal = json.dumps(str(number))
    return f'''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
TelecomManager = jclass("android.app.TelecomManager")
Uri = jclass("android.net.Uri")
Bundle = jclass("android.os.Bundle")

ctx = ActivityThread.currentApplication()
tm = ctx.getSystemService(ctx.TELECOM_SERVICE)
accounts = tm.getCallCapablePhoneAccounts()
if accounts is None or accounts.size() == 0:
    raise Exception("No call-capable phone accounts")
account = accounts.get(0)

# 已通话时 placeCall 会抛 "already in call"，先自查，报错更清晰
try:
    if tm.getCallState() != TelecomManager.CALL_STATE_IDLE:
        raise Exception("Target is already in a call")
except Exception as e:
    if "already in a call" in repr(e):
        raise

number = {number_literal}
extras = Bundle()
extras.putParcelable(TelecomManager.EXTRA_PHONE_ACCOUNT_HANDLE, account)
tm.placeCall(Uri.fromParts("tel", number, None), extras)

# placeCall 只是"广播出去"，如实说 dispatched，别谎称 in_call
r = json.dumps(
    {{"ok": True, "dispatched": True, "number": number,
      "account_id": account.getId()}},
    ensure_ascii=False,
)
'''


def build_hangup_code():
    """三条路径依次尝试，错误全部收集返回（不再相互覆盖）。"""
    return '''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
ctx = ActivityThread.currentApplication()

ok = False
errors = []

# 路径 1：公开 API（API 28+，需 MODIFY_PHONE_STATE / 系统签名）
try:
    TelecomManager = jclass("android.telecom.TelecomManager")
    tm = ctx.getSystemService(ctx.TELECOM_SERVICE)
    try:
        if tm.getCallState() == TelecomManager.CALL_STATE_IDLE:
            ok = True  # 幂等：本来就没通话，视为成功
    except Exception:
        pass
    if not ok:
        result = tm.getClass().getMethod("endCall").invoke(tm)
        ok = bool(result) if result is not None else True
        if not ok:
            errors.append("TelecomManager.endCall -> false")
except Exception as e:
    errors.append("TelecomManager: " + repr(e))

# 路径 2：隐藏 API ITelephony（注意 $ 内部类名）
if not ok:
    try:
        ServiceManager = jclass("android.os.ServiceManager")
        ITelephonyStub = jclass("com.android.internal.telephony.ITelephony$Stub")
        itel = ITelephonyStub.asInterface(ServiceManager.getService("phone"))
        ok = bool(itel.endCall())
        if not ok:
            errors.append("ITelephony.endCall -> false")
    except Exception as e:
        errors.append("ITelephony: " + repr(e))

# 路径 3：phone 服务反射（旧 ROM 兜底）
if not ok:
    try:
        tm = ctx.getSystemService("phone")
        m = tm.getClass().getDeclaredMethod("endCall")
        m.setAccessible(True)
        result = m.invoke(tm)
        ok = bool(result) if result is not None else True
        if not ok:
            errors.append("phone.endCall -> false")
    except Exception as e:
        errors.append("phone service: " + repr(e))

r = json.dumps(
    {{"ok": ok, "errors": [] if ok else errors}},
    ensure_ascii=False,
)
'''


def build_state_code():
    return '''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
TelecomManager = jclass("android.telecom.TelecomManager")
ctx = ActivityThread.currentApplication()
tm = ctx.getSystemService(ctx.TELECOM_SERVICE)

names = {
    TelecomManager.CALL_STATE_IDLE: "idle",
    TelecomManager.CALL_STATE_RINGING: "ringing",
    TelecomManager.CALL_STATE_OFFHOOK: "offhook",
}
try:
    r = json.dumps({"ok": True, "state": names.get(tm.getCallState(), "unknown")},
                   ensure_ascii=False)
except Exception as e:
    r = json.dumps({"ok": False, "error": repr(e)}, ensure_ascii=False)
'''


# ---------------------------------------------------------------------------
# 远端动作入口（保持 compose 风格：返回 JSON 字符串）
# ---------------------------------------------------------------------------

def dial(number):
    result = client_service.rpc(build_dial_code(str(number)), timeout=DIAL_TIMEOUT)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def hangup():
    result = client_service.rpc(build_hangup_code(), timeout=HANGUP_TIMEOUT)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def state():
    result = client_service.rpc(build_state_code(), timeout=STATE_TIMEOUT)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Chaquopy 自绘 UI（遵循 §4：组件只用 pyui_kit 白名单）
# ---------------------------------------------------------------------------

def build_view(context):
    import bootstrap
    import pyui_kit
    from android.text import InputType
    from android.widget import EditText

    page = pyui_kit.Page(
        context, "Dialer",
        "Place / end calls on the target. Last number per target is remembered."
    )

    # key -> {number, number_loaded, status, active}
    states = {}
    current = {"key": None, "topic": None}

    status = pyui_kit.make_text(context, "Ready")
    page.add(status)

    # §4 白名单：make_edit；退一步如果拿不到默认签名，用 EditText 也须 try 包
    try:
        number_input = pyui_kit.make_edit(context, "Phone number")
    except TypeError:
        number_input = EditText(context)
        number_input.setHint("Phone number")
    try:
        number_input.setInputType(InputType.TYPE_CLASS_PHONE)
        number_input.setSingleLine(True)
    except Exception:
        pass
    page.add(number_input, top=8)

    dial_btn = pyui_kit.make_button(context, "Call", lambda: None)
    hangup_btn = pyui_kit.make_button(context, "Hang up", lambda: None)
    refresh_btn = pyui_kit.make_button(context, "Refresh", lambda: None)
    hangup_btn.setEnabled(False)

    def new_state():
        return {"number": "", "number_loaded": False,
                "status": "Ready", "active": False}

    def state_for(key):
        return states.setdefault(key, new_state())

    def paint():
        # 只刷非输入控件；输入框文本只在 load_number / on_target 时写入，
        # 避免打断用户正在输入的内容（§4 精神）
        st = state_for(current["key"])
        status.setText(st["status"])
        hangup_btn.setEnabled(bool(st["active"]))

    # ------------------------------------------------------------------
    # 号码持久化：惰性加载（参考 feature_camera 的 facing_loaded 模式）
    # ------------------------------------------------------------------
    def load_number(key):
        st = state_for(key)
        if st["number_loaded"]:
            return
        st["number_loaded"] = True

        def work():
            raw = client_service.feature_settings("dialer", key)
            try:
                return str(json.loads(raw).get("number", ""))
            except (TypeError, ValueError):
                return ""

        def apply_ok(value):
            st["number"] = value or ""
            if current["key"] == key:
                try:
                    number_input.setText(st["number"])
                    number_input.setSelection(len(st["number"]))
                except Exception:
                    pass
                paint()

        # run_async 的 on_ok 回主线程；异常由它兜（§4.1）
        pyui_kit.run_async(work, apply_ok)

    def save_number(key, number):
        def work():
            client_service.update_feature_settings(
                "dialer", json.dumps({"number": number}), key
            )
        pyui_kit.run_async(work)

    # ------------------------------------------------------------------
    # Call
    # ------------------------------------------------------------------
    def on_dial():
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        number = number_input.getText().toString().strip()
        if not number:
            st["status"] = "Enter a number first"
            paint()
            return

        st["number"] = number
        st["status"] = "Dialing %s ..." % number
        paint()
        save_number(key, number)

        def work():
            raw = bootstrap.call_feature("dialer", "dial", number)
            try:
                dial_result = json.loads(raw)
            except Exception:
                return (pyui_kit.render_result(raw), False)
            if not dial_result.get("ok"):
                return (pyui_kit.render_result(raw), False)

            # placeCall 异步，等 TelecomManager 状态刷新再核对
            time.sleep(STATE_SETTLE)
            state_raw = bootstrap.call_feature("dialer", "state")
            try:
                state_result = json.loads(state_raw)
            except Exception:
                state_result = {}
            active = state_result.get("state") in ("offhook", "ringing")
            if active:
                return ("In call: %s" % number, True)
            # 已下发但状态未刷新，如实显示，让用户按 Refresh 复核
            return ("Dispatched %s (state: %s)" % (
                number, state_result.get("state", "unknown")), True)

        def apply_ok(payload):
            # payload 是 (status_text, active) 元组
            st["status"], st["active"] = payload
            paint()

        def apply_error(error):
            st["active"] = False
            st["status"] = "Dial failed: %s" % error
            paint()

        pyui_kit.run_async(
            work, apply_ok, apply_error,
            buttons=(dial_btn, hangup_btn, refresh_btn),
        )

    # ------------------------------------------------------------------
    # Hang up
    # ------------------------------------------------------------------
    def on_hangup():
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["status"] = "Hanging up ..."
        paint()

        def work():
            return bootstrap.call_feature("dialer", "hangup")

        def apply_ok(raw):
            try:
                result = json.loads(raw)
            except Exception:
                result = {"ok": False, "error": raw}
            if result.get("ok"):
                st["active"] = False
                st["status"] = "Call ended"
            else:
                details = result.get("errors") or [result.get("error", "?")]
                st["status"] = "Hang up failed: %s" % "; ".join(map(str, details))
            paint()

        def apply_error(error):
            st["status"] = "Hang up failed: %s" % error
            paint()

        pyui_kit.run_async(
            work, apply_ok, apply_error,
            buttons=(dial_btn, hangup_btn, refresh_btn),
        )

    # ------------------------------------------------------------------
    # Refresh（从设备端拉真实状态）
    # ------------------------------------------------------------------
    def on_refresh():
        key = current["key"]
        if not key:
            return
        st = state_for(key)

        def work():
            return bootstrap.call_feature("dialer", "state")

        def apply_ok(raw):
            try:
                result = json.loads(raw)
            except Exception:
                result = {"ok": False}
            if result.get("ok"):
                state_name = result.get("state", "unknown")
                st["active"] = state_name in ("offhook", "ringing")
                st["status"] = "State: %s" % state_name
            else:
                st["status"] = "State query failed"
            paint()

        def apply_error(error):
            st["status"] = "State query failed: %s" % error
            paint()

        pyui_kit.run_async(
            work, apply_ok, apply_error,
            buttons=(dial_btn, hangup_btn, refresh_btn),
        )

    pyui_kit.click(dial_btn, on_dial)
    pyui_kit.click(hangup_btn, on_hangup)
    pyui_kit.click(refresh_btn, on_refresh)

    page.bottom_add(refresh_btn)
    page.bottom_add(hangup_btn)
    page.bottom_add(dial_btn, weight=1.0)

    # ------------------------------------------------------------------
    # 目标切换
    # ------------------------------------------------------------------
    def on_target(key, topic, _cfg):
        current["key"] = key
        current["topic"] = topic
        page.set_target("target topic: " + topic)
        st = state_for(key)
        # 切目标时清空输入框，若本地已有号码则填回
        try:
            number_input.setText(st["number"] if st["number_loaded"] else "")
            if st["number"]:
                number_input.setSelection(len(st["number"]))
        except Exception:
            pass
        load_number(key)
        paint()

    pyui_kit.watch_target(page.root, on_target)
    return page.root