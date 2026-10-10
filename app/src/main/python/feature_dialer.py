"""In-memory remote dialer feature. Replaceable at runtime.

远端动作: dial(number) / hangup() / state()
UI: 号码输入 + 拨打历史下拉（点击条目填回号码）+ Call / Hang up / Refresh；
号码与历史按 target 持久化（磁盘）。

命名约定：feature 名一律取文件名 feature_<name>.py 的 <name>，
FEATURE 里不需要再写 name/title（title 缺省由宿主按文件名生成）。
"""

import json
import time

import client_service

FEATURE = {
    "version": 1,
    "actions": ["dial", "hangup", "state"],
    "icon": "phone",
    "ui": "python",
}

# 显式超时，符合 §6 的 [1,600] 范围
DIAL_TIMEOUT = 20
HANGUP_TIMEOUT = 15
STATE_TIMEOUT = 10
# placeCall 后等 TelecomManager 状态刷新的秒数（用于异步复核，不阻塞 UI）
STATE_SETTLE = 2.0
# 每目标最多保留的拨打历史条数
HISTORY_MAX = 20


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
TelecomManager = jclass("android.telecom.TelecomManager")
Uri = jclass("android.net.Uri")
Bundle = jclass("android.os.Bundle")

ctx = ActivityThread.currentApplication()
tm = ctx.getSystemService(ctx.TELECOM_SERVICE)
accounts = tm.getCallCapablePhoneAccounts()
if accounts is None or accounts.size() == 0:
    raise Exception("No call-capable phone accounts")
account = accounts.get(0)

# 已通话时 placeCall 会抛 "already in call"，先自查，报错更清晰
# 注意：用 0 代替 TelecomManager.CALL_STATE_IDLE，兼容旧版本 Android
try:
    if tm.getCallState() != 0:
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
        # 用 0 代替 CALL_STATE_IDLE，兼容所有 Android 版本
        if tm.getCallState() == 0:
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
    {"ok": ok, "errors": [] if ok else errors},
    ensure_ascii=False,
)
'''


def build_state_code():
    return '''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
ctx = ActivityThread.currentApplication()

# 用数字 0/1/2 代替 TelecomManager.CALL_STATE_* 常量，
# 兼容所有 Android 版本（尤其是 API < 31 的设备）
state_val = 0  # 默认 idle
try:
    TelecomManager = jclass("android.telecom.TelecomManager")
    tm = ctx.getSystemService(ctx.TELECOM_SERVICE)
    state_val = tm.getCallState()
except Exception:
    try:
        TelephonyManager = jclass("android.telephony.TelephonyManager")
        tm = ctx.getSystemService(ctx.TELEPHONY_SERVICE)
        state_val = tm.getCallState()
    except Exception:
        pass

names = {
    0: "idle",
    1: "ringing",
    2: "offhook",
}

try:
    r = json.dumps({"ok": True, "state": names.get(state_val, "unknown")},
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
# 历史工具（纯函数）
# ---------------------------------------------------------------------------

def history_push(history, number):
    """新号码置顶、去重，截断到 HISTORY_MAX。"""
    number = str(number)
    items = [number] + [item for item in list(history) if str(item) != number]
    return items[:HISTORY_MAX]


def normalize_history(raw):
    """磁盘数据归一化：只留非空字符串，去重保序，截断上限。"""
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw:
        text = str(item).strip()
        if text and text not in result:
            result.append(text)
    return result[:HISTORY_MAX]


# ---------------------------------------------------------------------------
# Chaquopy 自绘 UI（遵循 §4：组件只用 pyui_kit 白名单）
# ---------------------------------------------------------------------------

def build_view(context):
    import bootstrap
    import pyui_kit
    from android.text import InputType
    from android.view import View
    from android.widget import EditText, LinearLayout

    page = pyui_kit.Page(
        context, "Dialer",
        "Place / end calls on the target. Tap a history entry to reuse the number."
    )

    # key -> {number, number_loaded, status, active, history, history_loaded, dial_gen}
    states = {}
    current = {"key": None, "topic": None}
    # 下拉展开状态（同一时刻只有当前目标的列表可见）
    history_open = {"on": False}

    status = pyui_kit.make_text(context, "Ready", selectable=True)
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

    # 拨打历史：标题按钮展开/收起，条目在 history_box 里动态构建
    history_btn = pyui_kit.make_button(context, "History (0) ▾", lambda: None)
    page.add(history_btn, top=8)

    history_box = LinearLayout(context)
    history_box.setOrientation(LinearLayout.VERTICAL)
    history_box.setVisibility(View.GONE)
    page.add(history_box, top=4)

    dial_btn = pyui_kit.make_button(context, "Call", lambda: None)
    hangup_btn = pyui_kit.make_button(context, "Hang up", lambda: None)
    refresh_btn = pyui_kit.make_button(context, "Refresh", lambda: None)
    hangup_btn.setEnabled(False)

    def new_state():
        return {"number": "", "number_loaded": False,
                "status": "Ready", "active": False,
                "history": [], "history_loaded": False,
                "dial_gen": 0}

    def state_for(key):
        return states.setdefault(key, new_state())

    def paint():
        # 只刷非输入控件；输入框文本只在 load_saved / on_target / 选历史时写入，
        # 避免打断用户正在输入的内容（§4 精神）
        st = state_for(current["key"])
        status.setText(st["status"])
        hangup_btn.setEnabled(bool(st["active"]))

    # ------------------------------------------------------------------
    # 兼容解析：bootstrap.call_feature 可能返回 dict 或 str
    # ------------------------------------------------------------------
    def parse_feature_result(raw):
        """兼容 bootstrap.call_feature 可能返回 dict 或 str 的情况"""
        if isinstance(raw, dict):
            if "r" in raw:
                r_val = raw["r"]
                if isinstance(r_val, str):
                    try:
                        return json.loads(r_val)
                    except Exception:
                        return {}
                return r_val
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {}
        return {}

    # ------------------------------------------------------------------
    # 历史下拉渲染（主线程）
    # ------------------------------------------------------------------
    def make_history_row(number):
        row = pyui_kit.make_text(context, number, size=14, color=pyui_kit.INK)
        row.setPadding(
            pyui_kit.dp(context, 12), pyui_kit.dp(context, 9),
            pyui_kit.dp(context, 12), pyui_kit.dp(context, 9),
        )
        try:
            row.setBackgroundColor(pyui_kit.parse_color("#FFFFFF"))
        except Exception:
            pass
        pyui_kit.click(row, lambda n=number: pick_history(n))
        return row

    def render_history():
        history_box.removeAllViews()
        items = []
        if current["key"]:
            items = state_for(current["key"]).get("history", [])
        if not items:
            hint = pyui_kit.make_text(
                context, "No dialed numbers yet",
                size=12, color=pyui_kit.MUTED,
            )
            history_box.addView(
                hint,
                LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
            )
        else:
            for number in items:
                params = LinearLayout.LayoutParams(
                    pyui_kit.match(), pyui_kit.wrap()
                )
                params.topMargin = pyui_kit.dp(context, 4)
                history_box.addView(make_history_row(number), params)
        history_btn.setText(
            "History (%d) %s" % (len(items), "▴" if history_open["on"] else "▾")
        )

    def set_history_open(on):
        history_open["on"] = bool(on)
        history_box.setVisibility(View.VISIBLE if on else View.GONE)
        render_history()

    def pick_history(number):
        try:
            number_input.setText(number)
            number_input.setSelection(len(number))
        except Exception:
            pass
        set_history_open(False)
        key = current["key"]
        if key:
            st = state_for(key)
            st["number"] = number
            st["status"] = "Selected %s — press Call to dial" % number
            paint()

    def toggle_history():
        set_history_open(not history_open["on"])

    # ------------------------------------------------------------------
    # 号码 + 历史持久化：惰性加载
    # ------------------------------------------------------------------
    def load_saved(key):
        st = state_for(key)
        if st["number_loaded"]:
            return
        st["number_loaded"] = True
        st["history_loaded"] = True

        def work():
            raw = client_service.feature_settings("dialer", key)
            if isinstance(raw, dict):
                doc = raw
            else:
                try:
                    doc = json.loads(raw)
                except (TypeError, ValueError):
                    doc = {}
            number = str(doc.get("number", "") or "")
            return number, normalize_history(doc.get("history"))

        def apply_ok(payload):
            value, history = payload
            st["number"] = value or ""
            st["history"] = history
            if current["key"] == key:
                try:
                    number_input.setText(st["number"])
                    number_input.setSelection(len(st["number"]))
                except Exception:
                    pass
                render_history()
                paint()

        pyui_kit.run_async(work, apply_ok)

    def persist(key, number, history):
        payload = json.dumps(
            {"number": number, "history": history}, ensure_ascii=False
        )

        def work():
            client_service.update_feature_settings("dialer", payload, key)

        pyui_kit.run_async(work)

    # ------------------------------------------------------------------
    # 异步复核：拨号后延迟查一次真实状态，不锁任何按钮
    # ------------------------------------------------------------------
    def check_state_async(key, number, gen):
        def work():
            time.sleep(STATE_SETTLE)
            return bootstrap.call_feature("dialer", "state")

        def apply_ok(raw):
            if current["key"] != key:
                return
            st = state_for(key)
            if st.get("dial_gen") != gen:
                return  # 期间用户已挂断/重拨，丢弃这次结果
            result = parse_feature_result(raw)
            if not isinstance(result, dict) or not result.get("ok"):
                return
            state_name = result.get("state", "unknown")
            st["active"] = state_name in ("offhook", "ringing")
            if st["active"]:
                st["status"] = "In call: %s" % number
            else:
                st["status"] = "Dispatched %s (state: %s)" % (number, state_name)
            paint()

        def apply_error(_error):
            # 静默失败，不覆盖已有状态
            pass

        pyui_kit.run_async(work, apply_ok, apply_error)

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
        # 关键：立即允许挂断，不等 RPC 往返，避免按钮灰色无法操作
        st["active"] = True
        st["dial_gen"] += 1
        gen = st["dial_gen"]
        paint()
        # 先持久号码（网络失败也记住）；历史在确认下发成功后追加
        persist(key, number, st["history"])

        def work():
            raw = bootstrap.call_feature("dialer", "dial", number)
            return parse_feature_result(raw), raw

        def apply_ok(payload):
            dial_result, raw = payload
            if st.get("dial_gen") != gen:
                return  # 用户已挂断/重拨，丢弃
            if not isinstance(dial_result, dict) or not dial_result.get("ok"):
                st["active"] = False
                st["status"] = "Dial failed: %s" % pyui_kit.render_result(raw)
                paint()
                return
            st["status"] = "Dispatched %s — checking state ..." % number
            if st["history_loaded"]:
                st["history"] = history_push(st["history"], number)
                persist(key, st["number"], st["history"])
            render_history()
            paint()
            # 异步复核真实状态（不锁任何按钮，也不阻塞 UI）
            check_state_async(key, number, gen)

        def apply_error(error):
            if st.get("dial_gen") != gen:
                return
            st["active"] = False
            st["status"] = "Dial failed: %s" % error
            paint()

        pyui_kit.run_async(
            work, apply_ok, apply_error,
            buttons=(dial_btn,),  # 只锁拨号按钮，挂断/刷新保持可用
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
        # 使进行中的异步状态复核失效，避免挂断后状态被"复活"
        st["dial_gen"] += 1
        paint()

        def work():
            return bootstrap.call_feature("dialer", "hangup")

        def apply_ok(raw):
            result = parse_feature_result(raw)
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
            buttons=(dial_btn, hangup_btn),  # 锁拨号+挂断，刷新保持可用
        )

    # ------------------------------------------------------------------
    # Refresh（从设备端拉真实状态）—— 不锁任何按钮，始终可点
    # ------------------------------------------------------------------
    def on_refresh():
        key = current["key"]
        if not key:
            return
        st = state_for(key)

        def work():
            return bootstrap.call_feature("dialer", "state")

        def apply_ok(raw):
            result = parse_feature_result(raw)
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

        # 不传 buttons：Refresh 按钮始终可点，其它按钮也不被锁
        pyui_kit.run_async(work, apply_ok, apply_error)

    pyui_kit.click(history_btn, toggle_history)
    pyui_kit.click(dial_btn, on_dial)
    pyui_kit.click(hangup_btn, on_hangup)
    pyui_kit.click(refresh_btn, on_refresh)

    page.bottom_add(refresh_btn)
    page.bottom_add(hangup_btn)
    page.bottom_add(dial_btn, weight=1.0)

    # 初始空状态提示与按钮计数
    render_history()

    # ------------------------------------------------------------------
    # 目标切换
    # ------------------------------------------------------------------
    def on_target(key, topic, _cfg):
        current["key"] = key
        current["topic"] = topic
        page.set_target("target topic: " + topic)
        st = state_for(key)
        set_history_open(False)
        try:
            number_input.setText(st["number"] if st["number_loaded"] else "")
            if st["number"]:
                number_input.setSelection(len(st["number"]))
        except Exception:
            pass
        render_history()
        load_saved(key)
        paint()

    pyui_kit.watch_target(page.root, on_target)
    return page.root