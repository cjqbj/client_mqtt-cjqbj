"""目标端锁屏 feature。

在【当前选中目标】上创建一个全屏悬浮窗，拦截应用区域的触摸并显示提示文字，
同时动态显示当前时间（时:分:秒）；到点自动解锁，也可以手动 unlock。

所有参数（提示文字 / 锁屏时长 / 每日定时 / 开机锁屏）按 target 持久化：
输入框每次编辑立即保存，切换目标后自动加载该目标上次的值。

每日定时与开机锁屏依赖目标端 task_manager（xime 新版内置）。feature 进入页面
会先探测目标环境：旧版本没有 task_manager 时只保留手动 LOCK/UNLOCK，并给出
明确提示，不会报错崩溃。

依赖目标为 com.qgb.xime（Chaquopy）且已授予悬浮窗权限（SYSTEM_ALERT_WINDOW）。
"""
import json
import time

import client_service

FEATURE = {
    "name": "lockscreen",
    "title": "Lock Screen",
    "version": 3,
    "actions": [
        "lock", "unlock", "status",
        "probe", "schedule", "unschedule",
    ],
    "ui": "python",
    "icon": "lock",
    # 可调参数（长按 feature → Setting 的独立设置页，不占主界面）。
    "settings": [
        {"key": "timeout", "label": "RPC 超时（秒）", "type": "int",
         "default": None, "min": 1, "max": 600, "scope": "topic",
         "help": "留空 = 各动作 30s；填了则锁屏/解锁/状态查询统一用该值。"},
    ],
}

DEFAULT_TEXT = """因为你长时间没有敬礼， 屏幕已锁定


解锁方法 3选1 ：

1. 立马起身 拿我妈手机拍照敬礼图片发过来

2. 你可以外卖下单送点菜过来

3. 去附近废品站看看有没有报废车方向盘。装我那车上
"""
DEFAULT_SECONDS = "3600"
_SECONDS_MIN = 0.0
_SECONDS_MAX = 86400.0

# 目标端 task_manager 任务 id（固定，便于覆盖/停止）。
TASK_LOCK_ID = "lockscreen.daily_lock"
TASK_UNLOCK_ID = "lockscreen.daily_unlock"
TASK_BOOT_ID = "lockscreen.boot_lock"
_TASK_IDS = (TASK_LOCK_ID, TASK_UNLOCK_ID, TASK_BOOT_ID)

# 每日/开机触发的去重 token 在目标端运行时生成：每天（每天每分钟）唯一，
# 既能让 MQTT 重传的同一分钟指令保持幂等，又不会让第二天被误判为重复。
_DAILY_LOCK_TOKEN = "time.strftime('daily-lock-%Y-%m-%d %H:%M')"
_BOOT_LOCK_TOKEN = "time.strftime('boot-lock-%Y-%m-%d %H:%M')"

# 目标端执行代码。代理类全进程只建一次（Chaquopy 同一个 Java 接口只能
# dynamic_proxy 出一个类，重复建会让回调失效）。状态挂在 sys 上，跨 RPC 保留。
LOCK_CODE = r'''import json, sys, threading, time, traceback
from java import dynamic_proxy, jclass
from com.chaquo.python import Python
from android.graphics import Color, PixelFormat, Typeface
from android.os import Handler, Looper
from android.provider import Settings
from android.view import Gravity
from android.widget import LinearLayout, TextView

ctx = Python.getPlatform().getApplication()
wm = ctx.getSystemService(ctx.WINDOW_SERVICE)
main_handler = Handler(Looper.getMainLooper())

if not hasattr(sys, "_qgb_lock_runnable_cls"):
    class LockRunnable(dynamic_proxy(jclass("java.lang.Runnable"))):
        def __init__(self):
            super().__init__()
            self.fn = None

        def run(self):
            fn = self.fn
            if fn is None:
                return
            try:
                fn()
            except Exception:
                traceback.print_exc()
    sys._qgb_lock_runnable_cls = LockRunnable
LockRunnable = sys._qgb_lock_runnable_cls

if not hasattr(sys, "_qgb_lock_touch_cls"):
    class LockTouch(dynamic_proxy(jclass("android.view.View$OnTouchListener"))):
        def onTouch(self, view, event):
            return True
    sys._qgb_lock_touch_cls = LockTouch
LockTouch = sys._qgb_lock_touch_cls

state = sys.__dict__.setdefault("_qgb_lock_state", {})
state.setdefault("overlay", None)
state.setdefault("clock", None)
state.setdefault("touch", None)
state.setdefault("tick", None)
state.setdefault("timer", None)
state.setdefault("deadline", None)
state.setdefault("message", "")
state.setdefault("lock_token", None)
state.setdefault("lock_count", 0)


def _run_on_main(fn, timeout=5.0):
    done = threading.Event()
    job = {"fn": fn, "result": None, "error": None, "done": done}

    def call():
        try:
            job["result"] = job["fn"]()
        except Exception:
            job["error"] = traceback.format_exc()
        finally:
            job["done"].set()

    runnable = LockRunnable()
    runnable.fn = call
    main_handler.post(runnable)
    if not done.wait(timeout):
        raise RuntimeError("wait android main thread timeout")
    if job["error"]:
        raise RuntimeError(job["error"])
    return job["result"]


def _stop_clock():
    tick = state.get("tick")
    if tick is not None:
        try:
            main_handler.removeCallbacks(tick)
        except Exception:
            pass
    state["tick"] = None


def _start_clock():
    _stop_clock()
    tick = LockRunnable()

    def beat():
        overlay = state.get("overlay")
        clock_view = state.get("clock")
        if overlay is None or clock_view is None:
            return
        try:
            if not overlay.isAttachedToWindow():
                return
            clock_view.setText(time.strftime("%H:%M:%S"))
        except Exception:
            traceback.print_exc()
            return
        main_handler.postDelayed(tick, 1000)

    tick.fn = beat
    state["tick"] = tick
    clock_view = state.get("clock")
    if clock_view is not None:
        try:
            clock_view.setText(time.strftime("%H:%M:%S"))
        except Exception:
            pass
    main_handler.postDelayed(tick, 1000)


def _cancel_auto_unlock():
    timer = state.get("timer")
    if timer is not None:
        try:
            timer.cancel()
        except Exception:
            pass
    state["timer"] = None
    state["deadline"] = None


def _remove_overlay():
    overlay = state.get("overlay")
    if overlay is None:
        return False
    _stop_clock()
    try:
        wm.removeViewImmediate(overlay)
    except Exception:
        try:
            wm.removeView(overlay)
        except Exception:
            pass
    state["overlay"] = None
    state["clock"] = None
    state["touch"] = None
    return True


def _lock(text, timeout_s, token=None):
    if not Settings.canDrawOverlays(ctx):
        return {"ok": False, "error": "overlay_permission_denied"}

    # 去重：MQTT 重传会把同一条 lock 指令再执行一遍（token 相同）。不拦的话
    # 自动解锁倒计时会被反复推后、永远不解锁。
    if token is not None and state.get("lock_token") == token:
        return {
            "ok": True,
            "locked": state.get("overlay") is not None,
            "text": text,
            "clock": time.strftime("%H:%M:%S"),
            "auto_unlock_seconds": None,
            "duplicate": True,
            "lock_count": state.get("lock_count", 0),
        }
    state["lock_token"] = token

    # 幂等：已经锁屏且提示文字未变时，重复投递（连点）不重建悬浮窗、
    # 也不重置自动解锁倒计时。
    existing = state.get("overlay")
    if existing is not None:
        try:
            attached = bool(existing.isAttachedToWindow())
        except Exception:
            attached = False
        if attached and state.get("message") == text:
            return {
                "ok": True,
                "locked": True,
                "text": text,
                "clock": time.strftime("%H:%M:%S"),
                "auto_unlock_seconds": None,
                "already_locked": True,
                "lock_count": state.get("lock_count", 0),
            }

    _cancel_auto_unlock()
    _remove_overlay()
    state["lock_count"] = state.get("lock_count", 0) + 1
    state["message"] = text

    box = LinearLayout(ctx)
    box.setOrientation(LinearLayout.VERTICAL)
    box.setGravity(Gravity.CENTER)
    box.setBackgroundColor(Color.argb(235, 0, 0, 0))

    message = TextView(ctx)
    message.setText(text)
    message.setTextColor(Color.WHITE)
    message.setTextSize(22.0)
    message.setGravity(Gravity.CENTER)
    message.setPadding(60, 0, 60, 28)

    clock = TextView(ctx)
    clock.setTextColor(Color.WHITE)
    clock.setTextSize(46.0)
    clock.setTypeface(Typeface.DEFAULT, Typeface.BOLD)
    clock.setGravity(Gravity.CENTER)
    clock.setText(time.strftime("%H:%M:%S"))

    wrap = LinearLayout.LayoutParams(-1, -2)
    box.addView(message, wrap)
    box.addView(clock, wrap)

    touch = LockTouch()
    box.setOnTouchListener(touch)

    params = jclass("android.view.WindowManager$LayoutParams")(
        -1, -1, 2038, 0x00000100 | 0x00000200, PixelFormat.TRANSLUCENT,
    )
    params.gravity = Gravity.TOP | Gravity.LEFT
    params.x = 0
    params.y = 0
    params.setTitle("xime-lock-screen")

    wm.addView(box, params)
    state["overlay"] = box
    state["clock"] = clock
    state["touch"] = touch
    _start_clock()

    deadline = None
    if timeout_s > 0:
        viewing = {"done": False}

        def fire_auto_unlock():
            def remove():
                if viewing["done"]:
                    return
                viewing["done"] = True
                if state.get("overlay") is box:
                    _remove_overlay()
                if state.get("timer") is timer:
                    state["timer"] = None
                    state["deadline"] = None

            runner = LockRunnable()
            runner.fn = remove
            try:
                main_handler.post(runner)
            except Exception:
                traceback.print_exc()

        timer = threading.Timer(timeout_s, fire_auto_unlock)
        timer.daemon = True
        state["timer"] = timer
        deadline = time.monotonic() + timeout_s
        state["deadline"] = deadline
        timer.start()

    return {
        "ok": True,
        "locked": True,
        "text": text,
        "clock": time.strftime("%H:%M:%S"),
        "auto_unlock_seconds": timeout_s if timeout_s > 0 else None,
    }


def _unlock():
    _cancel_auto_unlock()
    removed = _remove_overlay()
    return {"ok": True, "unlocked": True, "removed": removed}


def _status():
    overlay = state.get("overlay")
    attached = False
    if overlay is not None:
        try:
            attached = bool(overlay.isAttachedToWindow())
        except Exception:
            attached = False
    deadline = state.get("deadline")
    remaining = None
    if deadline is not None:
        remaining = max(0.0, round(deadline - time.monotonic(), 1))
    return {
        "ok": True,
        "locked": overlay is not None,
        "attached": attached,
        "overlay_permission": bool(Settings.canDrawOverlays(ctx)),
        "auto_unlock_in_seconds": remaining,
        "clock": time.strftime("%H:%M:%S"),
        "lock_count": state.get("lock_count", 0),
    }


def _qgb_lock_run(action, text, timeout_s, token=None):
    try:
        if action == "lock":
            result = _run_on_main(lambda: _lock(text, timeout_s, token))
        elif action == "unlock":
            result = _run_on_main(_unlock)
        elif action == "status":
            result = _run_on_main(_status)
        else:
            result = {"ok": False, "error": "unknown_action:%s" % action}
    except Exception:
        result = {"ok": False, "error": traceback.format_exc()}
    return json.dumps(result, ensure_ascii=False)
'''

# 目标端环境探测：task_manager 是否可用、API 是否齐全、锁屏引擎与悬浮窗权限。
PROBE_CODE = r'''import json
out = {"ok": True}
try:
    import task_manager
    out["task_manager"] = True
    out["api"] = {
        "add_daily_task": hasattr(task_manager, "add_daily_task"),
        "add_event_task": hasattr(task_manager, "add_event_task"),
        "remove_task": hasattr(task_manager, "remove_task"),
    }
    tasks = task_manager.list_tasks()
    out["tasks"] = {}
    for tid, t in tasks.items():
        if not str(tid).startswith("lockscreen."):
            continue
        out["tasks"][tid] = {
            "kind": t.get("kind"),
            "at": t.get("at"),
            "event": t.get("event"),
            "enabled": t.get("enabled", True),
            "run_count": t.get("run_count", 0),
            "last_run": t.get("last_run"),
            "last_result": t.get("last_result"),
        }
except Exception as error:
    out["task_manager"] = False
    out["task_manager_error"] = repr(error)
try:
    from com.chaquo.python import Python
    from android.provider import Settings
    ctx = Python.getPlatform().getApplication()
    out["engine"] = True
    out["overlay_permission"] = bool(Settings.canDrawOverlays(ctx))
except Exception as error:
    out["engine"] = False
    out["engine_error"] = repr(error)
try:
    from com.kingzcheung.xime.util import RpcUiController
    info = json.loads(RpcUiController.buildInfo())
    out["version_name"] = info.get("version_name")
    out["version_code"] = info.get("version_code")
    out["git_hash"] = info.get("git_hash")
except Exception:
    out["version_name"] = None
r = json.dumps(out, ensure_ascii=False)
'''

UNSCHEDULE_CODE = r'''import json
r = {"ok": True}
try:
    import task_manager
except Exception as error:
    r = {"ok": False, "error": "task_manager 不可用: %r" % error}
else:
    removed = {}
    for tid in ("lockscreen.daily_lock", "lockscreen.daily_unlock", "lockscreen.boot_lock"):
        removed[tid] = task_manager.remove_task(tid)
    r = {"ok": True,
         "removed": removed,
         "tasks": sorted(task_manager.list_tasks().keys())}
r = json.dumps(r, ensure_ascii=False)
'''


def _normalize_seconds(value):
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        seconds = float(DEFAULT_SECONDS)
    return min(max(seconds, _SECONDS_MIN), _SECONDS_MAX)


def _exec_target(code, timeout=30):
    # 设置页配了 lockscreen.timeout 就优先用它，否则用各动作的默认值。
    result = client_service.rpc(
        code, timeout=client_service.feature_timeout("lockscreen", timeout)
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def _lock_task_code(text, token_expr):
    """「完整锁屏引擎 + 一次 lock」源码，供 task_manager 任务长期保存执行。"""
    return LOCK_CODE + "\nprint(_qgb_lock_run('lock', %s, %r, %s))\n" % (
        json.dumps(str(text if text is not None else DEFAULT_TEXT), ensure_ascii=False),
        0.0,
        token_expr,
    )


def _unlock_task_code():
    return LOCK_CODE + "\nprint(_qgb_lock_run('unlock', None, None, None))\n"


def _normalize_at(value):
    """接受 HH:MM / HH:MM:SS / HHMM（如 2300）；空串原样返回。"""
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.isdigit() and len(raw) in (3, 4):
        raw = raw.zfill(4)
        return raw[:2] + ":" + raw[2:]
    return raw


def _valid_at(value):
    value = str(value or "").strip()
    if not value:
        return True
    parts = value.split(":")
    if len(parts) not in (2, 3):
        return False
    try:
        numbers = [int(part) for part in parts]
    except ValueError:
        return False
    hour, minute = numbers[0], numbers[1]
    second = numbers[2] if len(numbers) == 3 else 0
    return 0 <= hour < 24 and 0 <= minute < 60 and 0 <= second < 60


def _build_schedule_code(lock_at, unlock_at, boot_lock, text):
    """生成目标端注册代码：先清旧任务，再按需注册每日锁/每日解锁/开机锁。"""
    lines = [
        "import json",
        "import task_manager",
        "res = {'ok': True, 'task_manager': True}",
    ]
    for task_id in _TASK_IDS:
        lines.append("task_manager.remove_task(%r)" % task_id)
    if lock_at:
        lines.append(
            "res['daily_lock'] = task_manager.add_daily_task(%r, %r, code=%r)"
            % (TASK_LOCK_ID, lock_at, _lock_task_code(text, _DAILY_LOCK_TOKEN))
        )
    if unlock_at:
        lines.append(
            "res['daily_unlock'] = task_manager.add_daily_task(%r, %r, code=%r)"
            % (TASK_UNLOCK_ID, unlock_at, _unlock_task_code())
        )
    if boot_lock:
        lines.append(
            "res['boot_lock'] = task_manager.add_event_task(%r, 'boot_completed', code=%r)"
            % (TASK_BOOT_ID, _lock_task_code(text, _BOOT_LOCK_TOKEN))
        )
    lines.append("res['tasks'] = sorted(task_manager.list_tasks().keys())")
    lines.append("r = json.dumps(res, ensure_ascii=False)")
    return "\n".join(lines)


def _run_action(action, text=None, timeout=None):
    if action == "lock":
        code = LOCK_CODE + "\nr = _qgb_lock_run('lock', %s, %r, %r)\n" % (
            json.dumps(str(text if text is not None else DEFAULT_TEXT), ensure_ascii=False),
            float(_normalize_seconds(timeout)),
            "%.6f" % time.time(),
        )
    else:
        code = LOCK_CODE + "\nr = _qgb_lock_run(%r, None, None, None)\n" % action
    return _exec_target(code)


def lock(text=None, timeout=None):
    return _run_action("lock", text, timeout)


def unlock():
    return _run_action("unlock")


def status():
    return _run_action("status")


def probe():
    """探测目标环境；旧版本没有 task_manager 时返回可用性说明而非异常。"""
    return _exec_target(PROBE_CODE)


def schedule(lock_at=None, unlock_at=None, boot_lock=False, text=None):
    """注册/覆盖每日锁定、每日解锁、开机自动锁屏任务。空时间表示不注册。"""
    lock_at = _normalize_at(lock_at)
    unlock_at = _normalize_at(unlock_at)
    if not _valid_at(lock_at):
        return json.dumps({"ok": False, "error": "每日锁定时间格式应为 HH:MM",
                           "at": lock_at}, ensure_ascii=False)
    if not _valid_at(unlock_at):
        return json.dumps({"ok": False, "error": "每日解锁时间格式应为 HH:MM",
                           "at": unlock_at}, ensure_ascii=False)
    if not lock_at and not unlock_at and not boot_lock:
        return json.dumps({"ok": False, "error": "没有可应用的定时项（锁定/解锁/开机均为空）"},
                          ensure_ascii=False)
    code = _build_schedule_code(lock_at, unlock_at, bool(boot_lock), text)
    return _exec_target(code)


def unschedule():
    """停止并删除本 feature 注册的全部定时/开机任务。"""
    return _exec_target(UNSCHEDULE_CODE)


def build_view(context):
    """自绘：提示文字 / 锁屏时长 / 每日定时 / 操作按钮 / 统一 RPC 结果区。"""
    import bootstrap
    import pyui_kit

    from android.widget import LinearLayout

    page = pyui_kit.Page(
        context, "Lock Screen", "Full-screen overlay on the target with live clock"
    )
    page.add(
        pyui_kit.make_text(
            context,
            "提示文字和锁屏时长按 target 保存，编辑即刻生效；0 秒表示不自动解锁。",
            size=12,
            color=pyui_kit.MUTED,
        )
    )

    message_label = pyui_kit.make_text(context, "提示文字", size=12, color=pyui_kit.MUTED)
    page.add(message_label, top=8)
    message_edit = pyui_kit.make_text_edit(context, DEFAULT_TEXT, lines=4)
    page.add(message_edit, top=4)

    seconds_label = pyui_kit.make_text(context, "锁屏时长（秒）", size=12, color=pyui_kit.MUTED)
    page.add(seconds_label, top=8)
    seconds_edit = pyui_kit.make_edit(context, DEFAULT_SECONDS, number=True)
    seconds_edit.setSingleLine(True)
    seconds_edit.setHint("0 = never auto unlock")
    page.add(seconds_edit, top=4)

    page.add(
        pyui_kit.make_text(
            context,
            "每日定时依赖目标端 task_manager（新版内置）；旧版本不支持时只能手动 LOCK/UNLOCK。"
            "修改提示文字或时间后需重新 APPLY SCHEDULE。",
            size=12,
            color=pyui_kit.MUTED,
        ),
        top=12,
    )

    page.add(
        pyui_kit.make_text(context, "每日锁定时间（HH:MM 或 HHMM，空=关闭）", size=12,
                           color=pyui_kit.MUTED),
        top=8,
    )
    lock_at_edit = pyui_kit.make_edit(context, "")
    lock_at_edit.setSingleLine(True)
    lock_at_edit.setHint("e.g. 23:00 / 2300")
    page.add(lock_at_edit, top=4)

    page.add(
        pyui_kit.make_text(context, "每日解锁时间（HH:MM 或 HHMM，空=关闭）", size=12,
                           color=pyui_kit.MUTED),
        top=8,
    )
    unlock_at_edit = pyui_kit.make_edit(context, "")
    unlock_at_edit.setSingleLine(True)
    unlock_at_edit.setHint("e.g. 07:00 / 0700")
    page.add(unlock_at_edit, top=4)

    boot_btn = pyui_kit.make_button(context, "开机自动锁屏：关", lambda: None)
    page.add(boot_btn, top=8)

    schedule_row = pyui_kit.make_hrow(context)
    apply_btn = pyui_kit.make_button(context, "APPLY SCHEDULE", lambda: None)
    stop_btn = pyui_kit.make_button(context, "STOP SCHEDULE", lambda: None)
    row_margin = pyui_kit.dp(context, 4)
    for schedule_btn in (apply_btn, stop_btn):
        params = LinearLayout.LayoutParams(0, -2, 1.0)
        params.setMargins(row_margin, 0, row_margin, 0)
        schedule_row.addView(schedule_btn, params)
    page.add(schedule_row, top=8)

    schedule_info = pyui_kit.make_text(context, "", size=12, color=pyui_kit.MUTED,
                                       selectable=True)
    page.add(schedule_info, top=6)

    result = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(result, top=10)

    lock_btn = pyui_kit.make_button(context, "LOCK", lambda: None)
    unlock_btn = pyui_kit.make_button(context, "UNLOCK", lambda: None)
    status_btn = pyui_kit.make_button(context, "STATUS", lambda: None)
    page.bottom_add(lock_btn, weight=1.0)
    page.bottom_add(unlock_btn, weight=1.0)
    page.bottom_add(status_btn, weight=1.0)

    current = {"key": None, "topic": None}
    states = {}
    suppress = {"on": False}

    def new_state():
        return {
            "text": DEFAULT_TEXT,
            "seconds": DEFAULT_SECONDS,
            "lock_at": "",
            "unlock_at": "",
            "boot_lock": "0",
            "loaded": False,
            "capability": None,
        }

    def state_for(key):
        if not key:
            return new_state()
        return states.setdefault(key, new_state())

    def current_text():
        return message_edit.getText().toString()

    def current_seconds():
        return seconds_edit.getText().toString()

    def persist():
        key = current["key"]
        if not key or suppress["on"]:
            return
        st = state_for(key)
        st["text"] = current_text()
        st["seconds"] = current_seconds()
        st["lock_at"] = lock_at_edit.getText().toString()
        st["unlock_at"] = unlock_at_edit.getText().toString()
        payload = json.dumps(
            {
                "text": st["text"],
                "seconds": st["seconds"],
                "lock_at": st["lock_at"],
                "unlock_at": st["unlock_at"],
                "boot_lock": st["boot_lock"],
            },
            ensure_ascii=False,
        )

        def work():
            client_service.update_feature_settings("lockscreen", payload, key)

        pyui_kit.run_async(work)

    def apply_saved(key, payload):
        st = state_for(key)
        if isinstance(payload, dict):
            if payload.get("text") is not None:
                st["text"] = str(payload.get("text"))
            if payload.get("seconds") is not None:
                st["seconds"] = str(payload.get("seconds"))
            if payload.get("lock_at") is not None:
                st["lock_at"] = str(payload.get("lock_at"))
            if payload.get("unlock_at") is not None:
                st["unlock_at"] = str(payload.get("unlock_at"))
            if payload.get("boot_lock") is not None:
                raw_boot = str(payload.get("boot_lock")).strip().lower()
                st["boot_lock"] = "1" if raw_boot in ("1", "true", "on", "yes") else "0"
        suppress["on"] = True
        try:
            message_edit.setText(st["text"] or DEFAULT_TEXT)
            message_edit.setSelection(len(message_edit.getText().toString()))
            seconds_edit.setText(st["seconds"] or DEFAULT_SECONDS)
            lock_at_edit.setText(st["lock_at"])
            unlock_at_edit.setText(st["unlock_at"])
        finally:
            suppress["on"] = False
        refresh_schedule_ui()

    def refresh_schedule_ui():
        st = state_for(current["key"])
        boot_on = st.get("boot_lock") == "1"
        boot_btn.setText("开机自动锁屏：开" if boot_on else "开机自动锁屏：关")
        capability = st.get("capability")
        if not capability:
            schedule_info.setText("定时能力：检测中…")
            apply_btn.setEnabled(False)
            stop_btn.setEnabled(False)
            return
        if not capability.get("ok", False):
            schedule_info.setText("定时能力：探测失败 · %s" % capability.get("error", "unknown"))
            apply_btn.setEnabled(False)
            stop_btn.setEnabled(False)
            return
        api = capability.get("api") or {}
        supported = bool(capability.get("task_manager")) and bool(api.get("add_daily_task")) \
            and bool(api.get("add_event_task"))
        if supported:
            tasks = capability.get("tasks") or {}
            detail = []
            for task_id in _TASK_IDS:
                item = tasks.get(task_id)
                if not item:
                    continue
                if item.get("kind") == "daily":
                    detail.append("%s @%s" % (task_id.split(".")[-1], item.get("at")))
                elif item.get("event"):
                    detail.append("%s on %s" % (task_id.split(".")[-1], item.get("event")))
            version = capability.get("version_name") or "unknown version"
            schedule_info.setText(
                "定时能力：task_manager 可用 · %s\n已注册：%s"
                % (version, "，".join(detail) if detail else "无")
            )
            apply_btn.setEnabled(True)
            stop_btn.setEnabled(True)
        else:
            reason = capability.get("task_manager_error") or "API 不完整（需要 add_daily_task / add_event_task）"
            schedule_info.setText(
                "定时能力：不支持（%s）\n该目标版本只能手动 LOCK/UNLOCK" % reason
            )
            apply_btn.setEnabled(False)
            stop_btn.setEnabled(False)
        if capability.get("engine") is False:
            schedule_info.setText(
                str(schedule_info.getText()) + "\n锁屏引擎不可用：%s"
                % capability.get("engine_error", "")
            )

    def load_saved(key):
        st = state_for(key)
        if st["loaded"] or not key:
            return
        st["loaded"] = True

        def work():
            raw = client_service.feature_settings("lockscreen", key)
            if isinstance(raw, dict):
                return raw
            try:
                value = json.loads(raw)
            except (TypeError, ValueError):
                return {}
            return value if isinstance(value, dict) else {}

        def apply_ok(payload):
            apply_saved(key, payload)

        pyui_kit.run_async(work, apply_ok)

    def probe_async(key):
        if not key:
            return
        state_for(key)["capability"] = None
        refresh_schedule_ui()

        def work():
            return bootstrap.call_feature("lockscreen", "probe")

        def apply_ok(raw):
            if current["key"] != key:
                return
            state_for(key)["capability"] = pyui_kit.rpc_dict(raw)
            refresh_schedule_ui()

        def apply_error(err):
            if current["key"] != key:
                return
            state_for(key)["capability"] = {"ok": False, "error": str(err)}
            refresh_schedule_ui()

        pyui_kit.run_async(work, apply_ok, apply_error)

    def show_status(text_value):
        page.set_subtitle(str(text_value))

    def run_action(action):
        key = current["key"]
        text = current_text()
        seconds = current_seconds()
        if key:
            st = state_for(key)
            st["text"] = text
            st["seconds"] = seconds
            persist()
        show_status("%s ..." % action)

        def work():
            if action == "lock":
                return bootstrap.call_feature("lockscreen", "lock", text, seconds)
            return bootstrap.call_feature("lockscreen", action)

        def apply_ok(raw):
            result.setText(pyui_kit.render_result(raw))
            # 结果区在页面最下方：动作结束后自动滚到底，避免完整 dict 看不到。
            from android.widget import ScrollView
            pyui_kit.on_main(
                lambda: page.scroll.fullScroll(ScrollView.FOCUS_DOWN)
            )
            data = pyui_kit.rpc_dict(raw)
            if action == "status":
                show_status(
                    "locked=%s  auto_unlock=%s"
                    % (data.get("locked"), data.get("auto_unlock_in_seconds"))
                )
            elif action == "unlock":
                show_status("unlocked=%s" % data.get("unlocked"))
            else:
                show_status("locked=%s  auto_unlock=%s"
                            % (data.get("locked"), data.get("auto_unlock_seconds")))

        def apply_error(error):
            show_status("%s failed: %s" % (action, error))
            result.setText("%s failed: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error,
                           buttons=(lock_btn, unlock_btn, status_btn))

    def apply_schedule():
        key = current["key"]
        if not key:
            return
        persist()
        st = state_for(key)
        lock_at = str(st.get("lock_at", "")).strip()
        unlock_at = str(st.get("unlock_at", "")).strip()
        boot_on = st.get("boot_lock") == "1"
        text = st.get("text") or DEFAULT_TEXT
        show_status("schedule ...")

        def work():
            return bootstrap.call_feature(
                "lockscreen", "schedule", lock_at, unlock_at, boot_on, text
            )

        def apply_ok(raw):
            result.setText(pyui_kit.render_result(raw))
            data = pyui_kit.rpc_dict(raw)
            show_status("schedule ok=%s" % ("true" if data.get("ok") else "false"))
            probe_async(key)

        def apply_error(error):
            show_status("schedule failed: %s" % error)
            result.setText("%s failed: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(apply_btn, stop_btn))

    def stop_schedule():
        key = current["key"]
        if not key:
            return
        show_status("stopping schedule ...")

        def work():
            return bootstrap.call_feature("lockscreen", "unschedule")

        def apply_ok(raw):
            result.setText(pyui_kit.render_result(raw))
            data = pyui_kit.rpc_dict(raw)
            show_status("schedule stopped ok=%s" % ("true" if data.get("ok") else "false"))
            probe_async(key)

        def apply_error(error):
            show_status("stop schedule failed: %s" % error)
            result.setText("%s failed: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(apply_btn, stop_btn))

    def toggle_boot():
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["boot_lock"] = "0" if st.get("boot_lock") == "1" else "1"
        persist()
        refresh_schedule_ui()

    def on_target(key, topic, _cfg):
        if key != current["key"]:
            current["key"] = key
            current["topic"] = topic
            page.set_target("target topic: " + str(topic))
            st = state_for(key)
            if st["loaded"]:
                apply_saved(key, st)
            else:
                suppress["on"] = True
                try:
                    message_edit.setText(DEFAULT_TEXT)
                    seconds_edit.setText(DEFAULT_SECONDS)
                    lock_at_edit.setText("")
                    unlock_at_edit.setText("")
                finally:
                    suppress["on"] = False
                load_saved(key)
            probe_async(key)

    pyui_kit.watch_edit(message_edit, lambda _text: persist())
    pyui_kit.watch_edit(seconds_edit, lambda _text: persist())
    pyui_kit.watch_edit(lock_at_edit, lambda _text: persist())
    pyui_kit.watch_edit(unlock_at_edit, lambda _text: persist())
    pyui_kit.click(lock_btn, lambda: run_action("lock"))
    pyui_kit.click(unlock_btn, lambda: run_action("unlock"))
    pyui_kit.click(status_btn, lambda: run_action("status"))
    pyui_kit.click(apply_btn, apply_schedule)
    pyui_kit.click(stop_btn, stop_schedule)
    pyui_kit.click(boot_btn, toggle_boot)
    pyui_kit.watch_target(page.root, on_target)
    return page.root
