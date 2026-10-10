"""最小在线探针（feature）。

在线探测的唯一实现是 client_service.probe_online()：向【当前选中目标】的
request topic 发最轻量代码，目标在线时返回
sys.executable / platform.node() / platform.machine() / platform.release()。

本 feature 只是 probe_online 的界面封装：
- 设置页的周期在线探测、online()、本页面按钮全部走同一个入口，结果一致；
- 不依赖任何业务模块，目标端是裸 multi_mqtt 也能应答。

topic 不写死：默认跟随当前选中目标（client_service 里的 request_topic），
切目标后探针自动打到新 topic；仍可显式传 topic 覆盖。

超时时间由页面底部的输入框指定，按 target 持久化保存到磁盘，缺省 2 秒。
取值会被夹到 [1, 600] 之间（参照 §6 的显式超时范围）。

结果区展示完整 RPC 响应（req_id / r / stdout / ok / server_time /
server_from / latency_ms / client_from），逐字段一行便于核对；
'r' 保持原始字符串形式，不做二次解析。
"""
import json
import pprint

import client_service

FEATURE = {
    "actions": ["run"],
    "icon": "bug",
    "version": 1,
    # 界面由本脚本用 Chaquopy 自绘（见 build_view），APK 只有通用宿主。
    "ui": "python",
}

DEFAULT_TIMEOUT = client_service.PROBE_TIMEOUT
# 超时输入的取值范围（秒），与 §6 的 [1,600] 一致
TIMEOUT_MIN = 1.0
TIMEOUT_MAX = 600.0


def _default_timeout_text():
    try:
        v = float(DEFAULT_TIMEOUT)
        return str(int(v)) if v.is_integer() else str(v)
    except (TypeError, ValueError):
        return "2"


DEFAULT_TIMEOUT_TEXT = _default_timeout_text()


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


def format_full(raw):
    """优先展示 MQTT RPC 原始响应，保留所有服务端和传输层字段。

    Older probe payloads without a response envelope remain readable.
    """
    data = raw
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return data

    if not isinstance(data, dict):
        return repr(data)

    try:
        response = data.get("response")
        if isinstance(response, dict):
            data = response
        return pprint.pformat(data, sort_dicts=False, width=80)
    except Exception:
        return repr(data)


def _parse_result(raw):
    """兼容 bootstrap.call_feature 可能返回 dict 或 str 的情况。

    仅内部逻辑（例如将来要做 ok 判断）使用，UI 展示不依赖它。
    """
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


def build_view(context):
    """Chaquopy 自绘：可滚动结果区 + 底部【超时输入框 + PROBE 按钮】。

    - topic 实时跟随选中目标；
    - timeout 按 target 持久化，缺省 2 秒，范围 [1,600]；
    - 结果区展示完整 RPC 响应，逐字段一行，可上下滚动。
    """
    import bootstrap
    import pyui_kit
    from android.text import InputType
    from android.widget import EditText, LinearLayout

    page = pyui_kit.Page(context, "Probe", "2s minimal platform probe")
    result = pyui_kit.make_text(
        context,
        "点按钮向【当前选中目标】发送最小探针；\n"
        "切换目标后按钮上的 topic 和超时会自动跟着变。\n"
        "结果区按字段逐行展示完整 RPC 响应。",
        selectable=True,
    )
    page.add(result)

    # ------------------------------------------------------------------
    # 页面底部：输入框在上、PROBE 按钮在下，紧贴；整体放入页面下部
    # ------------------------------------------------------------------
    bottom = LinearLayout(context)
    bottom.setOrientation(LinearLayout.VERTICAL)

    # 超时输入框：数字键盘；按 target 持久化，缺省 2 秒
    hint = "Timeout seconds (1-600, default %s)" % DEFAULT_TIMEOUT_TEXT
    try:
        timeout_input = pyui_kit.make_edit(context, hint)
    except TypeError:
        timeout_input = EditText(context)
        timeout_input.setHint(hint)
    try:
        timeout_input.setInputType(InputType.TYPE_CLASS_NUMBER)
        timeout_input.setSingleLine(True)
    except Exception:
        pass
    bottom.addView(
        timeout_input,
        LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
    )

    button = pyui_kit.make_button(context, "PROBE", lambda: None)
    bottom.addView(
        button,
        LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
    )

    page.bottom_add(bottom, weight=1.0)

    # key -> {"timeout": "2", "loaded": bool}
    states = {}
    current = {"key": None, "topic": None}

    def new_state():
        return {"timeout": DEFAULT_TIMEOUT_TEXT, "loaded": False}

    def state_for(key):
        if not key:
            return new_state()
        return states.setdefault(key, new_state())

    def clamp_timeout(value):
        try:
            v = float(str(value).strip())
        except (TypeError, ValueError):
            try:
                return float(DEFAULT_TIMEOUT)
            except (TypeError, ValueError):
                return 2.0
        if v < TIMEOUT_MIN:
            v = TIMEOUT_MIN
        if v > TIMEOUT_MAX:
            v = TIMEOUT_MAX
        return v

    def fmt_num(v):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return str(v)
        if f.is_integer():
            return str(int(f))
        return str(f)

    def read_input():
        try:
            text = timeout_input.getText().toString().strip()
        except Exception:
            text = ""
        if not text:
            text = DEFAULT_TIMEOUT_TEXT
        return text

    def set_input(text):
        try:
            timeout_input.setText(text)
            timeout_input.setSelection(len(text))
        except Exception:
            pass

    def refresh_label(_key=None, topic=None, _cfg=None):
        if _key is not None:
            current["key"] = _key
        if topic:
            current["topic"] = topic
        elif not current["topic"]:
            current["topic"] = current_topic()

        page.set_target("target topic: " + current["topic"])
        st = state_for(current["key"])
        t = clamp_timeout(st["timeout"])
        button.setText(
            "PROBE  topic=%s  timeout=%ss" % (current["topic"], fmt_num(t))
        )

    # ------------------------------------------------------------------
    # 超时按 target 持久化：惰性加载
    # ------------------------------------------------------------------
    def load_saved(key):
        st = state_for(key)
        if st["loaded"]:
            return
        st["loaded"] = True

        def work():
            raw = client_service.feature_settings("probe", key)
            if isinstance(raw, dict):
                doc = raw
            else:
                try:
                    doc = json.loads(raw)
                except (TypeError, ValueError):
                    doc = {}
            value = doc.get("timeout")
            if value is None or str(value).strip() == "":
                value = DEFAULT_TIMEOUT_TEXT
            return str(value)

        def apply_ok(value):
            st["timeout"] = value or DEFAULT_TIMEOUT_TEXT
            if current["key"] == key:
                set_input(st["timeout"])
                refresh_label(current["key"], current["topic"])

        pyui_kit.run_async(work, apply_ok)

    def persist(key, timeout_text):
        payload = json.dumps({"timeout": timeout_text}, ensure_ascii=False)

        def work():
            client_service.update_feature_settings("probe", payload, key)

        pyui_kit.run_async(work)

    # ------------------------------------------------------------------
    # 目标切换
    # ------------------------------------------------------------------
    def on_target(_key=None, topic=None, _cfg=None):
        current["key"] = _key
        current["topic"] = topic or current_topic()
        st = state_for(_key)
        if st["loaded"]:
            set_input(st["timeout"])
        else:
            set_input(DEFAULT_TIMEOUT_TEXT)
            if _key:
                load_saved(_key)
        refresh_label(_key, current["topic"])

    # ------------------------------------------------------------------
    # 点击探针：读取输入框 -> 更新缓存 -> 持久化 -> 用该 timeout 探测
    # ------------------------------------------------------------------
    def on_click():
        key = current["key"]
        st = state_for(key)

        text = read_input()
        st["timeout"] = text
        if key:
            persist(key, text)
        t = clamp_timeout(text)
        refresh_label(current["key"], current["topic"])

        button.setEnabled(False)
        result.setText(
            "probing topic=%s timeout=%ss ..." % (
                current["topic"], fmt_num(t),
            )
        )

        def work():
            # 不传 topic：run() 内部取当前选中目标的 request_topic；
            # 传 timeout：走页面上按 target 持久化的那个值。
            return bootstrap.call_feature("probe", "run", None, t)

        def apply_ok(raw):
            # 多行展示：每个字段一行，'r' 保持原字符串不解析。
            result.setText(format_full(raw))

        def apply_error(error):
            result.setText("ERROR %s: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(button,))

    pyui_kit.click(button, on_click)
    # 页面挂上后持续跟随当前目标；切目标即时更新按钮与提示行。
    pyui_kit.watch_target(page.root, on_target)

    # 首屏初始填充：watch_target 尚未触发时也能给出默认值
    st = state_for(current["key"])
    set_input(st["timeout"])
    refresh_label(current["key"], current["topic"])
    return page.root