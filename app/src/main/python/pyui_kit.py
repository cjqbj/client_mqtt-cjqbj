"""pyui_kit —— 自绘 feature 脚本共享的最小原生 UI 工具包（Chaquopy）。

所有 feature 的界面都由各自的 feature_*.py 通过本工具包动态 new 原生控件，
APK 端只有一个通用宿主（Kotlin PythonViewPage +
client_service.build_feature_view 桥）。新增/改版界面只热更脚本即可。

约束（真机踩坑结论）：
  - android.* / java.* 依赖只在函数内导入：桌面 Python 没有 android 模块，
    函数内导入可保证清单读取与单测在 PC 上照常 import；
  - 裸 Python 函数不能自动转 Java 单方法接口，必须 dynamic_proxy；
  - 同一个 Java 接口整个进程只能 dynamic_proxy 出一个 Python 类：重复
    "class X(dynamic_proxy(同一个接口))" 会让 Chaquopy 的回调链失效，
    Java 调进来时抛 "_chaquopyGetType is abstract and cannot be called"
    崩溃。所以代理类一律走下面的 _PROXY_CLASSES 单例，多实例共享；
  - build_view 在主线程只建 View 树，网络/RPC 一律丢 threading 线程，
    再用主线程 Handler.post 回写控件。

本模块不以 feature_ 开头，bootstrap.list_features 不会把它列成一个 feature。
"""
import json
import threading

import client_service

BG = "#F5F6FA"
INK = "#111827"
SUB = "#374151"
MUTED = "#6B7280"
ACCENT_BG = "#EDE9FE"


def dp(context, value):
    return int(value * context.getResources().getDisplayMetrics().density + 0.5)


def _match():
    from android.view import ViewGroup
    return ViewGroup.LayoutParams.MATCH_PARENT


def _wrap():
    from android.view import ViewGroup
    return ViewGroup.LayoutParams.WRAP_CONTENT


def match():
    return _match()


def wrap():
    return _wrap()


_PROXY_CLASSES = {}


def _runnable_class():
    """java.lang.Runnable 的唯一 dynamic_proxy 类（全进程共享，见模块约束）。"""
    cls = _PROXY_CLASSES.get("runnable")
    if cls is None:
        from java import dynamic_proxy
        from java.lang import Runnable

        class RunnableProxy(dynamic_proxy(Runnable)):
            def __init__(self, fn=None):
                super().__init__()
                self.fn = fn

            def run(self):
                if self.fn is not None:
                    self.fn()

        cls = _PROXY_CLASSES["runnable"] = RunnableProxy
    return cls


def _onclick_class():
    """View.OnClickListener 的唯一 dynamic_proxy 类。"""
    cls = _PROXY_CLASSES.get("onclick")
    if cls is None:
        from android.view import View
        from java import dynamic_proxy

        class OnClickProxy(dynamic_proxy(View.OnClickListener)):
            def __init__(self, fn=None):
                # dynamic_proxy 子类必须先初始化 Java 代理对象。
                super().__init__()
                self.fn = fn

            def onClick(self, _view):
                if self.fn is not None:
                    self.fn()

        cls = _PROXY_CLASSES["onclick"] = OnClickProxy
    return cls


def _attach_class():
    """View.OnAttachStateChangeListener 的唯一 dynamic_proxy 类。"""
    cls = _PROXY_CLASSES.get("attach")
    if cls is None:
        from android.view import View
        from java import dynamic_proxy

        class AttachProxy(dynamic_proxy(View.OnAttachStateChangeListener)):
            def __init__(self, on_attach=None, on_detach=None):
                super().__init__()
                self.on_attach_fn = on_attach
                self.on_detach_fn = on_detach

            def onViewAttachedToWindow(self, _view):
                if self.on_attach_fn is not None:
                    self.on_attach_fn()

            def onViewDetachedFromWindow(self, _view):
                if self.on_detach_fn is not None:
                    self.on_detach_fn()

        cls = _PROXY_CLASSES["attach"] = AttachProxy
    return cls


def _post(fn):
    """在主线程执行 fn（显式 Runnable 适配器，Chaquopy 不接受裸 callable）。"""
    from android.os import Handler, Looper

    Handler(Looper.getMainLooper()).post(_runnable_class()(fn))


def click(view, fn):
    """给 view 装 OnClickListener（dynamic_proxy，热加载脚本唯一可行写法）。"""
    view.setOnClickListener(_onclick_class()(fn))


def make_button(context, label, fn):
    from android.widget import Button
    button = Button(context)
    button.setText(label)
    click(button, fn)
    return button


def make_text(context, text="", size=14, color=SUB, bold=False):
    from android.graphics import Typeface
    from android.widget import TextView
    view = TextView(context)
    view.setText(text)
    view.setTextSize(size)
    view.setTextColor(_color(color))
    view.setLineSpacing(dp(context, 2), 1.0)
    if bold:
        view.setTypeface(Typeface.DEFAULT, Typeface.BOLD)
    return view


def _color(value):
    from android.graphics import Color
    return Color.parseColor(value)


def make_edit(context, text="", number=False):
    from android.text import InputType
    from android.widget import EditText
    view = EditText(context)
    view.setText(str(text))
    if number:
        view.setInputType(InputType.TYPE_CLASS_NUMBER)
    return view


def make_hrow(context):
    from android.widget import LinearLayout
    row = LinearLayout(context)
    row.setOrientation(LinearLayout.HORIZONTAL)
    return row


class Page:
    """标准页面骨架：标题 / 目标行 / 中部可滚动内容（weight=1）/ 底部操作区。

    按钮固定在屏幕下部，结果区占满中部并可滚动，不会把按钮顶出屏幕。
    """

    def __init__(self, context, title, subtitle=None):
        from android.graphics import Color
        from android.view import ViewGroup
        from android.widget import LinearLayout, ScrollView, TextView

        self.context = context
        self.root = LinearLayout(context)
        self.root.setOrientation(LinearLayout.VERTICAL)
        self.root.setBackgroundColor(Color.parseColor(BG))
        self.root.setPadding(dp(context, 16), dp(context, 20), dp(context, 16), dp(context, 14))

        if title:
            title_view = make_text(context, title, size=20, color=INK, bold=True)
            self.root.addView(
                title_view,
                LinearLayout.LayoutParams(_match(), _wrap()),
            )
        if subtitle:
            sub = make_text(context, subtitle, size=13, color=MUTED)
            params = LinearLayout.LayoutParams(_match(), _wrap())
            params.topMargin = dp(context, 2)
            self.root.addView(sub, params)

        # 当前目标 topic 行：由 watch_target 实时刷新（切目标立刻跟着变）。
        self.target_line = TextView(context)
        self.target_line.setTextSize(12)
        self.target_line.setTextColor(Color.parseColor(MUTED))
        target_params = LinearLayout.LayoutParams(_match(), _wrap())
        target_params.topMargin = dp(context, 6)
        target_params.bottomMargin = dp(context, 6)
        self.root.addView(self.target_line, target_params)

        self.scroll = ScrollView(context)
        self.body = LinearLayout(context)
        self.body.setOrientation(LinearLayout.VERTICAL)
        self.scroll.addView(
            self.body,
            ViewGroup.LayoutParams(_match(), _wrap()),
        )
        scroll_params = LinearLayout.LayoutParams(_match(), 0)
        scroll_params.weight = 1.0
        self.root.addView(self.scroll, scroll_params)

        self.bottom = LinearLayout(context)
        self.bottom.setOrientation(LinearLayout.HORIZONTAL)
        bottom_params = LinearLayout.LayoutParams(_match(), _wrap())
        bottom_params.topMargin = dp(context, 12)
        self.root.addView(self.bottom, bottom_params)

    def set_target(self, text):
        self.target_line.setText(text)

    def add(self, view, top=0, bottom=0):
        from android.widget import LinearLayout
        params = LinearLayout.LayoutParams(_match(), _wrap())
        if top:
            params.topMargin = dp(self.context, top)
        if bottom:
            params.bottomMargin = dp(self.context, bottom)
        self.body.addView(view, params)
        return view

    def clear(self):
        self.body.removeAllViews()

    def bottom_add(self, view, weight=0.0):
        from android.widget import LinearLayout
        params = LinearLayout.LayoutParams(0 if weight else _wrap(), _wrap(), float(weight))
        if weight:
            params.setMargins(dp(self.context, 6), 0, dp(self.context, 6), 0)
        self.bottom.addView(view, params)
        return view

    def bottom_space(self):
        self.bottom_add(make_text(self.context, "", size=1), weight=1.0)


def selected_config():
    """当前选中目标的配置（每次动作时现取，切目标后不 stale）。"""
    return client_service._device_config(None)


def watch_target(anchor, on_change):
    """轮询当前选中目标（800ms），变化时在主线程回调 on_change(key, topic, config)。

    自绘页面常驻不重建，切 topic 时页面不会被销毁；用 anchor 的 attach 状态
    管理轮询生命周期，离开窗口自动停、重新挂上自动续。
    """
    from android.os import Handler, Looper

    handler = Handler(Looper.getMainLooper())
    state = {"key": None}

    def read_config():
        cfg = client_service._device_config(None)
        key = str(cfg.get("id") or cfg.get("request_topic") or "")
        return key, str(cfg.get("request_topic") or ""), cfg

    # Runnable 代理类全进程唯一，tick 只是它的一个实例（见模块约束）。
    tick = _runnable_class()()

    def tick_fn():
        if not anchor.isAttachedToWindow():
            return
        try:
            key, topic, cfg = read_config()
            if key != state["key"]:
                state["key"] = key
                on_change(key, topic, cfg)
        except Exception:
            pass
        handler.postDelayed(tick, 800)

    tick.fn = tick_fn

    def on_attach():
        handler.removeCallbacks(tick)
        handler.post(tick)

    def on_detach():
        handler.removeCallbacks(tick)

    anchor.addOnAttachStateChangeListener(_attach_class()(on_attach, on_detach))
    if anchor.isAttachedToWindow():
        handler.post(tick)


def run_async(work, on_ok=None, on_error=None, buttons=()):
    """工作线程跑 work()，结果/异常回主线程；执行期间禁用给定按钮。"""
    buttons = tuple(buttons)

    def worker():
        try:
            value = work()
        except BaseException as error:  # noqa: BLE001 - UI 必须看到失败原因
            def apply_error():
                for button in buttons:
                    button.setEnabled(True)
                if on_error:
                    on_error(error)
            _post(apply_error)
            return

        def apply_ok():
            for button in buttons:
                button.setEnabled(True)
            if on_ok:
                on_ok(value)
        _post(apply_ok)

    for button in buttons:
        button.setEnabled(False)
    threading.Thread(target=worker, daemon=True).start()


def pretty(raw):
    """把 call_feature 返回的 JSON 字符串美化；解析失败原样返回。"""
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return str(raw)
    return json.dumps(value, indent=2, ensure_ascii=False)


def render_result(raw):
    """通用结果渲染：结构化 JSON + 目标端原始 stdout/stderr 原样追加。"""
    parts = [pretty(raw)]
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        value = None
    if isinstance(value, dict):
        stdout = value.get("_stdout")
        stderr = value.get("_stderr")
        if stdout:
            parts.append("Python stdout:\n" + str(stdout).rstrip())
        if stderr:
            parts.append("Python stderr:\n" + str(stderr).rstrip())
    return "\n\n".join(parts)


def copy_text(context, text):
    from android.content import ClipData, Context

    manager = context.getSystemService(Context.CLIPBOARD_SERVICE)
    manager.setPrimaryClip(ClipData.newPlainText("feature result", str(text)))
