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

# 桌面单测没有 java 包；设备上 java.lang.Throwable 单独一条 except，
# 保证 OOM 这类 java.lang.Error 同样穿不过 JVM 回调边界
# （Chaquopy 下它不一定挂在 Python BaseException 上）。
try:  # pragma: no cover - 平台分支
    from java.lang import Throwable as _JavaThrowable
except Exception:  # pragma: no cover
    _JavaThrowable = None


def report_feature_error(where, error):
    """feature 回调出错只记录、不外抛：logcat(主) + stderr(兜底)。

    铁律：任何 JVM->Python 回调里未捕获的异常都会变成主线程/工作线程的
    UncaughtException，Android 直接杀进程。feature 错误永远不能崩 client。
    """
    detail = "%s failed: %s: %s" % (where, type(error).__name__, error)
    try:  # pragma: no cover - 设备分支
        from android.util import Log
        Log.e("qgb-pyui", detail[:500])
    except Exception:
        pass
    print(detail[:500])


if _JavaThrowable is not None:  # pragma: no cover - 仅 Chaquopy 设备执行
    def guarded(where, fn):
        """把任意回调包成"永不抛出"版本（JVM 代理边界统一使用）。"""
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except BaseException as error:  # noqa: BLE001 - 边界必须全吞
                report_feature_error(where, error)
            except _JavaThrowable as error:  # noqa: BLE001 - java.lang.Error 兜底
                report_feature_error(where, error)
            return None
        return wrapper
else:
    def guarded(where, fn):
        """把任意回调包成"永不抛出"版本（桌面单测分支）。"""
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except BaseException as error:  # noqa: BLE001 - 边界必须全吞
                report_feature_error(where, error)
                return None
        return wrapper


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
                # 回调边界：feature 的 Runnable 异常只记录，绝不杀进程。
                if self.fn is not None:
                    guarded("runnable", self.fn)()

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
                # 点击回调里任何 feature 异常都不能穿透到 Android 主线程。
                if self.fn is not None:
                    guarded("click", self.fn)()

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
                    guarded("attach", self.on_attach_fn)()

            def onViewDetachedFromWindow(self, _view):
                if self.on_detach_fn is not None:
                    guarded("detach", self.on_detach_fn)()

        cls = _PROXY_CLASSES["attach"] = AttachProxy
    return cls


def _post(fn):
    """在主线程执行 fn（显式 Runnable 适配器，Chaquopy 不接受裸 callable）。"""
    from android.os import Handler, Looper

    Handler(Looper.getMainLooper()).post(_runnable_class()(fn))


def click(view, fn):
    """给 view 装 OnClickListener（dynamic_proxy，热加载脚本唯一可行写法）。"""
    view.setOnClickListener(_onclick_class()(fn))


def on_main(fn):
    """把 fn 投递到主线程执行（工作线程更新 View 的唯一安全入口）。

    回调经 Runnable 护栏包裹，fn 抛错只记 logcat，绝不穿透杀进程。
    """
    _post(guarded("on_main", fn))


def repeat_every(seconds, fn):
    """每隔 seconds 秒在主线程执行一次 fn，返回 stop()。

    大文件上传/下载等长任务用它做"隔几秒汇报一次"的心跳，不要求实时。
    典型用法：stop = repeat_every(3, lambda: set_status(...))，finally 里 stop()。
    """
    interval = max(0.2, float(seconds))
    stop_event = threading.Event()

    def loop():
        while not stop_event.wait(interval):
            _post(guarded("repeat_every", fn))

    threading.Thread(target=loop, daemon=True).start()
    return stop_event.set


def make_zoom_image(context, height_dp=300):
    """可双指缩放/拖动/双击还原的图片控件（原生优先，失败回退静态图）。

    原生控件规范：优先实例化 Kotlin 写好的
    com.qgb.clientmqtt.ZoomableImageView（手势、边界约束都在原生侧）；
    任何原因拿不到（旧 APK / 实例化失败）都回退成 Python 侧静态
    ImageView——不能缩放但页面完整、永不崩。返回的都是 ImageView 子类，
    feature 直接 setImageBitmap/setVisibility 即可。
    """
    from android.view import View
    from android.view import ViewGroup
    from android.widget import ImageView

    view = None
    try:
        from com.qgb.clientmqtt import ZoomableImageView
        view = ZoomableImageView(context)
    except BaseException as error:  # noqa: BLE001 - 兜底必须全吞
        report_feature_error("native zoom image unavailable, use static", error)
        view = ImageView(context)
        view.setAdjustViewBounds(True)
        view.setScaleType(ImageView.ScaleType.FIT_CENTER)
    view.setVisibility(View.GONE)
    # 预置固定高度的 LayoutParams；Page.add 识别 zoom 控件后沿用此高度。
    view.setLayoutParams(ViewGroup.LayoutParams(_match(), dp(context, height_dp)))
    view.setTag("qgb-zoom-image")
    return view


def make_button(context, label, fn):
    from android.widget import Button
    button = Button(context)
    button.setText(label)
    click(button, fn)
    return button


def make_text(context, text="", size=14, color=SUB, bold=False, selectable=False):
    from android.graphics import Typeface
    from android.widget import TextView
    view = TextView(context)
    view.setText(text)
    view.setTextSize(size)
    view.setTextColor(_color(color))
    view.setLineSpacing(dp(context, 2), 1.0)
    if bold:
        view.setTypeface(Typeface.DEFAULT, Typeface.BOLD)
    # 结果/日志类文本允许长按选择复制；列表行内文本不要开（会和整行点击抢手势）。
    if selectable:
        view.setTextIsSelectable(True)
    return view


def make_code_edit(context, text="", lines=6):
    """多行等宽代码输入框（REPL 用）：等宽字体、允许换行、竖向可滚动。"""
    from android.graphics import Typeface
    from android.text import InputType
    from android.view import Gravity
    from android.widget import EditText
    view = EditText(context)
    view.setText(str(text))
    view.setInputType(
        InputType.TYPE_CLASS_TEXT
        | InputType.TYPE_TEXT_FLAG_MULTI_LINE
        | InputType.TYPE_TEXT_FLAG_NO_SUGGESTIONS
    )
    view.setGravity(Gravity.TOP)
    view.setMinLines(lines)
    view.setMaxLines(lines * 2)
    view.setTypeface(Typeface.MONOSPACE)
    view.setHorizontallyScrolling(False)
    view.setPadding(dp(context, 10), dp(context, 8), dp(context, 10), dp(context, 8))
    return view


def _color(value):
    from android.graphics import Color
    return Color.parseColor(value)


def parse_color(value):
    """#RRGGBB 颜色字符串转 Android int（feature 给原生 View 着色时用）。"""
    return _color(value)


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
        # 副标题行固定在滚动区之外，不随内容滚动：长任务进度/状态借这里显示，
        # 避免文件列表很长时状态被滚走看不到（feature 用 set_subtitle 更新）。
        self.subtitle_view = None
        if subtitle:
            # 副标题行会被 feature 复用为长任务状态/日志行（如文件上传进度），
            # 统一允许长按选择复制。
            self.subtitle_view = make_text(
                context, subtitle, size=13, color=MUTED, selectable=True
            )
            params = LinearLayout.LayoutParams(_match(), _wrap())
            params.topMargin = dp(context, 2)
            self.root.addView(self.subtitle_view, params)

        # 当前目标 topic 行：由 watch_target 实时刷新（切目标立刻跟着变）。
        self.target_line = TextView(context)
        self.target_line.setTextIsSelectable(True)
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

    def set_subtitle(self, text):
        """更新固定副标题行（长任务进度/状态）；页面未建副标题时忽略。"""
        if self.subtitle_view is not None:
            self.subtitle_view.setText(str(text))

    def add(self, view, top=0, bottom=0, height_dp=0):
        from android.widget import LinearLayout
        # make_zoom_image 预置了固定高度（缩放手势需要确定高度的盒子）；
        # height_dp 显式传入时优先。
        fixed_height = None
        if height_dp:
            fixed_height = dp(self.context, height_dp)
        elif str(view.getTag()) == "qgb-zoom-image":
            existing = view.getLayoutParams()
            if existing is not None and existing.height > 0:
                fixed_height = existing.height
        params = LinearLayout.LayoutParams(
            _match(), fixed_height if fixed_height is not None else _wrap()
        )
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
                guarded("target change callback", lambda: on_change(key, topic, cfg))()
        except BaseException as error:  # noqa: BLE001 - 轮询 tick 绝不能炸
            report_feature_error("watch target", error)
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
    """工作线程跑 work()，结果/异常回主线程；执行期间禁用给定按钮。

    历史血泪坑：except ... as error 退出块时 CPython 会 del 掉 error，
    不能在 except 里定义引用 error 的闭包再 _post 延迟执行——主线程真正
    回调时会 NameError: cannot access free variable，直接崩 Activity。
    必须用默认参数在定义时把 error 绑死。
    """
    buttons = tuple(buttons)

    def _restore_buttons():
        for button in buttons:
            guarded("enable button", lambda b=button: b.setEnabled(True))()

    def worker():
        try:
            value = work()
        except BaseException as captured_error:  # noqa: BLE001 - UI 必须看到失败原因
            # 默认参数在定义时绑定，绕开 except 变量块结束被 del 的作用域坑。
            def apply_error(err=captured_error):
                _restore_buttons()
                if on_error:
                    guarded("on_error callback", lambda: on_error(err))()
            _post(apply_error)
            return

        def apply_ok(result=value):
            _restore_buttons()
            if on_ok:
                guarded("on_ok callback", lambda: on_ok(result))()
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
