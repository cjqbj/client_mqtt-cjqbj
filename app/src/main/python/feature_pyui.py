"""PyUI feature：界面完全由本 Python 脚本动态构建（Chaquopy 直接 new 原生控件）。

设计目标：新增/改版界面只热更新这个 feature_*.py，APK 端仅内置一个通用宿主
（Kotlin 侧 PythonViewPage + client_service.build_feature_view 桥），不需要改
客户端、不需要重新打包 APK，即"最小依赖 client apk"。

宿主与脚本之间的最小契约（建议后续所有自绘 feature 遵守）：
  FEATURE             清单字典；ui="python" 表示该 feature 由 Python 自绘
  build_view(context) 入参是 android.content.Context，返回一棵 android.view.View

线程纪律（Android 铁律）：
  - build_view 在主线程被调用，只做建 View 树，禁止阻塞网络；
  - 事件回调也在主线程，网络/RPC 必须丢到 threading 线程；
  - 算完用主线程 Handler.post 回写控件。
"""
import json
import threading

FEATURE = {
    "title": "PyUI",
    # 自绘 feature 不生成通用动作按钮，actions 留空。
    "actions": [],
    "icon": "info",
    "version": 1,
    # 宿主识别字段：ui=python 时走 PythonViewPage 通用宿主。
    "ui": "python",
}

PROBE_TOPIC = "q"
PROBE_TIMEOUT = 2.0


def _dp(context, value):
    density = context.getResources().getDisplayMetrics().density
    return int(value * density + 0.5)


def _format_probe(payload):
    if not isinstance(payload, dict):
        return repr(payload)
    if not payload.get("ok"):
        return "probe failed: %s\ntopic=%s timeout=%ss" % (
            payload.get("error", "unknown error"),
            payload.get("topic"), payload.get("timeout"),
        )
    elapsed_ms = payload.get("elapsed_ms")
    lines = [
        "target online  (topic=%s, %.0fs, elapsed=%s)" % (
            payload.get("topic"), float(payload.get("timeout") or 0),
            ("%sms" % elapsed_ms) if elapsed_ms is not None else "n/a",
        ),
        "node       : %s" % payload.get("node"),
        "machine    : %s" % payload.get("machine"),
        "release    : %s" % payload.get("release"),
        "executable : %s" % payload.get("executable"),
    ]
    return "\n".join(lines)


def build_view(context):
    # android.* 依赖只在真机（Chaquopy）存在；放在函数内导入，
    # 使本模块在桌面 Python 下仍可被安全 import（清单读取/单测不炸）。
    from android.graphics import Color
    from android.os import Handler, Looper
    from android.view import View, ViewGroup
    from android.widget import Button, LinearLayout, ScrollView, TextView
    from java import dynamic_proxy
    from java.lang import Runnable

    # 重要（Chaquopy 铁律，和网传"直接传函数"的说法不同）：
    # 1) 裸 Python 函数不能自动转 Java 单方法接口，会抛
    #    "Cannot convert function object to ...OnClickListener"；
    # 2) 热加载脚本里也【不能】直接 class X(View.OnClickListener)，会抛
    #    "Java classes can only be inherited using static_proxy or dynamic_proxy"；
    # 3) 正解是 dynamic_proxy(接口) 作第一基类（运行时 java.lang.reflect.Proxy，
    #    无需构建期生成），这正是"脚本热更、零 APK 改动"自绘的固定写法。
    class OnClick(dynamic_proxy(View.OnClickListener)):
        def __init__(self, fn):
            # dynamic_proxy 子类必须先初始化 Java 代理对象，再当 Java 对象使用。
            super().__init__()
            self._fn = fn

        def onClick(self, view):
            self._fn(view)

    class Post(dynamic_proxy(Runnable)):
        def __init__(self, fn):
            super().__init__()
            self._fn = fn

        def run(self):
            self._fn()

    main_handler = Handler(Looper.getMainLooper())

    root = LinearLayout(context)
    root.setOrientation(LinearLayout.VERTICAL)
    root.setBackgroundColor(Color.parseColor("#F5F6FA"))
    root.setPadding(_dp(context, 20), _dp(context, 28), _dp(context, 20), _dp(context, 20))

    title = TextView(context)
    title.setText("Python 自绘界面 (Chaquopy)")
    title.setTextSize(20)
    title.setTextColor(Color.parseColor("#111827"))
    root.addView(
        title,
        LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
        ),
    )

    # 结果区占满中部，可滚动；放在按钮之前（按钮固定屏幕下部）。
    scroll = ScrollView(context)
    result = TextView(context)
    result.setTextSize(14)
    result.setTextColor(Color.parseColor("#374151"))
    result.setLineSpacing(_dp(context, 2), 1.0)
    result.setText(
        "整个界面由 feature_pyui.py 动态构建，APK 只有通用宿主，\n"
        "改界面只需热更脚本，无需重新打包。\n\n"
        "点按钮向 topic=%s 发送 %s 秒最小探针。" % (PROBE_TOPIC, PROBE_TIMEOUT)
    )
    scroll.addView(
        result,
        ViewGroup.LayoutParams(
            ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
        ),
    )
    scroll_params = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT, 0
    )
    scroll_params.weight = 1.0
    scroll_params.topMargin = _dp(context, 12)
    root.addView(scroll, scroll_params)

    button = Button(context)
    button.setText("Probe  topic=%s  timeout=%ss" % (PROBE_TOPIC, PROBE_TIMEOUT))

    def on_click(_view):
        # 主线程：立即反馈，网络丢工作线程，禁止阻塞 UI。
        button.setEnabled(False)
        result.setText("probing ...")

        def work():
            try:
                import bootstrap
                raw = bootstrap.call_feature(
                    "probe", "run", PROBE_TOPIC, PROBE_TIMEOUT
                )
                text = _format_probe(json.loads(raw))
            except Exception as error:
                text = "ERROR %s: %s" % (type(error).__name__, error)

            def apply_result():
                result.setText(text)
                button.setEnabled(True)

            # 回到主线程：显式 Runnable 适配器（Chaquopy 不接受裸 callable）。
            main_handler.post(Post(apply_result))

        threading.Thread(target=work, daemon=True).start()

    # 显式 OnClickListener 适配器（Chaquopy 不接受裸 callable）。
    button.setOnClickListener(OnClick(on_click))
    button_params = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
    )
    button_params.topMargin = _dp(context, 16)
    root.addView(button, button_params)

    return root
