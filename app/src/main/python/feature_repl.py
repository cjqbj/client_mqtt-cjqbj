"""远程 Python REPL（feature）。

在【当前选中目标】上执行任意 Python 代码片段，复用现有 MQTT RPC 通道
（client_service.rpc，和桌面 client_mqtt repl 同一条链路），不引入任何新传输：
- 末行表达式的值（或代码里赋值的 r）原样回显；
- 目标端 print 的 stdout / traceback 的 stderr 原样展示；
- 超时可临时调整（默认 10s，裁剪到 1~120s）。

与桌面版 run_repl 的区别：桌面版是终端 cmd 循环（shell 命令 + % 魔法命令 +
历史），无法搬到安卓 UI；这里只保留内核——"发代码、收 r/stdout/stderr"。
需要 shell 时直接写：
    import subprocess
    subprocess.run(["id"], capture_output=True, text=True).stdout
"""
import json

import client_service

FEATURE = {
    "version": 1,
    "actions": ["eval"],
    "icon": "terminal",
    "ui": "python",
}

DEFAULT_TIMEOUT = 10
_TIMEOUT_MIN = 1
_TIMEOUT_MAX = 120
_HINT = (
    "在目标上执行 Python。\n"
    "- 末行表达式的值显示为 r；print 输出显示在 stdout\n"
    "- 可多行；需要 shell：import subprocess\n"
    "- 例：import os; sorted(os.listdir('.'))"
)


def _normalize_timeout(value):
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT
    return min(max(timeout, _TIMEOUT_MIN), _TIMEOUT_MAX)


def eval(code=None, timeout=DEFAULT_TIMEOUT):
    """UI 动作入口：执行代码片段，返回 JSON 字符串。

    rpc 信封里保留原始 r/stdout/stderr/error，不做 JSON 对象假设
    （REPL 的末行表达式可以是任意类型，这是它和业务 feature 的区别）。
    """
    code = str(code or "").strip()
    timeout = _normalize_timeout(timeout)
    if not code:
        return json.dumps({"ok": False, "error": "code is empty"}, ensure_ascii=False)

    result = client_service.rpc(code, timeout=timeout)
    if not isinstance(result, dict):
        return json.dumps(
            {"ok": False, "error": "unexpected client response: %r" % (result,)},
            ensure_ascii=False,
        )

    payload = {
        "ok": bool(result.get("ok", False)),
        "topic": result.get("topic"),
        "elapsed_ms": result.get("elapsed_ms"),
        "timeout": timeout,
        "r": result.get("r"),
        "stdout": result.get("stdout") if isinstance(result.get("stdout"), str) else "",
        "stderr": result.get("stderr") if isinstance(result.get("stderr"), str) else "",
        "error": result.get("error") if isinstance(result.get("error"), str) else "",
    }
    return json.dumps(payload, ensure_ascii=False)


def _format(payload):
    if not isinstance(payload, dict):
        return repr(payload)
    parts = []
    elapsed = payload.get("elapsed_ms")
    head = "ok=%s  elapsed=%s  timeout=%ss  topic=%s" % (
        payload.get("ok"),
        ("%sms" % elapsed) if elapsed is not None else "n/a",
        payload.get("timeout"),
        payload.get("topic"),
    )
    parts.append(head)
    error = payload.get("error")
    if error:
        parts.append("--- error ---\n%s" % error)
    stdout = (payload.get("stdout") or "").rstrip()
    if stdout:
        parts.append("--- stdout ---\n%s" % stdout)
    stderr = (payload.get("stderr") or "").rstrip()
    if stderr:
        parts.append("--- stderr ---\n%s" % stderr)
    if payload.get("r") is not None:
        parts.append("r = %s" % payload.get("r"))
    if not payload.get("ok") and not (error or stdout or stderr):
        parts.append("(no output; 检查目标是否在线 / 代码是否把结果放在末行或 r)")
    return "\n\n".join(parts)


def build_view(context):
    import bootstrap
    import pyui_kit

    page = pyui_kit.Page(
        context, "REPL",
        "Run Python on the selected target over the shared MQTT RPC channel."
    )
    page.set_target("target topic: " + str(
        client_service._device_config(None).get("request_topic") or ""
    ))

    hint = pyui_kit.make_text(context, _HINT, size=12, color=pyui_kit.MUTED)
    page.add(hint)

    editor = pyui_kit.make_code_edit(
        context,
        "import platform\nplatform.platform()",
        lines=6,
    )
    page.add(editor, top=8)

    timeout_input = pyui_kit.make_edit(context, str(DEFAULT_TIMEOUT), number=True)
    timeout_input.setHint("timeout seconds")
    timeout_input.setSingleLine(True)
    page.add(timeout_input, top=8)

    output = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(output, top=8)

    run_btn = pyui_kit.make_button(context, "RUN", lambda: None)
    copy_btn = pyui_kit.make_button(context, "COPY", lambda: None)
    row = pyui_kit.make_hrow(context)
    page.bottom_add(row)

    def _row_params():
        from android.widget import LinearLayout
        params = LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0)
        params.setMargins(pyui_kit.dp(context, 6), 0, pyui_kit.dp(context, 6), 0)
        return params

    row.addView(run_btn, _row_params())
    row.addView(copy_btn, _row_params())

    def on_run():
        code = editor.getText().toString()
        if not code.strip():
            output.setText("code is empty")
            return
        run_btn.setEnabled(False)
        output.setText("running ...")

        def work():
            return bootstrap.call_feature(
                "repl", "eval", code,
                timeout_input.getText().toString() or str(DEFAULT_TIMEOUT),
            )

        def apply_ok(raw):
            try:
                output.setText(_format(json.loads(raw)))
            except (TypeError, ValueError):
                output.setText(str(raw))

        def apply_error(error):
            output.setText("ERROR %s: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(run_btn,))

    def on_copy():
        pyui_kit.copy_text(context, output.getText().toString())

    pyui_kit.click(run_btn, on_run)
    pyui_kit.click(copy_btn, on_copy)

    # 切目标时只更新提示行；代码草稿保留，避免误删用户输入。
    def refresh_label(_key=None, topic=None, _cfg=None):
        page.set_target("target topic: " + str(topic or ""))

    pyui_kit.watch_target(page.root, refresh_label)
    return page.root
