"""通用的 per-feature 参数设置页（长按 feature → Setting）。

参数由 feature 自己声明，本模块只负责渲染与读写：

    FEATURE = {..., "settings": [
        {"key", "label", "type": int/float/bool/text,
         "default", "min", "max", "scope": "global"|"topic", "help"},
    ]}

每行固定三部分：参数名 + 作用域徽标、说明、输入框 + Save/Reset。
作用域在行上直接用中文标注，并在页头解释：

    全局     = 所有目标（topic）共用同一份值
    分 topic = 仅当前目标单独保存，切目标自动跟着变

值存哪里也由 client_service 按 scope 决定（global.json / device.json），
本页不直接碰文件。保存后立即生效：feature 代码在每次动作时现读参数，
不需要 Reload 脚本。
"""

import json

import client_service

SCOPE_HINT = "全局 = 所有目标共用 · 分 topic = 仅当前目标单独保存"


def build_view(context, feature):
    """构建某 feature 的设置页根 View（Chaquopy 主线程调用）。"""
    import pyui_kit
    from android.view import View
    from android.widget import LinearLayout

    name = str(feature or "").strip()
    page = pyui_kit.Page(context, "Settings \u00b7 %s" % name, SCOPE_HINT)
    # 目标 topic 已在本页信息行里展示，不再重复 Page 内置的目标行。
    page.target_line.setVisibility(View.GONE)

    info_line = pyui_kit.make_text(
        context, "", size=11, color=pyui_kit.MUTED, selectable=True
    )
    page.add(info_line, top=6)

    rows_box = LinearLayout(context)
    rows_box.setOrientation(LinearLayout.VERTICAL)
    page.add(rows_box, top=6)

    def set_info(text):
        info_line.setText(text)

    def add_row(item):
        key = item.get("key") or ""
        scope = item.get("scope") or "topic"
        scope_text = item.get("scope_text") or (
            "\u5168\u5c40" if scope == "global" else "\u5206 topic"
        )
        kind = item.get("type") or "text"

        title_row = pyui_kit.make_hrow(context)
        label = pyui_kit.make_text(
            context, item.get("label") or key, size=15,
            color=pyui_kit.INK, bold=True,
        )
        title_row.addView(label, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
        badge = pyui_kit.make_text(
            context, scope_text, size=11, color=pyui_kit.SUB
        )
        title_row.addView(badge, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
        rows_box.addView(
            title_row,
            LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
        )

        help_view = pyui_kit.make_text(
            context, item.get("help") or "", size=11,
            color=pyui_kit.MUTED, selectable=True,
        )
        rows_box.addView(
            help_view,
            LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
        )

        edit = pyui_kit.make_edit(
            context, item.get("value_text") or "",
            number=(kind == "int"),
        )
        save_button = pyui_kit.make_button(context, "Save", lambda: None)
        reset_button = pyui_kit.make_button(context, "Reset", lambda: None)
        edit_row = pyui_kit.make_hrow(context)
        edit_row.addView(edit, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
        edit_row.addView(save_button, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
        edit_row.addView(reset_button, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
        edit_params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
        edit_params.topMargin = pyui_kit.dp(context, 4)
        rows_box.addView(edit_row, edit_params)

        state_line = pyui_kit.make_text(
            context, "", size=11, color=pyui_kit.MUTED, selectable=True
        )
        rows_box.addView(
            state_line,
            LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
        )

        def describe(value, stored, source_note=""):
            if not stored:
                hint = "default" if value is None else "default = %s" % value
                return source_note or ("\u672a\u8bbe\u7f6e \u00b7 " + hint)
            return "\u5df2\u4fdd\u5b58 %s \u00b7 %s" % (value, scope_text)

        state_line.setText(describe(
            item.get("value"), bool(item.get("stored"))
        ))

        def on_save():
            text = str(edit.getText() or "").strip()
            try:
                raw = client_service.update_feature_setting(name, key, text)
                result = json.loads(raw)
            except BaseException as error:  # noqa: BLE001 - 设置页不能崩
                state_line.setText("Save failed: %s: %s" % (type(error).__name__, error))
                return
            if not result.get("ok"):
                state_line.setText("Save failed: %s" % result.get("error"))
                return
            saved = result.get("value")
            edit.setText("" if saved is None else str(saved))
            state_line.setText(describe(
                saved, bool(result.get("stored"))
            ))

        def on_reset():
            try:
                raw = client_service.reset_feature_setting(name, key)
                result = json.loads(raw)
            except BaseException as error:  # noqa: BLE001
                state_line.setText("Reset failed: %s: %s" % (type(error).__name__, error))
                return
            if not result.get("ok"):
                state_line.setText("Reset failed: %s" % result.get("error"))
                return
            fallback = result.get("value")
            edit.setText("" if fallback is None else str(fallback))
            state_line.setText(describe(fallback, False))

        pyui_kit.click(save_button, on_save)
        pyui_kit.click(reset_button, on_reset)

    def render():
        rows_box.removeAllViews()
        try:
            schema = json.loads(client_service.feature_settings_schema(name))
        except BaseException as error:  # noqa: BLE001
            set_info("Failed to load settings: %s: %s" % (type(error).__name__, error))
            return
        if not schema.get("ok"):
            set_info("Failed to load settings: %s" % schema.get("error"))
            return
        items = schema.get("settings") or []
        set_info(
            "topic: %s \u00b7 %d \u4e2a\u53c2\u6570" % (
                schema.get("topic") or "-", len(items)
            )
        )
        if not items:
            no_items = pyui_kit.make_text(
                context,
                "\u8fd9\u4e2a feature \u6ca1\u6709\u53ef\u8c03\u53c2\u6570\uff08FEATURE[\"settings\"] \u4e3a\u7a7a\uff09",
                size=12, color=pyui_kit.SUB,
            )
            rows_box.addView(
                no_items,
                LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
            )
            return
        for item in items:
            add_row(item)

    render()

    refresh_button = pyui_kit.make_button(context, "Refresh", lambda: None)
    pyui_kit.click(refresh_button, render)
    page.bottom_add(refresh_button, weight=1.0)

    return page.root
