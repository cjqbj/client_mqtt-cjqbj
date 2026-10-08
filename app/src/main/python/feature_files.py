"""Remote filesystem feature. Replaceable at runtime."""

import client_service
import json

FEATURE = {"name": "files", "title": "Files", "version": 1, "actions": ["scan", "upload"],
           "ui": "python", "icon": "files"}


def run():
    return FEATURE


def build_scan_code(root, offset=0, limit=100, recursive=False):
    root_value = json.dumps(str(root), ensure_ascii=False)
    offset_value = max(0, int(offset))
    limit_value = max(1, min(int(limit), 10000))
    recursive_value = bool(recursive)
    return f'''import json, os
root = os.path.abspath({root_value})
start = {offset_value}
limit = {limit_value}
recursive = {recursive_value!r}
items = []
errors = []
has_more = False
seen = 0
base = os.path.realpath(root)
if not os.path.isdir(root):
    r = json.dumps({{"ok": False, "error": "root is not a directory", "root": root}}, ensure_ascii=False)
else:
    for current, dirs, names in os.walk(root, followlinks=False):
        dirs.sort()
        names.sort()
        current_real = os.path.realpath(current)
        if not (current_real == base or current_real.startswith(base + os.sep)):
            dirs[:] = []
            continue
        dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(current, d))]
        entries = [(name, "directory") for name in dirs]
        entries.extend((name, "file") for name in names)
        entries.sort(key=lambda item: (item[1] != "directory", item[0].casefold()))
        for name, kind in entries:
            path = os.path.join(current, name)
            try:
                real = os.path.realpath(path)
                if not (real == base or real.startswith(base + os.sep)):
                    continue
                stat = os.stat(path, follow_symlinks=False)
                if seen < start:
                    seen += 1
                    continue
                if len(items) >= limit:
                    has_more = True
                    break
                items.append({{"path": os.path.relpath(path, root), "kind": kind, "size": stat.st_size, "modified": stat.st_mtime}})
                seen += 1
            except OSError as exc:
                errors.append({{"path": os.path.relpath(path, root), "error": repr(exc)}})
        if has_more or not recursive:
            break
    r = json.dumps({{"ok": True, "root": root, "items": items, "errors": errors, "has_more": has_more, "next_offset": start + len(items)}}, ensure_ascii=False)
'''


def build_upload_code(remote_path):
    path_value = json.dumps(str(remote_path), ensure_ascii=False)
    return f'''import json
import aliyun_git
path = {path_value}
try:
    url = aliyun_git.upload(path, file_path=path.rsplit("/", 1)[-1])
    r = json.dumps({{"ok": True, "url": url, "name": path.rsplit("/", 1)[-1]}}, ensure_ascii=False)
except Exception as exc:
    r = json.dumps({{"ok": False, "error": repr(exc)}}, ensure_ascii=False)
'''


def scan(root, offset=0, limit=100):
    result = client_service.rpc(build_scan_code(root, offset, limit))
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def upload(remote_path):
    result = client_service.rpc(build_upload_code(remote_path))
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘远程文件浏览器：状态行/列表（中部滚动）+ 页码/Up/Refresh（底部）。

    切目标后自动切到新目标的 remote_root 并刷新；点目录进入，点文件经
    OpenAPI 通道下载到控制机 downloads/（图片直接显示预览）。
    """
    import base64
    import bootstrap
    import pyui_kit
    from android.graphics import BitmapFactory
    from android.view import View
    from android.widget import ImageView, LinearLayout, TextView

    page = pyui_kit.Page(context, "Files", "Browse and download files on the target")
    state = {"root": "/data/data", "boundary": "/data/data", "limit": 100,
             "entries": [], "has_more": False, "next_offset": 0, "topic": None,
             "loading": False}

    status = pyui_kit.make_text(context, "Ready", size=13)
    page.add(status)

    preview = ImageView(context)
    preview.setAdjustViewBounds(True)
    preview.setScaleType(ImageView.ScaleType.FIT_CENTER)
    preview.setMaxHeight(pyui_kit.dp(context, 220))
    preview.setVisibility(View.GONE)
    page.add(preview, top=6)

    root_line = pyui_kit.make_text(context, "", size=12, color=pyui_kit.MUTED)
    page.add(root_line, top=4)

    rows_box = LinearLayout(context)
    rows_box.setOrientation(LinearLayout.VERTICAL)
    page.add(rows_box, top=4)

    load_more = pyui_kit.make_button(context, "Load more", lambda: None)
    load_more.setVisibility(View.GONE)
    page.add(load_more, top=6)

    def set_status(text):
        status.setText(text)

    def page_size():
        try:
            return max(1, min(int(str(limit_edit.getText()) or "100"), 10000))
        except ValueError:
            return 100

    def render_rows():
        rows_box.removeAllViews()
        for entry in state["entries"]:
            is_dir = entry.get("kind") == "directory"
            row = pyui_kit.make_hrow(context)
            row.setPadding(0, pyui_kit.dp(context, 7), 0, pyui_kit.dp(context, 7))
            column = LinearLayout(context)
            column.setOrientation(LinearLayout.VERTICAL)
            name_view = pyui_kit.make_text(
                context, str(entry.get("path")), size=14,
                color=pyui_kit.INK if is_dir else pyui_kit.SUB, bold=is_dir,
            )
            if is_dir:
                meta = pyui_kit.make_text(context, "Folder", size=11, color=pyui_kit.MUTED)
            else:
                meta = pyui_kit.make_text(
                    context, "%s B" % entry.get("size", 0), size=11, color=pyui_kit.MUTED
                )
            column.addView(name_view, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
            column.addView(meta, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
            row.addView(column, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
            tag = TextView(context)
            tag.setText("Open" if is_dir else "Download")
            tag.setTextSize(12)
            row.addView(tag, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
            pyui_kit.click(row, (lambda e=entry: on_entry_click(e)))
            params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
            params.bottomMargin = pyui_kit.dp(context, 2)
            rows_box.addView(row, params)
        root_line.setText(state["root"])
        load_more.setVisibility(View.VISIBLE if state["has_more"] else View.GONE)

    def load_page(start, reset=False):
        if state["loading"]:
            return
        state["loading"] = True
        scan_root = state["root"]
        limit = page_size()
        state["limit"] = limit
        if reset or start == 0:
            set_status("Scanning %s ..." % scan_root)
        else:
            set_status("Loading more from %s ..." % scan_root)

        def work():
            raw = bootstrap.call_feature("files", "scan", scan_root, start, limit)
            return json.loads(raw)

        def apply_ok(result):
            state["loading"] = False
            if not result.get("ok"):
                if start == 0:
                    state["entries"] = []
                state["has_more"] = False
                render_rows()
                set_status(result.get("error", "scan failed"))
                return
            page_items = result.get("items") or []
            state["entries"] = list(page_items) if start == 0 else state["entries"] + page_items
            state["has_more"] = bool(result.get("has_more"))
            state["next_offset"] = int(result.get("next_offset", start + len(page_items)))
            render_rows()
            set_status(
                "Loaded %d files%s"
                % (len(state["entries"]), " (more below)" if state["has_more"] else "")
            )

        def apply_error(error):
            state["loading"] = False
            set_status("Invalid scan response: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    def reset_to(new_root):
        state["root"] = new_root
        state["boundary"] = new_root.rstrip("/") or "/"
        state["entries"] = []
        state["has_more"] = False
        state["next_offset"] = 0
        preview.setVisibility(View.GONE)
        render_rows()
        load_page(0)

    def on_entry_click(entry):
        if entry.get("kind") == "directory":
            child = state["root"].rstrip("/") + "/" + str(entry.get("path"))
            state["root"] = child
            state["entries"] = []
            state["has_more"] = False
            render_rows()
            load_page(0)
            return
        remote_path = state["root"].rstrip("/") + "/" + str(entry.get("path"))
        name = str(entry.get("path")).rsplit("/", 1)[-1]
        is_image = name.lower().endswith((".jpg", ".jpeg", ".png", ".webp", ".gif"))
        set_status("Downloading %s ..." % name)
        preview.setVisibility(View.GONE)

        def work():
            transfer = json.loads(bootstrap.call_feature("files", "upload", remote_path))
            if not transfer.get("ok"):
                raise RuntimeError(transfer.get("error", "upload failed"))
            url = transfer.get("url")
            if not url:
                raise RuntimeError("upload returned no URL")
            client_service.download_remote_to_file(url, name)
            bitmap = None
            if is_image:
                encoded = client_service.download_transfer_base64(url)
                data = base64.b64decode(encoded)
                bitmap = BitmapFactory.decodeByteArray(data, 0, len(data))
            return bitmap

        def apply_ok(bitmap):
            if bitmap is not None:
                preview.setImageBitmap(bitmap)
                preview.setVisibility(View.VISIBLE)
            set_status("Saved to app script directory downloads/")

        def apply_error(error):
            set_status("Download failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    def on_load_more():
        if state["has_more"] and not state["loading"]:
            load_page(state["next_offset"])

    def on_up():
        boundary = state["boundary"]
        parent = state["root"].rstrip("/").rsplit("/", 1)[0] or "/"
        if len(parent) < len(boundary) or not parent.startswith(boundary.rstrip("/")):
            parent = boundary
        state["root"] = parent
        state["entries"] = []
        state["has_more"] = False
        render_rows()
        load_page(0)

    def on_refresh():
        state["entries"] = []
        state["has_more"] = False
        render_rows()
        load_page(0)

    pyui_kit.click(load_more, on_load_more)

    # 底部：页码输入 + Up / Refresh。
    bottom_box = LinearLayout(context)
    bottom_box.setOrientation(LinearLayout.VERTICAL)
    limit_edit = pyui_kit.make_edit(context, "100", number=True)
    bottom_box.addView(limit_edit, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
    nav_row = pyui_kit.make_hrow(context)
    up_button = pyui_kit.make_button(context, "Up", on_up)
    refresh_button = pyui_kit.make_button(context, "Refresh", on_refresh)
    nav_row.addView(up_button, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
    nav_row.addView(refresh_button, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
    nav_box_params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
    nav_box_params.topMargin = pyui_kit.dp(context, 6)
    bottom_box.addView(nav_row, nav_box_params)
    page.bottom.addView(
        bottom_box,
        LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
    )

    def on_target(_key, topic, cfg):
        page.set_target("target topic: " + topic)
        if state["topic"] is None:
            state["topic"] = topic
            reset_to(str(cfg.get("remote_root") or "/data/data"))
        elif topic != state["topic"]:
            # 切目标：自动落到新目标的 remote_root 并刷新。
            state["topic"] = topic
            reset_to(str(cfg.get("remote_root") or "/data/data"))

    pyui_kit.watch_target(page.root, on_target)
    return page.root
