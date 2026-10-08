"""Remote filesystem feature. Replaceable at runtime."""

import client_service
import json

FEATURE = {"name": "files", "title": "Files", "version": 1, "actions": ["scan", "upload"],
           "ui": "python", "icon": "files"}

# 可交给 audio feature 在目标端播放的扩展名（feature 互操作白名单）。
AUDIO_EXTENSIONS = (".mp3", ".wav", ".aac", ".m4a", ".ogg", ".oga", ".flac", ".opus", ".amr")


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
    # 目录遍历按 limit 有界，20s 足够；显式超时，不吃设备默认值。
    result = client_service.rpc(build_scan_code(root, offset, limit), timeout=20)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def upload(remote_path):
    # 上传走目标端 OpenAPI，大文件慢，给 120s；设备默认 timeout 不应掐断它。
    result = client_service.rpc(build_upload_code(remote_path), timeout=120)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘远程文件浏览器：状态行/列表（中部滚动）+ 页码/Up/Refresh（底部）。

    切目标后自动切到新目标的 remote_root 并刷新；点目录进入，点文件经
    OpenAPI 通道下载到控制机 downloads/（图片直接显示预览）。
    """
    import base64
    import time
    import bootstrap
    import pyui_kit
    from android.graphics import BitmapFactory
    from android.view import View
    from android.widget import LinearLayout, TextView

    page = pyui_kit.Page(context, "Files", "Browse and download files on the target")
    state = {"root": "/data/data", "boundary": "/data/data", "limit": 100,
             "entries": [], "has_more": False, "next_offset": 0, "topic": None,
             "loading": False}

    # 状态/进度不再放进可滚动的列表区：借用页面固定副标题行
    # （"Browse and download files on the target"），列表上下滚也不会把速度滚没。
    def set_status(text):
        page.set_subtitle(text)

    # 图片预览同样走原生缩放控件（与相机页一致），失败自动回退静态图。
    preview = pyui_kit.make_zoom_image(context, height_dp=240)
    page.add(preview, top=6)

    root_line = pyui_kit.make_text(
        context, "", size=12, color=pyui_kit.MUTED, selectable=True
    )
    page.add(root_line, top=4)

    rows_box = LinearLayout(context)
    rows_box.setOrientation(LinearLayout.VERTICAL)
    page.add(rows_box, top=4)

    load_more = pyui_kit.make_button(context, "Load more", lambda: None)
    load_more.setVisibility(View.GONE)
    page.add(load_more, top=6)

    def page_size():
        try:
            return max(1, min(int(str(limit_edit.getText()) or "100"), 10000))
        except ValueError:
            return 100

    def make_tag(label, handler):
        tag = TextView(context)
        tag.setText(label)
        tag.setTextSize(12)
        tag.setTextColor(pyui_kit.parse_color(pyui_kit.SUB))
        tag.setPadding(pyui_kit.dp(context, 10), pyui_kit.dp(context, 6),
                       pyui_kit.dp(context, 10), pyui_kit.dp(context, 6))
        pyui_kit.click(tag, handler)
        return tag

    def render_rows():
        rows_box.removeAllViews()
        for entry in state["entries"]:
            is_dir = entry.get("kind") == "directory"
            is_audio = str(entry.get("path", "")).lower().endswith(AUDIO_EXTENSIONS)
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
            if is_dir:
                row.addView(make_tag("Open", lambda e=entry: on_entry_click(e)),
                            LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
            else:
                # feature 互操作：音频文件先给 Play（调 audio feature 在目标端播放），
                # 再给 Download；两个标签各自消费点击，不互相干扰。
                if is_audio:
                    row.addView(make_tag("\u25b6 Play", lambda e=entry: on_play_entry(e)),
                                LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
                row.addView(make_tag("Download", lambda e=entry: on_entry_click(e)),
                            LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
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

    def on_play_entry(entry):
        """feature 互操作：Files -> Audio，在目标端扬声器播放该音频文件。

        唯一允许的跨 feature 调用方式是 client_service.call_feature
        （JSON 入参/JSON 结果，异常已被结构化为 ok:false，不穿透）。
        """
        remote_path = state["root"].rstrip("/") + "/" + str(entry.get("path"))
        name = str(entry.get("path")).rsplit("/", 1)[-1]
        set_status("\u25b6 Playing %s on target ..." % name)

        def work():
            return client_service.call_feature("audio", "play", remote_path)

        def apply_ok(raw):
            try:
                result = json.loads(raw)
            except (TypeError, ValueError):
                set_status(str(raw)[:200])
                return
            if result.get("ok"):
                duration = result.get("duration_ms")
                set_status(
                    "\u25b6 Playing on target: %s%s"
                    % (name, (" \u00b7 %s ms" % duration) if duration else "")
                )
            else:
                errors = result.get("errors") or [result.get("error") or "play failed"]
                set_status("Play failed: %s" % "; ".join(str(e) for e in errors)[:200])

        def apply_error(error):
            set_status("Play failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

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
        set_status("\u2191 Uploading %s from target ..." % name)
        preview.setVisibility(View.GONE)

        def work():
            # 阶段一：目标端上传到中转服务。RPC 是阻塞调用，拿不到真实字节进度，
            # 用 3s 心跳汇报已等待时长（不静默即可，不要求实时）。
            started = time.time()

            def upload_beat():
                set_status("\u2191 Uploading %s from target ... %.0fs"
                           % (name, time.time() - started))

            stop_beat = pyui_kit.repeat_every(3, upload_beat)
            try:
                transfer = json.loads(bootstrap.call_feature("files", "upload", remote_path))
                if not transfer.get("ok"):
                    raise RuntimeError(transfer.get("error", "upload failed"))
                url = transfer.get("url")
                if not url:
                    raise RuntimeError("upload returned no URL")
                upload_elapsed = time.time() - started
            finally:
                stop_beat()

            # 阶段二：本机从中转服务下载，按真实字节数隔几秒汇报大小/速度。
            def make_download_cb(prefix, clock):
                def on_progress(got, total):
                    el = max(0.1, time.time() - clock)
                    speed_kb = got / 1024 / el
                    if total:
                        message = (
                            "%s %s: %.2f/%.2fMB \u00b7 %.0f%% \u00b7 %.0f KB/s \u00b7 %.0fs"
                            % (prefix, name, got / 1048576, total / 1048576,
                               got * 100 // total, speed_kb, el)
                        )
                    else:
                        message = (
                            "%s %s: %.2fMB \u00b7 %.0f KB/s \u00b7 %.0fs"
                            % (prefix, name, got / 1048576, speed_kb, el)
                        )
                    pyui_kit.on_main(lambda m=message: set_status(m))
                return on_progress

            dl_started = time.time()
            client_service.download_remote_to_file(
                url, name, progress=make_download_cb("\u2193 Downloading", dl_started)
            )
            dl_elapsed = time.time() - dl_started
            bitmap = None
            if is_image:
                preview_started = time.time()
                encoded = client_service.download_transfer_base64(
                    url, progress=make_download_cb("Loading preview", preview_started)
                )
                data = base64.b64decode(encoded)
                bitmap = BitmapFactory.decodeByteArray(data, 0, len(data))
            return {
                "bitmap": bitmap,
                "upload_elapsed": upload_elapsed,
                "dl_elapsed": dl_elapsed,
                "total_elapsed": time.time() - started,
            }

        def apply_ok(payload):
            if payload.get("bitmap") is not None:
                preview.setImageBitmap(payload["bitmap"])
                preview.setVisibility(View.VISIBLE)
            set_status(
                "Saved to downloads/  (\u2191%.0fs  \u2193%.0fs  total %.0fs)"
                % (payload["upload_elapsed"], payload["dl_elapsed"], payload["total_elapsed"])
            )

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

    def on_back():
        # Android 全局返回键：仍在浏览根子目录里就回上一层（不退出应用）；
        # 已在浏览根则返回 False，交给宿主走"连按两次退出"。
        current = state["root"].rstrip("/") or "/"
        boundary = state["boundary"].rstrip("/") or "/"
        if state["loading"] or current == boundary or current == "/":
            return False
        on_up()
        return True

    client_service.set_feature_back_handler("files", on_back)

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
