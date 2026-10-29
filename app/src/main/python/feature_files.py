"""Remote filesystem feature. Replaceable at runtime.

目标端只跑标准库代码（os/json/base64/hashlib），不 import 任何 Android 或
aliyun 模块，因此 Android / Linux / Windows 目标都能浏览、读取、删除。
下载优先走 aliyun 中转：目标只要能 import aliyun_git 就直接走快路径（不限
系统），不再弹确认条；导入不到或中转失败才自动退回 MQTT 分块读取
（256KiB/块）。
"""

import base64
import hashlib
import json
import os
import time

import client_service

FEATURE = {"name": "files", "title": "Files", "version": 4,
           "actions": ["probe", "scan", "read", "hash", "upload", "delete"],
           "ui": "python", "icon": "files"}

# 可交给 audio feature 在目标端播放的扩展名（feature 互操作白名单）。
AUDIO_EXTENSIONS = (".mp3", ".wav", ".aac", ".m4a", ".ogg", ".oga", ".flac", ".opus", ".amr")
# 能直接 decodeFile 预览的图片扩展名。
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")
# 单次 read RPC 的原始字节数：base64 后约 342KiB，稳在 broker 1MiB 硬线内。
DEFAULT_CHUNK = 256 << 10
# 目标没有 aliyun 时走 MQTT 分块下载；超过该大小的文件直接拒绝（太慢且易中断）。
MAX_FETCH_BYTES = 96 << 20
# 分块下载后按 sha256 校验的默认大小上限（再大就不做二次全量读取）。
DEFAULT_VERIFY_LIMIT = 8 << 20


def run():
    return FEATURE


# =============================== 目标端代码生成 ===============================

_PROBE_TEMPLATE = r'''import json, os, platform, sys

requested = @@ROOT@@
android = False
try:
    import android
    android = True
except Exception:
    android = False

# 传输能力探测：不按平台写死，任何目标只要能导入 aliyun_git 就能走中转。
try:
    import aliyun_git  # noqa: F401
except BaseException as _aliyun_exc:  # 含 SystemExit：缺配置时 aliyun_git 会 sys.exit
    aliyun_ready = False
    aliyun_error = repr(_aliyun_exc)
else:
    aliyun_ready = True
    aliyun_error = ""


def norm(path):
    try:
        return os.path.abspath(os.path.expanduser(str(path)))
    except Exception:
        return None


def is_dir(path):
    try:
        return bool(path) and os.path.isdir(path)
    except Exception:
        return False


roots = []
if os.name == "posix":
    roots.append("/")
else:
    for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        drive = letter + ":" + os.sep
        try:
            if os.path.isdir(drive):
                roots.append(drive)
        except Exception:
            pass
home = norm("~")
cwd = norm(os.getcwd())
requested_path = norm(requested) if requested else None
picked = None
for candidate in [requested_path, home, cwd] + roots:
    if is_dir(candidate):
        picked = candidate
        break
info = dict(os_name=os.name, sep=os.sep, python=sys.version.split()[0],
            host=platform.node(), platform=platform.platform(),
            android=bool(android), home=home, cwd=cwd, roots=roots,
            requested=requested_path, requested_exists=is_dir(requested_path),
            aliyun=bool(aliyun_ready), aliyun_error=aliyun_error)
if picked:
    info["ok"] = True
    info["root"] = picked
else:
    info["ok"] = False
    info["root"] = None
    info["error"] = "no readable directory (requested/home/cwd/roots all missing)"
r = json.dumps(info, ensure_ascii=False)
'''

_SCAN_TEMPLATE = r'''import json, os

raw_root = @@ROOT@@
start = @@OFFSET@@
limit = @@LIMIT@@
recursive = @@RECURSIVE@@
text_root = str(raw_root or "").strip()
root = os.path.abspath(os.path.expanduser(text_root)) if text_root else os.getcwd()
exists = os.path.isdir(root)
# Windows 的 abspath 与 realpath 可能给出长短两种形式（8.3 短名），
# 两种都算"浏览根内"，否则合法条目会被误判成越界而整页为空。
base_keys = []
for candidate in (os.path.abspath(root), os.path.realpath(root)):
    key = os.path.normcase(candidate)
    if key not in base_keys:
        base_keys.append(key)
prefix_keys = []
for key in base_keys:
    prefix_keys.append(key if key.endswith(os.sep) else key + os.sep)

items = []
errors = []
position = 0
has_more = False


def inside(path):
    # 防止 os.walk 跟着指向浏览根之外的软链接跑出去。
    for key in (os.path.normcase(os.path.abspath(path)),
                os.path.normcase(os.path.realpath(path))):
        for index, base in enumerate(base_keys):
            if key == base or key.startswith(prefix_keys[index]):
                return True
    return False


def relposix(path):
    try:
        value = os.path.relpath(path, root)
    except Exception:
        value = os.path.basename(path)
    return value.replace(os.sep, "/")


def note_error(path, exc):
    try:
        name = relposix(path) if path else ""
    except Exception:
        name = str(path or "")
    errors.append(dict(path=name, error=repr(exc)))


if not exists:
    r = json.dumps(dict(ok=False, root=root, exists=os.path.exists(root),
                        error="root is not a directory", items=[], errors=[],
                        has_more=False, next_offset=0, parent=None),
                   ensure_ascii=False)
else:
    def on_error(exc):
        note_error(getattr(exc, "filename", "") or "", exc)

    stop = False
    for current, dirs, names in os.walk(root, followlinks=False, onerror=on_error):
        if stop:
            break
        dirs.sort()
        names.sort()
        if not inside(current):
            dirs[:] = []
            continue
        entries = [(name, "directory") for name in dirs]
        entries.extend((name, "file") for name in names)
        entries.sort(key=lambda item: (item[1] != "directory", item[0].casefold()))
        for name, kind in entries:
            path = os.path.join(current, name)
            if not inside(path):
                position += 1
                continue
            try:
                stat = os.stat(path, follow_symlinks=False)
            except OSError as exc:
                note_error(path, exc)
                position += 1
                continue
            if position < start:
                position += 1
                continue
            if len(items) >= limit:
                has_more = True
                stop = True
                break
            items.append(dict(path=relposix(path), kind=kind, size=stat.st_size,
                              modified=stat.st_mtime,
                              link=bool(os.path.islink(path))))
            position += 1
        if stop or not recursive:
            break
    parent = os.path.dirname(root)
    if not parent or os.path.normcase(parent) == os.path.normcase(root):
        parent = None
    r = json.dumps(dict(ok=True, root=root, parent=parent, items=items,
                        errors=errors, has_more=has_more,
                        next_offset=position, os_name=os.name, sep=os.sep),
                   ensure_ascii=False)
'''

_READ_TEMPLATE = r'''import base64, json, os

path = os.path.abspath(os.path.expanduser(@@PATH@@))
offset = @@OFFSET@@
length = @@LENGTH@@
try:
    stat = os.stat(path)
    if os.path.isdir(path):
        raise OSError("is a directory: " + path)
    size = stat.st_size
    start = max(0, min(int(offset), size))
    with open(path, "rb") as handle:
        handle.seek(start)
        data = handle.read(int(length))
    r = json.dumps(dict(ok=True, path=path, size=size, offset=start,
                        length=len(data), eof=(start + len(data) >= size),
                        mtime=stat.st_mtime,
                        data=base64.b64encode(data).decode("ascii")),
                   ensure_ascii=False)
except Exception as exc:
    r = json.dumps(dict(ok=False, path=path, error=repr(exc)), ensure_ascii=False)
'''

_HASH_TEMPLATE = r'''import hashlib, json, os

path = os.path.abspath(os.path.expanduser(@@PATH@@))
try:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            block = handle.read(1048576)
            if not block:
                break
            digest.update(block)
            size += len(block)
    r = json.dumps(dict(ok=True, path=path, size=size, sha256=digest.hexdigest()),
                   ensure_ascii=False)
except Exception as exc:
    r = json.dumps(dict(ok=False, path=path, error=repr(exc)), ensure_ascii=False)
'''

_DELETE_TEMPLATE = r'''import json, os, stat as statmod

paths = @@PATHS@@
recursive = @@RECURSIVE@@
raw_boundary = @@BOUNDARY@@
boundary = os.path.abspath(os.path.expanduser(raw_boundary)) if raw_boundary else ""
base_keys = []
prefix_keys = []
if boundary and os.path.isdir(boundary):
    for candidate in (os.path.abspath(boundary), os.path.realpath(boundary)):
        key = os.path.normcase(candidate)
        if key not in base_keys:
            base_keys.append(key)
            prefix_keys.append(key if key.endswith(os.sep) else key + os.sep)


def matches(key):
    for index, base in enumerate(base_keys):
        if key == base or key.startswith(prefix_keys[index]):
            return True
    return False


def inside(target):
    # 只允许删浏览根之内的内容，且浏览根自身不能删；目录还要求真实路径也在根内，
    # 防止借指向根外的目录软链接把外部数据一起删掉。
    if not base_keys:
        return True
    lexical = os.path.normcase(os.path.abspath(target))
    real = os.path.normcase(os.path.realpath(target))
    if lexical in base_keys or real in base_keys:
        return False
    if not (matches(lexical) or matches(real)):
        return False
    if os.path.isdir(target) and not os.path.islink(target):
        return matches(real)
    return True


def retire(target):
    try:
        os.remove(target)
        return
    except PermissionError:
        os.chmod(target, statmod.S_IWRITE)
        os.remove(target)


def prune(target):
    try:
        os.rmdir(target)
        return
    except PermissionError:
        os.chmod(target, statmod.S_IWRITE)
        os.rmdir(target)


def remove_tree(target):
    for current, dirs, names in os.walk(target, topdown=False, followlinks=False):
        for name in names:
            retire(os.path.join(current, name))
        for name in dirs:
            prune(os.path.join(current, name))
    prune(target)


results = []
for raw in paths:
    target = os.path.abspath(os.path.expanduser(str(raw)))
    try:
        if not os.path.lexists(target):
            results.append(dict(path=target, ok=False, error="not found"))
            continue
        if not inside(target):
            results.append(dict(path=target, ok=False,
                                error="refused: outside browsing root or the root itself"))
            continue
        is_dir = os.path.isdir(target) and not os.path.islink(target)
        if is_dir:
            if not recursive:
                results.append(dict(path=target, ok=False, error="is a directory"))
                continue
            remove_tree(target)
            results.append(dict(path=target, ok=True, kind="directory"))
        else:
            retire(target)
            results.append(dict(path=target, ok=True, kind="file"))
    except Exception as exc:
        results.append(dict(path=target, ok=False, error=repr(exc)))

ok = True
for item in results:
    if not item["ok"]:
        ok = False
        break
r = json.dumps(dict(ok=ok, results=results, boundary=boundary), ensure_ascii=False)
'''

_UPLOAD_TEMPLATE = r'''import json, os

path = @@PATH@@
name = os.path.basename(path.replace("\\", "/").rstrip("/")) or "file"
try:
    import aliyun_git
except BaseException as exc:  # 含 SystemExit：缺配置时 aliyun_git 会 sys.exit
    r = json.dumps(dict(ok=False, unsupported=True,
                        error="aliyun_git unavailable on target: " + repr(exc)),
                   ensure_ascii=False)
else:
    try:
        url = aliyun_git.upload(path, file_path=name)
        r = json.dumps(dict(ok=True, url=url, name=name), ensure_ascii=False)
    except BaseException as exc:
        r = json.dumps(dict(ok=False, error=repr(exc)), ensure_ascii=False)
'''


def build_probe_code(root=""):
    return _PROBE_TEMPLATE.replace("@@ROOT@@", json.dumps(str(root or ""), ensure_ascii=False))


def build_scan_code(root, offset=0, limit=100, recursive=False):
    return (_SCAN_TEMPLATE
            .replace("@@ROOT@@", json.dumps(str(root or ""), ensure_ascii=False))
            .replace("@@OFFSET@@", str(max(0, int(offset))))
            .replace("@@LIMIT@@", str(max(1, min(int(limit), 10000))))
            .replace("@@RECURSIVE@@", repr(bool(recursive))))


def build_read_code(path, offset=0, length=DEFAULT_CHUNK):
    return (_READ_TEMPLATE
            .replace("@@PATH@@", json.dumps(str(path or ""), ensure_ascii=False))
            .replace("@@OFFSET@@", str(max(0, int(offset))))
            .replace("@@LENGTH@@", str(max(1, min(int(length), DEFAULT_CHUNK)))))


def build_hash_code(path):
    return (_HASH_TEMPLATE
            .replace("@@PATH@@", json.dumps(str(path or ""), ensure_ascii=False)))


def build_delete_code(paths, recursive=True, boundary=""):
    if isinstance(paths, str):
        paths = [paths]
    payload = [str(item) for item in (paths or [])]
    return (_DELETE_TEMPLATE
            .replace("@@PATHS@@", json.dumps(payload, ensure_ascii=False))
            .replace("@@RECURSIVE@@", repr(bool(recursive)))
            .replace("@@BOUNDARY@@", json.dumps(str(boundary or ""), ensure_ascii=False)))


def build_upload_code(remote_path):
    return (_UPLOAD_TEMPLATE
            .replace("@@PATH@@", json.dumps(str(remote_path or ""), ensure_ascii=False)))


# =============================== feature actions ===============================

def probe(root=""):
    result = client_service.rpc(build_probe_code(root), timeout=15)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def scan(root, offset=0, limit=100, recursive=False):
    # 目录遍历按 limit 有界，20s 足够；显式超时，不吃设备默认值。
    result = client_service.rpc(build_scan_code(root, offset, limit, recursive), timeout=20)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def read(path, offset=0, length=DEFAULT_CHUNK):
    result = client_service.rpc(build_read_code(path, offset, length), timeout=45)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def hash(path):  # noqa: A001 - feature action 名固定为 hash
    result = client_service.rpc(build_hash_code(path), timeout=60)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def delete(paths, boundary="", recursive=True):
    result = client_service.rpc(build_delete_code(paths, recursive, boundary), timeout=30)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def upload(remote_path):
    # 上传走目标端 OpenAPI，大文件慢，给 120s；设备默认 timeout 不应掐断它。
    result = client_service.rpc(build_upload_code(remote_path), timeout=120)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


# =============================== 本机下载（分块 RPC） ===============================

def downloads_dir():
    """本机下载目录：scripts/downloads（与 client_service 的 downloads 一致）。"""
    candidates = []
    try:
        import bootstrap
        root = os.path.dirname(str(bootstrap.update_dir() or ""))
        if root:
            candidates.append(os.path.join(root, "downloads"))
    except Exception:
        pass
    try:
        root = str(getattr(client_service, "_STATE", {}).get("files_dir") or "")
        if root:
            candidates.append(os.path.join(root, "downloads"))
    except Exception:
        pass
    candidates.append(os.path.join(os.getcwd(), "downloads"))
    for path in candidates:
        try:
            os.makedirs(path, exist_ok=True)
            return path
        except OSError:
            continue
    return os.getcwd()


def _safe_file_name(name):
    base = str(name or "").replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    cleaned = "".join(ch if (ch.isalnum() or ch in "._- ()[]") else "_" for ch in base)
    cleaned = cleaned.strip().strip(".")
    return cleaned or "download.bin"


def fetch(path, name="", progress=None, verify=None):
    """目标没有 aliyun 时用分块 RPC 把文件拉到本机 downloads/。

    返回 dict(ok, path, size, verified, elapsed)；校验失败抛 RuntimeError。
    """
    dest_name = _safe_file_name(name or path)
    dest = os.path.join(downloads_dir(), dest_name)
    partial = dest + ".cmqpart"
    started = time.time()
    offset = 0
    size = 0
    stall = 0
    handle = open(partial, "wb")
    try:
        while True:
            payload = json.loads(read(path, offset, DEFAULT_CHUNK))
            if not payload.get("ok"):
                raise RuntimeError(payload.get("error") or "target read failed")
            size = int(payload.get("size") or 0)
            if size > MAX_FETCH_BYTES:
                raise RuntimeError(
                    "file is %.1fMB, too large for MQTT transfer (limit %dMB)"
                    % (size / 1048576.0, MAX_FETCH_BYTES // (1 << 20))
                )
            chunk = base64.b64decode(payload.get("data") or "")
            if not chunk and not payload.get("eof"):
                stall += 1
                if stall >= 3:
                    raise RuntimeError("target read stalled at offset %d" % offset)
                time.sleep(0.3)
                continue
            stall = 0
            if chunk:
                handle.write(chunk)
                offset += len(chunk)
            if progress is not None:
                try:
                    progress(offset, size)
                except Exception:
                    pass
            if payload.get("eof") or offset >= size:
                break
        handle.flush()
    finally:
        try:
            handle.close()
        except Exception:
            pass
    verified = None
    limit = DEFAULT_VERIFY_LIMIT if verify is None else (0 if not verify else None)
    if size and (limit is None or size <= int(limit)):
        expected = json.loads(hash(path))
        digest_value = expected.get("sha256") if expected.get("ok") else ""
        if digest_value:
            digest = hashlib.sha256()
            with open(partial, "rb") as reader:
                while True:
                    block = reader.read(1048576)
                    if not block:
                        break
                    digest.update(block)
            verified = digest.hexdigest() == digest_value
            if not verified:
                try:
                    os.remove(partial)
                except OSError:
                    pass
                raise RuntimeError("checksum mismatch for %s" % dest_name)
    try:
        os.replace(partial, dest)
    except OSError:
        try:
            os.remove(partial)
        except OSError:
            pass
        raise
    return dict(ok=True, path=dest, size=size, verified=verified,
                elapsed=round(time.time() - started, 1))


# =============================== 自绘页面 ===============================

def build_view(context):
    """Chaquopy 自绘远程文件浏览器：状态行/列表（中部滚动）+ 页码/Up/Refresh（底部）。

    切目标时先 probe 目标（平台/家目录/根候选），按
    “目标配置的 remote_root → home → cwd → 系统根” 解析浏览根，
    因此在 Android / Linux / Windows 目标上都能落到可读目录。
    点目录进入，点文件下载（aliyun 优先，缺失时 MQTT 分块），图片直接预览；
    每个文件行有 Delete，删除前弹行内确认条，且拒绝删到浏览根之外。
    """
    import bootstrap
    import pyui_kit
    from android.graphics import BitmapFactory
    from android.view import View
    from android.widget import LinearLayout, TextView

    page = pyui_kit.Page(
        context, "Files", "Browse, download and delete files on any target"
    )
    # 左上角已经显示当前目标（别名），这里不再重复一行 topic 文字。
    page.target_line.setVisibility(View.GONE)
    state = {
        "topic": None, "key": None, "root": "", "boundary": "", "resolved": "",
        "entries": [], "has_more": False, "next_offset": 0, "limit": 100,
        "loading": False, "info": {}, "last_parent": None, "pending": None,
        "no_aliyun": False, "cache": {}, "roots": {}, "gen": 0,
    }

    def set_status(text):
        page.set_subtitle(text)

    def rpc_one_line(raw):
        """只取一行元数据（ok / 延迟 / server_from / req_id），不重复原始响应。"""
        return pyui_kit.result_summary(pyui_kit.rpc_dict(raw))

    # 图片预览同样走原生缩放控件（与相机页一致），失败自动回退静态图。
    preview = pyui_kit.make_zoom_image(context, height_dp=240)
    page.add(preview, top=6)

    root_line = pyui_kit.make_text(
        context, "", size=12, color=pyui_kit.MUTED, selectable=True
    )
    page.add(root_line, top=4)

    # 统一 RPC 结果区：与其它 feature 一致，展示最近一次 RPC 返回的 dict。
    rpc_line = pyui_kit.make_text(
        context, "", size=11, color=pyui_kit.MUTED, selectable=True
    )
    page.add(rpc_line, top=4)

    rows_box = LinearLayout(context)
    rows_box.setOrientation(LinearLayout.VERTICAL)
    page.add(rows_box, top=4)

    load_more = pyui_kit.make_button(context, "Load more", lambda: None)
    load_more.setVisibility(View.GONE)
    page.add(load_more, top=6)

    # ---- 底部：页码输入 / Up / Refresh / 删除确认条 ----
    bottom_box = LinearLayout(context)
    bottom_box.setOrientation(LinearLayout.VERTICAL)
    limit_edit = pyui_kit.make_edit(context, "100", number=True)
    bottom_box.addView(limit_edit, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
    nav_row = pyui_kit.make_hrow(context)
    up_button = pyui_kit.make_button(context, "Up", lambda: None)
    refresh_button = pyui_kit.make_button(context, "Refresh", lambda: None)
    nav_row.addView(up_button, LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
    nav_row.addView(refresh_button, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
    nav_params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
    nav_params.topMargin = pyui_kit.dp(context, 6)
    bottom_box.addView(nav_row, nav_params)

    confirm_bar = LinearLayout(context)
    confirm_bar.setOrientation(LinearLayout.VERTICAL)
    confirm_bar.setVisibility(View.GONE)
    confirm_text = pyui_kit.make_text(context, "", size=12, color=pyui_kit.SUB)
    confirm_bar.addView(confirm_text, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
    confirm_row = pyui_kit.make_hrow(context)
    confirm_yes = pyui_kit.make_button(context, "Delete", lambda: None)
    confirm_no = pyui_kit.make_button(context, "Cancel", lambda: None)
    confirm_row.addView(confirm_yes, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
    confirm_row.addView(confirm_no, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
    confirm_row_params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
    confirm_row_params.topMargin = pyui_kit.dp(context, 4)
    confirm_bar.addView(confirm_row, confirm_row_params)
    confirm_params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
    confirm_params.topMargin = pyui_kit.dp(context, 8)
    bottom_box.addView(confirm_bar, confirm_params)

    page.bottom.addView(
        bottom_box,
        LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()),
    )

    # ---- 路径工具（目标返回的 relative 一律是 POSIX 分隔） ----
    def norm_key(path):
        text = str(path or "").replace("\\", "/").rstrip("/")
        if (state["info"] or {}).get("os_name") == "nt":
            text = text.lower()
        return text

    def within(path):
        base = norm_key(state["boundary"])
        key = norm_key(path)
        if not key:
            return False
        return key == base or key.startswith(base + "/") or base == ""

    def join_path(root, relative):
        base = str(root or "")
        if (state["info"] or {}).get("os_name") == "nt":
            base = base.rstrip("\\/")
        else:
            base = base.rstrip("/")
        return base + "/" + str(relative or "").lstrip("/")

    def local_parent(path):
        text = str(path or "").replace("\\", "/").rstrip("/")
        if not text:
            return ""
        cut = text.rsplit("/", 1)[0]
        if not cut:
            return "/"
        if (state["info"] or {}).get("os_name") == "nt" and len(cut) == 2 and cut[1] == ":":
            return cut + "/"
        return cut

    def page_size():
        try:
            return max(1, min(int(str(limit_edit.getText()) or "100"), 10000))
        except ValueError:
            return 100

    def line_text(entry):
        size = int(entry.get("size") or 0)
        if entry.get("kind") == "directory":
            return "Folder"
        moment = entry.get("modified")
        stamp = ""
        try:
            stamp = time.strftime("%Y-%m-%d %H:%M", time.localtime(float(moment)))
        except (TypeError, ValueError, OSError):
            stamp = ""
        if stamp:
            return "%.1f KB \u00b7 %s" % (size / 1024.0, stamp)
        return "%s B" % size

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
            name = str(entry.get("path"))
            is_audio = name.lower().endswith(AUDIO_EXTENSIONS)
            row = LinearLayout(context)
            row.setOrientation(LinearLayout.VERTICAL)
            row.setPadding(0, pyui_kit.dp(context, 7), 0, pyui_kit.dp(context, 7))
            name_view = pyui_kit.make_text(
                context, name, size=14,
                color=pyui_kit.INK if is_dir else pyui_kit.SUB, bold=is_dir,
            )
            row.addView(name_view, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
            meta_row = pyui_kit.make_hrow(context)
            meta = pyui_kit.make_text(context, line_text(entry), size=11, color=pyui_kit.MUTED)
            meta_row.addView(meta, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
            if is_dir:
                meta_row.addView(make_tag("Open", lambda e=entry: on_entry_click(e)),
                                 LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
            else:
                # feature 互操作：音频文件先给 Play（调 audio feature 在目标端播放）。
                if is_audio:
                    meta_row.addView(make_tag("\u25b6 Play", lambda e=entry: on_play_entry(e)),
                                     LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
                meta_row.addView(make_tag("Download", lambda e=entry: on_entry_click(e)),
                                 LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
                meta_row.addView(make_tag("Delete", lambda e=entry: begin_delete(e)),
                                 LinearLayout.LayoutParams(pyui_kit.wrap(), pyui_kit.wrap()))
            row.addView(meta_row, LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap()))
            pyui_kit.click(row, (lambda e=entry: on_entry_click(e)))
            params = LinearLayout.LayoutParams(pyui_kit.match(), pyui_kit.wrap())
            params.bottomMargin = pyui_kit.dp(context, 2)
            rows_box.addView(row, params)
        info = state["info"] or {}
        platform = "android" if info.get("android") else (info.get("os_name") or "?")
        host = str(info.get("host") or "")
        root_show = state["root"] or "(no root)"
        relay = "relay: aliyun" if info.get("aliyun") else "relay: mqtt"
        if host:
            root_line.setText("%s  \u00b7  %s  \u00b7  %s  \u00b7  %s"
                              % (root_show, platform, host, relay))
        else:
            root_line.setText("%s  \u00b7  %s  \u00b7  %s"
                              % (root_show, platform, relay))
        load_more.setVisibility(View.VISIBLE if state["has_more"] else View.GONE)

    def cache_key(root):
        return "%s|%s" % (state["topic"], norm_key(root))

    def cache_current_directory():
        if not state["root"]:
            return
        state["cache"][cache_key(state["root"])] = {
            "entries": list(state["entries"]),
            "has_more": state["has_more"],
            "next_offset": state["next_offset"],
            "limit": state["limit"],
            "last_parent": state["last_parent"],
            "scroll_y": int(page.scroll.getScrollY()),
        }

    def restore_directory(root):
        cached = state["cache"].get(cache_key(root))
        if cached is None:
            return False
        state["root"] = root
        state["entries"] = list(cached["entries"])
        state["has_more"] = cached["has_more"]
        state["next_offset"] = cached["next_offset"]
        state["limit"] = cached["limit"]
        state["last_parent"] = cached.get("last_parent")
        render_rows()
        set_status(
            "Loaded %d files%s"
            % (len(state["entries"]), " (more below)" if state["has_more"] else "")
        )
        scroll_y = cached["scroll_y"]
        pyui_kit.on_main(lambda: page.scroll.scrollTo(0, scroll_y))
        return True

    def hide_pending():
        state["pending"] = None
        confirm_bar.setVisibility(View.GONE)

    def load_page(start, reset=False):
        if state["loading"] or not state["root"]:
            return
        state["loading"] = True
        generation = state["gen"]
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
            if generation != state["gen"]:
                return  # 切目标前发出的扫描：结果已过期，不能覆盖新目标
            state["loading"] = False
            if not isinstance(result, dict):
                set_status("Invalid scan response")
                return
            if not result.get("ok"):
                if start == 0:
                    state["entries"] = []
                state["has_more"] = False
                render_rows()
                fallback = state.get("resolved")
                if (start == 0 and result.get("exists") is False
                        and fallback and norm_key(fallback) != norm_key(scan_root)):
                    set_status("%s not found, falling back to %s" % (scan_root, fallback))
                    reset_to(fallback)
                    return
                set_status(result.get("error", "scan failed"))
                return
            page_items = result.get("items") or []
            if start == 0:
                state["entries"] = list(page_items)
                state["last_parent"] = result.get("parent")
            else:
                state["entries"] = state["entries"] + list(page_items)
            state["has_more"] = bool(result.get("has_more"))
            state["next_offset"] = int(result.get("next_offset", start + len(page_items)))
            render_rows()
            errors = result.get("errors") or []
            note = ""
            if errors:
                note = " \u00b7 %d error(s): %s" % (len(errors), errors[0].get("error", "?"))
            set_status(
                "Loaded %d files%s \u00b7 %s%s"
                % (len(state["entries"]), " (more below)" if state["has_more"] else "",
                   rpc_one_line(result), note[:120])
            )

        def apply_error(error):
            if generation != state["gen"]:
                return
            state["loading"] = False
            set_status("Invalid scan response: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    def reset_to(new_root):
        state["root"] = str(new_root or "")
        state["boundary"] = str(new_root or "")
        state["entries"] = []
        state["has_more"] = False
        state["next_offset"] = 0
        state["last_parent"] = None
        hide_pending()
        preview.setVisibility(View.GONE)
        if not restore_directory(state["root"]):
            page.scroll.scrollTo(0, 0)
            render_rows()
            load_page(0)

    def on_play_entry(entry):
        """feature 互操作：Files -> Audio，在目标端扬声器播放该音频文件。"""
        remote_path = join_path(state["root"], entry.get("path"))
        name = str(entry.get("path")).replace("\\", "/").rsplit("/", 1)[-1]
        set_status("\u25b6 Playing %s on target ..." % name)

        def work():
            return client_service.call_feature("audio", "play", remote_path)

        def apply_ok(raw):
            rpc_line.setText(rpc_one_line(raw))
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

    def begin_delete(entry):
        state["pending"] = entry
        confirm_text.setText("Delete %s ?" % entry.get("path"))
        confirm_yes.setText("Delete")
        confirm_no.setText("Cancel")
        confirm_bar.setVisibility(View.VISIBLE)
        set_status("Confirm delete: %s (no undo)" % entry.get("path"))

    def cancel_pending():
        hide_pending()
        set_status("Loaded %d files%s"
                   % (len(state["entries"]), " (more below)" if state["has_more"] else ""))

    def confirm_delete(entry):
        remote_path = join_path(state["root"], entry.get("path"))
        boundary = state["boundary"]
        set_status("Deleting %s ..." % entry.get("path"))

        def work():
            raw = bootstrap.call_feature("files", "delete", [remote_path], boundary, True)
            return json.loads(raw) if isinstance(raw, str) else raw

        def apply_ok(result):
            rpc_line.setText(rpc_one_line(result))
            results = result.get("results") or []
            first = results[0] if results else {}
            if result.get("ok") and first.get("ok"):
                state["entries"] = [item for item in state["entries"] if item is not entry]
                state["cache"].pop(cache_key(state["root"]), None)
                render_rows()
                set_status("Deleted %s" % entry.get("path"))
            else:
                set_status("Delete failed: %s"
                           % (first.get("error") or result.get("error") or "unknown"))

        def apply_error(error):
            set_status("Delete failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(confirm_yes,))

    def confirm_pending():
        """底部确认条只剩删除一种确认。"""
        entry = state.get("pending")
        if not entry:
            return
        hide_pending()
        confirm_delete(entry)

    def on_entry_click(entry):
        if entry.get("kind") == "directory":
            cache_current_directory()
            hide_pending()
            child = join_path(state["root"], entry.get("path"))
            state["root"] = child
            state["entries"] = []
            state["has_more"] = False
            state["last_parent"] = None
            page.scroll.scrollTo(0, 0)
            render_rows()
            load_page(0)
            return
        download_entry(entry)

    def make_download_cb(prefix, clock, name):
        def on_progress(got, total):
            elapsed = max(0.1, time.time() - clock)
            speed_kb = got / 1024.0 / elapsed
            if total:
                message = ("%s %s: %.2f/%.2fMB \u00b7 %.0f%% \u00b7 %.0f KB/s \u00b7 %.0fs"
                           % (prefix, name, got / 1048576.0, total / 1048576.0,
                              got * 100 // max(total, 1), speed_kb, elapsed))
            else:
                message = ("%s %s: %.2fMB \u00b7 %.0f KB/s \u00b7 %.0fs"
                           % (prefix, name, got / 1048576.0, speed_kb, elapsed))
            pyui_kit.on_main(lambda m=message: set_status(m))
        return on_progress

    def download_entry(entry, transfer=None):
        # 目标能 import aliyun_git 就直接走 aliyun 快路径（不弹确认），
        # 导入不到才走 MQTT；中转失败同样在 work() 内自动回退 MQTT。
        if transfer is None:
            transfer = ("aliyun" if bool((state["info"] or {}).get("aliyun"))
                        else "mqtt")
        remote_path = join_path(state["root"], entry.get("path"))
        name = str(entry.get("path")).replace("\\", "/").rsplit("/", 1)[-1]
        is_image = name.lower().endswith(IMAGE_EXTENSIONS)
        use_aliyun = transfer == "aliyun"
        set_status("\u2191 Transferring %s%s ..."
                   % (name, " via aliyun" if use_aliyun else " via MQTT"))
        preview.setVisibility(View.GONE)

        def work():
            started = time.time()
            method = "aliyun" if use_aliyun else "mqtt"
            local = ""
            size = 0
            transfer_raw = ""
            reason = ""
            if use_aliyun:
                def beat():
                    set_status("\u2191 Uploading %s from target ... %.0fs"
                               % (name, time.time() - started))

                stop_beat = pyui_kit.repeat_every(3, beat)
                try:
                    transfer_raw = bootstrap.call_feature("files", "upload", remote_path)
                    transfer = json.loads(transfer_raw) if isinstance(transfer_raw, str) else transfer_raw
                    if not isinstance(transfer, dict):
                        transfer = {"ok": False, "error": "non-JSON upload result"}
                    if transfer.get("ok") and transfer.get("url"):
                        try:
                            saved_raw = client_service.download_remote_to_file(
                                transfer["url"], name,
                                progress=make_download_cb("\u2193 Downloading",
                                                          time.time(), name),
                            )
                            saved = json.loads(saved_raw) if isinstance(saved_raw, str) else saved_raw
                            if not isinstance(saved, dict) or not saved.get("ok", True):
                                raise RuntimeError((saved or {}).get("error") or "local download failed")
                            local = str(saved.get("path") or "")
                            size = int(saved.get("size") or 0)
                        except Exception as exc:
                            # 中转下载失败（跨网/代理）时退回 MQTT 分块，不让下载彻底失败。
                            reason = str(exc)
                            method = "mqtt"
                    else:
                        if transfer.get("unsupported"):
                            state["no_aliyun"] = True
                        reason = str(transfer.get("error") or "target upload unavailable")
                        method = "mqtt"
                finally:
                    stop_beat()
            if method == "mqtt":
                info = fetch(
                    remote_path, name,
                    progress=make_download_cb("\u2193 MQTT", time.time(), name),
                )
                local = info.get("path", "")
                size = int(info.get("size") or 0)
            bitmap = None
            if is_image and local and os.path.isfile(local):
                bitmap = BitmapFactory.decodeFile(local)
            return dict(bitmap=bitmap, local=local, size=size, method=method,
                        reason=reason, transfer_raw=transfer_raw,
                        elapsed=time.time() - started)

        def apply_ok(payload):
            rpc_line.setText(rpc_one_line(payload.get("transfer_raw") or ""))
            if payload.get("bitmap") is not None:
                preview.setImageBitmap(payload["bitmap"])
                preview.setVisibility(View.VISIBLE)
            status = ("Saved %s \u00b7 %.2fMB \u00b7 %s \u00b7 %.0fs"
                      % (name, (payload.get("size") or 0) / 1048576.0,
                         "aliyun" if payload.get("method") == "aliyun" else "mqtt",
                         payload.get("elapsed") or 0))
            if payload.get("reason"):
                status += " \u00b7 fallback: %s" % str(payload["reason"])[:120]
            set_status(status)

        def apply_error(error):
            set_status("Download failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    def on_load_more():
        if state["has_more"] and not state["loading"]:
            load_page(state["next_offset"])

    def on_up():
        if state["loading"] or not state["root"]:
            return
        current = state["root"]
        parent = state.get("last_parent") or ""
        if not parent or not within(parent) or norm_key(parent) == norm_key(current):
            parent = local_parent(current)
        if not parent or not within(parent) or norm_key(parent) == norm_key(current):
            set_status("Already at the top of %s" % state["boundary"])
            return
        cache_current_directory()
        hide_pending()
        if restore_directory(parent):
            return
        state["root"] = parent
        state["entries"] = []
        state["has_more"] = False
        state["last_parent"] = None
        page.scroll.scrollTo(0, 0)
        render_rows()
        load_page(0)

    def on_back():
        # Android 全局返回键：删除确认条优先关掉；仍在浏览根子目录里就回上一层；
        # 已在浏览根则返回 False，交给宿主走"连按两次退出"。
        if state["pending"]:
            cancel_pending()
            return True
        current = state["root"]
        boundary = state["boundary"]
        if state["loading"] or not current or norm_key(current) == norm_key(boundary):
            return False
        on_up()
        return True

    client_service.set_feature_back_handler("files", on_back)

    def on_refresh():
        state["cache"].pop(cache_key(state["root"]), None)
        state["entries"] = []
        state["has_more"] = False
        state["last_parent"] = None
        hide_pending()
        page.scroll.scrollTo(0, 0)
        render_rows()
        load_page(0)

    pyui_kit.click(load_more, on_load_more)
    pyui_kit.click(up_button, on_up)
    pyui_kit.click(refresh_button, on_refresh)
    pyui_kit.click(confirm_yes, confirm_pending)
    pyui_kit.click(confirm_no, cancel_pending)

    def on_target(key, topic, cfg):
        if topic == state["topic"] and key == state["key"]:
            return
        if state["topic"] and state["root"]:
            state["roots"][state["topic"]] = state["root"]
            cache_current_directory()
        state["topic"] = topic
        state["key"] = key
        state["gen"] += 1
        state["loading"] = False
        state["info"] = {}
        state["entries"] = []
        state["has_more"] = False
        state["last_parent"] = None
        state["no_aliyun"] = False
        hide_pending()
        preview.setVisibility(View.GONE)
        render_rows()
        configured = str((cfg or {}).get("remote_root") or "").strip()
        generation = state["gen"]
        set_status("Probing %s ..." % (configured or topic))

        def work():
            raw = bootstrap.call_feature("files", "probe", configured)
            return json.loads(raw) if isinstance(raw, str) else raw

        def apply_ok(info):
            if generation != state["gen"]:
                return
            if not isinstance(info, dict):
                info = {"ok": False, "error": "invalid probe result"}
            state["info"] = info
            if not info.get("ok"):
                state["root"] = ""
                render_rows()
                set_status("Probe failed: %s" % (info.get("error") or "unknown"))
                return
            state["resolved"] = str(info.get("root") or "")
            remembered = state["roots"].get(topic) or state["roots"].get(key)
            reset_to(remembered or state["resolved"])

        def apply_error(error):
            set_status("Probe failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    pyui_kit.watch_target(page.root, on_target)
    return page.root
