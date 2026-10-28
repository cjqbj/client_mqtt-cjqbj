# ==================== MCP 文件工具：经 MQTT RPC 在「远端」读写/编辑 ====================
#
# 结构说明（与 pty_* 工具的区别）：
#   - pty 通道：ai_bridge.run() 把命令行敲进远端交互 shell（占 busy 锁、
#     受 TTY 行规程与引号影响）；
#   - rpc 通道：ai_bridge.transport.request(code) 把 Python 源码发到远端
#     server_mqtt.py，在远端进程里 exec 并回传 r。不经过 shell、不占 busy
#     锁，就是 client_mqtt.rpc(code, topic) 使用的同一条路；
#   - 本文件的 fs_* 工具走第二条：实现函数（_fs_op_*）由客户端打包成源码，
#     首次调用在远端 exec 并缓存，之后每次只发少量的调用参数。因此路径
#     语义完全是**远端机器**的路径（例如 /root/build_xime_home/...），
#     而不是 pty 客户端本机。
#
# 权限模型：
#   - 读写默认都可作用于远端任意路径。pty_run 本就能执行任意代码，给写操作
#     加根限制没有实际意义；
#   - --fs-read-only：可选的全局只读开关（默认关闭），写家族全部禁用；
#   - 单次读写上限 5MB；grep 有文件数与耗时上限，避免拖死远端进程。

import base64
import fnmatch
import glob as _globmod
import hashlib
import inspect
import json

_FS_READ_ONLY = globals().setdefault('_FS_READ_ONLY', False)  # 只读模式：写家族一律拒绝
_FS_LOCK = globals().setdefault('_FS_LOCK', threading.RLock())  # 客户端串行化打包/配置
_FS_READ_MAX_BYTES = 5 * 1024 * 1024
_FS_WRITE_MAX_BYTES = 5 * 1024 * 1024
_FS_READ_MAX_LINES = 5000
_FS_LINE_MAX_CHARS = 2000
_FS_GREP_MAX_BYTES = 2 * 1024 * 1024
_FS_GREP_MAX_FILES = 20000
_FS_GREP_MAX_SECONDS = 5.0
_FS_GREP_SKIP_DIRS = frozenset((
    ".git", ".hg", ".svn", "__pycache__", ".venv", "venv", "node_modules",
    ".gradle", "build", "dist", ".idea", ".mypy_cache", ".pytest_cache",
))
# 单次远端请求的超时（含 grep 的 5 秒扫描窗口与网络往返）
_FS_CALL_TIMEOUT = globals().setdefault('_FS_CALL_TIMEOUT', 60.0)
# 远端源码包缓存：第一次调用时打包并缓存，避免每次 inspect 重新拼
_FS_SRC_CACHE = globals().setdefault('_FS_SRC_CACHE', None)


def _fs_fail(msg):
    return {"ok": False, "error": str(msg)}


def _fs_abs(path, base=None):
    """相对路径按 base（默认进程 cwd）解析成绝对路径。

    这段代码在远端执行，所以 cwd 就是远端 server_mqtt.py 进程的工作目录。
    """
    p = str(path or "").strip() or "."
    p = os.path.expanduser(p)
    if not os.path.isabs(p):
        p = os.path.join(base or os.getcwd(), p)
    return os.path.abspath(p)


def _fs_configure(read_only=False):
    """配置只读开关（main 在挂载 MCP 前调用；也可 RPC 热调）。"""
    global _FS_READ_ONLY
    _FS_READ_ONLY = bool(read_only)
    return {"ok": True, "read_only": _FS_READ_ONLY}


def _fs_require_writable():
    """写家族统一入口：只读模式返回错误文本，否则 None。"""
    if _FS_READ_ONLY:
        return "只读模式（--fs-read-only）：写操作已禁用"
    return None


def _fs_require_file(path):
    real = os.path.realpath(_fs_abs(path))
    if not os.path.exists(real):
        return "文件不存在: %s" % real
    if not os.path.isfile(real):
        return "不是普通文件: %s" % real
    return None


def _fs_decode(data, path):
    if b"\x00" in data[:8192]:
        return None, "看起来是二进制文件（含 NUL 字节），拒绝按文本处理: %s" % path
    try:
        return data.decode("utf-8"), None
    except UnicodeDecodeError as exc:
        return None, "不是 UTF-8 文本，拒绝按文本处理: %s (%s)" % (path, exc)


def _fs_clip_line(line):
    if len(line) <= _FS_LINE_MAX_CHARS:
        return line
    return line[:_FS_LINE_MAX_CHARS] + "…[本行超长已截断]"


def _fs_count_lines(text):
    if not text:
        return 0
    return len(text.splitlines())


def _fs_walk_files(base, name_glob=""):
    """遍历待搜索文件：单文件直接返回，目录按跳过表剪枝。"""
    if os.path.isfile(base):
        yield base
        return
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in _FS_GREP_SKIP_DIRS]
        for name in filenames:
            full = os.path.join(dirpath, name)
            if name_glob:
                rel = os.path.relpath(full, base).replace(os.sep, "/")
                if not (fnmatch.fnmatch(name, name_glob)
                        or fnmatch.fnmatch(rel, name_glob)):
                    continue
            yield full


# ---------------- 实现层（在远端 exec 执行，勿依赖客户端状态） ----------------

def _fs_op_read(file_path, offset=0, limit=2000):
    err = _fs_require_file(file_path)
    if err:
        return _fs_fail(err)
    real = os.path.realpath(_fs_abs(file_path))
    try:
        size = os.path.getsize(real)
        if size > _FS_READ_MAX_BYTES:
            return _fs_fail("文件过大（%d 字节，上限 %d）：请用 fs_grep 定位后分段读取"
                            % (size, _FS_READ_MAX_BYTES))
        with open(real, "rb") as f:
            data = f.read(_FS_READ_MAX_BYTES + 1)
    except OSError as exc:
        return _fs_fail("读取失败: %s" % exc)
    text, err = _fs_decode(data, real)
    if err:
        return _fs_fail(err)
    lines = text.splitlines()
    total = len(lines)
    off = max(0, int(offset))
    lim = max(1, min(int(limit), _FS_READ_MAX_LINES))
    chunk = lines[off:off + lim]
    content = "\n".join("%6d→%s" % (off + i + 1, _fs_clip_line(line))
                        for i, line in enumerate(chunk))
    return {
        "ok": True,
        "file_path": real,
        "content": content,
        "start_line": off + 1 if chunk else 0,
        "total_lines": total,
        "returned_lines": len(chunk),
        "truncated": off + lim < total,
        "bytes": len(data),
    }


def _fs_op_ls(path=".", show_hidden=True):
    root = _fs_abs(path)
    if not os.path.isdir(root):
        return _fs_fail("不是目录或不存在: %s" % root)
    entries = []
    try:
        with os.scandir(root) as it:
            for entry in it:
                if not show_hidden and entry.name.startswith("."):
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                    size, mtime = st.st_size, st.st_mtime
                except OSError:
                    size, mtime = None, None
                if entry.is_symlink():
                    kind = "link"
                elif entry.is_dir():
                    kind = "dir"
                elif entry.is_file():
                    kind = "file"
                else:
                    kind = "other"
                entries.append({"name": entry.name, "type": kind,
                                "size": size, "mtime": mtime})
    except OSError as exc:
        return _fs_fail("读取目录失败: %s" % exc)
    entries.sort(key=lambda x: (x["type"] != "dir", x["name"].lower()))
    text = "\n".join(e["name"] + ("/" if e["type"] == "dir" else "") for e in entries)
    return {"ok": True, "path": root, "total": len(entries),
            "entries": entries, "text": text}


def _fs_op_glob(pattern, path=".", max_results=200):
    if not str(pattern or "").strip():
        return _fs_fail("pattern 不能为空")
    root = _fs_abs(path)
    pat = str(pattern)
    full = pat if os.path.isabs(pat) else os.path.join(root, pat)
    cap = max(1, min(int(max_results), 1000))
    try:
        found = _globmod.glob(full, recursive=True)
    except OSError as exc:
        return _fs_fail("查找失败: %s" % exc)
    items = []
    for p in found:
        if not os.path.isfile(p):
            continue
        try:
            st = os.stat(p)
        except OSError:
            continue
        items.append({"path": os.path.abspath(p),
                      "size": st.st_size, "mtime": st.st_mtime})
    items.sort(key=lambda x: x["mtime"], reverse=True)
    total = len(items)
    items = items[:cap]
    return {"ok": True, "path": root, "pattern": pat, "total": total,
            "matches": items, "truncated": total > cap,
            "text": "\n".join(x["path"] for x in items)}


def _fs_op_grep(pattern, path=".", glob="", ignore_case=False,
                output_mode="content", context=0, head_limit=100, offset=0):
    mode = str(output_mode or "content").strip().lower()
    if mode not in ("content", "files_with_matches", "count"):
        return _fs_fail("output_mode 只能是 content / files_with_matches / count")
    try:
        rx = re.compile(str(pattern), re.IGNORECASE if ignore_case else 0)
    except re.error as exc:
        return _fs_fail("正则无效: %s" % exc)
    base = _fs_abs(path)
    if not os.path.exists(base):
        return _fs_fail("路径不存在: %s" % base)
    cap = max(1, min(int(head_limit), 500))
    skip = max(0, int(offset))
    ctx = max(0, min(int(context), 20))
    deadline = time.monotonic() + _FS_GREP_MAX_SECONDS
    rows = []
    files_hit = []
    counts = []
    scanned = 0
    truncated = False

    for fp in _fs_walk_files(base, str(glob or "")):
        if scanned >= _FS_GREP_MAX_FILES or time.monotonic() > deadline:
            truncated = True
            break
        scanned += 1
        try:
            if os.path.getsize(fp) > _FS_GREP_MAX_BYTES:
                continue
            with open(fp, "rb") as f:
                data = f.read(_FS_GREP_MAX_BYTES + 1)
        except OSError:
            continue
        if b"\x00" in data[:4096]:
            continue
        text = data.decode("utf-8", "replace")
        lines = text.splitlines()
        hit_lines = [i for i, line in enumerate(lines) if rx.search(line)]
        if not hit_lines:
            continue
        real = os.path.abspath(fp)
        files_hit.append(real)
        counts.append((real, len(hit_lines)))
        if mode != "content":
            continue
        hit_set = set(hit_lines)
        keep = set()
        for i in hit_lines:
            keep.update(range(max(0, i - ctx), min(len(lines), i + ctx + 1)))
        last = None
        for i in sorted(keep):
            if last is not None and i != last + 1:
                rows.append("--")
            mark = ":" if i in hit_set else "-"
            rows.append("%s%s%d%s%s" % (real, mark, i + 1, mark, lines[i]))
            last = i

    if mode == "files_with_matches":
        rows = files_hit
    elif mode == "count":
        rows = ["%s:%d" % (p, n) for p, n in counts]
    total = len(rows)
    picked = rows[skip:skip + cap]
    return {
        "ok": True,
        "mode": mode,
        "pattern": pattern,
        "path": base,
        "scanned_files": scanned,
        "matched_files": len(files_hit),
        "total": total,
        "returned": len(picked),
        "truncated": truncated or total > skip + cap,
        "text": "\n".join(picked),
    }


def _fs_op_write(file_path, content):
    target = _fs_abs(file_path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not isinstance(content, str):
        return _fs_fail("content 必须是字符串")
    data = content.encode("utf-8")
    if len(data) > _FS_WRITE_MAX_BYTES:
        return _fs_fail("内容过大（%d 字节，上限 %d）" % (len(data), _FS_WRITE_MAX_BYTES))
    parent = os.path.dirname(target) or os.getcwd()
    if not os.path.isdir(parent):
        return _fs_fail("父目录不存在: %s" % parent)
    if os.path.isdir(target):
        return _fs_fail("目标是目录，不能整体写入: %s" % target)
    created = not os.path.exists(target)
    try:
        with _FS_LOCK:
            with open(target, "wb") as f:
                f.write(data)
    except OSError as exc:
        return _fs_fail("写入失败: %s" % exc)
    return {"ok": True, "file_path": target, "bytes": len(data),
            "created": created, "total_lines": _fs_count_lines(content)}


def _fs_op_edit(file_path, old_string, new_string, replace_all=False):
    err = _fs_require_file(file_path)
    if err:
        return _fs_fail(err)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not isinstance(old_string, str) or old_string == "":
        return _fs_fail("old_string 不能为空")
    if not isinstance(new_string, str):
        return _fs_fail("new_string 必须是字符串")
    real = os.path.realpath(_fs_abs(file_path))
    try:
        with open(real, "rb") as f:
            data = f.read(_FS_READ_MAX_BYTES + 1)
    except OSError as exc:
        return _fs_fail("读取失败: %s" % exc)
    text, err = _fs_decode(data, real)
    if err:
        return _fs_fail(err)
    count = text.count(old_string)
    if count == 0 and "\r\n" in text:
        # 文件是 CRLF、调用方给的是 LF：先改写成 CRLF 再匹配，写回仍保持 CRLF
        alt_old = old_string.replace("\r\n", "\n").replace("\n", "\r\n")
        if alt_old != old_string and text.count(alt_old):
            old_string = alt_old
            new_string = new_string.replace("\r\n", "\n").replace("\n", "\r\n")
            count = text.count(alt_old)
    if count == 0:
        return _fs_fail("未找到 old_string（文件 %d 字节；空白、缩进、换行必须完全一致）"
                        % len(data))
    if count > 1 and not replace_all:
        return _fs_fail("old_string 命中 %d 处，未开 replace_all 拒绝替换：请补充上下文使其唯一"
                        % count)
    idx = text.find(old_string)
    first_line = text.count("\n", 0, idx) + 1
    if replace_all:
        new_text = text.replace(old_string, new_string)
    else:
        new_text = text.replace(old_string, new_string, 1)
    out = new_text.encode("utf-8")
    try:
        with _FS_LOCK:
            with open(real, "wb") as f:
                f.write(out)
    except OSError as exc:
        return _fs_fail("写回失败: %s" % exc)
    return {"ok": True, "file_path": real,
            "replacements": count if replace_all else 1,
            "first_line": first_line, "bytes": len(out)}


def _fs_op_mkdir(path, parents=True):
    target = _fs_abs(path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if os.path.isdir(target):
        return {"ok": True, "path": target, "created": False}
    if os.path.exists(target):
        return _fs_fail("已存在同名非目录: %s" % target)
    try:
        with _FS_LOCK:
            if parents:
                os.makedirs(target, exist_ok=True)
            else:
                os.mkdir(target)
    except OSError as exc:
        return _fs_fail("创建目录失败: %s" % exc)
    return {"ok": True, "path": target, "created": True}


def _fs_op_move(src, dst, overwrite=False):
    s = _fs_abs(src)
    d = _fs_abs(dst)
    if not os.path.exists(s) and not os.path.islink(s):
        return _fs_fail("源不存在: %s" % s)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    try:
        with _FS_LOCK:
            if os.path.exists(d) or os.path.islink(d):
                if not overwrite:
                    return _fs_fail("目标已存在（overwrite=true 可覆盖）: %s" % d)
                if os.path.isdir(d) and not os.path.islink(d):
                    shutil.rmtree(d)
                else:
                    os.remove(d)
            shutil.move(s, d)
    except OSError as exc:
        return _fs_fail("移动失败: %s" % exc)
    return {"ok": True, "src": s, "dst": d}


def _fs_op_delete(path, recursive=False):
    target = _fs_abs(path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not os.path.lexists(target):
        return _fs_fail("不存在: %s" % target)
    is_dir = os.path.isdir(target) and not os.path.islink(target)
    if is_dir and not recursive:
        return _fs_fail("目标是目录，需 recursive=true 才能删除: %s" % target)
    try:
        with _FS_LOCK:
            if is_dir:
                shutil.rmtree(target)
            else:
                os.remove(target)
    except OSError as exc:
        return _fs_fail("删除失败: %s" % exc)
    return {"ok": True, "path": target, "type": "dir" if is_dir else "file"}


# ---------------- 远端源码打包与调用 ----------------

# 远端执行的源码包含：固定 import 前导 + 上述常量/函数源码。注意不能包含
# 客户端专有的东西（_FS_SRC_CACHE、_FS_CALL_TIMEOUT、MCP 注册、日志出口）。
_FS_REMOTE_PREAMBLE = (
    "import fnmatch, os, re, shutil, threading, time\n"
    "import glob as _globmod\n"
    "_FS_LOCK = threading.RLock()\n"
)
_FS_REMOTE_NAMES = (
    "_FS_READ_ONLY",
    "_FS_READ_MAX_BYTES", "_FS_WRITE_MAX_BYTES",
    "_FS_READ_MAX_LINES", "_FS_LINE_MAX_CHARS",
    "_FS_GREP_MAX_BYTES", "_FS_GREP_MAX_FILES", "_FS_GREP_MAX_SECONDS",
    "_FS_GREP_SKIP_DIRS",
    "_fs_fail", "_fs_abs", "_fs_require_writable", "_fs_require_file",
    "_fs_decode", "_fs_clip_line", "_fs_count_lines", "_fs_walk_files",
    "_fs_op_read", "_fs_op_ls", "_fs_op_glob", "_fs_op_grep",
    "_fs_op_write", "_fs_op_edit", "_fs_op_mkdir", "_fs_op_move",
    "_fs_op_delete",
)


def _fs_remote_source():
    """把实现层源码打包成可远端 exec 的字符串，返回 (源码, 哈希)。"""
    cached = globals().get('_FS_SRC_CACHE')
    if cached:
        return cached
    with _FS_LOCK:
        cached = globals().get('_FS_SRC_CACHE')
        if cached:
            return cached
        chunks = [_FS_REMOTE_PREAMBLE]
        for name in _FS_REMOTE_NAMES:
            obj = globals().get(name)
            if obj is None:
                raise RuntimeError("远端文件工具源码缺少 %s" % name)
            if callable(obj):
                chunks.append(inspect.getsource(obj))
            else:
                chunks.append("%s = %r" % (name, obj))
        src = "\n\n".join(chunks)
        cached = (src, hashlib.sha256(src.encode("utf-8")).hexdigest())
        globals()['_FS_SRC_CACHE'] = cached
        return cached


def _fs_remote_code(op, params):
    """构造发往远端的 Python：按需 exec 源码，再调用指定实现并回传 JSON。"""
    src, src_hash = _fs_remote_source()
    b64_src = base64.b64encode(src.encode("utf-8")).decode("ascii")
    payload = json.dumps(params, ensure_ascii=False)
    b64_params = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    return (
        "import base64 as _b64, json as _json\n"
        "if globals().get('_FS_SRC_HASH') != %r:\n"
        "    exec(_b64.b64decode(%r).decode('utf-8'), globals())\n"
        "    _FS_SRC_HASH = %r\n"
        "_fs_out = globals()[%r](**_json.loads(_b64.b64decode(%r).decode('utf-8')))\n"
        "r = _b64.b64encode(_json.dumps(_fs_out, ensure_ascii=False, default=str)"
        " .encode('utf-8')).decode('ascii')\n"
        % (src_hash, b64_src, src_hash, op, b64_params)
    )


def _fs_remote(op, params, write=False, timeout=None):
    """把一次文件操作发到远端执行，返回远端返回的 dict。

    走 ai_bridge.transport.request：与 client_mqtt.rpc 同一条 MQTT 一问一答
    通道，不占用 PTY 交互 shell 的 busy 锁，也不受 TTY 行规程影响。
    """
    if write:
        err = _fs_require_writable()
        if err:
            return _fs_fail(err)
    transport = getattr(ai_bridge, "transport", None)
    if transport is None:
        return _fs_fail("PTY 会话未建立：文件工具需要经 MQTT 在远端执行，请先连上远端")
    try:
        resp = transport.request(_fs_remote_code(op, params),
                                 timeout=timeout or _FS_CALL_TIMEOUT)
    except Exception as exc:
        return _fs_fail("远端请求异常: %r" % (exc,))
    if not resp:
        return _fs_fail("远端无响应（超时 %g 秒）" % (timeout or _FS_CALL_TIMEOUT))
    err = resp.get("error") or ""
    if err:
        return _fs_fail("远端执行失败: %s" % err)
    raw = resp.get("r")
    if not raw:
        return _fs_fail("远端返回为空")
    try:
        result = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        return _fs_fail("远端返回值无法解析: %.200r" % (raw,))
    if write and isinstance(result, dict) and result.get("ok"):
        if op == "_fs_op_move":
            _emit_local("[fs] move %s -> %s\n" % (result.get("src"), result.get("dst")))
        else:
            _emit_local("[fs] %s %s\n" % (
                op[len("_fs_op_"):],
                result.get("file_path") or result.get("path") or ""))
    return result


# ---------------- MCP 工具层（参数校验 + 发远端） ----------------

def fs_read(file_path: str, offset: int = 0, limit: int = 2000) -> dict:
    """读取远端 UTF-8 文本文件的一段，返回带行号前缀的内容（只读）。

    :param file_path: 远端机器上的文件路径（绝对路径，或相对远端进程工作目录）
    :param offset: 起始行，0 基（默认 0，即第一行）
    :param limit: 最多返回行数（默认 2000，上限 5000）
    返回 {ok, file_path, content, start_line, total_lines, returned_lines,
    truncated, bytes}；二进制或非 UTF-8 文件拒绝按文本读取。
    """
    return _fs_remote("_fs_op_read", {"file_path": file_path,
                                      "offset": offset, "limit": limit})


def fs_ls(path: str = ".", show_hidden: bool = True) -> dict:
    """列出远端目录条目（只读）：名称/类型/大小/修改时间，目录排在前面。

    :param path: 远端目录路径（默认远端进程工作目录）
    :param show_hidden: 是否包含点开头条目（默认 true）
    """
    return _fs_remote("_fs_op_ls", {"path": path, "show_hidden": show_hidden})


def fs_glob(pattern: str, path: str = ".", max_results: int = 200) -> dict:
    """在远端按 glob 模式查找文件（只读，支持 ** 递归），按修改时间倒序返回。

    :param pattern: 模式，如 "**/*.py"；也可传绝对路径模式
    :param path: 远端基准目录（默认远端进程工作目录）
    :param max_results: 最多返回条数（默认 200，上限 1000）
    """
    return _fs_remote("_fs_op_glob", {"pattern": pattern, "path": path,
                                      "max_results": max_results})


def fs_grep(pattern: str, path: str = ".", glob: str = "",
            ignore_case: bool = False, output_mode: str = "content",
            context: int = 0, head_limit: int = 100, offset: int = 0) -> dict:
    """在远端按正则搜索文件内容（只读），自动跳过 .git/__pycache__ 与二进制。

    :param pattern: Python 正则表达式
    :param path: 远端文件或目录（默认远端进程工作目录）
    :param glob: 可选文件名过滤，如 "*.py"（对文件名与相对路径都匹配）
    :param ignore_case: 忽略大小写
    :param output_mode: content=逐行结果；files_with_matches=仅文件路径；count=每文件命中数
    :param context: content 模式每处命中前后附加的行数（默认 0，上限 20）
    :param head_limit: 最多返回结果条数（默认 100，上限 500）
    :param offset: 跳过前面多少条结果
    """
    return _fs_remote("_fs_op_grep", {
        "pattern": pattern, "path": path, "glob": glob,
        "ignore_case": ignore_case, "output_mode": output_mode,
        "context": context, "head_limit": head_limit, "offset": offset,
    })


def fs_write(file_path: str, content: str) -> dict:
    """整文件覆盖写入远端 UTF-8 文本（父目录必须已存在，不自动建目录）。

    :param file_path: 远端目标文件路径
    :param content: 完整文件内容
    """
    return _fs_remote("_fs_op_write", {"file_path": file_path,
                                       "content": content}, write=True)


def fs_edit(file_path: str, old_string: str, new_string: str,
            replace_all: bool = False) -> dict:
    """在远端精确替换字符串（old_string 必须唯一，除非 replace_all=true）。

    :param file_path: 远端目标文件路径
    :param old_string: 要替换的原文（不能为空；必须与文件内容逐字符一致）
    :param new_string: 替换后的文本
    :param replace_all: 是否替换全部命中（默认 false，多命中会报错）
    CRLF 文件允许传 LF 版本的 old_string，写回时自动保持 CRLF。
    """
    return _fs_remote("_fs_op_edit", {
        "file_path": file_path, "old_string": old_string,
        "new_string": new_string, "replace_all": replace_all,
    }, write=True)


def fs_mkdir(path: str, parents: bool = True) -> dict:
    """在远端创建目录（默认递归创建父目录；已存在视为成功）。

    :param path: 远端目录路径
    :param parents: true=递归建父目录，false=父目录必须已存在
    """
    return _fs_remote("_fs_op_mkdir", {"path": path, "parents": parents},
                      write=True)


def fs_move(src: str, dst: str, overwrite: bool = False) -> dict:
    """在远端移动/重命名文件或目录。

    :param src: 远端源路径
    :param dst: 远端目标路径
    :param overwrite: 目标已存在时是否覆盖（默认 false）
    """
    return _fs_remote("_fs_op_move", {"src": src, "dst": dst,
                                      "overwrite": overwrite}, write=True)


def fs_delete(path: str, recursive: bool = False) -> dict:
    """删除远端文件或目录（目录需 recursive=true）。

    :param path: 远端目标路径
    :param recursive: 是否递归删除目录
    """
    return _fs_remote("_fs_op_delete", {"path": path, "recursive": recursive},
                      write=True)


# ---------------- MCP 注册 ----------------

_FS_TOOLS = (
    fs_read, fs_write, fs_edit, fs_glob, fs_grep, fs_ls,
    fs_mkdir, fs_move, fs_delete,
)


def _mcp_register_fs_tools(server) -> None:
    """把 9 个文件工具注册到 MCP server（与 pty_* 工具同一个 server 实例）。

    统一包一层 _mcp_tool：请求到达时先写本地日志（原始参数），不等执行完。
    """
    for fn in _FS_TOOLS:
        try:
            server.tool()(_mcp_tool(fn))
        except Exception as exc:  # 老版本 SDK 没有 tool() 工厂时退回 add_tool
            try:
                server.add_tool(_mcp_tool(fn))
            except Exception:
                _emit_local("[pty][MCP] 文件工具注册失败 %s: %r\n"
                            % (getattr(fn, "__name__", "?"), exc))
