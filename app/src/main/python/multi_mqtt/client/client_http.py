import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from multi_mqtt import get_duplicated_kargs


def _req_repr(data=None, files=None, proxies=None, method='POST',
              verify=False, timeout=9, url='', max_bytes=200):
    def _bytes_repr(b):
        if b is None:
            return 'None'
        if isinstance(b, (bytes, bytearray)):
            b = bytes(b)
            if len(b) > max_bytes:
                shown = repr(b[:max_bytes])
                return f"{shown}...#{_hsize(len(b))}"
            return repr(b)
        return repr(b)

    def _hsize(n):
        for unit in ('B', 'KiB', 'MiB', 'GiB'):
            if n < 1024 or unit == 'GiB':
                return f"{n/1 if unit=='B' else n/1024:.3f} {unit}"
            n /= 1024

    parts = [
        f"data={_bytes_repr(data)}",
        f"files={files!r}",
        f"proxies={proxies or {}}",
        f"method={method!r}",
        f"verify={verify!r}",
        f"timeout={timeout!r}",
        f"url={url!r}",
    ]
    return "requests.request(" + ",".join(parts) + ",)"


def rpc_set_file(local_file, remote_file=None, base='http://localhost:1144/',
                 proxy=0, print_req=1, verify_file=1, timeout=9, debug=0, **ka):
    print_req = get_duplicated_kargs(ka, 'print_req', 'pr', 'p', default=print_req)
    proxy = get_duplicated_kargs(ka, 'proxy', 'proxies', 'x', default=proxy)

    import os, hashlib, urllib.parse, requests

    if isinstance(local_file, (bytes, bytearray)):
        body = bytes(local_file)
        if not remote_file:
            raise ValueError("remote_file is required when local_file is bytes")
    else:
        with open(local_file, 'rb') as f:
            body = f.read()
        if not remote_file:
            remote_file = local_file
        elif remote_file.endswith('/'):
            remote_file = remote_file + os.path.basename(local_file)

    sha = hashlib.sha256(body).hexdigest()

    lines = [
        "import os, hashlib, traceback",
        "__b = (request.rfile.read(int(request.headers.get('Content-Length',0) or 0))"
        " if hasattr(request,'rfile') else"
        " request.environ['wsgi.input'].read(int(request.environ.get('CONTENT_LENGTH',0) or 0)))",
        f"__p = {remote_file!r}",
        f"__expect_sha = {sha!r}",
        "try:",
        "    __d = os.path.dirname(__p)",
        "    if __d: os.makedirs(__d, exist_ok=True)",
        "    __f = open(__p, 'wb'); __f.write(__b); __f.close()",
        "    __sha = hashlib.sha256(__b).hexdigest()",
    ]
    if verify_file:
        lines += [
            "    if __expect_sha and __sha != __expect_sha:",
            "        raise ValueError('SHA_MISMATCH expected=%s actual=%s size=%d path=%s' "
            "% (__expect_sha, __sha, len(__b), __p))",
        ]
    lines += [
        "    r = __p",
        "except Exception:",
        "    try:\n        response.set_status(500)\n    except Exception:\n        pass",
        "    r = traceback.format_exc()",
    ]
    code = "\n".join(lines)
    url = base.rstrip('/') + '/' + urllib.parse.quote(code, safe='')

    if proxy == 0:
        proxies = {'http': None, 'https': None}
    else:
        proxies = ka.pop('proxies', None)
    proxies = proxies or {}

    if print_req:
        print(_req_repr(data=body, files=None, proxies=proxies,
                        method='POST', verify=False, timeout=timeout, url=url))
    if debug:
        print(f"[rpc_set_file] HTTP …")

    resp = requests.request(method='POST', url=url, data=body, files=None,
                            proxies=proxies, verify=False, timeout=timeout, **ka)
    if debug:
        print(f"[rpc_set_file] HTTP {resp.status_code} {resp.text[:300]!r}")
    return resp


# ===================== rpc_sync_dir 及辅助 =====================

def _fmt_size(n):
    n = float(n)
    for u in ('B', 'KiB', 'MiB', 'GiB', 'TiB'):
        if n < 1024 or u == 'TiB':
            return f"{int(n)} B" if u == 'B' else f"{n:.3f} {u}"
        n /= 1024


_REQ_KEYS = {'params', 'headers', 'cookies', 'auth', 'allow_redirects',
             'hooks', 'stream', 'cert', 'json'}


def _rpc_call(base, code, body=b'', proxies=None, timeout=9, debug=0, **ka):
    import urllib.parse, requests
    ka = {k: v for k, v in ka.items() if k in _REQ_KEYS}
    url = base.rstrip('/') + '/' + urllib.parse.quote(code, safe='')
    resp = requests.request('POST', url, data=body, files=None,
                            proxies=proxies or {}, verify=False,
                            timeout=timeout, **ka)
    if debug:
        print(f"[rpc_sync] {resp.status_code} {resp.text[:200]!r}")
    return resp


# 服务端注入前缀：纯标准库，不依赖 response。
# - _read_body(): Flask(request.get_data) → request.rfile → environ['wsgi.input']
# - set_response_status_code(): 多框架兜底设置状态码，失败返回 False 不抛异常
_SRV_PREFIX = (
    "import os,sys,json,hashlib,traceback\n"
    "def _read_body():\n"
    "    try:\n"
    "        return request.get_data()\n"
    "    except Exception:\n"
    "        pass\n"
    "    try:\n"
    "        return request.rfile.read(int(request.headers.get('Content-Length',0) or 0))\n"
    "    except Exception:\n"
    "        pass\n"
    "    try:\n"
    "        return request.environ['wsgi.input'].read(int(request.environ.get('CONTENT_LENGTH',0) or 0))\n"
    "    except Exception:\n"
    "        return b''\n"
    "def set_response_status_code(code):\n"
    "    try:\n"
    "        response.set_status(code); return True\n"
    "    except Exception:\n"
    "        pass\n"
    "    try:\n"
    "        response.status_code = code; return True\n"
    "    except Exception:\n"
    "        pass\n"
    "    try:\n"
    "        self.send_response(code); return True\n"
    "    except Exception:\n"
    "        return False\n"
)


def _server_code(user_code):
    """包装用户代码：成功 → r = {'ok':True,'result':...}；异常 → r = {'ok':False,'error':...}。"""
    body = "".join("    " + ln + "\n" for ln in user_code.split("\n"))
    return (_SRV_PREFIX
            + "try:\n"
            + "    __body = _read_body()\n"
            + body
            + "    r = json.dumps({'ok': True, 'result': __result})\n"
            + "except Exception:\n"
            + "    r = json.dumps({'ok': False, 'error': traceback.format_exc()})\n")


def _rpc_json(base, code, body=b'', proxies=None, timeout=9, debug=0, **ka):
    import json
    resp = _rpc_call(base, code, body, proxies, timeout, debug, **ka)
    text = resp.text
    try:
        data = json.loads(text)
    except Exception:
        raise RuntimeError(
            f"RPC non-JSON (HTTP {resp.status_code}, {len(text)} bytes): {text[:500]!r}")
    if not data.get('ok'):
        raise RuntimeError(
            f"RPC error (HTTP {resp.status_code}):\n{data.get('error')}")
    return data.get('result')


def _rpc_list_dir(base, remote_dir, proxies=None, timeout=9, **ka):
    uc = (
        "import os\n"
        f"__root = {remote_dir!r}\n"
        "__result = {}\n"
        "if os.path.isdir(__root):\n"
        "    for __dp, __dn, __fn in os.walk(__root):\n"
        "        for __f in __fn:\n"
        "            __full = os.path.join(__dp, __f)\n"
        "            __rel = os.path.relpath(__full, __root).replace(os.sep, '/')\n"
        "            __result[__rel] = os.path.getsize(__full)\n"
    )
    return _rpc_json(base, _server_code(uc), proxies=proxies, timeout=timeout, **ka)


def _rpc_upload_chunk(base, remote_file, data, offset, proxies=None, timeout=9, **ka):
    uc = (
        "import os\n"
        f"__p = {remote_file!r}\n"
        f"__offset = {offset!r}\n"
        "__d = os.path.dirname(__p)\n"
        "if __d:\n"
        "    os.makedirs(__d, exist_ok=True)\n"
        "if __offset == 0:\n"
        "    __f = open(__p, 'wb')\n"
        "else:\n"
        "    __f = open(__p, 'r+b')\n"
        "    __f.seek(__offset)\n"
        "__f.write(__body)\n"
        "__f.close()\n"
        "__result = os.path.getsize(__p)\n"
    )
    actual = _rpc_json(base, _server_code(uc), body=data,
                       proxies=proxies, timeout=timeout, **ka)
    expected = offset + len(data)
    if actual != expected:
        raise RuntimeError(
            f"upload_chunk size mismatch: expected {expected}, remote {actual}, "
            f"file={remote_file!r} offset={offset} chunk={len(data)}")
    return actual


def _rpc_delete_file(base, remote_file, proxies=None, timeout=9, **ka):
    uc = (
        "import os\n"
        f"__p = {remote_file!r}\n"
        "if os.path.exists(__p):\n"
        "    os.remove(__p)\n"
        "__result = 'ok'\n"
    )
    return _rpc_json(base, _server_code(uc), proxies=proxies, timeout=timeout, **ka)


def _rpc_sha256_remote(base, remote_file, proxies=None, timeout=9, **ka):
    uc = (
        "import hashlib\n"
        f"__p = {remote_file!r}\n"
        "__h = hashlib.sha256()\n"
        "with open(__p, 'rb') as __f:\n"
        "    for __c in iter(lambda: __f.read(1 << 20), b''):\n"
        "        __h.update(__c)\n"
        "__result = __h.hexdigest()\n"
    )
    return _rpc_json(base, _server_code(uc), proxies=proxies, timeout=timeout, **ka)


def _upload_one_file(base, local_path, remote_path, size, chunk_size,
                     show_speed, speed_threshold, proxies, timeout,
                     verify_file, debug=0, **ka):
    import os, sys, time, hashlib

    hasher = hashlib.sha256() if verify_file else None
    uploaded = 0
    t_start = time.time()
    show = show_speed and size >= speed_threshold

    if size == 0:
        actual = _rpc_upload_chunk(base, remote_path, b'', 0,
                                   proxies=proxies, timeout=timeout, **ka)
        if actual != 0:
            raise RuntimeError(f"empty file upload mismatch: remote size={actual}")
        return 0, (hashlib.sha256(b'').hexdigest() if verify_file else None)

    with open(local_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            if hasher is not None:
                hasher.update(chunk)
            t0 = time.time()
            _rpc_upload_chunk(base, remote_path, chunk, uploaded,
                              proxies=proxies, timeout=timeout, **ka)
            dt = max(time.time() - t0, 1e-9)
            uploaded += len(chunk)
            if show:
                pct = 100.0 * uploaded / size
                speed = len(chunk) / dt
                elapsed = time.time() - t_start
                avg = uploaded / elapsed if elapsed > 0 else 0
                sys.stdout.write(
                    "\r  " + os.path.basename(local_path) +
                    f"  {pct:6.2f}%  {_fmt_size(uploaded)}/{_fmt_size(size)}" +
                    f"  {_fmt_size(speed)}/s  avg {_fmt_size(avg)}/s\033[K")
                sys.stdout.flush()

    if show:
        sys.stdout.write("\n")
        sys.stdout.flush()

    sha = hasher.hexdigest() if hasher is not None else None
    if verify_file and sha:
        remote_sha = _rpc_sha256_remote(base, remote_path,
                                        proxies=proxies, timeout=timeout, **ka)
        if remote_sha != sha:
            raise RuntimeError(
                f"SHA mismatch after upload {local_path} -> {remote_path}: "
                f"local={sha} remote={remote_sha}")
    return uploaded, sha


def rpc_sync_dir(local_dir, remote_dir, base='http://192.168.1.134:1133/',
                 proxy=0, print_req=1, timeout=9, debug=0,
                 show_speed=1, chunk_size=1024 * 1024,
                 speed_threshold=1024 * 1024,
                 delete_extra=1, verify_file=0,
                 continue_on_error=1, **ka):
    import os

    print_req = get_duplicated_kargs(ka, 'print_req', 'pr', 'p', default=print_req)
    proxy = get_duplicated_kargs(ka, 'proxy', 'proxies', 'x', default=proxy)
    show_speed = get_duplicated_kargs(ka, 'show_speed', 'speed', default=show_speed)
    chunk_size = get_duplicated_kargs(ka, 'chunk_size', 'chunk', default=chunk_size)
    speed_threshold = get_duplicated_kargs(ka, 'speed_threshold', 'big', default=speed_threshold)
    delete_extra = get_duplicated_kargs(ka, 'delete_extra', 'del_extra', 'clean', default=delete_extra)
    verify_file = get_duplicated_kargs(ka, 'verify_file', 'verify', default=verify_file)
    continue_on_error = get_duplicated_kargs(ka, 'continue_on_error', 'continue', default=continue_on_error)

    if proxy == 0:
        proxies = {'http': None, 'https': None}
        ka.pop('proxies', None)
    else:
        proxies = ka.pop('proxies', None) or {}

    local_dir = os.path.abspath(local_dir)
    if not os.path.isdir(local_dir):
        raise ValueError(f"local_dir is not a directory: {local_dir}")

    local_files = {}
    for dp, _dn, fn in os.walk(local_dir):
        for f in fn:
            abs_p = os.path.join(dp, f)
            rel_p = os.path.relpath(abs_p, local_dir).replace(os.sep, '/')
            local_files[rel_p] = (abs_p, os.path.getsize(abs_p))

    remote_files = _rpc_list_dir(base, remote_dir, proxies=proxies,
                                 timeout=timeout, **ka)

    to_upload, unchanged = [], []
    for rel, (abs_p, size) in local_files.items():
        if rel not in remote_files:
            to_upload.append((rel, abs_p, size, 'new'))
        elif remote_files[rel] != size:
            to_upload.append((rel, abs_p, size, 'modified'))
        else:
            unchanged.append(rel)

    to_delete = [r for r in remote_files if r not in local_files] if delete_extra else []
    total_bytes = sum(s for _r, _a, s, _w in to_upload)

    if print_req:
        print(f"[rpc_sync_dir] {local_dir} -> {remote_dir}")
        print(f"  local={len(local_files)} remote={len(remote_files)} "
              f"upload={len(to_upload)} ({_fmt_size(total_bytes)}) "
              f"delete={len(to_delete)} unchanged={len(unchanged)}")
        if to_upload:
            print("  --- to upload ---")
            for rel, _a, size, why in sorted(to_upload):
                print(f"    [{why:>8}] {rel}  ({_fmt_size(size)})")
        if to_delete:
            print("  --- to delete ---")
            for rel in sorted(to_delete):
                print(f"    [  delete] {rel}")

    deleted_ok, failed = [], []
    for rel in to_delete:
        remote_path = remote_dir.rstrip('/') + '/' + rel
        try:
            _rpc_delete_file(base, remote_path, proxies=proxies, timeout=timeout, **ka)
            deleted_ok.append(rel)
            if print_req:
                print(f"  deleted: {rel}")
        except Exception as e:
            failed.append((rel, 'delete', e))
            if print_req:
                print(f"  [FAILED delete] {rel}: {e}")
            if not continue_on_error:
                raise

    n = len(to_upload)
    uploaded_list = []
    for i, (rel, abs_p, size, why) in enumerate(to_upload, 1):
        remote_path = remote_dir.rstrip('/') + '/' + rel
        if print_req:
            print(f"  [{i}/{n}] {why}: {rel}  ({_fmt_size(size)})")
        try:
            _upload_one_file(
                base, abs_p, remote_path, size, chunk_size,
                show_speed, speed_threshold, proxies, timeout,
                verify_file, debug=debug, **ka,
            )
            uploaded_list.append(rel)
            if print_req and not (show_speed and size >= speed_threshold):
                print(f"  [{i}/{n}] done: {rel}")
        except Exception as e:
            failed.append((rel, 'upload', e))
            if print_req:
                print(f"  [{i}/{n}] [FAILED upload] {rel}: {e}")
            if not continue_on_error:
                raise

    if print_req:
        print(f"[rpc_sync_dir] done. uploaded={len(uploaded_list)} "
              f"deleted={len(deleted_ok)} unchanged={len(unchanged)} "
              f"failed={len(failed)}")
        if failed:
            print("  --- failed ---")
            for rel, op, e in failed:
                print(f"    [{op:>6}] {rel}: {e}")

    return {
        'uploaded': uploaded_list,
        'deleted': deleted_ok,
        'unchanged': unchanged,
        'failed': failed,
        'local_dir': local_dir,
        'remote_dir': remote_dir,
    }