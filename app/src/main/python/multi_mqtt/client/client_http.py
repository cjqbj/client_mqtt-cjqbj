import os
import sys

# 本模块已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容直接运行与「from client import client_http」包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from multi_mqtt import get_duplicated_kargs


def _req_repr(data=None, files=None, proxies=None, method='POST',
              verify=False, timeout=9, url='', max_bytes=200):
    """仿 requests.request(...) 调用的一行摘要（长 bytes 截断，末尾标 #<大小>）。"""
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
    """
    通过 server_http / server_http_wsgi 上传本地文件到远端。

    服务端代码只用 Python 标准库（os / hashlib / traceback）。

    返回值（HTTP body）：
        成功 → 远端文件路径（例如 '/tmp/hugging-face-mqtt-error.zip'）
        失败 → 完整 traceback（HTTP 状态码为 500）

    :param print_req: 非 0 → 打印一行仿 requests.request(...) 的请求摘要
                      （别名 pr 通过 **ka 传入亦可）
    """
    print_req=get_duplicated_kargs(ka,'print_req','pr','p',default=print_req)
    proxy=get_duplicated_kargs(ka,'proxy','proxies','x',default=proxy)

    import os, hashlib, urllib.parse, requests

    # ---- 1. 读本地文件 ----
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

    # ---- 2. 组装服务端代码（纯标准库）----
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
            "        raise ValueError("
            "'SHA_MISMATCH expected=%s actual=%s size=%d path=%s' "
            "% (__expect_sha, __sha, len(__b), __p))",
        ]
    lines += [
        "    r = __p",                      # 成功：远端文件路径
        "except Exception:",
        "    response.set_status(500)",     # 失败：500 + traceback
        "    r = traceback.format_exc()",
    ]
    code = "\n".join(lines)

    url = base.rstrip('/') + '/' + urllib.parse.quote(code, safe='')

    # ---- 3. 代理 ----
    if proxy == 0:
        proxies = {'http': None, 'https': None}
    else:
        proxies = ka.pop('proxies', None)
    proxies = proxies or {}

    # ---- 4. 打印请求摘要 ----
    if print_req:
        print(_req_repr(
            data=body, files=None, proxies=proxies,
            method='POST', verify=False, timeout=timeout, url=url,
        ))
    if debug:
        print(f"[rpc_set_file] HTTP …")

    # ---- 5. 发请求 ----
    resp = requests.request(
        method='POST',
        url=url,
        data=body,
        files=None,
        proxies=proxies,
        verify=False,
        timeout=timeout,
        **ka,
    )
    if debug:
        print(f"[rpc_set_file] HTTP {resp.status_code} {resp.text[:300]!r}")

    return resp