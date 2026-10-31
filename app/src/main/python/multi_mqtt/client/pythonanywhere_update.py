#!/usr/bin/env python3
"""把本地 multi_mqtt 当前代码同步到 pythonanywhere 站点并热重载。

用法:
    python client/pythonanywhere_update.py https://<user>.pythonanywhere.com/- \
        /home/<user>/multi_mqtt                              # 同步整仓并热重载
    python client/pythonanywhere_update.py --check https://<host>/-   # 只对比不上传
    python client/pythonanywhere_update.py --local-root client \
        https://ip:port/- /other/dir                      # 只推 client 子树

地址形态（base 的最后一段就是 server_http 的 key，没有单独的 --key 参数）:
    无 key:    base='https://<host>/'    → /1+1
    key='abc': base='https://<host>/abc' → /abc1+1（key 只占一个路径段）
    上传发生在重载之前，线上可能还是旧 key（典型是首次推 '-' 时线上仍为
    空 key），所以脚本会先探测线上实际生效的 key，再决定同步用哪种 URL；
    SIGHUP 之后则改用 base 里带的 key 轮询与验证。

流程（全部走 HTTP RPC 通道，复用 client/client_http.py）:
    1. 收集本地源码文件（按扩展名/黑名单过滤，跳过 .git、__pycache__、*.db 等）
    2. 与远程逐文件 sha256 对比，只上传有差异的文件（分块 + 上传后 sha 校验）
    3. 上传后先 py_compile 全量语法检查，然后向 uWSGI master（容器 pid 1）
       发 SIGHUP 触发整站优雅重载——等价于控制台 Reload 按钮，master 才会
       重新从磁盘 import 全部模块（REPL 命名空间与别名注册表被清空，属预期）。
       两个无效/有害的替代方案（均实测过）：
       * 杀 worker：worker 由 master fork，respawn 出的副本仍继承 master
         启动时已 import 的旧模块，磁盘新代码不会生效；
       * exec 热替换运行中模块：uWSGI 持有的旧 application 闭包体按旧
         签名调用全局（如 _WSGIHandlerShim(environ) 单参数），与新类
         签名不兼容会导致所有 RPC 请求 500。
    4. 验证 1+1 / ALIAS_KEYWORDS / alias 开关 / #alias 全链路。

WSGI 入口 /var/www/{u}_pythonanywhere_com_wsgi.py 无需改动:
    from server_http_wsgi import application

文件下载挂载（服务端限速；域名与账号都由调用方传 base）:
    base = 'https://<host>/-'                  # 站点根 + 线上 key 前缀
    alias_file_download(base)                  # 挂 /ime.apk，服务端 200KB/s 可调
    test_file_download(base, dest='ime.apk')   # 客户端全速下载，量服务端限速效果
    线上 /-<code> 的写法：URL 路径就是 URL 编码后的代码，例如 /-#alias、
    /-import%20os...；详见下方“文件下载挂载”分节。
"""
import hashlib
import json
import os
import sys
import time
import urllib.parse

_HERE = os.path.dirname(os.path.abspath(__file__))
# 仓库根：本脚本在 client/ 下，而线上 /home/<u>/multi_mqtt 是整仓部署
# （根目录有 server_http.py，client/ 是子目录），默认同步仓库根；只想推
# client 子树时用 --local-root client，或把远程目录指到 .../multi_mqtt/client。
REPO_ROOT = os.path.dirname(_HERE)
for _p in (_HERE, REPO_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import urllib3
    urllib3.disable_warnings()
except Exception:
    pass

# 复用 client_http 的分块上传 / sha 校验 / RPC 调用
from client_http import (
    _rpc_call, _rpc_json, _rpc_list_dir,
    _rpc_sha256_remote, _rpc_delete_file, _upload_one_file,
)

DEFAULT_USER=''
DEFAULT_BASE =f'https://{DEFAULT_USER}.pythonanywhere.com/'
DEFAULT_REMOTE_DIR=f'/home/{DEFAULT_USER}/multi_mqtt'

EXCLUDE_DIRS = {'.git', '.trae', '__pycache__', '.pytest_cache', '.idea',
                '.vscode', '.stfolder'}
EXCLUDE_FILES = {
    'rpc.json', 'client_history.db', 'cmd_client_history.db',
    'realtime_editor.txt',    # .gitignore 里的本地运行产物
    'glm5.3=全库审计报告.md',  # 本地审计报告，不属于项目代码
}
INCLUDE_EXTS = {'.py', '.sh', '.md', '.txt', '.bat'}

PROXIES = {}  # 直连，不走环境代理


def collect_local(local_root):
    """返回 {相对路径posix: (绝对路径, 大小, sha256)}，只含源码类文件。"""
    import hashlib
    files = {}
    for dp, dns, fns in os.walk(local_root):
        dns[:] = [d for d in dns if d not in EXCLUDE_DIRS]
        for fn in fns:
            if fn in EXCLUDE_FILES or fn.endswith('.db'):
                continue
            ext = os.path.splitext(fn)[1].lower()
            if fn not in ('.gitattributes', '.gitignore') and ext not in INCLUDE_EXTS:
                continue
            abs_p = os.path.join(dp, fn)
            rel = os.path.relpath(abs_p, local_root).replace(os.sep, '/')
            with open(abs_p, 'rb') as f:
                body = f.read()
            files[rel] = (abs_p, len(body), hashlib.sha256(body).hexdigest())
    return files


def sync_files(base, local_root, remote_dir, timeout, check_only=False, key=''):
    """对比并上传差异文件，返回 (已上传列表, 跳过列表, 失败列表)。"""
    local_files = collect_local(local_root)
    remote_files = _rpc_list_dir(base, remote_dir, proxies=PROXIES,
                                 timeout=timeout, key=key)

    to_upload, unchanged, failed = [], [], []
    for rel in sorted(local_files):
        abs_p, size, sha = local_files[rel]
        remote_path = remote_dir.rstrip('/') + '/' + rel
        if rel in remote_files:
            if remote_files[rel] == size:
                rsha = _rpc_sha256_remote(base, remote_path,
                                          proxies=PROXIES, timeout=timeout,
                                          key=key)
                if rsha == sha:
                    unchanged.append(rel)
                    continue
        to_upload.append((rel, abs_p, size, remote_path))

    total = sum(s for _r, _a, s, _p in to_upload)
    print(f"[sync] local={len(local_files)} remote={len(remote_files)} "
          f"upload={len(to_upload)} ({total} B) unchanged={len(unchanged)}")
    for rel, _a, _s, _p in to_upload:
        print(f"  [upload] {rel}")
    if check_only:
        return [], unchanged, failed

    for i, (rel, abs_p, size, remote_path) in enumerate(to_upload, 1):
        try:
            _upload_one_file(base, abs_p, remote_path, size,
                             chunk_size=1024 * 1024, show_speed=0,
                             speed_threshold=1024 * 1024, proxies=PROXIES,
                             timeout=timeout, verify_file=1, key=key)
            print(f"  [{i}/{len(to_upload)}] done: {rel}")
        except Exception as e:
            print(f"  [{i}/{len(to_upload)}] FAILED: {rel}: {e}")
            failed.append((rel, e))
    return [r for r, _a, _s, _p in to_upload], unchanged, failed


def split_base_key(base):
    """把带 key 的 base 拆成 (站点根, key)：'https://<host>/-' → ('https://<host>/', '-')。

    server_http 的 key 只占一个 URL 前缀段（其自身会去掉首尾 '/'），所以
    base 的最后一个路径段就是 key（如 '-'、'abc'）；路径为空或以 '/' 结尾
    则视为没有 key（''）。host 之后、key 之前的路径前缀原样保留（反向代理
    子路径场景），查询串/锚点丢弃。base 是用户当场给的参数，写错立即报错。
    """
    parts = urllib.parse.urlsplit(str(base or '').strip())
    if not parts.scheme or not parts.netloc:
        raise ValueError(
            f'base 必须是完整地址，例如 https://<host>/-: {base!r}')
    path = parts.path
    if not path or path == '/' or path.endswith('/'):
        key, root_path = '', (path or '/')
    else:
        head, _sep, key = path.rpartition('/')
        root_path = head + '/' if head else '/'
    root = urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, root_path, '', ''))
    return root, key


def _probe_key(base, timeout, key):
    """探测线上当前真正生效的 URL 前缀，返回 '' 或 key。

    同步发生在重载之前：首次把 key 推上线时线上还是旧的无前缀地址，之后
    再跑线上已是新 key。两种状态都要能上传，所以按线上实际响应探测而不是
    写死。探测只发 1+1 这个无副作用表达式：走错形态只会拿到 403 或语法
    错误（-1+1），不会产生任何副作用。
    """
    for candidate in ([key, ''] if key else ['']):
        try:
            resp = _rpc_call(base, '1+1', proxies=PROXIES, timeout=timeout,
                             key=candidate)
        except Exception:
            continue
        if resp.status_code == 200 and resp.text.strip() == '2':
            return candidate
    raise RuntimeError(
        f"无法探测线上 key（{base}）：试过 {[key, ''] if key else ['']} 都失败；"
        "线上已启用 key 时把 key 写进 base，例如 https://<host>/-")


def syntax_check(base, remote_dir, timeout, key=''):
    """远程 py_compile 全量检查上传后的 .py，坏文件直接抛错不进入重载。"""
    code = (
        "import py_compile\n"
        f"__root = {remote_dir!r}\n"
        "import os\n"
        "__bad = []\n"
        "for __dp, __dn, __fn in os.walk(__root):\n"
        "    if '.git' in __dp or '__pycache__' in __dp:\n"
        "        continue\n"
        "    for __f in __fn:\n"
        "        if not __f.endswith('.py'):\n"
        "            continue\n"
        "        __p = os.path.join(__dp, __f)\n"
        "        try:\n"
        "            py_compile.compile(__p, doraise=True)\n"
        "        except Exception as __e:\n"
        "            __bad.append((__p, str(__e)))\n"
        "__result = __bad\n"
    )
    from client_http import _server_code
    bad = _rpc_json(base, _server_code(code), proxies=PROXIES, timeout=timeout,
                    key=key)
    if bad:
        raise RuntimeError(f"remote py_compile failed: {bad}")
    print("[check] remote py_compile all OK")


def activate(base, timeout, live_key='', post_key=''):
    """向 uWSGI master 发 SIGHUP 触发整站优雅重载（等价控制台 Reload 按钮）。

    live_key: 重载前线上生效的 key（发 SIGHUP 的请求本身还走旧地址）；
    post_key: 重载后新代码的 key（轮询恢复必须用新地址，否则会一直 403/500）。

    pythonanywhere 的 worker 由 master fork：杀 worker 只会 fork 出继承旧
    模块的副本，磁盘新代码不会生效；必须重载 master。master 即容器 pid 1，
    先校验父进程 cmdline 含 uWSGI master 再发信号，避免误伤其它 supervisor。
    """
    code = (
        "import os, signal\n"
        "ppid = os.getppid()\n"
        "name = open(f'/proc/{ppid}/cmdline','rb').read()\n"
        "if b'uWSGI master' not in name:\n"
        "    raise RuntimeError(f'parent {ppid} is not uWSGI master: {name!r}')\n"
        "os.kill(ppid, signal.SIGHUP)\n"
        "__result = f'SIGHUP sent to uWSGI master pid {ppid}'\n"
    )
    from client_http import _server_code
    try:
        print("[reload]", _rpc_json(base, _server_code(code),
                                    proxies=PROXIES, timeout=timeout,
                                    key=live_key))
    except Exception as e:
        # master 重载会切断在途请求，连接中断属预期
        print(f"[reload] reload 请求中断（预期）: {type(e).__name__}")
    for i in range(60):
        time.sleep(2)
        try:
            # 新代码可能换过 key，必须按新地址探测；'1+1' 而不是 '1'，
            # 这样 key='-' 时 /-1+1 才执行 1+1 而不是把 -1 当表达式。
            resp = _rpc_call(base, "1+1", proxies=PROXIES, timeout=15,
                             key=post_key)
            if resp.status_code == 200 and resp.text.strip() == "2":
                print(f"[reload] web app back after {(i + 1) * 2}s")
                return
        except Exception:
            continue
    raise RuntimeError("reload 后 120s 内未恢复；请到 pythonanywhere 控制台手动点 Reload")


def verify(base, timeout, key=''):
    """验证新代码在线上生效: r 回退 / 关键字 list / alias 全链路。"""
    def get(code):
        resp = _rpc_call(base, code, proxies=PROXIES, timeout=timeout, key=key)
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        return resp.text.strip()

    checks = [
        ("expr r-fallback", "1+1", "2"),
        ("keyword list live", "server_http.rpc_executor.ALIAS_KEYWORDS", "'alias'"),
        # 激活哨兵：旧版 executor 没有该属性——防止“master 未真正重载、
        # 仍在跑旧内存代码”的情况蒙混过关（worker fork 不重新 import 磁盘）
        ("alias switch live", "RPCRequestHandler.executor.alias", "True"),
    ]
    for name, code, expect in checks:
        got = get(code)
        if expect not in got:
            raise RuntimeError(f"verify[{name}] expect {expect!r} got {got!r}")
        print(f"[verify] {name}: {got[:80]}")

    # alias 全链路: 定义 → 使用 → 列表 → 删除（不在生产留测试别名）
    got = get("#alias update_selfcheck\n40+2")
    if "42" not in got:
        raise RuntimeError(f"verify[alias define] got {got!r}")
    got = get("update_selfcheck")
    if "42" not in got:
        raise RuntimeError(f"verify[alias use] got {got!r}")
    got = get("#alias")
    if "update_selfcheck" not in got:
        raise RuntimeError(f"verify[alias list] got {got!r}")
    get("#unalias update_selfcheck")
    print("[verify] alias define/use/list/unalias 全链路 OK")


# --------------------------------------------------------------------------- #
# 文件下载挂载（服务端限速） + 客户端全速自测
# --------------------------------------------------------------------------- #
# 线上 /-<code> 的写法：URL 路径本身就是 URL 编码后的 Python 代码，代码在
# web app 的持久命名空间里执行（server_http_wsgi 默认 RPC_KEY=
# RPC_ALIAS_URL='raw'）。挂载一个可下载端点只需要一次注入：
#
#   1. GET /-<code> 把流式 body 写进 executor.aliases[名字]（不执行 body）；
#   2. 之后 GET /<名字> 就是下载地址（alias_url='raw' 下已注册别名免 key）。
#
# 为什么不用 #alias：别名 body 是流式输出，而 #alias 是“定义即执行”，
# 注册那次请求会立刻把整包文件传完；直接写 aliases 字典只注册、不执行。
#
# 关于 rpc.json 的 http_aliases：由 server_http._update_persisted_aliases()
# 在 #alias 定义 / #unalias 之后（或本函数 persist=True 时）读-改-写
# （模块锁 + 临时文件 + os.replace，json.dump(ensure_ascii=False, indent=2)），
# 其它配置键原样保留；启动时 load_persisted_aliases() 恢复任意非空白名字
# （ime.apk 也可以），且不覆盖内存同名。因此 persist=True 的挂载可跨
# SIGHUP 重载长期有效；persist=False 只活在进程内存。



def _alias_url(base, code, key=''):
    """拼 /<key><urlencoded-code>：'https://<host>/-' + '#alias' → /-%23alias。"""
    root = str(base or '').rstrip().rstrip('/')
    key = str(key or '')
    if key and not root.endswith(key):
        root = root + '/' + key
    return root + urllib.parse.quote(str(code), safe='')


def _alias_run(base, code, key='', timeout=60, proxies=None, verify_tls=True):
    """GET 一段代码并返回文本；HTTP 非 200 抛 RuntimeError（附响应片段）。"""
    import requests
    resp = requests.get(_alias_url(base, code, key=key),
                        timeout=(10, timeout),
                        proxies=PROXIES if proxies is None else proxies,
                        verify=verify_tls)
    try:
        if resp.status_code != 200:
            raise RuntimeError('alias HTTP %s: %s'
                               % (resp.status_code, resp.text[:300]))
        return resp.text
    finally:
        resp.close()


def _site_file_url(base, path, key=''):
    """站点根文件地址：'https://<host>/-' + 'ime.apk' → 'https://<host>/ime.apk'。"""
    root = str(base or '').rstrip()
    key = str(key or '')
    if key and root.rstrip('/').endswith(key):
        root = root.rstrip('/')[:-len(key)]
    root = root.rstrip('/')
    return root + '/' + urllib.parse.quote(str(path).lstrip('/'), safe='/')


def _serve_body(remote_path, file_name, rate_limit, chunk_size, content_type):
    """别名 body：首行 #serve-file 规格，后面保留等价的可执行代码。

    server_http 看到首行规格后直接读文件流式输出（不经过执行器、不占执行
    锁，客户端断开立即停）；旧版服务端 / 其它通道不认识规格时，仍按后面
    的可执行代码分块输出（先等配额再写，整段耗时 ≈ 文件大小 / rate_limit）。
    """
    disposition = 'attachment; filename="%s"' % str(file_name).replace('"', '_')
    spec = json.dumps({
        'path': str(remote_path),
        'filename': str(file_name),
        'content_type': str(content_type),
        'rate_limit': int(rate_limit or 0),
        'chunk_size': max(1024, int(chunk_size or 65536)),
    }, ensure_ascii=True)
    body = "\n".join([
        "import os, time",
        "__p = os.path.abspath(os.path.expanduser(%r))" % str(remote_path),
        "__rate = %d" % int(rate_limit or 0),
        "__chunk = %d" % max(1024, int(chunk_size or 65536)),
        "if not os.path.isfile(__p):",
        "    response.set_status(404)",
        "    response.set_data('not found: %s' % __p)",
        "else:",
        "    __size = os.path.getsize(__p)",
        "    response.set_status(200)",
        "    response.set_header('Content-Type', %r)" % str(content_type),
        "    response.set_header('Content-Length', str(__size))",
        "    response.set_header('Content-Disposition', %r)" % disposition,
        "    __sent = 0",
        "    __t0 = time.monotonic()",
        "    try:",
        "        with open(__p, 'rb') as __f:",
        "            while True:",
        "                __b = __f.read(__chunk)",
        "                if not __b: break",
        "                if __rate > 0:",
        "                    __gap = __t0 + (__sent + len(__b)) / float(__rate)",
        "                    __gap -= time.monotonic()",
        "                    if __gap > 0: time.sleep(__gap)",
        "                response.write(__b)",
        "                __sent += len(__b)",
        "    except (BrokenPipeError, ConnectionResetError):",
        "        pass",
    ])
    return "#serve-file " + spec + "\n" + body


def _mount_body(url_name, body, persist):
    """生成“只写 executor.aliases、不执行 body”的远端注入代码。"""
    lines = [
        "import json, time",
        "__name = %r" % str(url_name),
        "__body = %r" % body,
        "__now = int(time.time() * 1000)",
        "__ex = getattr(globals().get('RPCRequestHandler'), 'executor', None)",
        "if __ex is None: __ex = globals().get('_APP_EXECUTOR')",
        "if __ex is None:",
        "    import server_http",
        "    __ex = getattr(server_http.RPCRequestHandler, 'executor', None)",
        "if __ex is None or not getattr(__ex, 'alias', False):",
        "    raise RuntimeError('alias channel unavailable: executor not found')",
        "__old = __ex.aliases.get(__name) or {}",
        "__ex.aliases[__name] = {'code': __body,",
        "    'created': __old.get('created', __now), 'updated': __now,",
        "    'hits': __old.get('hits', 0), 'last_used': __old.get('last_used')}",
    ]
    if persist:
        lines += [
            "import server_http",
            "__persisted = server_http._update_persisted_aliases(",
            "    updates={__name: __body})",
        ]
    else:
        lines += ["__persisted = False"]
    lines += [
        "r = json.dumps({'name': __name, 'persisted': __persisted,",
        "    'count': len(__ex.aliases)})",
    ]
    return "\n".join(lines)

# 服务端字节/秒（按 1024 字节/KB）；0 = 不限速。
# 默认 500KB：PythonAnywhere 的 web 请求有 300 秒上限（实测 200KB/s 下
# 61,538,304 字节（正好 300.48s）被网关切断），66MB 的 APK 在 300KB/s
# 约 3.6 分钟已接近上限；500KB/s 约 2.2 分钟、限速本身也够直观。
ALIAS_RATE_LIMIT = 500 * 1024
def alias_file_download(base, remote_path='~/ime.apk', url_name=None,
                        rate_limit=ALIAS_RATE_LIMIT, chunk_size=64 * 1024,
                        content_type='application/vnd.android.package-archive',
                        persist=True, key='-', timeout=60,
                        proxies=None, verify_tls=True):
    """把远端文件挂成免 key 的下载 URL（默认 /ime.apk），服务端限速。

    * remote_path: 相对 / 绝对 / '~/ime.apk' 均可——远端用
      os.path.expanduser + abspath 解析，'~/ime.apk' 与
      '/home/<user>/ime.apk' 等价。
    * rate_limit: 服务端字节/秒，默认 500KB/s，0 = 全速。PythonAnywhere
      对单个 web 请求有 300 秒上限：限速调得太低会让 66MB 的 APK 在
      300 秒处被网关切断（200KB/s 实测在 61.5MB 处断流），500KB/s 下
      66MB 约 132 秒，留足余量。
    * url_name: 下载路径，默认取文件名（ime.apk → /ime.apk）。
    * persist: True（默认）把 body 写进 rpc.json 的 http_aliases，
      SIGHUP 重载 / 重启后自动恢复；False 只注册到进程内存。

    返回 {'name','url','remote_path','rate_limit','persisted'}。
    """
    name = str(url_name or os.path.basename(str(remote_path).rstrip('/\\')) or '')
    name = name.strip().lstrip('/')
    if not name or '/' in name:
        raise ValueError('url_name 必须是单个路径段: %r' % (name,))
    file_name = os.path.basename(str(remote_path).rstrip('/\\')) or name
    body = _serve_body(remote_path, file_name, rate_limit, chunk_size, content_type)
    mounted = json.loads(_alias_run(base, _mount_body(name, body, persist),
                                    key=key, timeout=timeout,
                                    proxies=proxies, verify_tls=verify_tls))
    shown = _alias_run(base, '#alias ' + name, key=key, timeout=timeout,
                       proxies=proxies, verify_tls=verify_tls)
    if name not in shown:
        raise RuntimeError('mount 后回读校验失败: %r' % (shown[:300],))
    return {
        'name': name,
        'url': _site_file_url(base, name, key=key),
        'remote_path': str(remote_path),
        'rate_limit': int(rate_limit or 0),
        'persisted': bool(mounted.get('persisted')),
    }


def test_file_download(base, path='ime.apk', dest=None, chunk_size=64 * 1024,
                       expected_sha256=None, expected_size=None, key='-',
                       timeout=60, verify_tls=True, proxies=None, progress=None):
    """客户端全速下载（不做任何限速），用来量服务端 rate_limit 的实际效果。

    path 默认 /ime.apk，也可直接传完整 http(s) URL。
    返回 {'url','path','size','sha256','md5','seconds','speed'}（speed 为
    字节/秒，直接对比 rate_limit 看服务端限速是否生效）。
    给了 expected_sha256 / expected_size 就校验，不一致抛 ValueError，
    临时 .part 保留便于排查。
    """
    import requests

    raw = str(path)
    url = (raw if raw.lower().startswith(('http://', 'https://'))
           else _site_file_url(base, path, key=key))
    dest = os.path.abspath(dest or os.path.basename(raw.rstrip('/\\'))
                           or 'download.bin')
    part = dest + '.part'
    parent = os.path.dirname(dest)
    if parent:
        os.makedirs(parent, exist_ok=True)

    started = time.monotonic()
    resp = requests.get(url, stream=True, timeout=(10, timeout),
                        proxies=PROXIES if proxies is None else proxies,
                        verify=verify_tls)
    try:
        if resp.status_code != 200:
            raise RuntimeError('download HTTP %s: %s'
                               % (resp.status_code, resp.text[:300]))
        length = resp.headers.get('Content-Length')
        total = int(length) if length and length.isdigit() else None
        sha, md5 = hashlib.sha256(), hashlib.md5()
        done = 0
        with open(part, 'wb') as out:
            for block in resp.iter_content(chunk_size=chunk_size):
                if not block:
                    continue
                out.write(block)
                sha.update(block)
                md5.update(block)
                done += len(block)
                if progress is not None:
                    elapsed = max(1e-9, time.monotonic() - started)
                    progress(done, total, done / elapsed)
    finally:
        resp.close()

    if total is not None and done != total:
        raise ValueError('SIZE_MISMATCH expected=%d actual=%d path=%s'
                         % (total, done, part))
    if expected_size is not None and done != int(expected_size):
        raise ValueError('SIZE_MISMATCH expected=%d actual=%d path=%s'
                         % (int(expected_size), done, part))
    actual = sha.hexdigest()
    if expected_sha256 and actual.lower() != str(expected_sha256).strip().lower():
        raise ValueError('SHA_MISMATCH expected=%s actual=%s size=%d path=%s'
                         % (expected_sha256, actual, done, part))
    os.replace(part, dest)
    seconds = time.monotonic() - started
    return {
        'url': url,
        'path': dest,
        'size': done,
        'sha256': actual,
        'md5': md5.hexdigest(),
        'seconds': seconds,
        'speed': done / seconds if seconds else 0,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description='同步本地 multi_mqtt 到 pythonanywhere 并通过 SIGHUP 热重载',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('base', nargs='?', default=DEFAULT_BASE,
                        help='站点根 + key 前缀，如 https://<user>.pythonanywhere.com/-；'
                             '无 key 时写 https://<user>.pythonanywhere.com/')
    parser.add_argument('remote_dir', nargs='?', default=DEFAULT_REMOTE_DIR,
                        help='远程项目目录')
    parser.add_argument('--local-root', '-local-root', default=REPO_ROOT,
                        help="本地待同步根目录（默认仓库根；只推 client 用 client）")
    parser.add_argument('--check', action='store_true', help='只对比不上传')
    parser.add_argument('--force-reload', action='store_true',
                        help='即使根目录 .py 无变化也强制重载')
    args = parser.parse_args()
    # base 自带 key（最后一段），拆出站点根给 client_http 的 key= 参数用
    base, new_key = split_base_key(args.base)
    remote_dir = args.remote_dir
    check_only = args.check
    force_reload = args.force_reload
    local_root = os.path.abspath(args.local_root)
    timeout = 60

    print(f"{__file__} {local_root} -> {base}  remote_dir={remote_dir}"
          f"  key={new_key!r}{'  CHECK-ONLY' if check_only else ''}"
          f"  (from base={args.base})")
    # 扫清 1+1 这类无副作用请求，判断线上此刻认哪种地址
    live_key = _probe_key(base, timeout, new_key)
    print(f"[sync] live key={live_key!r}  (上传后新 key={new_key!r})")
    uploaded, unchanged, failed = sync_files(base, local_root, remote_dir,
                                             timeout, check_only=check_only,
                                             key=live_key)
    if check_only:
        return
    if failed:
        raise SystemExit(f"{__file__} {len(failed)} 个文件上传失败，中止")

    # 清理本脚本此前版本可能留下的探测文件
    try:
        _rpc_delete_file(base, remote_dir.rstrip('/') + '/.upload_test',
                         proxies=PROXIES, timeout=timeout, key=live_key)
    except Exception:
        pass

    # 根目录任意 .py 变化都可能影响 app imports（不只三个核心模块），
    # 需要 respawn；tests/、client/ 等子目录不影响运行中的 web app。
    root_py_changed = force_reload or any(
        u.endswith('.py') and '/' not in u for u in uploaded)
    if not root_py_changed:
        print("[reload] 运行时代码无变化，跳过 worker 重启")
        verify(base, timeout, key=live_key)
        print(f"{__file__} done")
        return

    syntax_check(base, remote_dir, timeout, key=live_key)
    activate(base, timeout, live_key=live_key, post_key=new_key)
    time.sleep(1)
    verify(base, timeout, key=new_key)
    print(f"{__file__} done (uWSGI master reloaded)")


if __name__ == '__main__':
    main()
