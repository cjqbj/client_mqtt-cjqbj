#!/usr/bin/env python3
"""把本地 multi_mqtt 当前代码同步到 .pythonanywhere.com 并热重载。

用法:
    python pythonanywhere_update.py                              # 默认同步到 .pythonanywhere.com
    python pythonanywhere_update.py https://ip:port/  /other/dir # 自定义 base 与远程目录
    python pythonanywhere_update.py --check                      # 只对比不上传
    
.pythonanywhere.com/r=RPCRequestHandler.executor.alias 查询是否alias

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
"""
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.join(_HERE, 'client'), _HERE):
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

EXCLUDE_DIRS = {'.git', '.trae', '__pycache__', '.pytest_cache', '.idea', '.vscode'}
EXCLUDE_FILES = {
    'rpc.json', 'client_history.db', 'cmd_client_history.db',
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


def sync_files(base, local_root, remote_dir, timeout, check_only=False):
    """对比并上传差异文件，返回 (已上传列表, 跳过列表, 失败列表)。"""
    local_files = collect_local(local_root)
    remote_files = _rpc_list_dir(base, remote_dir, proxies=PROXIES, timeout=timeout)

    to_upload, unchanged, failed = [], [], []
    for rel in sorted(local_files):
        abs_p, size, sha = local_files[rel]
        remote_path = remote_dir.rstrip('/') + '/' + rel
        if rel in remote_files:
            if remote_files[rel] == size:
                rsha = _rpc_sha256_remote(base, remote_path,
                                          proxies=PROXIES, timeout=timeout)
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
                             timeout=timeout, verify_file=1)
            print(f"  [{i}/{len(to_upload)}] done: {rel}")
        except Exception as e:
            print(f"  [{i}/{len(to_upload)}] FAILED: {rel}: {e}")
            failed.append((rel, e))
    return [r for r, _a, _s, _p in to_upload], unchanged, failed


def syntax_check(base, remote_dir, timeout):
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
    bad = _rpc_json(base, _server_code(code), proxies=PROXIES, timeout=timeout)
    if bad:
        raise RuntimeError(f"remote py_compile failed: {bad}")
    print("[check] remote py_compile all OK")


def activate(base, timeout):
    """向 uWSGI master 发 SIGHUP 触发整站优雅重载（等价控制台 Reload 按钮）。

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
                                    proxies=PROXIES, timeout=timeout))
    except Exception as e:
        # master 重载会切断在途请求，连接中断属预期
        print(f"[reload] reload 请求中断（预期）: {type(e).__name__}")
    for i in range(60):
        time.sleep(2)
        try:
            resp = _rpc_call(base, "1", proxies=PROXIES, timeout=15)
            if resp.status_code == 200:
                print(f"[reload] web app back after {(i + 1) * 2}s")
                return
        except Exception:
            continue
    raise RuntimeError("reload 后 120s 内未恢复；请到 pythonanywhere 控制台手动点 Reload")


def verify(base, timeout):
    """验证新代码在线上生效: r 回退 / 关键字 list / alias 全链路。"""
    def get(code):
        resp = _rpc_call(base, code, proxies=PROXIES, timeout=timeout)
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


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('-')]
    check_only = '--check' in sys.argv
    force_reload = '--force-reload' in sys.argv
    base = args[0] if len(args) > 0 else DEFAULT_BASE
    remote_dir = args[1] if len(args) > 1 else DEFAULT_REMOTE_DIR
    local_root = _HERE
    timeout = 60

    print(f"{__file__} {local_root} -> {base}  remote_dir={remote_dir}"
          f"{'  CHECK-ONLY' if check_only else ''}")
    uploaded, unchanged, failed = sync_files(base, local_root, remote_dir,
                                             timeout, check_only=check_only)
    if check_only:
        return
    if failed:
        raise SystemExit(f"{__file__} {len(failed)} 个文件上传失败，中止")

    # 清理本脚本此前版本可能留下的探测文件
    try:
        _rpc_delete_file(base, remote_dir.rstrip('/') + '/.upload_test',
                         proxies=PROXIES, timeout=timeout)
    except Exception:
        pass

    # 根目录任意 .py 变化都可能影响 app imports（不只三个核心模块），
    # 需要 respawn；tests/、client/ 等子目录不影响运行中的 web app。
    root_py_changed = force_reload or any(
        u.endswith('.py') and '/' not in u for u in uploaded)
    if not root_py_changed:
        print("[reload] 运行时代码无变化，跳过 worker 重启")
        verify(base, timeout)
        print(f"{__file__} done")
        return

    syntax_check(base, remote_dir, timeout)
    activate(base, timeout)
    time.sleep(1)
    verify(base, timeout)
    print(f"{__file__} done (uWSGI master reloaded)")


if __name__ == '__main__':
    main()
