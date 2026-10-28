# ==================== AI 桥调用方便捷函数（运行在外部调用方进程，只走本机 HTTP，不连 broker） ====================
#
# 与 AIBridge 本身（上面，运行在常驻窗口进程里）相反：下面这些函数给 AI /
# 其他进程导入调用，一次 localhost HTTP 即复用常驻窗口的全部 broker 长连接。
# 只用标准库 urllib，不依赖 requests。

DEFAULT_AI_BASE = "http://127.0.0.1:1188/"


def local_rpc(code, base=DEFAULT_AI_BASE, timeout=90):
    """向常驻 pty_client_mqtt 的本地 HTTP RPC 口下发一段 Python 代码，返回响应文本。"""
    import urllib.parse
    import urllib.request
    url = str(base).rstrip("/") + "/" + urllib.parse.quote(str(code), safe="")
    # data=b"" 强制 POST；显式绕开系统代理（localhost 不该走代理）。
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, data=b"", method="POST")
    with opener.open(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _call_ai_bridge(expr, base=DEFAULT_AI_BASE, timeout=90):
    """让窗口进程把表达式结果以 JSON 写回 body，本地解析成 dict。"""
    import json
    body = local_rpc(
        "import json as _j; p.set_data(_j.dumps(%s, ensure_ascii=False))" % expr,
        base=base, timeout=timeout)
    return json.loads(body)


def ai_pty_run(cmd, timeout=60, base=DEFAULT_AI_BASE, acquire_timeout=2):
    """让常驻 PTY 窗口代跑一条 shell 命令。

    返回 ``{'ok': bool, 'rc': int|None, 'out': str, 'timed_out': bool, ...}``；
    命令回显与输出同时在窗口里可见。命令串行执行，重叠调用立即返回
    ``{'ok': False, 'busy': True}``。
    """
    return _call_ai_bridge(
        "ai_bridge.run(%r, timeout=%r, acquire_timeout=%r)"
        % (str(cmd), float(timeout), float(acquire_timeout)),
        base=base, timeout=float(timeout) + 30)


def ai_pty_send(data, base=DEFAULT_AI_BASE, timeout=30):
    """向常驻 PTY 原样喂键（str，如 ``"y\\n"`` / ``"\\x03"`` Ctrl-C），不回收输出。"""
    return _call_ai_bridge("ai_bridge.send(%r)" % (str(data),),
                           base=base, timeout=timeout)


def ai_pty_status(base=DEFAULT_AI_BASE, timeout=15):
    """查询常驻 PTY 状态：会话是否建立/已结束、是否忙、broker 在线数。"""
    return _call_ai_bridge("ai_bridge.status()", base=base, timeout=timeout)


