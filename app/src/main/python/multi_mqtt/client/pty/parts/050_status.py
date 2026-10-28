# ============================ 连接详情 ============================

def _broker_status(transport):
    """返回 (在线数, 总数, 在线 broker 名列表)。拿不到状态时总数退化为配置数。"""
    net = getattr(getattr(transport, "node", None), "mqtt_net", None)
    clients = getattr(net, "clients", None)
    if not clients:
        return 0, len(BROKER_LIST), []
    hosts = []
    for host, cli in clients.items():
        try:
            if cli.is_connected():
                hosts.append(str(host))
        except Exception:
            pass
    return len(hosts), len(clients), hosts


def _stderr_writer():
    """懒初始化 stderr 异步写线程（锁定真实 stderr 的二进制缓冲）。

    与 _bin_stdout 同理：只解析一次，不每次现取 sys.stderr.buffer。
    """
    global _ERR_WRITER
    if _ERR_WRITER is None:
        binerr = _bin_stderr()
        if binerr is not None:
            _ERR_WRITER = _TerminalWriter(binerr, name="pty-err")
    return _ERR_WRITER


def _stderr_write(text) -> bool:
    """非阻塞写 stderr（str/bytes 皆可）；无可用流时返回 False。"""
    w = _stderr_writer()
    if w is None:
        return False
    if isinstance(text, str):
        text = text.encode("utf-8", "replace")
    w.write(text)
    return True


