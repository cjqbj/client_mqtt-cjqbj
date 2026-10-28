def _hard_exit(console, code: int, message: str, pty=None, leave=False):
    """学 client_mqtt.py 的退出风格：直接 ``os._exit``，干脆不拖泥带水。

    旧路径退出时要 pty.close() 干等 end 帧 1.5s + join 输入线程 2s +
    transport.stop() 逐个 broker disconnect——服务器已死时这些等待全都
    纯属浪费，界面就是这样"卡住"的。``os._exit`` 会跳过所有 finally，
    所以这里必须自己先把终端模式恢复。

    leave=True（Ctrl-]/命令栏 detach 的本地脱离路径）：走 pty.leave()，
    只清本地订阅、不发 stop——远端 shell/tmux 按承诺继续运行，孤儿会话
    由服务端 TTL 收割。leave=False（判死/会话已结束/中断等退出路径）：
    stop 帧只 fire-and-forget（能发出去就让远端顺手收尸，发不出去也不等）。
    """
    if pty is not None:
        try:
            pty.leave() if leave else pty.detach()
        except Exception:
            pass
    try:
        console.exit()
    except Exception:
        pass
    # 本地终端复位：复位序列排进 stdout 写线程（顺序在全部远端输出之后），
    # 退出提示走 stderr。
    _terminal_cleanup()
    if message:
        _stderr_write(stime() + message)
    # 给写线程有限时间把队列排空（复位序列/退出提示随之落盘）；终端读取方
    # 卡死、写线程堵在原生 IO 上时超时放弃，照样 os._exit，不再重蹈卡死覆辙。
    if _OUT_WRITER is not None:
        _OUT_WRITER.close(timeout=1.5)
    if _ERR_WRITER is not None:
        _ERR_WRITER.close(timeout=0.5)
    os._exit(code)


def _join_pre_command(parts) -> str:
    """把 REMAINDER 位置参数拼成一行前置命令：多段按空格连接（与 ssh 一致），
    去掉 argparse REMAINDER 可能保留的 ``--`` 分隔符；无命令返回空串。"""
    if not parts:
        return ""
    items = [str(x) for x in parts]
    if items and items[0] == "--":
        items = items[1:]
    return " ".join(items).strip()


