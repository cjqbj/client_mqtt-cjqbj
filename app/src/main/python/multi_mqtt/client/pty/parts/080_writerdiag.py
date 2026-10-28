# ---- 写线程诊断与自愈（AI 经 RPC 可直接调用） ----

# 自愈次数：诊断用，也是"每次 reset 是否真的换了新通道"的旁证
_WRITER_RESETS = globals().setdefault('_WRITER_RESETS', 0)

# 自动换通道次数上限：正常一次就够；上限只兜"换了也没用"的极端场景，
# 避免像旧版那样每 60 秒留一个永久阻塞线程（实测堆到 62 个）。
_AUTO_RESET_MAX = 50


def _writer_sink_snapshot() -> dict:
    """当前 stdout 通道快照（写线程卡滞告警用；不含写线程自身状态）。"""
    w = _OUT_WRITER
    snap = getattr(w._stream, "snapshot", None) if w is not None else None
    return snap() if snap is not None else {}


def _writer_state(w):
    if w is None:
        return None
    state = {
        "queued_bytes": w._queued_bytes,
        "queue_items": w._q.qsize(),
        "dropped": w.dropped,
        "stall_seconds": round(w.stall_seconds(), 2),
        "abandon": w._abandon,
        "thread": (w._thread.ident, w._thread.name, w._thread.is_alive()),
        "stream_kind": type(w._stream).__name__,
    }
    snap = getattr(w._stream, "snapshot", None)
    # 通道快照：holder=正在原生写里的线程、zero_progress=0 进度次数、
    # busy_seconds=本条原生写持续秒数，一眼定位"锁死/零进度"两类冻结。
    state["sink"] = snap() if snap is not None else None
    return state


def writer_diag() -> dict:
    """返回 stdout/stderr 两个写线程的真实状态。

    RPC 持久命名空间是启动时快照，直接查 ``_OUT_WRITER`` 会得到 None
    （它在快照之后的 run_session 里创建）；本函数运行时动态读模块全局，
    一条 RPC 即可看清卡滞现场：``r=writer_diag()``。
    """
    ts = [t for t in threading.enumerate() if t.name.startswith("pty-out")]
    return {
        "out": _writer_state(_OUT_WRITER),
        "err": _writer_state(_ERR_WRITER),
        "pty_out_threads": len(ts),
        "resets": _WRITER_RESETS,
        "mcp_port": globals().get("_MCP_PORT"),
    }


def reset_out_writer() -> dict:
    """stdout 写线程卡死时自愈：旧 writer 写完当前块即丢弃积压退出，
    新建 writer + **全新控制台通道** 接管后续输出。

    2026-10-25 实证：旧实现复用 _bin_stdout() 缓存的同一个 BufferedWriter，
    新旧 writer 共享同一把被零进度 flush 占住的锁，换线程毫无意义，只会
    每 60 秒多留一个永久阻塞线程（实测堆到 62 个）。必须换 _ConsoleSink
    新实例（新锁 + 新 CONOUT$ 句柄）才能真正恢复输出。
    """
    global _OUT_WRITER, _WRITER_RESETS
    old = _OUT_WRITER
    if old is None:
        return {"ok": False, "error": "无 stdout writer（会话未建立？）"}
    stall = old.stall_seconds()
    old_state = _writer_state(old)
    old.abandon_pending()
    if _bin_stdout() is None:
        return {"ok": False, "error": "终端输出流不可用"}
    _OUT_WRITER = _TerminalWriter(_new_console_sink("pty-out"), name="pty-out")
    _WRITER_RESETS += 1
    return {
        "ok": True,
        "stall_seconds": round(stall, 2),
        "resets": _WRITER_RESETS,
        "old": old_state,
        "new": _writer_state(_OUT_WRITER),
    }


# ---- QuickEdit 选择模式冻结：自动解除（保留左键选择/右键粘贴） ----

# 写线程卡滞超过该秒数且 pty 窗口在前台：合成 ESC 解除 conhost 标记模式
_SELECTION_STALL_AUTO_UNSTICK = 25.0

# 合成 ESC 的泄漏兜底：标记模式在注入前一刻恰被解除时，ESC 可能进入应用输入。
# 注入后极短时间窗内丢弃恰好读出的单个 ESC 字节（真实按键同窗口同键的概率可忽略）。
_ESC_GUARD = {"until": 0.0}


def _arm_esc_guard(window: float = 1.0) -> None:
    _ESC_GUARD["until"] = time.monotonic() + window


def _filter_injected_esc(data: bytes) -> bytes:
    if data == b"\x1b" and _ESC_GUARD["until"] and \
            time.monotonic() < _ESC_GUARD["until"]:
        _ESC_GUARD["until"] = 0.0
        return b""
    return data


def _cancel_console_selection() -> bool:
    """合成一次 ESC 解除 conhost 标记（选择）模式，解除输出冻结。

    QuickEdit 左键拖选时 conhost 冻结该控制台输出，标记模式下 ESC 由 conhost
    自行消费、不进入应用，被冻住的 WriteFile 随即完成、积压一次性刷出。
    仅当本控制台窗口处于前台才发送——否则 ESC 会打进用户正在操作的其他程序。
    """
    if sys.platform != "win32":
        return False
    try:
        import ctypes
        k = ctypes.windll.kernel32
        u = ctypes.windll.user32
        if u.GetForegroundWindow() != k.GetConsoleWindow():
            return False
        # keybd_event（等价 SendInput 的键盘路径）：ESC 0x1B，down + up
        u.keybd_event(0x1B, 0, 0, 0)
        u.keybd_event(0x1B, 0, 0x0002, 0)  # KEYEVENTF_KEYUP
        _arm_esc_guard()
        return True
    except Exception:
        return False


