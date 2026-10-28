# ============================ 退出时的本地终端复位 ============================

# 脱离/被杀时远端全屏程序（tmux/vim）来不及发"恢复屏幕"序列，最后一帧
# （tmux 绿色状态条等）会冻在本地终端里。退出前本地主动复位：
#   ?1049l 退出备用屏幕（tmux attach 时进入的那个，退出即恢复 attach 前画面）
#   ?1000/2/3/6l 关掉可能被远端打开的鼠标上报
#   0m 复位颜色/粗体；?25h 显示光标
_TERMINAL_CLEANUP = (b"\x1b[?1049l\x1b[?1000l\x1b[?1002l\x1b[?1003l"
                     b"\x1b[?1006l\x1b[0m\x1b[?25h\r\n")


_BIN_STDOUT_CACHE = globals().setdefault('_BIN_STDOUT_CACHE', None)  # reload 重绑 None 会触发二次解析：会话期 sys.stdout 正被 _RingTextIO/redirect 替换，重解析可能锁错流；setdefault 保住首解析结果
_BIN_STDERR_CACHE = globals().setdefault('_BIN_STDERR_CACHE', None)  # 同 _BIN_STDOUT_CACHE：stderr 会话期被 _RingTextIO 导流，首解析结果必须跨 reload 保活


def _bin_stdout():
    """锁定真正终端的二进制输出流（带缓存）。

    ai_bridge 控制口执行 RPC 代码时会 ``redirect_stdout(StringIO())`` 临时
    替换全局 ``sys.stdout`` 以捕获 print；StringIO 没有 ``buffer`` 属性，
    PTY 渲染/菜单线程若每次现取 sys.stdout.buffer 就会 AttributeError 直接
    崩掉整个会话。会话期 sys.stdout/sys.stderr 还会被换成 _RingTextIO
    （本地日志导流），同样没有 buffer。所以解析一次后固定用：当前流无
    buffer（正被重定向）时退回解释器原始流（sys.__stdout__，永不受重定向
    影响）。
    """
    global _BIN_STDOUT_CACHE
    if _BIN_STDOUT_CACHE is None:
        for stream in (sys.stdout, getattr(sys, "__stdout__", None)):
            buf = getattr(stream, "buffer", None)
            if buf is not None:
                _BIN_STDOUT_CACHE = buf
                break
    return _BIN_STDOUT_CACHE


def _bin_stderr():
    """锁定真正终端的二进制错误流（带缓存），与 _bin_stdout 同理。

    会话期 sys.stderr 被换成 _RingTextIO（本地日志导流）后，致命退出提示
    仍需写到真实 stderr（此时终端已复位）；这里固定解析一次，回退
    sys.__stderr__。
    """
    global _BIN_STDERR_CACHE
    if _BIN_STDERR_CACHE is None:
        for stream in (sys.stderr, getattr(sys, "__stderr__", None)):
            buf = getattr(stream, "buffer", None)
            if buf is not None:
                _BIN_STDERR_CACHE = buf
                break
    return _BIN_STDERR_CACHE


# 排队字节上限：终端读取方长时间不消费时，队列涨到这个量后新字节被丢弃
# 而不是拖住调用方。8MB 足以吸收正常突发（整屏 cat 也就几 KB~几十 KB）。
_TERM_QUEUE_MAX = 8 * 1024 * 1024

# 会话期 stdout 异步写线程（run_session 创建）；启动前为 None。
_OUT_WRITER = globals().setdefault('_OUT_WRITER', None)  # 必须防 reload：110_session/080_writerdiag 判空创建 _TerminalWriter（各带独立写线程），reload 把名字重绑 None 后会再起一条写线程，双线程并发写同一控制台；setdefault 保住活线程引用
# stderr 异步写线程，懒初始化（_info 在 main 最早阶段就会用到）。
_ERR_WRITER = globals().setdefault('_ERR_WRITER', None)  # 同 _OUT_WRITER：050_status 判空懒建，reload 重绑 None = 再起一条 err 写线程，与旧线程并发写终端


class _TerminalWriter:
    """终端字节异步写：单写线程 + 有界（按字节计）队列。

    背景（py-spy 实证）：Windows conhost / 终端读取方停止消费时，对控制台
    的同步 write+flush 会在原生 OS 调用上无限阻塞并占住 GIL；调用它的渲染
    主线程一挂，进程内所有 Python 线程（含本地 HTTP 控制口）全部饿死，
    status 等一切请求超时。

    本类把唯一可能阻塞的原生 IO 隔离到一个可牺牲的 daemon 写线程上：
    调用方 write() 只做非阻塞入队（队列满则丢新字节并记账），关键路径
    永不碰原生写；单队列 + 单写线程保证字节顺序与原同步写一致。flush()
    语义由写线程每次写后完成，调用方不再等待——等待会重新引入阻塞。
    """

    _SENTINEL = object()

    def __init__(self, stream, max_bytes=_TERM_QUEUE_MAX, name="term-writer"):
        self._stream = stream
        self._max_bytes = int(max_bytes)
        # 无界 Queue：背压靠字节记账自己判断，不依赖 Queue 的条数限制
        self._q = queue.Queue()
        self._queued_bytes = 0
        self._lock = threading.Lock()
        self.dropped = 0
        self._closed = False
        # 最近一次 flush 成功的时间：写线程卡在原生 WriteFile 上时它停止前进，
        # 是"窗口冻结"最直接的可观测信号。
        self._last_flush_ts = time.monotonic()
        # 自愈标记：置位后当前块写完即丢弃全部积压并退出，由新 writer 接管
        self._abandon = False
        self._thread = threading.Thread(target=self._loop, name=name, daemon=True)
        self._thread.start()

    def write(self, data: bytes) -> None:
        if not data or self._closed:
            return
        with self._lock:
            if self._queued_bytes >= self._max_bytes:
                # 写线程正卡在原生 IO 上（终端不消费）：丢新字节保进程，
                # 绝不在这里等。
                self.dropped += len(data)
                return
            room = self._max_bytes - self._queued_bytes
            if len(data) > room:
                # 按 UTF-8 字符边界截断：硬切 data[:room] 会切出半个汉字，
                # 这种残尾正是控制台零进度死循环的燃料（2026-10-25 实证）。
                clipped = _clip_utf8(data, room)
                self.dropped += len(data) - len(clipped)
                data = clipped
            if not data:
                return
            # 卡滞计时从本次入队起算：空闲期后第一笔数据若沿用上次 flush
            # 时间戳，stall_seconds 会把空闲秒数虚报成卡滞（实测虚报 11s+）。
            if self._queued_bytes == 0:
                self._last_flush_ts = time.monotonic()
            self._queued_bytes += len(data)
        self._q.put(data)

    def flush(self) -> None:
        """刷盘由写线程在每次写后完成；刻意不等待（等待=重新引入阻塞）。"""

    def close(self, timeout: float = 1.5) -> None:
        """队尾放哨兵后等写线程排空；写线程堵在原生 IO 上时超时即返回。"""
        if self._closed:
            return
        self._closed = True
        self._q.put(self._SENTINEL)
        self._thread.join(timeout)

    def stall_seconds(self) -> float:
        """有积压未写出时，距上次成功 flush 的秒数；无积压返回 0。

        队列里没数据（正卡在一条原生写上）时改用通道自身的忙碌时长：
        否则零进度 carry 僵住会被误判成"空闲"，自愈永远不触发。
        """
        with self._lock:
            pending = self._queued_bytes
        s = 0.0
        if pending > 0:
            s = max(0.0, time.monotonic() - self._last_flush_ts)
        busy = getattr(self._stream, "busy_seconds", None)
        if busy is not None:
            s = max(s, busy())
        return s

    def abandon_pending(self) -> None:
        """自愈：当前块写完（或原生阻塞解除）后丢弃全部剩余积压并退出。

        卡在 WriteFile 上的线程任何代码都无法中止；但解除后它只需写完
        当前一个块就能在这里下线，后续输出交给新 writer，避免旧流解冻后
        与新流重复拼接。
        """
        self._abandon = True
        # 线程可能空闲阻塞在 get()：放哨兵唤醒它直接退出；
        # 正卡在 write 上时，哨兵在队尾，会被 _drain_remaining 吞掉。
        self._q.put(self._SENTINEL)

    def _loop(self) -> None:
        while True:
            item = self._q.get()
            if item is self._SENTINEL:
                return
            if not self._abandon:
                try:
                    self._stream.write(item)
                    self._stream.flush()
                    self._last_flush_ts = time.monotonic()
                except Exception:
                    # 写线程不能死：死了队列只涨不刷。终端出错后输出静默丢弃。
                    pass
            with self._lock:
                self._queued_bytes -= len(item)
            if self._abandon:
                self._drain_remaining()
                return

    def _drain_remaining(self) -> None:
        """丢弃队列里所有未处理数据块（记账），遇到哨兵也不复活。"""
        while True:
            try:
                it = self._q.get_nowait()
            except queue.Empty:
                return
            if it is self._SENTINEL:
                continue
            with self._lock:
                self._queued_bytes -= len(it)


def _terminal_cleanup(sync: bool = False):
    """复位本地终端（退 alt screen/关鼠标/复位颜色/显光标）。

    默认把复位序列排进会话 stdout 写线程队列：它必须在所有已排队的远端
    输出之后到达终端，直接先写会插到队前造成序列错位。sync=True（控制台
    关闭信号兜底，进程马上要被系统结束）时才直接同步写。
    """
    if not sync and _OUT_WRITER is not None:
        _OUT_WRITER.write(_TERMINAL_CLEANUP)
        return
    try:
        # 同步兜底也走独立控制台通道：绝不再碰 BufferedWriter
        # （它可能正被一条零进度的 flush 占着锁）。
        sink = _ConsoleSink("cleanup")
        sink.write(_TERMINAL_CLEANUP)
        sink.flush()
    except Exception:
        pass


def install_console_guards(console, on_signal=None):
    """在终端被关 X / Ctrl-Break / kill 信号时尽量恢复本地终端模式。

    正常脱离路径本来就会 console.exit()；这里兜底的是"没走正常路径"的
    退出：强杀后 cmd 残留在 VT 输入模式，方向键会变成转义序列、历史命令
    调不出来。注意任务管理器/TerminateProcess 对任何程序都不可拦截，
    这里只能接住控制台关闭事件、Ctrl-Break 与 POSIX 信号。

    返回 restore()，幂等，正常退出路径也可复用。
    """
    state = {"done": False}

    def restore():
        if state["done"]:
            return
        state["done"] = True
        try:
            console.exit()
        except Exception:
            pass
        # 信号路径：进程随后即被系统结束，等不及异步队列，直接同步写
        _terminal_cleanup(sync=True)

    if sys.platform == "win32":
        try:
            import ctypes
            k32 = ctypes.windll.kernel32
            handler_t = ctypes.WINFUNCTYPE(ctypes.c_int, ctypes.c_uint)

            def _handler(ctrl_type):
                # 2=关闭控制台窗口(X) 1=Ctrl-Break 5=注销 6=关机
                if ctrl_type in (1, 2, 5, 6):
                    try:
                        if on_signal is not None:
                            on_signal()
                    finally:
                        restore()
                return 0  # 交还系统默认处理：随后进程被结束

            cb = handler_t(_handler)
            # 必须保活回调对象，否则 GC 后 ctypes 会崩
            console._cmq_ctrl_cb = cb
            k32.SetConsoleCtrlHandler(cb, True)
        except Exception:
            pass
    else:
        import signal

        def _handler(signum, _frame):
            try:
                if on_signal is not None:
                    on_signal()
            finally:
                restore()
                os._exit(128 + signum)

        for _name in ("SIGHUP", "SIGTERM", "SIGQUIT"):
            _sig = getattr(signal, _name, None)
            if _sig is not None:
                try:
                    signal.signal(_sig, _handler)
                except (ValueError, OSError):
                    pass
    return restore


