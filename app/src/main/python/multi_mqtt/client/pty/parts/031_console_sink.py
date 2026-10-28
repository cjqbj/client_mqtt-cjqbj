# ============ 控制台输出通道（UTF-8 跨帧重组 + 零进度保护） ============
#
# 2026-10-25 冻结实证（1188 RPC + py-spy 现场）：
#   1) 远端输出按字节分帧，切点可能落在 UTF-8 字符中间（汉字 3B / emoji 4B）；
#   2) _io._WindowsConsoleIO.write 对"末尾不完整的多字节序列"返回 0 字节且
#      不抛异常（实测 b"abc\\xe4\\xb8" 只写 3B，剩下的 b"\\xe4\\xb8" 永远返回 0）；
#   3) CPython BufferedWriter.flush() 的 C 循环在 0 进度上无限重试：持锁 + 烧核
#      （实测 3 秒采样 73% 单核、线程停在 pty_client_mqtt.py 的 flush 行）；
#   4) 旧 _TerminalWriter 全部共用 _bin_stdout() 缓存的同一个 BufferedWriter，
#      于是 reset_out_writer 换线程也换不掉那把锁，60 秒一次的自愈反而每 60 秒
#      多留一个永久阻塞线程（实测 62 个 pty-out 线程在等同一把锁）。
#
# 本通道的对策：
#   * 绕开 BufferedWriter，直接 Win32 WriteConsoleW（非控制台退回 os.write）；
#   * 写前把末尾不完整的 UTF-8 字节留进 carry，等下一帧补齐再写，半字符永不
#     进入控制台；
#   * 原生写返回 0 进度时只记账并保留剩余数据，绝不原地空转重试；
#   * 每个 sink 自带锁和自己的 CONOUT$ 句柄，reset 即换新实例：旧实例就算卡死
#     在原生写里，也拖不住新写线程。

_UTF8_TAIL_SCAN = 4                 # 末尾最多回看 3 字节找 UTF-8 起始字节
_CONSOLE_CARRY_MAX = 64 * 1024      # carry 上限：异常时也不让内存无限涨


def _utf8_complete_len(data: bytes) -> int:
    """返回 data 中"最后一个完整 UTF-8 字符"结束处的偏移。

    末尾若是半个字符（如 b"abc\\xe4\\xb8" 里的 \\xe4\\xb8），返回截断点；
    数据本身非法（4 字节内都找不到起始字节）时按完整处理，交给控制台做替换。
    """
    n = len(data)
    for back in range(1, _UTF8_TAIL_SCAN + 1):
        if back > n:
            return n                        # 全是延续字节：非法数据，按完整处理
        lead = data[n - back]
        if lead < 0x80:                     # ASCII：末尾即完整
            return n
        if 0x80 <= lead <= 0xBF:            # 延续字节：继续往前找起始字节
            continue
        if 0xC0 <= lead <= 0xF7:            # 合法起始字节：按长度判断是否补齐
            need = 2 if lead <= 0xDF else (3 if lead <= 0xEF else 4)
            return n if back >= need else n - back
        return n                            # 非法字节（0xF8+）：按完整处理
    return n


def _clip_utf8(data: bytes, room: int) -> bytes:
    """把 data 截到 room 字节内，且切点必须落在字符边界（不切断多字节字符）。"""
    if room <= 0:
        return b""
    if len(data) <= room:
        return data
    return data[:_utf8_complete_len(data[:room])]


class _ConsoleSink:
    """单一控制台输出通道：自有锁 + UTF-8 carry + 零进度保护。

    与旧设计（共用 sys.stdout.buffer 那个 BufferedWriter）的关键差别：锁和
    控制台句柄都是本对象私有的。reset 时换新实例即换新通道，旧实例即使卡在
    原生写里也不会阻塞新写线程。
    """

    def __init__(self, name: str = "stdout"):
        self._name = name
        self._lock = threading.Lock()
        self._carry = b""
        self._handle = None
        self._own_handle = False
        self._win32 = False
        self._fd = 1
        self._open_console()
        self.created_ts = time.monotonic()
        self._busy_since = 0.0
        self._last_progress_ts = time.monotonic()
        # 诊断计数（writer_diag 直接可见）
        self.writes = 0
        self.written_bytes = 0
        self.zero_progress = 0     # 原生写返回 0 进度的次数
        self.partial_writes = 0    # 原生写只写了一部分的次数
        self.dropped_bytes = 0
        self.holder = None         # 正在原生写里的线程名（持锁者）

    # ---- 通道建立 ----

    def _open_console(self) -> None:
        """优先新开 CONOUT$ 句柄；拿不到控制台句柄时退回 fd 直写（管道/文件）。"""
        if sys.platform != "win32":
            return
        try:
            import ctypes
            from ctypes import wintypes
            k = ctypes.windll.kernel32
            # 显式声明 64 位安全的签名：默认 c_int 会截断 HANDLE
            k.CreateFileW.restype = wintypes.HANDLE
            k.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD,
                                      wintypes.DWORD, wintypes.LPVOID,
                                      wintypes.DWORD, wintypes.DWORD,
                                      wintypes.HANDLE]
            k.GetStdHandle.restype = wintypes.HANDLE
            k.GetStdHandle.argtypes = [wintypes.DWORD]
            k.GetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.LPDWORD]
            k.WriteConsoleW.restype = wintypes.BOOL
            k.WriteConsoleW.argtypes = [wintypes.HANDLE, wintypes.LPCWSTR,
                                        wintypes.DWORD, wintypes.LPDWORD,
                                        wintypes.LPVOID]
            k.CloseHandle.argtypes = [wintypes.HANDLE]
            invalid = wintypes.HANDLE(-1).value
            # 先确认进程 stdout 本身是不是控制台：重定向到文件/管道时必须
            # 继续写 fd（尊重 > out.txt、| tee 这类用法），不能偷写 CONOUT$。
            std = k.GetStdHandle(0xFFFFFFF5)  # STD_OUTPUT_HANDLE(-11)
            mode = wintypes.DWORD()
            if not (std and std != invalid
                    and k.GetConsoleMode(std, ctypes.byref(mode))):
                return
            h = k.CreateFileW("CONOUT$", 0x40000000 | 0x80000000,  # GENERIC_READ|WRITE
                              0x1 | 0x2,                           # FILE_SHARE_READ|WRITE
                              None, 3, 0, None)                    # OPEN_EXISTING
            if h and h != invalid:
                if k.GetConsoleMode(h, ctypes.byref(mode)):
                    self._handle = h
                    self._own_handle = True
                    self._win32 = True
                    return
                k.CloseHandle(h)
            self._handle = std
            self._win32 = True
        except Exception:
            self._win32 = False
            self._handle = None

    # ---- 原生写 ----

    def _raw_write(self, data: bytes) -> int:
        """返回实际消费的字节数；0 表示没有进度（调用方保留剩余数据）。"""
        if self._win32:
            import ctypes
            from ctypes import wintypes
            text = data.decode("utf-8", "replace")
            if not text:
                return len(data)
            buf = ctypes.create_unicode_buffer(text)
            n = wintypes.DWORD(0)
            ok = ctypes.windll.kernel32.WriteConsoleW(
                self._handle, buf, len(buf) - 1, ctypes.byref(n), None)
            if not ok:
                return 0
            # 控制台把整串收进屏幕缓冲区，n 为 0 才算零进度
            return len(data) if n.value else 0
        return os.write(self._fd, data)

    # ---- 对外接口（与 _TerminalWriter 期望的 write/flush 兼容） ----

    def write(self, data) -> int:
        data = bytes(data)
        if not data:
            return 0
        with self._lock:
            self.holder = threading.current_thread().name
            self._busy_since = time.monotonic()
            try:
                buf = self._carry + data
                self._carry = b""
                cut = _utf8_complete_len(buf)
                if cut < len(buf):
                    # 末尾半个字符：留到下一帧补齐，绝不交给控制台
                    self._carry = buf[cut:]
                    buf = buf[:cut]
                if not buf:
                    return 0
                total = len(buf)
                done = 0
                while done < total:
                    try:
                        k = self._raw_write(buf[done:])
                    except Exception:
                        k = 0
                    if not k or k < 0:
                        # 零进度：记账 + 保留剩余（含 carry），不原地重试
                        self.zero_progress += 1
                        rest = buf[done:] + self._carry
                        if len(rest) > _CONSOLE_CARRY_MAX:
                            self.dropped_bytes += len(rest) - _CONSOLE_CARRY_MAX
                            rest = rest[-_CONSOLE_CARRY_MAX:]
                        self._carry = rest
                        break
                    if k < total - done:
                        self.partial_writes += 1
                    done += k
                    self._last_progress_ts = time.monotonic()
                self.writes += 1
                self.written_bytes += done
                return done
            finally:
                self._busy_since = 0.0
                self.holder = None

    def flush(self) -> None:
        """Win32 控制台/裸 fd 都是无缓冲写，flush 无事可做。"""

    def busy_seconds(self) -> float:
        """当前原生写已持续多久（秒）；卡在 OS 调用上时它会持续增长。"""
        since = self._busy_since
        s = max(0.0, time.monotonic() - since) if since else 0.0
        # carry 长期写不出去 = 零进度僵住，同样算卡滞
        if self._carry:
            s = max(s, time.monotonic() - self._last_progress_ts)
        return s

    def snapshot(self) -> dict:
        # 刻意不加 self._lock：写线程可能正持锁卡在原生写里，诊断/自愈必须
        # 随时可读（属性读取在 GIL 下足够安全），否则 writer_diag 也会被冻住。
        carry = self._carry
        return {
            "kind": "writeconsolew" if self._win32 else "fd",
            "handle": int(self._handle) if self._handle else None,
            "writes": self.writes,
            "written_bytes": self.written_bytes,
            "zero_progress": self.zero_progress,
            "partial_writes": self.partial_writes,
            "dropped_bytes": self.dropped_bytes,
            "carried_bytes": len(carry),
            "carry_head": carry[:8].hex(),
            "holder": self.holder,
            "busy_seconds": round(self.busy_seconds(), 2),
            "age_seconds": round(time.monotonic() - self.created_ts, 1),
        }


def _new_console_sink(name: str = "stdout") -> "_ConsoleSink":
    """新建一个独立控制台通道（新锁 + 新 CONOUT$ 句柄）。

    reset_out_writer 必须走这里：只有换新实例才能真正甩掉旧通道里卡死的
    原生写调用；沿用同一个流对象是上一版自愈失效的直接原因。
    """
    return _ConsoleSink(name)
