# ============================ 本地终端：输出 ANSI 支持 ============================

def _enable_output_vt() -> bool:
    """让本地终端能渲染远端 ANSI 输出。POSIX 原生支持；Windows 打开 VT 处理 + UTF-8。

    刻意不动 QuickEdit：左键选择 / 右键粘贴是日常功能，选择时输出暂停也
    是应有语义；长时间忘记退出选择模式的冻结由写线程卡滞检测 + 合成 ESC
    自动解除（见 _cancel_console_selection），而非禁用选择。
    """
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        k = ctypes.windll.kernel32
        k.SetConsoleOutputCP(65001)
        h = k.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_uint32()
        if not k.GetConsoleMode(h, ctypes.byref(mode)):
            return False
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        return bool(k.SetConsoleMode(h, mode.value | 0x0004))
    except Exception:
        return False


# ============================ 本地终端：raw 输入 ============================

class _PosixRawConsole:
    """termios raw：ISIG/ICANON/ECHO 全关，每个按键直接可读。"""

    def __init__(self):
        import termios
        self._termios = termios
        self.fd = sys.stdin.fileno()
        self.old = termios.tcgetattr(self.fd)

    def enter(self):
        import tty
        tty.setraw(self.fd, self._termios.TCSANOW)

    def read(self) -> bytes:
        return os.read(self.fd, 4096)

    def exit(self):
        self._termios.tcsetattr(self.fd, self._termios.TCSANOW, self.old)


def _new_input_transcoder(cp):
    """把按"控制台输入代码页"编码的 VT 输入字节增量转成 UTF-8。

    VT 输入流里的可打印字符沿用控制台输入 CP（中文系统默认 936/GBK），
    右键粘贴 / 中文 IME 敲入的汉字到达 ``os.read`` 时是 GBK 而不是 UTF-8，
    直接透传给远端 UTF-8 PTY 就乱码。不能靠把输入 CP 改成 65001 解决：
    实测 conhost 自带的粘贴路径在 CP65001 下会把每个汉字写成固定坏字节
    （U+00B0），信息不可逆丢失；保留系统 CP 时字节是合法 GBK，可完整转码。

    用增量解码器：一次 read 可能恰好落在双字节字符中间，半截序列必须留到
    下次拼接，绝不能当场替换成 U+FFFD。所有 VT 控制序列都是纯 ASCII，
    转码对转义字节透明。
    """
    name = "utf-8" if cp in (0, 65001) else "cp%d" % cp
    try:
        dec = codecs.getincrementaldecoder(name)(errors="replace")
    except LookupError:
        dec = codecs.getincrementaldecoder("utf-8")(errors="replace")

    def feed(data: bytes) -> bytes:
        return dec.decode(data).encode("utf-8", "replace")

    return feed


class _WinRawConsole:
    """Windows 输入：优先 ENABLE_VIRTUAL_TERMINAL_INPUT（ReadFile 直接给 VT 序列），
    老系统 / 失败时退回 msvcrt.getwch + 按键翻译。"""

    ENABLE_VIRTUAL_TERMINAL_INPUT = 0x0200

    def __init__(self):
        import ctypes
        self.k = ctypes.windll.kernel32
        self.h = self.k.GetStdHandle(-10)  # STD_INPUT_HANDLE
        self.old_mode = None
        self.use_vt = False
        self._transcode = _new_input_transcoder(65001)

    def enter(self):
        import ctypes
        mode = ctypes.c_uint32()
        if self.k.GetConsoleMode(self.h, ctypes.byref(mode)):
            self.old_mode = mode.value
            if self.k.SetConsoleMode(self.h, self.ENABLE_VIRTUAL_TERMINAL_INPUT):
                self.use_vt = True
        self.k.SetConsoleOutputCP(65001)
        # 只改输出 CP，不动输入 CP：见 _new_input_transcoder 的说明。
        try:
            self._transcode = _new_input_transcoder(self.k.GetConsoleCP())
        except Exception:
            pass

    def read(self) -> bytes:
        if self.use_vt:
            return self._transcode(os.read(0, 4096))
        return self._msvcrt_read()

    def exit(self):
        if self.old_mode is not None:
            self.k.SetConsoleMode(self.h, self.old_mode)

    @staticmethod
    def _msvcrt_read() -> bytes:
        import msvcrt
        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            code = ord(msvcrt.getwch())
            table = {
                72: b"\x1b[A",    # Up
                80: b"\x1b[B",    # Down
                77: b"\x1b[C",    # Right
                75: b"\x1b[D",    # Left
                71: b"\x1b[H",    # Home
                79: b"\x1b[F",    # End
                73: b"\x1b[5~",   # PgUp
                81: b"\x1b[6~",   # PgDn
                82: b"\x1b[2~",   # Insert
                83: b"\x1b[3~",   # Delete
                59: b"\x1bOP", 60: b"\x1bOQ", 61: b"\x1bOR", 62: b"\x1bOS",  # F1-F4
                63: b"\x1b[15~", 64: b"\x1b[17~", 65: b"\x1b[18~", 66: b"\x1b[19~",
                67: b"\x1b[20~", 68: b"\x1b[21~",                              # F5-F10
            }
            return table.get(code, b"")
        if ch == "\x08":  # Backspace -> DEL（远端 tty 约定）
            return b"\x7f"
        return ch.encode("utf-8", "replace")


def _make_raw_console():
    return _WinRawConsole() if sys.platform == "win32" else _PosixRawConsole()


# ============================ 本地剪贴板粘贴 ============================

# Shift+Insert 在 conhost 的 VT 输入模式下不会触发系统粘贴：控制台把按键
# 翻译成 xterm 序列直接交给应用（现象：远端 shell 收到字面 ^[[2;2~）。
# 客户端截下该序列，自己读剪贴板发 UTF-8，既补回粘贴功能，也绕开 conhost
# 自带粘贴在非 UTF-8 输入 CP 下发本地编码、在 CP65001 下直接丢汉字的两条坏路。
_PASTE_KEY_SEQ = b"\x1b[2;2~"


def _normalize_paste_text(text: str) -> bytes:
    """剪贴板文本转成发往 PTY 的字节：UTF-8 编码，换行统一为 CR。

    conhost 右键粘贴与回车键上报的换行都是 ``\\r``；剪贴板里常见的
    ``\\r\\n`` / ``\\n`` 不统一会导致 tmux/全屏程序光标行为不一致。
    """
    text = text.replace("\r\n", "\r").replace("\n", "\r")
    return text.encode("utf-8", "replace")


def _read_clipboard_text_win() -> str:
    """纯 ctypes 读 CF_UNICODETEXT 剪贴板文本；无文本/失败返回空串。"""
    import ctypes
    from ctypes import wintypes
    k32 = ctypes.windll.kernel32
    u32 = ctypes.windll.user32
    # 必须显式声明 64 位句柄原型，否则 ctypes 默认 c_int 返回值截断句柄。
    u32.GetClipboardData.restype = wintypes.HANDLE
    u32.GetClipboardData.argtypes = [wintypes.UINT]
    k32.GlobalLock.restype = wintypes.LPVOID
    k32.GlobalLock.argtypes = [wintypes.HGLOBAL]
    k32.GlobalUnlock.argtypes = [wintypes.HGLOBAL]
    if not u32.OpenClipboard(0):
        return ""
    try:
        hdata = u32.GetClipboardData(13)  # CF_UNICODETEXT
        if not hdata:
            return ""
        ptr = k32.GlobalLock(hdata)
        if not ptr:
            return ""
        try:
            return ctypes.wstring_at(ptr)
        finally:
            k32.GlobalUnlock(hdata)
    finally:
        u32.CloseClipboard()


