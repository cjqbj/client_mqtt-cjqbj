# ==================== 本地日志：环形缓冲 + 浏览器实时日志台，绝不写 PTY 终端 ====================
#
# 架构（终端与日志彻底分离）：
# - 终端（stdout）只渲染**远端 shell**字节流（_OUT_WRITER→真实控制台缓冲）；
# - 本地一切诊断——_info/[WARN]/banner、multi_mqtt/paho 的 logging、每笔
#   [RPC] 请求行、任何库的 print / sys.stdout / sys.stderr 直写——全部进
#   本进程唯一的 _LOCAL_LOG 环形缓冲；
# - 查看通道有三个，都不碰 PTY 终端：
#   ① 浏览器全屏实时日志台（仿 realtime_editor）：
#       http://192.168.1.3:1188/         （根路径自动跳 log_html）
#       WebSocket /wslog 先推全量快照再实时增量推送，跟普通控制台一样；
#   ② HTTP RPC 取文本：curl "http://.../r=get_log()" / r=get_log(50)；
#   ③ 窗口前的人用本地命令栏 log（自擦覆盖层）。
# --port 0 关闭 HTTP 口时没有查看通道，退回旧行为：日志镜像到 stderr。

_LOG_MAX_CHARS = 256 * 1024   # 缓冲约 256KB，超量丢最旧
_LOG_MAX_LINES = 2000
_LOG_LINE_CLIP = 4096         # 单行（异常栈/帧转储）超长截断，防一条撑爆缓冲


class _LogRing:
    """线程安全的定长日志环形缓冲（行数 + 字符数双限，超量从最旧开始丢）。

    每行带进程内单调递增序号；订阅在同一把锁内完成并原子返回当前快照，
    因此订阅者"快照 + 之后增量"不会重行也不会漏行。监听回调只允许非阻塞
    （LogHub 用有界队列接收），绝不能拖慢写日志的业务线程。
    """

    def __init__(self, max_chars=_LOG_MAX_CHARS, max_lines=_LOG_MAX_LINES):
        self._lines = deque()
        self._seq = 0
        self._chars = 0
        self._max_chars = int(max_chars)
        self._max_lines = int(max_lines)
        self._lock = threading.Lock()
        self._listeners = []

    def _append_locked(self, line: str) -> None:
        self._seq += 1
        seq = self._seq
        self._lines.append((seq, line))
        self._chars += len(line) + 1
        while (len(self._lines) > self._max_lines
               or self._chars > self._max_chars):
            old_seq, old = self._lines.popleft()
            self._chars -= len(old) + 1
        for cb in self._listeners:
            try:
                cb(seq, line)
            except Exception:
                # 监听者出错不影响日志主链路
                pass

    def write(self, text) -> None:
        if not text:
            return
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        with self._lock:
            for line in str(text).splitlines():
                if len(line) > _LOG_LINE_CLIP:
                    line = line[:_LOG_LINE_CLIP] \
                        + "…(共%d字符，已截断)" % len(line)
                self._append_locked(line)

    def tail(self, n=200) -> str:
        with self._lock:
            items = list(self._lines) if (n is None or n <= 0) \
                else list(self._lines)[-int(n):]
        return "\n".join(line for _seq, line in items)

    def subscribe(self, cb):
        """登记监听回调 cb(seq, line)；原子返回 (当前快照, 退订函数)。

        快照在同一把锁内取，回调只会收到快照之后的新行。
        """
        with self._lock:
            self._listeners.append(cb)
            snapshot = list(self._lines)

        def unsubscribe():
            with self._lock:
                try:
                    self._listeners.remove(cb)
                except ValueError:
                    pass

        return snapshot, unsubscribe

    def clear(self) -> None:
        with self._lock:
            self._lines.clear()
            self._chars = 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._lines)


_LOCAL_LOG = _LogRing()
# RPC 口未成功起监听前保持旧行为（镜像 stderr），保证启动致命错误仍可见；
# main() 起口成功后置 False，此后本地日志只进环形缓冲。
_LOG_MIRROR_STDERR = True


def _emit_local(text) -> None:
    """本地诊断日志的唯一入口：先入环形缓冲；无 RPC 查看通道时再镜像 stderr。"""
    _LOCAL_LOG.write(text)
    if _LOG_MIRROR_STDERR:
        _stderr_write(text)


def get_log(n: int = 200) -> str:
    """返回最近 n 行本地日志（n<=0 或 None 返回缓冲内全部）。

    经本地 HTTP RPC 口调用，不与 PTY 画面争抢终端::

        curl "http://127.0.0.1:1188/r=get_log()"
        curl "http://192.168.1.3:1188/r=get_log(100)"
    """
    return _LOCAL_LOG.tail(n)


def clear_log() -> dict:
    """清空本地日志环形缓冲（HTTP RPC：``/r=clear_log()``）。"""
    _LOCAL_LOG.clear()
    return {"ok": True, "cleared": True}


class _RingTextIO(io.TextIOBase):
    """把 print/sys.stdout/sys.stderr 的直写导入日志环的文本流替身。

    PTY 会话期 sys.stdout/sys.stderr 会被替换成本类实例：任何库的 stray
    print、traceback、未走 logging 的输出都进浏览器日志台，而真实 PTY 终端
    一个字节都收不到。刻意**不提供 buffer 属性**——这样 PTY 渲染侧的
    _bin_stdout/_bin_stderr 解析会自动回退到 sys.__stdout__/__stderr__
    （解释器原始真实控制台，永远不受重定向影响）。
    """

    def __init__(self, name: str):
        self.name = name
        self._tail = ""
        self._lock = threading.Lock()

    # 3.14 起 io.TextIOBase 的 encoding/errors/line_buffering 是只读属性，
    # 不能在 __init__ 里直接赋值；用 property 覆盖。
    @property
    def encoding(self) -> str:
        return "utf-8"

    @property
    def errors(self) -> str:
        return "replace"

    @property
    def line_buffering(self) -> bool:
        return True

    def writable(self) -> bool:
        return True

    def readable(self) -> bool:
        return False

    def isatty(self) -> bool:
        return False

    def fileno(self):
        raise OSError("ring log stream has no underlying file descriptor")

    def write(self, data) -> int:
        if data is None:
            return 0
        if isinstance(data, bytes):
            data = data.decode("utf-8", "replace")
        if not data:
            return 0
        # \r\n、单独 \r（进度条覆写）统一按换行切；末尾无换行的残片留到下次
        with self._lock:
            buf = self._tail + str(data)
            parts = re.split(r"\r\n|\r|\n", buf)
            self._tail = parts.pop()
        for line in parts:
            _LOCAL_LOG.write(line + "\n")
        return len(data)

    def flush(self) -> None:
        # 行级即时入环，无底层缓冲要刷
        pass


class _RingLogHandler(logging.Handler):
    """把 logging 记录（MultiMQTT/paho/cmd_client_mqtt/server_http 等）导入环形缓冲。"""

    def emit(self, record) -> None:
        try:
            _LOCAL_LOG.write(self.format(record) + "\n")
        except Exception:
            # 日志失败永远不能反噬业务线程
            pass


