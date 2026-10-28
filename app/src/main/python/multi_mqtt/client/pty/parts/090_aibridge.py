# ============================ AI 桥：外部进程复用本常驻 PTY ============================

# CSI / SGR / OSC / 单字符转义，用于把 AI 收回的终端字节还原成纯文本
_ANSI_RE = re.compile(rb"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")


def _strip_ansi(b: bytes) -> str:
    return _ANSI_RE.sub(b"", b).replace(b"\r\n", b"\n").replace(b"\r", b"\n").decode("utf-8", "replace")


# ---- tmux 管道收流的运行参数 ----
# 远端 pipe 文件目录：tmux 的 pipe-pane 是远端 shell 执行的，路径必须是远端路径；
# 每次 run 用启动时间 tag 独立命名，run 结束即删（避免长会话把 /tmp 写满）。
_AI_PIPE_DIR = "/tmp"
# 单次增量读取上限（128 KiB，等于 RemoteShell 的 WIRE_BUDGET）：防止一条超大
# 输出把一次 MQTT 往返撑爆，轮询循环里会继续补齐剩余部分。取 128 KiB 而不是
# 更大值，是因为超过这个尺寸远端会改走暂存分片（每次 fetch 又是一次完整往返），
# 单次读取的实际耗时反而成倍增长。
_AI_PIPE_READ_MAX = 131072
# pipe 文件轮询间隔（秒）：0.12s 在"实时感"与"MQTT 往返开销"之间取平衡。
_AI_POLL = 0.12
# 人按 Ctrl-C 打断的识别与宽限期（秒）。
# 打断时整条 "echo BEGIN; sh -c ...; echo END" 前台作业链一并被中止，END 根本
# 不会输出；若死等到 timeout，AI 侧要白等几十秒才知道被中断。这里识别终端
# 对 Ctrl-C 的回显（行首 ^C），再给一小段宽限期收尾输出，仍未出现 END 就提前
# 返回并标记 interrupted=True。判据限定"行首"，避免匹配命令输出正文里的 ^C。
_AI_INTERRUPT_RE = re.compile(rb"(?:^|\r?\n)\^C\r?\n")
_AI_INTERRUPT_GRACE = 1.0

# ---- 服务端主动推送（流式响应）参数 ----
# 每个会话一条独立 topic（随机后缀），避免与 RemoteShell.stream 的周期汇报
# 或另一台客户端的命令流相互串帧；订阅在会话期内只做一次（见 _push_ready），
# 之后每 run 只是远端起一个推送 worker，客户端零额外订阅往返。
_AI_STREAM_TOPIC_PREFIX = "sys/device/ai"
# 实时预览缓冲上限：tail() 只回最后这么多原始字节，够看长命令进度又不吃内存。
_AI_LIVE_MAX = 65536


def _tmux_rows(data: bytes, n: int):
    """把 ``tmux -F 'a|b|c'`` 的输出切成字段列表（不足 n 列的脏行直接丢弃）。"""
    rows = []
    for line in data.decode("utf-8", "replace").splitlines():
        parts = line.split("|")
        if len(parts) >= n:
            rows.append(parts)
    return rows


# ==================== AI 命令严格编码（base64 透明传输，任意特殊字符不破裂） ====================
#
# 背景（实证根因）：AIBridge.run() 是把一整行文本「敲」进远端**交互式
# 登录 shell** 的 PTY。旧实现用 ``sh -c '<cmd>'``（shlex.quote，外层单
# 引号），命令自身的单引号靠 ``'"'"'`` 拼接——在 bash 下通常无碍，但在
# BusyBox ash 或 PTY→tmux→ssh 多层嵌套等环境里，交互行编辑/多层引号
# 只要吃掉一层配对，内层命令即被截断，典型现象：
#   printf '<内容>' >> file  →  printf '' >> file（文件被创建，0 字节）。
# 而「外层单引号/双引号 + 手工转义」这类方案都必须逐字符对抗**两层 shell
# 解析 + TTY 行规程**：``$``、`` ` ``、``"``、``\``、``!``（交互 bash
# 历史展开）、裸换行与控制字节（ICANON 下直接当回车/Ctrl-C），各有各的
# 坑，无法对任意字节严格无损。
#
# 方案（成熟库严格编码）：用标准库 base64 把整条命令编码为纯 ASCII 安全
# 字符集（A-Za-z0-9+/=，不含任何 shell/TTY 元字符，也不产生裸换行），
# 远端解码后作为**单个 argv** 交给 sh -c；命令原始字节完全不经过外层交互壳
# 的引号解析，从根本上消除嵌套破裂面。外层即「双引号包裹」：
#   sh -c "$(printf '%s' <B64> | <解码管线>)"
# 双引号内命令替换的结果不再做单词拆分/glob，也不会重新解析替换文本里的
# 引号/$/反引号，所以 sh 拿到的命令字符串与编码前逐字节一致（仅去掉尾随
# 换行——对 shell 语义无影响）。解码管线全是固定字面量，内层只用单引号，
# 与外层双引号不同型、互不嵌套；解码器代码里的括号也在单引号保护内，不会
# 提前闭合 $( )。
#
# 解码器三级回退：base64（coreutils 与 BusyBox 默认都带该 applet）→
# openssl（enc -d -base64 -A，单行）→ python3/python（远端本来就运行着
# Python，最终兜底）。已在 bash/dash/ash 语法与 base64/openssl/python
# 三条解码分支上实测通过。

# 注意：本程序文本必须**不含单引号**（它被单引号裹在 python -c '...' 里），
# 也不含双引号/$/反引号（外层是 sh -c "..."）；只用字母数字与 . , ; ( )。
_AI_B64_DECODER_PY = (
    "import sys,base64;"
    "sys.stdout.buffer.write(base64.b64decode(sys.stdin.buffer.read()))"
)
_AI_B64_DECODE_PIPELINE = (
    "{ base64 -d 2>/dev/null"
    " || openssl enc -d -base64 -A 2>/dev/null"
    " || python3 -c '%s' 2>/dev/null"
    " || python -c '%s' 2>/dev/null; }"
    % (_AI_B64_DECODER_PY, _AI_B64_DECODER_PY)
)


def _build_ai_run_line_b64(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造 run() 要敲进远端交互 shell 的命令行（**不含尾部回车**）。

    见上方「AI 命令严格编码」块注释：``cmd`` 经 base64 透明传输，远端
    POSIX shell（sh/bash/ash/dash）解码后交给 ``sh -c``，单双引号、
    ``$``、反引号、分号、裸换行等任意字符都不会改变命令本体。前置条件
    （与旧实现相同）：外层登录 shell 为 POSIX 系，且远端至少有
    base64 / openssl / python 三者之一。

    尾部回车由投递方决定：走 PTY 时追加 ``\\r``，走 tmux 时另发一个
    ``Enter`` 键（见 :meth:`AIBridge._send_keys`），因为 tmux 的
    ``send-keys -l`` 把 ``\\r`` 当字面字节写进 pane，语义不如单发 Enter 清晰。
    """
    b64 = base64.b64encode(str(cmd).encode("utf-8", "replace")).decode("ascii")
    # b64 字符全部落在 shell 安全字符集（字母数字 + /+=），printf '%s'
    # 原样吐出即可，无需再包引号；模板里的 %%s 经 Python % 格式化后是字面 %s。
    return (
        'echo %s; sh -c "$(printf \'%%s\' %s | %s)"; __ai_rc=$?; '
        'echo %s:$__ai_rc'
        % (begin_tag, b64, _AI_B64_DECODE_PIPELINE, end_tag)
    )


def _build_ai_run_line_raw(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造「命令原文可见」模式的命令行（**不含尾部回车**）。

    与 b64 版的唯一区别：命令本体不进 base64，而是用 ``shlex.quote`` 做
    **一层** POSIX 单引号转义后直接交给 ``sh -c``。人切到该 tmux 窗口时
    看到的就是：

        echo __AI_BEGIN_xxxx__; sh -c '用户实际要跑的命令'; __ai_rc=$?; echo __AI_END_xxxx__:$__ai_rc

    为什么这里一层转义就够、不会重蹈旧实现「多层嵌套引号被吃」的覆辙：
    旧实现是 ``sh -c "$(printf %s B64 | base64 -d)"``，命令文本要穿过
    「外层交互 shell 的双引号 + 命令替换 + 内层 sh -c」多层；现在命令文本
    只穿过「目标 shell 的一层词法解析」，shlex.quote 产出的单引号片段在
    任何 POSIX shell（sh/bash/ash/dash）里都是严格无损的——内部的单引号
    由 ``'"'"'`` 展开，不会与外层配对出错。

    已知边界（因此 run() 会自动降级到 b64 模式）：
    - 命令含裸换行/控制字节：交互 shell 会当回车或 Ctrl-C 提交，无法作为
      一行完整输入；
    - 命令超过 pty 规范模式单行上限 MAX_CANON（Linux 通常 4096 字节）时，
      交互 shell 读走前写入端会阻塞，极端情况下被截断。
    """
    return (
        "echo %s; sh -c %s; __ai_rc=$?; echo %s:$__ai_rc"
        % (begin_tag, shlex.quote(str(cmd)), end_tag)
    )


# 支持 ANSI-C 引用 $'...' 的交互 shell（按可执行文件名判断）。dash/POSIX sh
# 不支持：在 dash 里 $'a\nb' 不会报错，而是退化成字面量，命令会静默跑错，
# 所以「猜」是绝对不能做的——探测不到一律走 base64。
_AI_DOLLAR_SHELLS = frozenset(("bash", "zsh", "ksh", "ksh93", "mksh", "ash"))


def _shell_basename(path) -> str:
    """从 shell 路径（/bin/bash、-bash）取可执行文件名，用于能力判断。"""
    text = str(path or "").strip().replace("\\", "/")
    return text.rsplit("/", 1)[-1].strip().lower().lstrip("-")


def _dollar_quote(cmd: str) -> str:
    """把任意文本转成 ``$'...'`` 形式：输出里不含任何裸控制字节。

    ``$'...'`` 内部只解析反斜杠序列，``$``/反引号/``!`` 都是字面量。转义
    规则：反斜杠和单引号各自加倍，换行/制表/回车写成 ``\\n``/``\\t``/
    ``\\r``，其余 C0 与 DEL 写成 ``\\xHH``。NUL 无法表示，调用方应改用
    base64。
    """
    out = []
    for ch in str(cmd):
        if ch == "\\":
            out.append("\\\\")
        elif ch == "'":
            out.append("\\'")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\t":
            out.append("\\t")
        elif ch == "\r":
            out.append("\\r")
        elif ord(ch) < 32 or ord(ch) == 127:
            out.append("\\x%02x" % ord(ch))
        else:
            out.append(ch)
    return "$'" + "".join(out) + "'"


def _build_ai_run_line_dollar(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造「多行命令仍可读」的命令行（**不含尾部回车**）。

    ``$'...'`` 由交互 shell（bash/zsh）解析成一个字符串交给 ``sh -c``，所以
    窗口里看到的是一条带 ``\\n`` 的可读单行，而不是 base64 密文；命令本体
    不经过任何引号嵌套，特殊字符不会破裂。只对 :data:`_AI_DOLLAR_SHELLS`
    里的 shell 使用（见 ``AIBridge._pick_encoding``）。
    """
    return (
        "echo %s; sh -c %s; __ai_rc=$?; echo %s:$__ai_rc"
        % (begin_tag, _dollar_quote(str(cmd)), end_tag)
    )


class AIBridge:
    """把这个常驻 PTY 暴露给 AI / 外部进程调用，命令全程可见。

    背景：每次 ``python cmd_client_mqtt.py`` 都是新进程，要重新并发连接
    十几个公共 broker（首个上线通常 <1s，慢时数秒）。本进程启动时已内置
    一个本地 HTTP RPC 端口（默认 1188，见 main 的 server_http），外部
    进程**不新建 MQTT 连接**，HTTP 下发一行 Python 即可复用本 PTY：

        import json
        p.set_data(json.dumps(ai_bridge.run("uname -a"), ensure_ascii=False))
        ai_bridge.send("y\\n")      # 给交互提示喂键（密码/y/n）
        ai_bridge.status()          # 会话存活 / broker 在线数 / 是否忙

    run() 把命令包在一对带启动时间的标记之间写入同一个常驻 shell，回收两个标记
    之间的输出与退出码；输入回显和输出照样实时渲染在监控窗口。PTY 只有
    一个 shell，命令用锁串行化，重叠调用立刻拿到 busy 而不是互相串台。
    """

    def __init__(self):
        self.pty = None
        self.transport = None
        self._listeners = []
        self._llock = threading.Lock()
        self._busy = threading.Lock()
        # ---- AI 目标窗口（tmux）相关状态 ----
        # _pane_id 是**固化后**的默认目标 pane（tmux 的 pane_id，形如 %2）。
        # 为什么不存 "0:2" 这种 target：0 是 session 名、2 是 window_index，
        # 二者都会被改名/重排（renumber-windows 在窗口关闭后会重排 index、
        # base-index 可配成非 0、move-window/swap-window 会互换 index），
        # 只有 pane_id（%N）在整个 tmux server 生命周期内稳定不变。
        self._pane_id = None
        self._pane_source = ""     # auto（首次解析）/ cli（--ai-pane）/ rpc（pty_pane）
        # _tmux_ok 三态：None=尚未探测；True=可用；False=不可用（回落原生 PTY 投递）
        self._tmux_ok = None
        self._shell = None          # 延迟创建的 RemoteShell（只在 tmux 路径才需要）
        # AI 命令编码：raw=命令原文直接可见（默认）；base64=旧严格编码兜底；
        # dollar=$'...'  ANSI-C 引用（仅会话 shell 支持时才自动启用）
        self._encoding = "raw"
        # 会话启动时由 set_shell 从服务端登录 shell 推断，用于 $'...' 可用性判断
        self._shell_name = ""
        # ---- 服务端主动推送（流式）状态 ----
        self._push_topic = None     # 本会话订阅的命令流 topic（首次 push 时创建）
        self._push_runs = {}        # sid -> 该次 run 的收帧回调（网络线程读）
        # 正在执行 AI 命令的 pane：命令跨窗口运行时（人不在该窗口）按键不能
        # 写本 PTY，必须用 tmux send-keys 才能落到同一条前台作业上。
        self._active_pane = None
        # 实时预览：当前/最近一次命令已产出的输出（tail() 用，不占 busy 锁）
        self._live = {"cmd": None, "buf": bytearray(), "start": None,
                      "channel": None}
        self._live_lock = threading.Lock()

    # ---- 会话侧调用（run_session 内挂载） ----

    def attach(self, pty, transport):
        self.pty = pty
        self.transport = transport
        # 每次重新 attach 都要清掉上一个会话的 tmux 探测结果与固化窗口：
        # 新会话可能连的是另一台机器/另一个 tmux server，旧 pane_id 必然失效。
        self._shell = None
        self._tmux_ok = None
        self._pane_id = None
        self._pane_source = ""
        # 推送订阅属于 transport（新会话 = 新 transport），一并作废重订。
        self._push_topic = None
        self._push_runs = {}
        self._active_pane = None
        self._shell_name = ""  # 新会话的 shell 可能不同，等 set_shell 重新注入
        with self._live_lock:
            self._live = {"cmd": None, "buf": bytearray(), "start": None,
                          "channel": None}

    def configure(self, pane="", encoding="") -> None:
        """会话启动时注入 CLI 配置（--ai-pane / --ai-encoding）。

        pane 为空 = 保持 auto（首次 run 时解析「index 最小的窗口」并固化）；
        非空 = 用户显式指定，直接固化为 CLI 来源，不再自动解析。
        """
        if pane is not None and str(pane).strip():
            self._pane_id = str(pane).strip()
            self._pane_source = "cli"
        enc = str(encoding or "").strip().lower()
        if enc in ("raw", "base64", "dollar"):
            self._encoding = enc

    def set_shell(self, shell) -> None:
        """会话启动时记录远端登录 shell（pty.open 的 env['shell']）。

        只用于判断 ``$'...'`` 是否可用：bash/zsh/ksh/mksh/ash 支持，dash 与
        未知一律不猜（含控制字符的命令退回 base64）。在 attach 之后调用，
        同一实例跨会话重连时由 attach 清空。
        """
        self._shell_name = _shell_basename(shell)

    def _dollar_ok(self) -> bool:
        """当前会话的交互 shell 是否支持 ANSI-C 引用 ``$'...'``。"""
        return self._shell_name in _AI_DOLLAR_SHELLS

    def feed(self, chunk: bytes):
        """PTY 下行分流：主窗口渲染之外，给每个 run() 收集器一份副本。"""
        with self._llock:
            qs = list(self._listeners)
        for q in qs:
            try:
                q.put_nowait(chunk)
            except queue.Full:
                pass

    # ---- AI 侧调用（经本地 HTTP RPC 进入本进程） ----

    def status(self) -> dict:
        pty, tr = self.pty, self.transport
        online, total, hosts = _broker_status(tr) if tr is not None else (0, 0, [])
        return {
            "ok": True,
            "attached": pty is not None,
            "end_reason": getattr(pty, "end_reason", None),
            "busy": self._busy.locked(),
            "sid": getattr(pty, "sid", None),
            "brokers_online": online,
            "brokers_total": total,
            "brokers": hosts,
            # AI 目标窗口：pane 为 None 说明 tmux 不可用或尚未探测，
            # 此时 run() 会走原生 PTY 投递（等价改造前的行为）。
            "ai_pane": self._pane_id,
            "ai_pane_source": self._pane_source or "auto",
            "ai_encoding": self._encoding,
            # 会话 shell 名（bash/zsh/…）与 $'...' 可用性，便于排查为什么
            # 多行命令走了 base64 而不是可读的 dollar 模式。
            "ai_shell": self._shell_name,
            "ai_dollar_ok": self._dollar_ok(),
            "tmux_ok": self._tmux_ok,
            # 最近一次命令实际走的通道：pty / tmux（轮询）/ tmux+push（推送）
            "ai_channel": self._live["channel"],
            "ai_stream_topic": self._push_topic,
            # 终端写线程卡滞秒数（窗口冻结的直接信号）；>2 建议 redraw
            "writer_stall": round(_OUT_WRITER.stall_seconds(), 2)
            if _OUT_WRITER is not None else 0.0,
        }

    def send(self, data) -> dict:
        """原样发按键/字节，不做标记回收（回答密码、y/n、进 vim 后操作等）。"""
        pty = self.pty
        if pty is None or getattr(pty, "end_reason", None):
            return {"ok": False,
                    "error": "pty 未连接或已结束: %s" % getattr(pty, "end_reason", None)}
        if isinstance(data, str):
            data = data.encode("utf-8")
        # AI 命令正在**另一个 tmux 窗口**里执行时，Ctrl-C 只有送进那个 pane
        # 才能打断前台作业；写本 PTY 只会把 ^C 打到人自己的 shell 上。
        if b"\x03" in data and self._active_pane:
            r = self._tmux(["send-keys", "-t", self._active_pane, "C-c"])
            return {"ok": r is not None and r.rc == 0, "sent": len(data),
                    "pane": self._active_pane, "interrupt": True}
        pty.send(bytes(data))
        return {"ok": True, "sent": len(data)}

    def run(self, cmd, timeout: float = 60.0, acquire_timeout: float = 2.0,
            pane=None, encoding=None, stream=None, on_output=None) -> dict:
        """在常驻 shell / 指定 tmux 窗口里跑一条命令，返回 rc 与输出。

        三条投递通道，同一套时间戳 marker：

        1. **tmux 推送通道（默认，tmux 可用时）**：命令行经远端 ``tmux_stream``
           一次下发，服务端把 pane 输出 pipe-pane 落盘后**本地增量读、逐帧
           主动 publish**；客户端只订阅收流，不再"每读一次 = 一次完整 RPC"。
           首字节延迟与总耗时都不随输出体积线性增长（大包 fetch 的节流跳变
           被彻底绕开）。
        2. **tmux 轮询通道（推送不可用时的回落）**：服务端不认识 ``tmux_stream``
           （旧版本）或推送启动失败时，退回"客户端按 offset 轮询增量读"。
        3. **原生 PTY 通道（tmux 不可用时的回落）**：命令行直接写入本 PTY 的
           shell，输出从 feed 分流队列回收，行为与改造前一致。

        参数 ``pane``：目标窗口；``None``/空 = 用固化的默认窗口（连接后首次
        run 解析「index 最小的窗口」并固化），非空 = 本次临时指定，不改默认。
        参数 ``encoding``：``raw``（默认自动：无控制字节原文直发；有裸换行且
        会话 shell 为 bash/zsh 时用 ``$'...'`` 保持可读；否则 base64）、
        ``dollar``（强制 ANSI-C 引用）、``base64``（强制严格编码）。
        选择只看字符构成，与命令长度无关。
        参数 ``stream``：``auto``（默认，按上面的三级回落）或 ``off``（强制
        走轮询通道，便于对比/排障）。
        参数 ``on_output``：可选回调 ``fn(chunk: bytes)``，命令执行过程中**边
        产生边回调**（推送通道下在网络线程里调用，回调要足够快）；配合
        :meth:`tail` 可让外部进程实时看到长命令进度，而不用等整条返回。

        - 超时只停止回收，不杀远端命令（输出继续在窗口里刷，可用 send 干预）；
        - 退出码取自命令尾部 ``echo END:$?``（tmux 没有能取 pane 内子命令
          rc 的 API；``#{pane_dead_status}`` 只在 pane 主进程死亡时才有值）。
        """
        pty = self.pty
        if pty is None:
            return {"ok": False, "error": "pty 尚未连接"}
        if pty.end_reason:
            return {"ok": False, "error": "pty 已结束: %s" % pty.end_reason}
        if not self._busy.acquire(timeout=max(0.0, float(acquire_timeout))):
            return {"ok": False, "busy": True,
                    "error": "busy：上一条 AI 命令尚未结束"}

        # 标记带**客户端本地时间**（精确到毫秒）而不是随机串：
        # `__AI_BEGIN_20261028_141530_123__` 在窗口/日志/历史回看时能一眼看出
        # 这条命令是什么时候开始跑的；毫秒级且命令串行执行，天然不重复。
        _now = time.time()
        tag = (time.strftime("%Y%m%d_%H%M%S", time.localtime(_now))
               + "_%03d" % int((_now % 1) * 1000))
        begin = "__AI_BEGIN_%s__" % tag
        end = "__AI_END_%s__" % tag
        with self._live_lock:
            self._live = {"cmd": str(cmd), "buf": bytearray(),
                          "start": time.monotonic(), "channel": None}
        try:
            target = self._pick_pane(pane)
            self._active_pane = target
            enc = self._pick_encoding(cmd, encoding)
            if target is None:
                return self._run_via_pty(str(cmd), float(timeout), begin, end, enc,
                                         on_output)
            if str(stream if stream is not None else "auto").lower() != "off":
                res = self._run_via_tmux_push(target, str(cmd), float(timeout),
                                              begin, end, enc, tag, on_output)
                if res is not None:
                    return res
            return self._run_via_tmux(target, str(cmd), float(timeout),
                                      begin, end, enc, tag, on_output)
        finally:
            self._active_pane = None
            self._busy.release()

    # ---- 实时预览：不占 busy 锁，命令执行期间随时可查 ----

    def _live_add(self, chunk: bytes) -> None:
        with self._live_lock:
            buf = self._live["buf"]
            buf += chunk
            if len(buf) > _AI_LIVE_MAX:
                del buf[:-_AI_LIVE_MAX]

    def _live_channel(self, name: str) -> None:
        with self._live_lock:
            self._live["channel"] = name

    def tail(self, max_bytes: int = 8000) -> dict:
        """看**当前正在跑**（或刚跑完）命令的最新输出，不需要等它结束。

        与 ``run`` 共用一份实时缓冲，但不占 busy 锁，所以长命令执行期间外部
        可以并发轮询本方法观察进度（MCP 侧由 ``pty_tail`` 暴露）。
        """
        n = max(1, min(int(max_bytes or 8000), _AI_LIVE_MAX))
        with self._live_lock:
            buf = bytes(self._live["buf"])
            elapsed = (round(time.monotonic() - self._live["start"], 2)
                       if self._live["start"] else None)
            info = {"cmd": self._live["cmd"],
                    "channel": self._live["channel"],
                    "elapsed": elapsed, "total": len(buf)}
        info.update({"ok": True, "tail": _strip_ansi(buf[-n:])})
        return info

    # ---- 投递通道：原生 PTY（tmux 不可用时的回落，等价改造前行为） ----

    def _run_via_pty(self, cmd, timeout, begin, end, enc, on_output=None) -> dict:
        pty = self.pty
        q = self._add_listener()
        line = self._build_line(cmd, begin, end, enc)
        buf = b""
        timed_out = False
        m_end = None
        end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
        self._live_channel("pty")
        try:
            pty.send((line + "\r").encode("utf-8"))
            deadline = time.monotonic() + timeout
            while True:
                if pty.end_reason:
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    timed_out = True
                    break
                try:
                    chunk = q.get(timeout=min(0.5, remain))
                except queue.Empty:
                    continue
                buf += chunk
                self._live_add(chunk)
                if on_output is not None:
                    try:
                        on_output(chunk)
                    except Exception:
                        pass
                m_end = end_re.search(buf)
                if m_end:
                    break
        finally:
            self._remove_listener(q)
        return self._pack_result(buf, m_end, begin, end, timed_out,
                                 {"pane": None, "channel": "pty", "encoding": enc,
                                  "end_reason": self.pty.end_reason})

    # ---- 投递通道：tmux（send-keys 投递 + pipe-pane 增量收流） ----

    def _run_via_tmux(self, target, cmd, timeout, begin, end, enc, tag,
                      on_output=None) -> dict:
        path = "%s/.pty_ai_%s.log" % (_AI_PIPE_DIR, tag)
        end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
        buf = b""
        off = 0
        timed_out = False
        interrupted = False
        interrupt_seen = False
        grace_deadline = None
        m_end = None
        # 顺序必须是「先挂管道，再敲命令」：pipe-pane 只捕获挂载之后该 pane
        # 产生的输出，反过来的话 BEGIN 附近的最早几行会丢。
        started = self._pipe_start(target, path)
        self._live_channel("tmux")
        try:
            if not started:
                return {"ok": False, "pane": target, "channel": "tmux",
                        "error": "tmux pipe-pane 失败（窗口不存在或 tmux 不可用）"}
            if not self._send_keys(target, self._build_line(cmd, begin, end, enc)):
                return {"ok": False, "pane": target, "channel": "tmux",
                        "error": "tmux send-keys 失败（窗口不存在或 tmux 不可用）"}
            deadline = time.monotonic() + timeout
            while True:
                if self.pty.end_reason:
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    timed_out = True
                    break
                # 轮询而不是让远端长驻 tail -f：后者要额外管理生命周期与
                # 清理，而 0.12s 一次的 MQTT 往返开销远小于一次整帧传输。
                time.sleep(min(_AI_POLL, max(0.02, remain)))
                chunk, off = self._pipe_read(path, off)
                if chunk:
                    buf += chunk
                    self._live_add(chunk)
                    if on_output is not None:
                        try:
                            on_output(chunk)
                        except Exception:
                            pass
                    m_end = end_re.search(buf)
                    if m_end:
                        break
                    # 尾部窗口比对：行首 ^C 可能跨两次读取，只看最后 128 字节。
                    if not interrupt_seen and _AI_INTERRUPT_RE.search(buf[-128:]):
                        interrupt_seen = True
                        grace_deadline = time.monotonic() + _AI_INTERRUPT_GRACE
                if grace_deadline is not None and time.monotonic() >= grace_deadline:
                    interrupted = True
                    break
            # 命中 END 时最后一段可能还在管道缓冲里，再补读一次。
            chunk, off = self._pipe_read(path, off)
            buf += chunk
            self._live_add(chunk)
            m_end = m_end or end_re.search(buf)
        finally:
            if started:
                self._pipe_stop(target)
            self._run_shell("rm -f " + shlex.quote(path))
        return self._pack_result(buf, m_end, begin, end, timed_out,
                                 {"pane": target, "channel": "tmux",
                                  "encoding": enc,
                                  "interrupted": interrupted,
                                  "end_reason": self.pty.end_reason})

    # ---- 投递通道：tmux 服务端主动推送（流式响应，客户端零往返收流） ----

    # 推送启动回包异常时，等一小段确认远端是否其实已在推流（回包丢/超时）。
    _PUSH_START_WAIT = 1.5

    def _push_ready(self) -> bool:
        """确保本会话已在命令流 topic 上挂好分发。返回是否具备推送能力。"""
        if self._push_topic:
            return True
        tr = self.transport
        if tr is None or not callable(getattr(tr, "stream_subscribe", None)) \
                or not callable(getattr(tr, "stream_unsubscribe", None)):
            return False
        topic = "%s/%s" % (_AI_STREAM_TOPIC_PREFIX, os.urandom(4).hex())
        try:
            tr.stream_subscribe(topic, self._on_push_frame)
        except Exception:
            return False
        self._push_topic = topic
        return True

    def _on_push_frame(self, data) -> None:
        """网络线程入口：按 sid 把帧转给对应 run 的收帧回调。"""
        if not isinstance(data, dict):
            return
        sid = data.get("stream")
        if not sid:
            return
        cb = self._push_runs.get(sid)
        if cb is None:
            return
        try:
            cb(data)
        except Exception:
            pass

    def _run_via_tmux_push(self, target, cmd, timeout, begin, end, enc, tag,
                           on_output=None):
        """服务端主动推送通道；返回 None 表示**未执行命令**、可安全回落轮询。

        安全性：只有"远端不认识本 op（旧版本）"才回落——此时没有任何副作用。
        一旦命令可能已被敲进 pane（op 成功，或回包丢失但帧已到），本方法要么
        继续收流，要么把错误结果返回给调用方，**绝不复用轮询再投一次**，
        避免同一条命令被执行两遍。
        """
        if not self._push_ready():
            return None
        sid = "ai-%s" % tag
        path = "%s/.pty_ai_%s.log" % (_AI_PIPE_DIR, tag)
        end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
        st = {"buf": bytearray(), "end": None, "rc": None, "frames": 0}
        lock = threading.Lock()
        wake = threading.Event()
        bufs = []

        def _on_chunks(chunks):
            with lock:
                bufs.extend(chunks)
            wake.set()

        # 复用 PTY 通道同一套重排器：同一帧会经每个 broker 各到一份，且路径
        # 抖动会让后发的帧先到；输出必须按 seq 连续拼装才不串字。
        reorder = _PtyReorderBuffer(_on_chunks)

        def _on_frame(data):
            if data.get("end"):
                st["end"] = str(data.get("reason") or "end")
                st["rc"] = data.get("rc")
                wake.set()
                return
            chunk = data.get("d")
            if isinstance(chunk, str):
                chunk = chunk.encode("latin-1")
            elif not isinstance(chunk, (bytes, bytearray)):
                return
            if not chunk:
                return
            st["frames"] += 1
            try:
                reorder.add(int(data.get("seq", 0)), bytes(chunk))
            except (TypeError, ValueError):
                reorder.add(0, bytes(chunk))

        sh = self._shell_obj()
        if sh is None:
            return None
        self._push_runs[sid] = _on_frame
        self._live_channel("tmux+push")
        started = False
        try:
            sh.tmux_stream(
                sid=sid, pane=target,
                line=self._build_line(cmd, begin, end, enc),
                path=path, end_marker=end, topic=self._push_topic,
                ttl=min(max(10.0, timeout + 30.0), 7200.0))
            started = True
        except Exception as e:
            # 回包可能只是丢了（远端其实已在推流）：等一小段确认是否真有帧。
            wait_until = time.monotonic() + self._PUSH_START_WAIT
            while (time.monotonic() < wait_until and st["end"] is None
                   and st["frames"] == 0):
                wake.wait(0.1)
                wake.clear()
            if st["frames"] == 0 and st["end"] is None:
                self._push_runs.pop(sid, None)
                reorder.close()
                try:
                    sh.tmux_stream_stop(sid, req_timeout=5.0)
                except Exception:
                    pass
                if isinstance(e, RemoteOpError) and "unknown op" in str(e):
                    return None      # 旧服务端：无副作用，安全回落轮询
                return {"ok": False, "pane": target, "channel": "tmux+push",
                        "error": "推送通道启动失败: %s" % (e,),
                        "encoding": enc}
            started = True           # 帧已到：worker 活着，走正常收流

        timed_out = False
        try:
            deadline = time.monotonic() + timeout
            while True:
                if self.pty.end_reason:
                    break
                with lock:
                    fresh = b"".join(bufs)
                    del bufs[:]
                if fresh:
                    st["buf"] += fresh
                    self._live_add(fresh)
                    if on_output is not None:
                        try:
                            on_output(fresh)
                        except Exception:
                            pass
                    if end_re.search(st["buf"]):
                        break
                if st["end"] is not None:
                    # 结束帧可能先于乱序数据帧到达：短暂收尾，避免吃掉尾巴。
                    wake.wait(0.25)
                    wake.clear()
                    with lock:
                        fresh = b"".join(bufs)
                        del bufs[:]
                    if fresh:
                        st["buf"] += fresh
                        self._live_add(fresh)
                        if on_output is not None:
                            try:
                                on_output(fresh)
                            except Exception:
                                pass
                    break
                remain = deadline - time.monotonic()
                if remain <= 0:
                    timed_out = True
                    break
                wake.wait(min(0.1, remain))
                wake.clear()
        finally:
            reorder.close()
            self._push_runs.pop(sid, None)
            # 命中 END（本地标记或远端结束帧）时服务端 worker 已自行退出并
            # 清理管道，再发一次 stop 只是白白多一轮 RPC（实测约 0.4s）。
            if started and st["end"] is None \
                    and not end_re.search(bytes(st["buf"])):
                sh.tmux_stream_stop(sid, req_timeout=5.0)
        m_end = end_re.search(bytes(st["buf"]))
        return self._pack_result(
            bytes(st["buf"]), m_end, begin, end, timed_out,
            {"pane": target, "channel": "tmux+push", "encoding": enc,
             "frames": st["frames"],
             "interrupted": st["end"] == "interrupted",
             "stream_end": st["end"],
             "end_reason": self.pty.end_reason})

    # ---- 目标窗口解析 ----

    def _pick_pane(self, pane):
        """决定本次命令投递到哪个 pane；返回 None 表示走原生 PTY 通道。

        优先级：显式参数 > 固化值（--ai-pane / pty_pane / 首次 auto 解析）>
        现场解析。只有现场解析失败（tmux 不可用/没有 session）才回落。
        """
        explicit = str(pane or "").strip()
        if explicit and explicit.lower() != "auto":
            return explicit
        if self._pane_id:
            return self._pane_id
        return self._resolve_pane()

    def _resolve_pane(self):
        """解析默认窗口：**session 内 window_index 最小的窗口的 active pane**。

        为什么不能写死 "0:0"：
        - ``base-index`` 可配成 1 或任意值，0 号窗口可能根本不存在；
        - ``renumber-windows`` 打开时，窗口关闭后剩余窗口的 index 会被重排，
          "0:0" 指向的 pane 会漂移；
        - ``move-window`` / ``swap-window`` 也会改 index。
        所以每次都按**当前实际列表**取最小 index，并把结果的 pane_id（%N）
        固化下来——pane_id 在整个 tmux server 生命周期内不会变。

        会话选择：优先 attached>0 且 ``session_last_attached`` 最大的会话。
        实测同一台机器有 8 个 client attach 到同一 session，``select-window``
        只改某个 client 的视图，与"人现在看哪个"不是一回事，而这个字段稳定。
        """
        r = self._tmux(["list-sessions", "-F",
                        "#{session_name}|#{session_attached}|#{session_last_attached}"])
        if r is None or r.rc != 0:
            self._tmux_ok = False
            return None
        self._tmux_ok = True
        rows = _tmux_rows(r.stdout, 3)
        if not rows:
            self._tmux_ok = False
            return None
        try:
            session = max(rows, key=lambda x: (int(x[1] or 0), int(x[2] or 0)))[0]
        except ValueError:
            session = rows[0][0]
        # -s 才是"整个 session 的所有 pane"；不带 -s 只列 target window 的 pane。
        r = self._tmux(["list-panes", "-s", "-t", session, "-F",
                        "#{window_index}|#{pane_index}|#{pane_id}|#{pane_active}"])
        if r is None or r.rc != 0:
            self._tmux_ok = False
            return None
        rows = _tmux_rows(r.stdout, 4)
        if not rows:
            self._tmux_ok = False
            return None

        def _key(x):
            # 主键 window_index 数值最小；同窗口内 active pane 优先，再按 pane_index。
            try:
                return (int(x[0]), -int(x[3] or 0), int(x[1]))
            except ValueError:
                return (1 << 30, 0, 1 << 30)

        rows.sort(key=_key)
        self._pane_id = rows[0][2]
        self._pane_source = self._pane_source or "auto"
        return self._pane_id

    def _pick_encoding(self, cmd, encoding) -> str:
        """选本次命令的编码：raw / dollar / base64。

        - ``raw``（默认）：命令**没有**控制字节就原样直发（窗口可见）；
          有裸换行/控制字节时：会话 shell 属于 :data:`_AI_DOLLAR_SHELLS`
          才转 ``$'...'``（窗口仍可读），否则退回 base64。dash 里 ``$'...'``
          会静默变成字面量，所以**绝不猜**。
        - ``dollar``：显式要求 ANSI-C 引用；NUL 无法表示，只能退 base64。
        - ``base64``：强制严格编码。

        选择只看字符构成，与命令长度无关（长度受 pty 单行上限影响，见
        ``_build_ai_run_line_raw`` 的已知边界）。
        """
        text = str(cmd)
        enc = str(encoding or self._encoding or "raw").strip().lower()
        if enc not in ("raw", "base64", "dollar"):
            enc = "raw"
        if enc == "dollar":
            return "base64" if "\x00" in text else "dollar"
        if enc == "base64":
            return "base64"
        if not any(ord(ch) < 32 or ord(ch) == 127 for ch in text):
            return "raw"  # 没有需要转义的字符：保持原文可见，别做无谓编码
        if "\x00" in text:
            return "base64"
        return "dollar" if self._dollar_ok() else "base64"

    @staticmethod
    def _build_line(cmd, begin, end, enc) -> str:
        if enc == "base64":
            return _build_ai_run_line_b64(cmd, begin, end)
        if enc == "dollar":
            return _build_ai_run_line_dollar(cmd, begin, end)
        return _build_ai_run_line_raw(cmd, begin, end)

    @staticmethod
    def _pack_result(buf, m_end, begin, end, timed_out, extra) -> dict:
        # 真起点是「独占一行的 BEGIN + 换行」；输入回显里 BEGIN 后面是 ';'，
        # 不会误中。结尾 END:<数字> 同理（回显里是 :$__ai_rc 字面量）。
        m_begin = re.search(re.escape(begin.encode()) + rb"\r?\n", buf)
        start = m_begin.end() if m_begin else 0
        if m_end:
            body, rc = buf[start:m_end.start()], int(m_end.group(1))
        else:
            body, rc = buf[start:], None
        res = {
            "ok": rc == 0 if rc is not None else False,
            "rc": rc,
            "timed_out": timed_out,
            "out": _strip_ansi(body).strip("\n"),
            "raw_len": len(body),
        }
        res.update(extra or {})
        return res

    # ---- 远端 tmux 辅助（全部走 RemoteShell 通道，不占本 PTY） ----

    def _shell_obj(self):
        if self._shell is None and self.transport is not None:
            self._shell = RemoteShell(self.transport, timeout=30)
        return self._shell

    def _run_shell(self, cmd):
        """在另一条远端命令通道执行普通 shell 命令，返回 CmdResult。

        这条通道不占用本 PTY，所以不会干扰人在窗口里的输入；任何异常都吞掉
        返回 None，让调用方走回落/报错分支。
        """
        sh = self._shell_obj()
        if sh is None:
            return None
        try:
            return sh.run(cmd)
        except Exception:
            return None

    def _tmux(self, args):
        """执行一条 tmux 子命令。

        参数逐项 shlex.quote 后拼进远端 ``sh -c``：这里只有**一层** POSIX
        单引号解析（远端 sh 剥掉引号后把原样字符串作为 argv 交给 tmux），
        不存在旧实现"外层交互 shell + 命令替换 + 内层 sh -c"的多层嵌套，
        因此任意特殊字符都不会被吃掉。
        """
        return self._run_shell("tmux " + " ".join(
            shlex.quote(str(a)) for a in args))

    def _pipe_start(self, pane, path) -> bool:
        """把该 pane 的输出字节流 append 到 path（tmux 原生能力）。

        与 capture-pane 的本质区别：capture-pane 只是把"当前屏幕"拍成快照
        （受 history-limit 限制、丢失中间过程）；pipe-pane 是 tmux 在 pane
        的 pty 输出处挂一根管道，拿到的是**完整、连续的字节流**。另一个关键
        好处是它不含 tmux 客户端整屏重绘的回放字节——旧实现 run() 输出里
        夹带大量历史与旧 marker，根因正是重绘混进了 PTY 流。

        不能用 ``-o``（"仅在没有 pipe 时打开"）：每次 run 都换新文件，带 -o
        会复用上一条管道继续写旧文件；不带 -o 时 tmux 先关旧管道再开新的，
        正好是这里要的语义。
        """
        r = self._tmux(["pipe-pane", "-t", pane, "cat >> " + shlex.quote(path)])
        return r is not None and r.rc == 0

    def _pipe_stop(self, pane) -> None:
        """关闭该 pane 的管道（命令留空 = 关闭）。失败忽略：下次 run 会重开。"""
        self._tmux(["pipe-pane", "-t", pane])

    def _pipe_read(self, path, off):
        """从 path 的 off 偏移增量读取，返回 (新字节, 新偏移)。

        ``tail -c +N`` 是 1 基偏移（+0 等于从头），所以传 off+1。单次用
        ``head -c`` 截断，避免一条超大输出把一次 MQTT 往返撑爆；调用方在
        轮询里继续补齐剩余部分。
        """
        cmd = ("tail -c +%d %s 2>/dev/null | head -c %d"
               % (off + 1, shlex.quote(path), _AI_PIPE_READ_MAX))
        r = self._run_shell(cmd)
        data = r.stdout if r is not None else b""
        return data, off + len(data)

    def _send_keys(self, pane, line) -> bool:
        """把命令行投递到指定 pane。

        ``-l`` 必须加：不加时 tmux 会把参数里的 ``Enter``/``C-c`` 之类当**键名**
        解析，命令文本里恰好含有这些单词就会被误解释成按键；``-l`` 禁用键名
        解析，传入什么字符就敲什么字符。``--`` 防止命令行以 ``-`` 开头时被
        tmux 当成选项。

        回车单独发一个 ``Enter`` 键，而不是在文本尾部拼 ``\\r``：pane 里的
        shell 处于规范模式/readline，单独按键的语义最接近真人敲回车。

        注意 ``-l`` 只保证 tmux 层不做键名解析，字符进入 pane 的 pty 后仍要
        被目标 shell 做一次词法解析——这正是命令本体必须用 ``sh -c`` + 单引号
        包裹（或 base64）的原因，与"能否看到原文"无关。
        """
        r = self._tmux(["send-keys", "-t", pane, "-l", "--", line])
        if r is None or r.rc != 0:
            return False
        r = self._tmux(["send-keys", "-t", pane, "Enter"])
        return r is not None and r.rc == 0

    # ---- RPC / MCP 动态调整默认窗口 ----

    def get_pane(self) -> dict:
        return {
            "ok": True,
            "pane": self._pane_id,
            "pane_source": self._pane_source or "auto",
            "tmux_ok": self._tmux_ok,
            "encoding": self._encoding,
        }

    def set_pane(self, target="") -> dict:
        """动态调整 AI 默认窗口。

        ``""``/``auto`` = 清空固化值并立即按"index 最小"重新解析；
        其他值 = 固化用户给的 target（``%N`` 最稳，也接受 ``0:2`` / 窗口名）。
        """
        t = str(target or "").strip()
        if not t or t.lower() == "auto":
            self._pane_id = None
            self._pane_source = "auto"
            p = self._resolve_pane()
            res = self.get_pane()
            res["ok"] = p is not None
            if p is None:
                res["error"] = "tmux 不可用或没有可解析的窗口"
            return res
        self._pane_id = t
        self._pane_source = "rpc"
        return self.get_pane()

    def _add_listener(self) -> queue.Queue:
        q = queue.Queue(maxsize=20000)
        with self._llock:
            self._listeners.append(q)
        return q

    def _remove_listener(self, q: queue.Queue):
        with self._llock:
            try:
                self._listeners.remove(q)
            except ValueError:
                pass


# 模块级单例：必须在 main() 调 start_rpc_server 之前就存在，
# 这样 HTTP RPC 的持久命名空间拿到的是本对象引用，会话建立后 attach 即生效。
ai_bridge = AIBridge()


