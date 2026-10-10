#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt —— PTY over 公共 MQTT broker（SSH 式交互终端）。

模型（和 SSH 一样）
===================
- 仿 cmd_client_mqtt 建立 MQTT 连接（默认 ``sys/device/request`` ↔ response）；
- 连接成功后，client 生成会话 id 并提出 in/out 两个**新 topic**，协商成功后
  用于实时数据传输（底层仍完全复用 multi_mqtt.MultiMQTTManager，不新建连接）；
- 服务端只要求运行通用 server_mqtt.py：PTY 全部功能代码（openpty + 常驻
  用户 shell + 读写线程）由本 client 在握手时**整段下发执行**；shell 只启动
  一次，之后每个按键都经 broker 写入同一个 PTY，绝不在每条命令上重启 bash；
- 服务端主动发送频率可用 ``--interval`` 设定（默认 0=有输出立刻实时回传；
  >0 时按该秒数攒批合并输出，适合高延迟网络减少帧数）。

用法
====
    python client/pty_client_mqtt.py                    # 交互式 PTY（默认参数）
    python client/pty_client_mqtt.py -i 0.2             # 服务端最多 0.2s 攒批
    python client/pty_client_mqtt.py --shell /bin/bash --cwd /root
    python client/pty_client_mqtt.py -t q -k 2333 "tmux at"
                                                        # SSH 式：连上自动执行
                                                        # 前置命令，随后留在会话里

AI 常驻调用（监控窗口模型，避免每条命令重连 broker）
====================================================
本进程启动时内置一个本地 HTTP RPC 口（默认 1188）。人类先在一个终端里
开着本窗口，AI / 其他进程不新建 MQTT 连接，直接 HTTP 复用同一个 PTY，
命令回显与输出照样在本窗口实时可见：

    from client.pty_client_mqtt import ai_pty_run, ai_pty_status, ai_pty_send
    ai_pty_status()                 # {'attached': True, 'brokers_online': 13, ...}
    ai_pty_run("uname -a")          # -> {'ok': True, 'rc': 0, 'out': '...'}
    ai_pty_run("apt install -y htop", timeout=300)
    ai_pty_send("y\\n")             # 回答交互提示（密码/确认）

裸 HTTP 等价写法（POST 一段 Python 到本地口）：

    curl "http://127.0.0.1:1188/$(python -c \
"import urllib.parse;print(urllib.parse.quote('import json;p.set_data(json.dumps(ai_bridge.run(\\\"uname -a\\\"),ensure_ascii=False))'))")"

本地诊断日志与 PTY 画面彻底分离
====================================
本终端窗口**只渲染远端 shell**；broker 连接/重连、每笔 [RPC] 请求、
告警、任何库的 print / stdout / stderr 一律不写终端，全部进进程内
环形缓冲，查看通道有三个：

  ① 浏览器全屏实时日志台（首选，和普通控制台一样，无工具栏）：

        http://192.168.1.3:1188/        # 根路径自动跳 log_html 页面
        # 页面经 WebSocket /wslog 先收全量快照再实时增量推送，
        # 滚到底自动跟随，向上翻则暂停，带极简 ANSI 着色；
        # 手机/同局域网其他机器也能直接开

  ② HTTP RPC 取文本：

        curl "http://192.168.1.3:1188/r=get_log()"     # 最近 200 行
        curl "http://192.168.1.3:1188/r=get_log(50)"   # 最近 50 行
        curl "http://192.168.1.3:1188/r=clear_log()"   # 清空

  ③ 窗口前的人按命令栏热键输 log（自擦覆盖层，不留痕迹）。

--port 0 关闭 HTTP 口时没有查看通道，退回旧行为：日志镜像到 stderr。

注意：AI 命令串行执行（一个 shell），重叠调用立刻返回 busy；run() 只
回收非交互命令的输出，vim/top 等全屏程序请人工在窗口里操作。仅本机
调用建议 --host 127.0.0.1。

断开
====
- 在远端 shell 里 ``exit`` / Ctrl-D（会话自然结束，客户端立即退出）；
- 本地脱离：Ctrl-]（默认，``--detach-key`` 可改），只断 client，远端
  tmux/shell 原封不动继续跑，稍后重连再 ``tmux at`` 即可；
- 本地命令栏：Ctrl+Alt+Insert（默认，``--menu-key`` 可改），可执行
  detach、status，并热调 interval/heartbeat/ttl/dead 等时间参数，
  输入的内容不会发到远端；
- 服务器进程被关 / 网络中断：心跳超时（``--heartbeat`` / ``--dead-timeout``）
  后客户端自动立即退出，绝不在 broker 仍在线时无限干等。

所有退出路径都学 client_mqtt.py 直接 ``os._exit``：先恢复本地终端模式、
fire-and-forget 发一帧 stop，不做 pty.close/transport.stop 那套慢清理。
"""
from __future__ import annotations

import argparse
import base64
import codecs
import io
import json
import logging
import os
import queue
import re
import shutil
import socket
import sys
import threading
import time
from collections import deque

# 本文件已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/pty_client_mqtt.py」直接运行与包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# 必须在 import multi_mqtt 之前：multi_mqtt 在 import 时会 basicConfig 往 root
# 挂写 stderr 的 StreamHandler，paho/broker 的 INFO 会插进 PTY 远端画面。
# 本进程的终端只渲染远端 shell，一切本地日志走环形缓冲 + 浏览器实时日志台。
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")
# 直接按脚本启动时没有包上下文，相对导入会失败，显式补上。
if not __package__:
    __package__ = "client"

from . import client_mqtt as _cm            # noqa: E402  复用统一别名表/_cli_opts
from .cmd_client_mqtt import (              # noqa: E402
    MqttTransport, add_connection_args,
)
from .remote_cmd import (                   # noqa: E402
    RemotePty, RemoteError, DEFAULT_PTY_TTL,
)

from multi_mqtt import stime, BROKER_LIST

# ================= 统一别名表（小写变量名） =================
# 与 client_mqtt.py 同一套规矩：语义别名不带 "--"/"%" 前缀，三处共用一份：
#   1) CLI 参数   -> _cm._cli_opts(*alias_xxx) 自动生成 --x/-x/x_x/x-x
#   2) 本地魔术栏 -> cmd in alias_xxx（输入里的 - 先归一化成 _）
#   3) 其他引用   -> 直接 import 本模块的 alias_xxx
# 连接类（request_topic/reply_topic/private_key/allow/timeout）直接复用
# client_mqtt 的表，由 cmd_client_mqtt.add_connection_args 注册，这里不重复；
# status/help/exit 也直接引用 client_mqtt 的别名，禁止在 PTY 侧另立名字。

# ---- PTY 会话参数（CLI 选项 + 可被魔术栏热调/引用） ----
alias_shell        = ('shell', 'sh')
alias_term         = ('term', 'terminal')
alias_cwd          = ('cwd', 'dir', 'workdir')
alias_interval     = ('interval', 'i', 'flush_interval', 'flush')
alias_heartbeat    = ('heartbeat', 'hb')
alias_ttl          = ('max_shell_live_time','ttl','DEFAULT_PTY_TTL')
alias_dead_timeout = ('dead_timeout', 'deadtime', 'dead')
alias_size         = ('size', 'geometry')
alias_no_login     = ('no_login', 'nologin')
alias_detach_key   = ('detach_key', 'detachkey')
alias_menu_key     = ('menu_key', 'menukey', 'magic_key')
alias_rpc_port     = ('port', 'rpc_port', 'p')
alias_rpc_host     = ('host', 'rpc_host')
alias_command      = ('command', 'cmd')
# 本地命令栏查看诊断日志（只读，不碰会话参数）
alias_log          = ('log', 'logs')
# 整屏重绘 / 写线程卡滞自愈（不会把命令发给远端 shell，只发控制帧）
alias_redraw       = ('redraw', 'refresh', 'rd')

# 魔术栏的"脱离"：语义等同退出，exit/quit 直接复用 client_mqtt 的别名表，
# PTY 语境再补 detach 系列（顺序无所谓，匹配一律用 in）。
alias_detach = tuple(dict.fromkeys(
    ('detach', 'd', 'bye', 'q', 'x') + tuple(_cm.alias_exit)))


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


# ============================ 退出时的本地终端复位 ============================

# 脱离/被杀时远端全屏程序（tmux/vim）来不及发"恢复屏幕"序列，最后一帧
# （tmux 绿色状态条等）会冻在本地终端里。退出前本地主动复位：
#   ?1049l 退出备用屏幕（tmux attach 时进入的那个，退出即恢复 attach 前画面）
#   ?1000/2/3/6l 关掉可能被远端打开的鼠标上报
#   0m 复位颜色/粗体；?25h 显示光标
_TERMINAL_CLEANUP = (b"\x1b[?1049l\x1b[?1000l\x1b[?1002l\x1b[?1003l"
                     b"\x1b[?1006l\x1b[0m\x1b[?25h\r\n")


_BIN_STDOUT_CACHE = None
_BIN_STDERR_CACHE = None


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
_OUT_WRITER = None
# stderr 异步写线程，懒初始化（_info 在 main 最早阶段就会用到）。
_ERR_WRITER = None


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
                self.dropped += len(data) - room
                data = data[:room]
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
        """有积压未写出时，距上次成功 flush 的秒数；无积压返回 0。"""
        with self._lock:
            pending = self._queued_bytes
        if pending <= 0:
            return 0.0
        return max(0.0, time.monotonic() - self._last_flush_ts)

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
        buf = _bin_stdout()
        if buf is not None:
            buf.write(_TERMINAL_CLEANUP)
            buf.flush()
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


# ============================ 本地热键：按键名解析 ============================

# 多字节热键（如 Ctrl+Alt+Insert 的 ESC[2;7~）到达时，ESC 可能与后续字节
# 被拆成两次读；先缓存这么久等剩余字节，超时就把 ESC 原样放给远端。
_HOTKEY_GAP = 0.025

# 单字节控制键
_CTRL_CHARS = {
    "ctrl-@": 0, "ctrl-[": 27, "ctrl-\\": 28, "ctrl-]": 29,
    "ctrl-^": 30, "ctrl-_": 31,
}
for _i in range(26):
    _CTRL_CHARS["ctrl-%s" % chr(ord("a") + _i)] = _i + 1
_CTRL_CHARS.update({
    "space": 32, "tab": 9, "enter": 13, "cr": 13,
    "esc": 27, "escape": 27,
})
# 以 ~ 结尾的 CSI 键：名字 -> 中间数字
_CSI_TILDE_KEYS = {
    "insert": 2, "delete": 3, "pageup": 5, "pgup": 5,
    "pagedown": 6, "pgdn": 6, "home": 1, "end": 4,
    "f5": 15, "f6": 17, "f7": 18, "f8": 19, "f9": 20, "f10": 21,
    "f11": 23, "f12": 24,
}
# 单字母 CSI 键：方向键 [A..[D；Home/End [H/[F
_CSI_LETTER_KEYS = {
    "up": "A", "down": "B", "right": "C", "left": "D",
    "home": "H", "end": "F",
}
# SS3 功能键 F1-F4：OP/OQ/OR/OS
_SS3_KEYS = {"f1": "P", "f2": "Q", "f3": "R", "f4": "S"}
_MOD_WORDS = {"ctrl": 4, "control": 4, "alt": 2, "meta": 2, "shift": 1}


def _parse_key_spec(spec) -> bytes:
    """把按键名解析成终端实际上报的字节序列。

    支持：ctrl-]/ctrl-a（单字节）、f1-f12、insert/delete/home/end/方向键、
    以及 shift/alt/ctrl 修饰组合（xterm 编码），如 ``ctrl-alt-insert`` ->
    ``ESC[2;7~``。无法识别时按 latin-1 原样返回（兼容直接传原始字节）；
    none/off/空串表示禁用。
    """
    if spec is None:
        return b""
    # 只剥普通空白：str.strip() 会把 0x1c-0x1f（含 Ctrl-] 的 0x1d）当空白
    # 剥掉，导致直接传原始控制字节做 --detach-key 时被判成"禁用"
    s = str(spec).strip(" \t\r\n").lower().replace("+", "-").replace("_", "-")
    if s in ("", "none", "off", "-", "no"):
        return b""
    if s in _CTRL_CHARS:
        return bytes([_CTRL_CHARS[s]])
    tokens = s.split("-")
    mod = 0
    while tokens and tokens[0] in _MOD_WORDS:
        mod |= _MOD_WORDS[tokens.pop(0)]
    base = "-".join(tokens)
    if base in _CTRL_CHARS:
        # 修饰键配单字节键不产生稳定序列，只接受无修饰的写法
        return bytes([_CTRL_CHARS[base]]) if mod == 0 else b""
    if base in _CSI_TILDE_KEYS:
        n = _CSI_TILDE_KEYS[base]
        mid = (";%d" % (mod + 1)) if mod else ""
        return ("\x1b[%d%s~" % (n, mid)).encode("latin-1")
    if base in _CSI_LETTER_KEYS:
        ch = _CSI_LETTER_KEYS[base]
        if mod:
            return ("\x1b[1;%d%s" % (mod + 1, ch)).encode("latin-1")
        return ("\x1b[%s" % ch).encode("latin-1")
    if base in _SS3_KEYS:
        ch = _SS3_KEYS[base]
        if mod:
            return ("\x1b[1;%d%s" % (mod + 1, ch)).encode("latin-1")
        return ("\x1bO%s" % ch).encode("latin-1")
    return str(spec).encode("latin-1", "ignore")


# 字节序列 -> 人类可读名字（banner 用），由上面的表反向构建
def _build_key_display_map():
    m = {}
    for name, code in _CTRL_CHARS.items():
        m.setdefault(bytes([code]), name)
    labels = ["", "shift-", "alt-", "alt-shift-", "ctrl-", "ctrl-shift-",
              "ctrl-alt-", "ctrl-alt-shift-"]
    for label in labels:
        for name in _CSI_TILDE_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
        for name in _CSI_LETTER_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
        for name in _SS3_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
    return m


_KEY_DISPLAY = _build_key_display_map()


def _describe_key(seq: bytes) -> str:
    if not seq:
        return "禁用"
    return _KEY_DISPLAY.get(seq, repr(seq.decode("latin-1", "replace")))


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


# ==================== 浏览器实时日志台（WebSocket，仿 realtime_editor） ====================

# 每个日志台连接的内存积压上限：浏览器卡住/慢消费时丢旧行，绝不反压业务线程
_LOG_WS_QUEUE = 1000
# 增量帧的攒批窗口：把 0.15s 内的行合并成一帧，日志风暴时不刷爆浏览器
_LOG_WS_BATCH = 0.15


class LogHub:
    """环形日志到浏览器 WebSocket 的广播枢纽。

    每个连接：先原子取当前快照（snapshot 帧），之后新行经有界队列 + 独立
    写线程增量推送（lines 帧，攒批合帧）；队列满丢最旧并在恢复时插一行
    丢弃提示。慢/死连接只影响自己，绝不拖慢写日志的 broker/渲染线程。
    """

    def __init__(self, ring: "_LogRing"):
        self._ring = ring

    def _frame(self, msg_type: str, text: str, **extra) -> str:
        payload = {"type": msg_type, "text": text,
                   "server_time": time.time(), **extra}
        return json.dumps(payload, ensure_ascii=False)

    def serve(self, websocket, _request) -> None:
        q: "queue.Queue" = queue.Queue(maxsize=_LOG_WS_QUEUE)
        state = {"dropped": 0}

        def push(seq, line):
            try:
                q.put_nowait((seq, line))
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait((seq, line))
                    state["dropped"] += 1
                except (queue.Empty, queue.Full):
                    state["dropped"] += 1

        snapshot, unsubscribe = self._ring.subscribe(push)
        stop_ev = threading.Event()

        def writer_loop():
            # 先发快照（订阅在发快照之前完成，期间新行已在队列，顺序天然正确）
            snap_text = "\n".join(line for _s, line in snapshot)
            try:
                websocket.send(self._frame(
                    "snapshot", snap_text, lines=len(snapshot),
                    buffered=len(self._ring)))
            except OSError:
                stop_ev.set()
                return
            while not stop_ev.is_set():
                try:
                    first = q.get(timeout=_LOG_WS_BATCH)
                except queue.Empty:
                    continue
                batch = [first[1]]
                # 顺带抽干队列里已到的行合成一帧
                while len(batch) < 500:
                    try:
                        batch.append(q.get_nowait()[1])
                    except queue.Empty:
                        break
                dropped = state["dropped"]
                if dropped:
                    state["dropped"] = 0
                    batch.insert(0, "…日志台积压，已丢弃 %d 旧行…" % dropped)
                try:
                    websocket.send(self._frame("lines", "\n".join(batch)))
                except OSError:
                    stop_ev.set()
                    return

        writer = threading.Thread(target=writer_loop, name="log-ws-writer",
                                  daemon=True)
        writer.start()
        try:
            while not stop_ev.is_set():
                try:
                    raw = websocket.receive()
                except (ConnectionError, OSError, ValueError):
                    break
                if raw is None:
                    break
                # 页面只发心跳；其他内容忽略（日志台对本机进程只读）
                if raw.strip() == "ping":
                    try:
                        websocket.send(json.dumps({"type": "pong",
                                                   "server_time": time.time()}))
                    except OSError:
                        break
        finally:
            stop_ev.set()
            unsubscribe()
            try:
                websocket.close()
            except Exception:
                pass


# 模块级单例：必须在 main() 调 start_rpc_server 之前存在（handler 引用它）
_LOG_HUB = LogHub(_LOCAL_LOG)


def _lan_ip() -> str:
    """取本机主网卡 IPv4（仅用于提示浏览器/手机访问地址；失败返回 127.0.0.1）。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 53))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def log_html(response):
    """浏览器全屏实时日志台页面（HTTP RPC：``/log_html(p)``，根路径自动跳转）。

    页面无任何上下工具栏：整个视口就是一个黑底等宽控制台，WebSocket 接
    /wslog，先收快照再收增量，滚到底部自动跟随、向上翻则暂停。
    """
    response.set_header("Content-Type", "text/html; charset=utf-8")
    response.set_header("Cache-Control", "no-store")
    response.set_data(_LOG_PAGE)


_LOG_PAGE = r"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pty local log</title>
<style>
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; padding: 0; overflow: hidden;
  background: #0c0c0c; color: #d8d8d8; }
#log {
  height: 100vh; width: 100vw; margin: 0; padding: 6px 10px 10px;
  border: 0; outline: 0; overflow: auto; white-space: pre-wrap;
  word-break: break-all; scrollbar-width: thin;
  font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, "Cascadia Mono", monospace;
}
#jump {
  position: fixed; left: 50%; bottom: 14px; transform: translateX(-50%);
  padding: 5px 14px; border-radius: 14px; display: none; cursor: pointer;
  background: rgba(60, 120, 200, .92); color: #fff; font: 12px/1.4 system-ui, sans-serif;
  user-select: none; box-shadow: 0 2px 10px rgba(0,0,0,.5);
}
</style>
</head>
<body>
<div id="log"></div>
<div id="jump">新日志 ↓ 点击回到底部</div>
<script>
const logEl = document.getElementById('log');
const jumpEl = document.getElementById('jump');
let socket, reconnectTimer, follow = true, documentCount = 0;

// ---- 极简 ANSI SGR 着色（其余 CSI/OSC 序列剥掉，保证只做日志查看不做终端仿真） ----
const SGR = {
  0:['',''], 1:['font-weight:bold',''], 2:['opacity:.75',''], 3:['font-style:italic',''],
  4:['text-decoration:underline',''], 9:['text-decoration:line-through',''],
  22:['font-weight:normal;opacity:1;font-style:normal',''], 23:['font-style:normal',''],
  24:['text-decoration:none',''], 27:['',''], 39:['color:',''], 49:['background:',''],
};
const FG30=['#2e2e2e','#c73535','#3f9b45','#b5a326','#3b6ec4','#a347b8','#2e8f8f','#c8c8c8'];
const FG90=['#707070','#f06868','#5fd070','#e8d250','#6c9cf0','#cd78e0','#4ec9c9','#ffffff'];
function cube(n){const v=[0,95,135,175,215,255];
  return 'rgb('+v[Math.floor(n/36)]+','+v[Math.floor((n%36)/6)]+','+v[n%6]+')';}
function color256(n){return n<16?null:n<232?cube(n-16):'rgb('+[8+10*(n-232),8+10*(n-232),8+10*(n-232)].join(',')+')';}
let curStyle='';
function sgrStyle(params){
  let css=curStyle;
  for(let i=0;i<params.length;i++){
    const p=params[i];
    if(p===0) css='';
    else if(SGR[p]) css += ';' + (p===39||p===49 ? SGR[p][0] : SGR[p][0]);
    else if(p>=30&&p<=37) css += ';color:' + FG30[p-30];
    else if(p>=90&&p<=97) css += ';color:' + FG90[p-90];
    else if(p>=40&&p<=47) css += ';background:' + FG30[p-40];
    else if(p>=100&&p<=107) css += ';background:' + FG90[p-100];
    else if((p===38||p===48)&&params[i+1]===5){const c=color256(+params[i+2]);if(c)css+=';'+(p===38?'color:':'background:')+c;i+=2;}
    else if((p===38||p===48)&&params[i+1]===2){const[r,g,b]=params.slice(i+2,i+5);css+=';'+(p===38?'color:':'background:')+`rgb(${r},${g},${b})`;i+=4;}
  }
  return css;
}
const ANSI_RE = /\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])/g;
function appendText(raw){
  if(!raw) return;
  let last=0, css=curStyle;
  raw.replace(ANSI_RE,(m,off)=>{
    if(off>last) pushSpan(raw.slice(last,off),css);
    if(m[1]==='['){
      const code=m.charCodeAt(m.length-1);
      if(code===109){const params=m.slice(2,-1).split(';').map(x=>x===''?0:+x);curStyle=sgrStyle(params);css=curStyle;}
    }
    last=off+m.length; return m;
  });
  if(last<raw.length) pushSpan(raw.slice(last),css);
}
function pushSpan(text,css){
  if(!text) return;
  let span=document.createElement('span');
  if(css){span.setAttribute('style',css.replace(/^;+/,''));}
  span.textContent=text; logEl.appendChild(span);
}
function appendBlock(text){
  if(!text) return;
  const wasFollow=follow;
  const parts=text.split('\n');
  for(let i=0;i<parts.length;i++){
    if(i>0) logEl.appendChild(document.createElement('br'));
    appendText(parts[i]);
  }
  // 帧边界永远落在行末：补一个换行，下一帧第一行不会接到本行尾巴
  logEl.appendChild(document.createElement('br'));
  documentCount += text.length + 1;
  if(documentCount > 4_000_000) {
    logEl.textContent=''; documentCount=0;
    appendText('…前面的日志已被浏览器裁剪…');
    logEl.appendChild(document.createElement('br'));
  }
  if(wasFollow) pinBottom();
}
function atBottom(){ return logEl.scrollHeight - logEl.scrollTop - logEl.clientHeight < 24; }
function pinBottom(){ logEl.scrollTop = logEl.scrollHeight; }
logEl.addEventListener('scroll',()=>{
  follow=atBottom(); jumpEl.style.display=follow?'none':'block';
},{passive:true});
jumpEl.addEventListener('click',()=>{follow=true;pinBottom();jumpEl.style.display='none';});
document.addEventListener('click',()=>{ if(!follow){follow=true;pinBottom();jumpEl.style.display='none';} },true);

function connect(){
  clearTimeout(reconnectTimer);
  socket=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/wslog');
  socket.onopen=()=>{ document.title='pty local log'; };
  socket.onclose=()=>{ appendBlock('\n[连接断开，1s 后重连]\n'); reconnectTimer=setTimeout(connect,1000); };
  socket.onerror=()=>{};
  socket.onmessage=(ev)=>{
    const msg=JSON.parse(ev.data);
    if(msg.type==='snapshot'){ logEl.textContent=''; curStyle='';
      appendText(msg.text);
      if(msg.text) logEl.appendChild(document.createElement('br'));
      pinBottom(); }
    else if(msg.type==='lines'){ appendBlock(msg.text); if(follow) pinBottom(); }
    else if(msg.type==='pong'){ /* keepalive */ }
  };
}
setInterval(()=>{ if(socket&&socket.readyState===WebSocket.OPEN) socket.send('ping'); }, 10000);
connect();
</script>
</body>
</html>"""


def _install_ring_logging() -> None:
    """会话期所有 logging 只进环形缓冲（幂等）。

    正常路径下 multi_mqtt import 前已置 CMQ_NO_STDERR_LOG，root 上只有
    NullHandler；这里仍防御性移除一切 StreamHandler——paho 或第三方库也可能
    自己往 root 挂控制台 handler——再补 ring handler，级别维持 INFO。
    """
    root = logging.getLogger()
    if not any(isinstance(h, _RingLogHandler) for h in root.handlers):
        handler = _RingLogHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        root.addHandler(handler)
    for h in list(root.handlers):
        # _RingLogHandler 直接继承 logging.Handler，不会被这里误删
        if isinstance(h, logging.StreamHandler):
            root.removeHandler(h)
    if root.level == logging.NOTSET or root.level > logging.INFO:
        root.setLevel(logging.INFO)


def _install_log_capture() -> None:
    """开启"终端只显示远端 shell"模式：logging + stdout + stderr 全部进日志环。

    必须在起好 HTTP 日志口之后调用。先锁定解释器原始控制台的二进制流
    （PTY 渲染/致命错误仍直写真实终端），再接管 logging 与文本级
    stdout/stderr；幂等。
    """
    _bin_stdout()
    _bin_stderr()
    _install_ring_logging()
    if not isinstance(sys.stdout, _RingTextIO):
        sys.stdout = _RingTextIO("<stdout>")
    if not isinstance(sys.stderr, _RingTextIO):
        sys.stderr = _RingTextIO("<stderr>")


def _info(msg):
    _emit_local("[%s] %s\n" % (stime(), msg))


# ---- 写线程诊断与自愈（AI 经 RPC 可直接调用） ----

def _writer_state(w):
    if w is None:
        return None
    return {
        "queued_bytes": w._queued_bytes,
        "queue_items": w._q.qsize(),
        "dropped": w.dropped,
        "stall_seconds": round(w.stall_seconds(), 2),
        "abandon": w._abandon,
    }


def writer_diag() -> dict:
    """返回 stdout/stderr 两个写线程的真实状态。

    RPC 持久命名空间是启动时快照，直接查 ``_OUT_WRITER`` 会得到 None
    （它在快照之后的 run_session 里创建）；本函数运行时动态读模块全局，
    一条 RPC 即可看清卡滞现场：``r=writer_diag()``。
    """
    return {"out": _writer_state(_OUT_WRITER), "err": _writer_state(_ERR_WRITER)}


def reset_out_writer() -> dict:
    """stdout 写线程卡死时自愈：旧 writer 写完当前块即丢弃积压退出，
    新建 writer 接管后续输出。调用方随后应发 redraw 让远端整屏重绘。"""
    global _OUT_WRITER
    old = _OUT_WRITER
    if old is None:
        return {"ok": False, "error": "无 stdout writer（会话未建立？）"}
    stall = old.stall_seconds()
    old.abandon_pending()
    term_out = _bin_stdout()
    if term_out is None:
        return {"ok": False, "error": "终端输出流不可用"}
    _OUT_WRITER = _TerminalWriter(term_out, name="pty-out")
    return {"ok": True, "stall_seconds": round(stall, 2)}


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


# ============================ AI 桥：外部进程复用本常驻 PTY ============================

# CSI / SGR / OSC / 单字符转义，用于把 AI 收回的终端字节还原成纯文本
_ANSI_RE = re.compile(rb"\x1b(?:\][^\x07\x1b]*(?:\x07|\x1b\\)|\[[0-?]*[ -/]*[@-~]|[@-Z\\-_])")


def _strip_ansi(b: bytes) -> str:
    return _ANSI_RE.sub(b"", b).replace(b"\r\n", b"\n").replace(b"\r", b"\n").decode("utf-8", "replace")


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


def _build_ai_run_line(cmd: str, begin_tag: str, end_tag: str) -> str:
    """构造 run() 要敲进远端交互 PTY 的命令行（以 ``\\r`` 结尾）。

    见上方「AI 命令严格编码」块注释：``cmd`` 经 base64 透明传输，远端
    POSIX shell（sh/bash/ash/dash）解码后交给 ``sh -c``，单双引号、
    ``$``、反引号、分号、裸换行等任意字符都不会改变命令本体。前置条件
    （与旧实现相同）：外层登录 shell 为 POSIX 系，且远端至少有
    base64 / openssl / python 三者之一。
    """
    b64 = base64.b64encode(str(cmd).encode("utf-8", "replace")).decode("ascii")
    # b64 字符全部落在 shell 安全字符集（字母数字 + /+=），printf '%s'
    # 原样吐出即可，无需再包引号；模板里的 %%s 经 Python % 格式化后是字面 %s。
    return (
        'echo %s; sh -c "$(printf \'%%s\' %s | %s)"; __ai_rc=$?; '
        'echo %s:$__ai_rc\r'
        % (begin_tag, b64, _AI_B64_DECODE_PIPELINE, end_tag)
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

    run() 把命令包在一对随机标记之间写入同一个常驻 shell，回收两个标记
    之间的输出与退出码；输入回显和输出照样实时渲染在监控窗口。PTY 只有
    一个 shell，命令用锁串行化，重叠调用立刻拿到 busy 而不是互相串台。
    """

    def __init__(self):
        self.pty = None
        self.transport = None
        self._listeners = []
        self._llock = threading.Lock()
        self._busy = threading.Lock()

    # ---- 会话侧调用（run_session 内挂载） ----

    def attach(self, pty, transport):
        self.pty = pty
        self.transport = transport

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
        pty.send(bytes(data))
        return {"ok": True, "sent": len(data)}

    def run(self, cmd, timeout: float = 60.0, acquire_timeout: float = 2.0) -> dict:
        """在常驻 shell 里跑一条命令，等结束标记，返回 rc/输出；窗口全程可见。

        - 超时只停止回收，不杀远端命令（输出继续在窗口里刷，可用 send 干预）；
        - 退出码取自 ``sh -c "<cmd>"``；命令经 base64 严格编码后透传（见
          :func:`_build_ai_run_line`），单双引号/``$``/反引号/裸换行等任意
          特殊字符都不会让外层交互 shell 的引号嵌套破裂。前置条件：外层登录
          shell 为 POSIX 系（sh/bash/ash/dash），远端有 base64/openssl/python
          任一解码器。
        """
        pty = self.pty
        if pty is None:
            return {"ok": False, "error": "pty 尚未连接"}
        if pty.end_reason:
            return {"ok": False, "error": "pty 已结束: %s" % pty.end_reason}
        if not self._busy.acquire(timeout=max(0.0, float(acquire_timeout))):
            return {"ok": False, "busy": True,
                    "error": "busy：上一条 AI 命令尚未结束"}

        tag = os.urandom(4).hex()
        begin = "__AI_BEGIN_%s__" % tag
        end = "__AI_END_%s__" % tag
        q = self._add_listener()
        line = _build_ai_run_line(str(cmd), begin, end)
        buf = b""
        timed_out = False
        m_end = None
        try:
            pty.send(line.encode("utf-8"))
            deadline = time.monotonic() + float(timeout)
            end_re = re.compile(re.escape(end.encode()) + rb":(\d+)")
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
                m_end = end_re.search(buf)
                if m_end:
                    break
        finally:
            self._remove_listener(q)
            self._busy.release()

        # 真起点是「独占一行的 BEGIN + 换行」；输入回显里 BEGIN 后面是 ';'，
        # 不会误中。结尾 END:<数字> 同理（回显里是 :$__ai_rc 字面量）。
        m_begin = re.search(re.escape(begin.encode()) + rb"\r?\n", buf)
        start = m_begin.end() if m_begin else 0
        if m_end:
            body, rc = buf[start:m_end.start()], int(m_end.group(1))
        else:
            body, rc = buf[start:], None
        return {
            "ok": rc == 0 if rc is not None else False,
            "rc": rc,
            "timed_out": timed_out,
            "out": _strip_ansi(body).strip("\n"),
            "raw_len": len(body),
            "end_reason": pty.end_reason,
        }

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


# ============================ PTY 会话 ============================

# 可设置参数规格表（唯一真相源；别名表在文件前面统一声明）：
#   (规范名/live 键, 别名组, RemotePty.configure 关键字[None=纯本地],
#    (下限, 上限), 说明)
_PTY_MAGIC_SPECS = (
    ("interval", alias_interval, "interval", (0.0, 60.0),
     "服务端输出攒批间隔秒，0=实时，上限 60"),
    ("heartbeat", alias_heartbeat, "heartbeat", (0.0, 3600.0),
     "心跳间隔秒，0=关闭，上限 3600"),
    ("ttl", alias_ttl, "ttl", (60.0, 86400.0),
     "孤儿会话存活秒，60~86400"),
    ("dead_timeout", alias_dead_timeout, None, (0.0, 3600.0),
     "本地判死超时秒，0=不检测（立即生效）"),
)


def _alias_usage(aliases) -> str:
    return "|".join(aliases)


def _magic_help_lines():
    """帮助文本由别名表/规格表生成，命令增删改时不可能和实现脱节。"""
    lines = ["本地命令（不会发到远端）："]
    lines.append("  %s   脱离（远端 tmux/shell 继续运行）"
                 % _alias_usage(alias_detach))
    lines.append("  %s   会话与 broker 状态"
                 % _alias_usage(_cm.alias_status))
    lines.append("  %s [n]   查看最近本地日志（不写终端，默认 20 行，上限 100；"
                 "更多走 HTTP RPC r=get_log(n)）"
                 % _alias_usage(alias_log))
    lines.append("  %s   写线程卡顿时切换写线程，并通知远端整屏重绘"
                 % _alias_usage(alias_redraw))
    for canon, aliases, _remote, _bounds, desc in _PTY_MAGIC_SPECS:
        lines.append("  %s <秒>   %s"
                     % (_alias_usage((canon,) + aliases[1:]), desc))
    lines.append("  %s   本帮助；空行/Esc/Ctrl-C 取消"
                 % _alias_usage(_cm.alias_help))
    return lines


def _run_magic(line, ctx):
    """执行一行本地魔术命令，返回 ``(是否请求脱离, 要本地打印的行列表)``。"""
    body = (line or "").strip()
    if not body:
        return False, []
    parts = body.split(None, 1)
    # - 与 _ 等价（dead-timeout / dead_timeout 同一命令）
    cmd = parts[0].lower().replace("-", "_")
    arg = parts[1].strip() if len(parts) > 1 else ""
    pty = ctx["pty"]
    live = ctx["live"]

    if cmd in _cm.alias_help:
        return False, _magic_help_lines()
    if cmd in alias_detach:
        return True, ["正在脱离（远端会话保持运行）…"]
    if cmd in _cm.alias_status:
        info = getattr(pty, "server_info", None) or {}
        online, total, _hosts = _broker_status(ctx.get("transport"))
        lines = [
            "sid        = %s" % getattr(pty, "sid", None),
            "远端       = %s pid=%s owner=%s"
            % (info.get("host"), info.get("pid"), getattr(pty, "owner", None)),
            "broker     = 在线 %d/%d" % (online, total),
            "interval   = %s 秒（服务端攒批）" % live.get("interval"),
            "heartbeat  = %s 秒" % live.get("heartbeat"),
            "ttl        = %s 秒" % live.get("ttl"),
            "dead       = %s 秒（0=不检测）" % live.get("dead_timeout"),
            "丢弃影子帧 = %d" % getattr(pty, "foreign_frames", 0),
            "写线程卡滞 = %s 秒（>2 可执行 redraw）"
            % (round(_OUT_WRITER.stall_seconds(), 1)
               if _OUT_WRITER is not None else 0.0),
            "结束原因   = %s" % getattr(pty, "end_reason", None),
        ]
        return False, lines
    if cmd in alias_log:
        # 只读查看：覆盖层自擦，终端不留痕；行数夹 1~100，防止整屏滚动
        if arg:
            try:
                n = int(float(arg))
            except ValueError:
                return False, ["log 参数应为行数（数字），收到: %r" % arg]
        else:
            n = 20
        n = max(1, min(n, 100))
        text = get_log(n)
        body = text.split("\n") if text else ["（暂无本地日志）"]
        header = ("本地日志（最近 %d 行，缓冲共 %d 行；"
                  "更多走 HTTP RPC：r=get_log(n)）："
                  % (len(body) if text else 0, len(_LOCAL_LOG)))
        return False, [header] + body
    if cmd in alias_redraw:
        lines = []
        # 写线程真卡滞（>2s）时先切换 writer，避免解冻后旧流与重绘流拼接
        if _OUT_WRITER is not None and _OUT_WRITER.stall_seconds() > 2.0:
            res = reset_out_writer()
            if res.get("ok"):
                lines.append("检测到终端写卡滞 %.1fs，已切换写线程"
                             % res["stall_seconds"])
            else:
                lines.append("切换写线程失败: %s" % res.get("error"))
        try:
            pty.configure(redraw=True)  # killpg(SIGWINCH)：tmux/vim 整屏重绘
            lines.append("已通知远端整屏重绘")
        except RemoteError as exc:
            return False, ["redraw 失败: %s" % exc]
        return False, lines
    for canon, aliases, remote_key, (lo, hi), _desc in _PTY_MAGIC_SPECS:
        if cmd not in aliases:
            continue
        if not arg:
            return False, ["%s = %s" % (canon, live.get(canon))]
        try:
            val = float(arg)
        except ValueError:
            return False, ["%s 需要一个数字（秒），收到: %r" % (canon, arg)]
        val = min(max(lo, val), hi)
        if remote_key is None:
            live[canon] = val
            return False, ["%s <- %s 秒（立即生效）" % (canon, val)]
        try:
            applied = pty.configure(**{remote_key: val})
        except RemoteError as exc:
            return False, ["设置失败: %s" % exc]
        live[canon] = applied[remote_key]
        return False, ["%s <- %s 秒（已通知服务端热调）"
                       % (canon, applied[remote_key])]
    return False, ["未知本地命令: %s（输入 help 查看）" % cmd]


def _hard_exit(console, code: int, message: str, pty=None):
    """学 client_mqtt.py 的退出风格：直接 ``os._exit``，干脆不拖泥带水。

    旧路径退出时要 pty.close() 干等 end 帧 1.5s + join 输入线程 2s +
    transport.stop() 逐个 broker disconnect——服务器已死时这些等待全都
    纯属浪费，界面就是这样"卡住"的。``os._exit`` 会跳过所有 finally，
    所以这里必须自己先把终端模式恢复；stop 帧只 fire-and-forget（能发
    出去就让远端顺手收尸，发不出去也不等）。
    """
    if pty is not None:
        try:
            pty.detach()
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


def run_session(transport: MqttTransport, args, rows: int, cols: int) -> int:
    global _OUT_WRITER
    pty = RemotePty(transport, timeout=args.timeout)
    # 挂到 AI 桥：外部经本地 HTTP RPC 调用 ai_bridge 即复用本 PTY，
    # attach 放在 open 之前，保证最早的握手回显也能被 feed 分流到。
    ai_bridge.attach(pty, transport)
    outq: "queue.Queue[bytes]" = queue.Queue()
    stop_ev = threading.Event()
    term = args.term or os.environ.get("TERM") or "xterm-256color"
    detach_key = _parse_key_spec(getattr(args, "detach_key", "\x1d"))
    menu_key = _parse_key_spec(getattr(args, "menu_key", "ctrl-alt-insert"))

    # ---- 存活检测：输出帧和心跳帧都算"服务器还活着"的信号 ----
    heartbeat = max(0.0, float(args.heartbeat))
    dead_timeout = max(0.0, float(args.dead_timeout))
    if heartbeat <= 0:
        # 没有心跳时，shell 长时间静默和"服务器已死"无法区分，不做误杀
        dead_timeout = 0.0
    elif dead_timeout > 0:
        # 至少容忍连续 3 次心跳缺失，避免公共 broker 短暂重连被误判
        dead_timeout = max(dead_timeout, heartbeat * 3)
    signal_state = {"last": time.monotonic()}

    def on_data(chunk: bytes):
        signal_state["last"] = time.monotonic()
        outq.put(chunk)
        ai_bridge.feed(chunk)

    def on_heartbeat(_ts):
        signal_state["last"] = time.monotonic()

    def _responders_warning(responders) -> str:
        # 多应答者（持相同 key 的多个服务端进程同时应答）告警文本；
        # 后台 gather 到齐后走本地日志，不阻塞首屏。
        winner = next((r for r in responders if r.get("winner")),
                      responders[0])
        lines = [
            f"[pty][WARN] 检测到 {len(responders)} 个持相同 key 的服务端同时应答！"
            f"仅保留 {winner.get('host')} pid={winner.get('pid')}，"
            f"其余影子 PTY 已被通知立即关闭。"]
        for r in responders:
            if not r.get("winner"):
                lines.append("           影子: %s pid=%s owner=%s"
                             % (r.get("host"), r.get("pid"), r.get("owner")))
        lines.append("           请停掉多余机器/容器上的旧 server_mqtt 进程，"
                     "否则每次连接都会重复拉起并短暂干扰首屏。")
        return "\n".join(lines) + "\n"

    # 协商：订阅 out topic + 下发 PTY 启动代码（shell 在服务端只启动这一次）
    _info("正在远端启动 PTY：shell=%s cwd=%s 窗口=%dx%d，等待握手回包（超时 %.0fs）..."
          % (args.shell or "服务端登录 shell", args.cwd or "远端 HOME",
             rows, cols, args.timeout))
    env = pty.open(rows, cols, shell=(args.shell or None), term=term,
                   cwd=args.cwd, login=not args.no_login,
                   flush_interval=args.interval, ttl=args.ttl,
                   heartbeat=heartbeat, on_heartbeat=on_heartbeat,
                   on_data=on_data,
                   on_responders=lambda rs: _emit_local(
                       _responders_warning(rs)))

    banner = (
        f"[{stime()}] connected shell={env['shell']} pid={env['pid']} "
        f"({env['rows']}x{env['cols']}, interval={env['flush_interval']})\t"
        f"{env['in_topic']}\t{env['out_topic']}\n"
        f"[pty] {_describe_key(detach_key)} 本地脱离（远端 tmux 不受影响）；"
        f"{_describe_key(menu_key)} 本地命令栏（detach/status/时间参数）；"
        f"远端 exit/Ctrl-D 结束会话\n")
    if env.get("cwd_warning"):
        # 服务端对不存在的 cwd 已自行回退（HOME→/），会话照常用；只提示不退出
        banner += (f"[pty] 注意: {env['cwd_warning']}，"
                   f"已回退到 {env['cwd']}\n")
    # 多应答者告警由 on_responders 后台回调输出（首包不阻塞 gather 窗）。
    # 同步 gather 回退路径（transport 不支持后台收集）下这里仍兜底拼一次。
    responders = getattr(pty, "responders", None) or []
    if len(responders) > 1:
        banner += _responders_warning(responders)
    if dead_timeout > 0:
        banner += (f"[pty] 心跳 {heartbeat:g}s：服务器关闭/断连后最多 "
                   f"{dead_timeout:g}s 自动退出\n")
    if getattr(args, "port", 0):
        banner += ("[pty] 本地日志与本画面完全分离：浏览器开 "
                   "http://<本机IP>:%d/ 即全屏实时日志台；"
                   "命令栏输 log，或 curl 同口 /r=get_log()\n"
                   % args.port)
    if sys.platform == "win32":
        banner += "[pty] 粘贴：右键 或 Shift+Insert，中文按 UTF-8 发送\n"
    # banner 及之后的本地日志只进环形缓冲（RPC 口开启时终端零污染），
    # 不再 _stderr_write 插进远端画面。
    _emit_local(banner)

    # 会话期可变参数（本地命令栏热调；dead_timeout 纯本地，其余经 set 帧下发）
    live = {
        "interval": float(args.interval),
        "heartbeat": heartbeat,
        "ttl": float(args.ttl),
        "dead_timeout": dead_timeout,
    }
    magic_ctx = {"pty": pty, "transport": transport, "live": live}

    console = _make_raw_console()
    # 终端输出全部交给异步写线程：唯一可能阻塞的原生控制台 IO 只发生在
    # 那个可牺牲线程上，渲染主线程 / RPC 线程再也不会被控制台堵死。
    # term_out 仍锁定真实二进制流（RPC 线程会把 sys.stdout 换成 StringIO）。
    term_out = _bin_stdout()
    if term_out is not None:
        _OUT_WRITER = _TerminalWriter(term_out, name="pty-out")

    def _out_write(data: bytes) -> None:
        if _OUT_WRITER is not None:
            _OUT_WRITER.write(data)

    # 关窗口/Ctrl-Break/kill 信号时也要恢复本地终端模式，否则 cmd 残留在
    # raw/VT 输入模式（方向键失灵）；顺手 fire-and-forget 一帧 stop。
    install_console_guards(console, on_signal=lambda: pty.detach())

    keyq: "queue.Queue[bytes]" = queue.Queue()
    menu_ev = threading.Event()

    def reader_loop():
        # 唯一触碰本地终端输入的线程：只负责读出来塞进 keyq，热键匹配/命令栏/
        # 透传全部在 input_loop 做，避免热键字节被拆读时错过。
        try:
            console.enter()
            while not stop_ev.is_set():
                try:
                    data = console.read()
                except OSError:
                    break
                if not data:
                    break
                data = _filter_injected_esc(data)
                if not data:
                    continue
                keyq.put(data)
        finally:
            stop_ev.set()

    def _local_out(text: str):
        _out_write(b"\x1b[0m" + text.encode("utf-8", "replace"))

    def _read_local_line():
        """命令栏本地行编辑（提示符由调用方画）：回显不发给远端；
        返回 str，Esc/Ctrl-C 返回 None。回车/取消都不换行——整行随后由
        调用方统一归位擦除，避免任何菜单字符残留在终端上。"""
        line = bytearray()
        while not stop_ev.is_set():
            try:
                data = keyq.get(timeout=0.3)
            except queue.Empty:
                continue
            for ch in data:
                if ch in (13, 10):
                    return line.decode("utf-8", "replace")
                if ch == 3 or ch == 27:
                    return None  # Ctrl-C / Esc：本地取消，绝不发给远端
                if ch in (127, 8):
                    if line:
                        del line[-1]
                        _local_out("\b \b")
                elif ch >= 32:
                    line.append(ch)
                    _out_write(bytes([ch]))
        return None

    def _open_menu():
        # 打开期间主线程暂停渲染远端输出（暂存在 held 里），避免 tmux 刷新
        # 打花本地命令行；关闭后先彻底擦掉菜单再补画。
        # ESC 7 保存远端光标（含 shell 提示符位置），ESC 8 归位：
        # 每轮提示符都归位并 ESC[0J 清掉下方，所以 help 那 9 行、^C 等
        # 不会累积；退出时同样归位+清下方，菜单零残留，随后 held 补画，
        # tmux 全屏重绘 / shell 接着原来的提示符位置输出。
        menu_ev.set()
        _local_out("\x1b7\r\n\x1b[2K[pty] >> ")
        try:
            while True:
                line = _read_local_line()
                if line is None:
                    return
                if not line.strip():
                    return
                do_detach, lines = _run_magic(line, magic_ctx)
                if do_detach:
                    stop_ev.set()
                    return
                # 归位擦除旧提示符/上次输出，在同一区域重画（高度不累积）
                _local_out("\x1b8\x1b[0J\r\n")
                for ln in lines:
                    _local_out(ln + "\r\n")
                _local_out("[pty] >> ")
        finally:
            # 必须先擦干净再放行渲染，否则补画的远端流会和菜单字符拼成花屏。
            # ESC8/0J 覆盖普通 shell（光标归位+清下方）；菜单行数超过光标
            # 下方空间时备用屏幕可能已滚屏，再让 tmux/vim 收到 SIGWINCH
            # 整屏重绘兜底（进程即将退出的 detach 路径就不必发了）。
            _local_out("\x1b8\x1b[0J")
            menu_ev.clear()
            if not stop_ev.is_set():
                try:
                    pty.configure(redraw=True)
                except Exception:
                    pass

    def _do_paste() -> None:
        # Shift+Insert：conhost 已把按键交给本进程，直接读本地剪贴板发远端，
        # 不经过 conhost 的粘贴路径，任何语言文本都是干净 UTF-8。
        if sys.platform != "win32":
            return
        try:
            text = _read_clipboard_text_win()
        except Exception:
            text = ""
        if not text:
            return
        data = _normalize_paste_text(text)
        if data:
            try:
                pty.send(data)
            except Exception:
                pass

    def _do_hotkey(name) -> bool:
        if name == "detach":
            _local_out("\r\n[pty] 本地脱离\r\n")
            stop_ev.set()
            return True
        if name == "menu":
            _open_menu()
        elif name == "paste":
            _do_paste()
        return False

    def input_loop():
        # 热键精确匹配：一个读入突发恰好等于热键序列才触发（粘贴文本里
        # 恰好含同样字节不会误触）。多字节热键以 ESC 开头且被拆读时，
        # 按 _HOTKEY_GAP 等剩余字节；超时则把 ESC 照常放给远端。
        seqs = [(s, n) for s, n in
                ((detach_key, "detach"), (menu_key, "menu")) if s]
        # Shift+Insert 本地粘贴（仅 Windows：POSIX 终端一般自行完成粘贴，
        # 不会把序列交给应用；万一收到也照旧透传）。
        if sys.platform == "win32":
            seqs.append((_PASTE_KEY_SEQ, "paste"))
        multi = [s for s, _ in seqs if len(s) > 1]
        try:
            while not stop_ev.is_set():
                try:
                    chunk = keyq.get(timeout=0.3)
                except queue.Empty:
                    continue
                burst = bytearray(chunk)
                while True:  # 把同一次按键已到齐的字节合并成一个突发
                    try:
                        burst += keyq.get_nowait()
                    except queue.Empty:
                        break
                burst = bytes(burst)
                hit = next((n for s, n in seqs if burst == s), None)
                if hit is None and multi and burst[:1] == b"\x1b" \
                        and any(s.startswith(burst) for s in multi):
                    deadline = time.monotonic() + _HOTKEY_GAP
                    extra = b""
                    while time.monotonic() < deadline:
                        try:
                            c = keyq.get(
                                timeout=max(0.0, deadline - time.monotonic()))
                            extra += c
                            deadline = time.monotonic() + _HOTKEY_GAP
                        except queue.Empty:
                            break
                    if extra:
                        burst += extra
                        hit = next((n for s, n in seqs if burst == s), None)
                if hit is not None:
                    if _do_hotkey(hit):
                        return
                    continue
                try:
                    pty.send(burst)
                except Exception:
                    pass
        finally:
            stop_ev.set()

    def resize_watch():
        # last=None：连 broker / 握手期间窗口可能已被拖动过，线程一启动
        # 先无条件同步一次真实尺寸。
        # 尺寸变化立即发 winsz；停稳后的几个 tick 再补发同尺寸、更高 iseq
        # 的新帧兜底——多 broker 路径乱序时，配合服务端的 iseq 单调闸门，
        # 保证最终生效的一定是当前尺寸：拖动时经慢 broker 迟到的旧（更大）
        # 尺寸要么被服务端丢弃，要么被这里的补发覆盖，远端 PTY 不会被卡在
        # 比本地窗口宽的尺寸上（否则 pip/gradle 的 \r 进度条会全线错位）。
        last = None
        resend = 0
        while not stop_ev.wait(0.25):
            cur_sz = shutil.get_terminal_size((80, 24))
            cur = (cur_sz.lines, cur_sz.columns)
            if cur != last:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                last = cur
                resend = 3
            elif resend > 0:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                resend -= 1

    t_read = threading.Thread(target=reader_loop, name="pty-read", daemon=True)
    t_in = threading.Thread(target=input_loop, name="pty-input", daemon=True)
    t_resize = threading.Thread(target=resize_watch, name="pty-resize",
                                daemon=True)
    t_read.start()
    t_in.start()
    t_resize.start()

    pre_cmd = _join_pre_command(getattr(args, "command", None))
    if pre_cmd:
        # SSH 式前置命令（ssh host "tmux at"）：握手一完成就把整行敲进常驻
        # shell。tty 行规程会先把字节缓存在内核输入队列里等 shell 读取，
        # 无需 sleep 等提示符。加 \r 提交；命令结束后会话不关闭，人继续
        # 留在 shell / tmux 里。
        try:
            pty.send(pre_cmd + "\r")
            _emit_local("[pty] 已自动执行前置命令: %s\n" % pre_cmd)
        except Exception as exc:
            _emit_local("[pty][WARN] 前置命令发送失败: %s\n" % exc)

    # 主线程：远端输出原样渲染；任何退出路径都走 _hard_exit 立即收场
    held = bytearray()  # 本地命令栏打开期间暂存远端输出，关闭后补画
    last_stall_warn = [0.0]  # 卡滞告警节流（每 30s 一条）
    auto_unstick_done = [False]  # 本轮冻结是否已自动解除（解冻后重新武装）
    try:
        while not stop_ev.is_set():
            try:
                chunk = outq.get(timeout=0.3)
            except queue.Empty:
                chunk = None
            # 菜单刚关闭、远端又静默时，暂存的输出也要立刻补画，不能等新帧
            if chunk is None and held and not menu_ev.is_set():
                _out_write(bytes(held))
                held.clear()
            if chunk is None:
                # 写线程卡滞：屏幕虽冻结但日志 ring 照写，浏览器日志台 / AI
                # 经 r=writer_diag() 可见，并给出处置路径
                _stall = (_OUT_WRITER.stall_seconds()
                          if _OUT_WRITER is not None else 0.0)
                if _stall > 10 and (time.monotonic() - last_stall_warn[0]) > 30:
                    last_stall_warn[0] = time.monotonic()
                    _emit_local(
                        "[pty][WARN] 终端写卡滞 %.0fs：若窗口处于选择/"
                        "标记模式请按 ESC 解除，或命令栏执行 redraw "
                        "切换写线程\n" % _stall)
                # 长时间冻结（通常是选了文本忘记退出标记模式）：前台时
                # 合成 ESC 自动解除，鼠标左键选择/右键粘贴照常保留
                if _stall >= _SELECTION_STALL_AUTO_UNSTICK \
                        and not auto_unstick_done[0]:
                    if _cancel_console_selection():
                        auto_unstick_done[0] = True
                        _emit_local(
                            "[pty] 窗口选择模式冻结输出超过 %.0f 秒，"
                            "已自动解除\n" % _SELECTION_STALL_AUTO_UNSTICK)
                if auto_unstick_done[0] and _stall == 0.0:
                    auto_unstick_done[0] = False  # 解冻恢复，允许处理下次冻结
                if pty.end_reason is not None:
                    _hard_exit(
                        console, 0,
                        f"\r\n[pty] session ended: {pty.end_reason}\r\n", pty)
                _dt = float(live["dead_timeout"])
                if _dt > 0 and (time.monotonic()
                                - signal_state["last"]) > _dt:
                    _hard_exit(
                        console, 3,
                        f"\r\n[pty] {_dt:g}s 收不到任何远端帧"
                        f"（输出/心跳），服务器可能已关闭，直接断开\r\n", pty)
                continue
            if menu_ev.is_set():
                held.extend(chunk)
                continue
            if held:
                _out_write(bytes(held))
                held.clear()
            _out_write(chunk)
    except KeyboardInterrupt:
        _hard_exit(console, 130, "\r\n[pty] interrupted\r\n", pty)

    # input_loop 结束：Ctrl-] 本地脱离 / 本地 stdin 关闭
    _hard_exit(console, 0, "\r\n[pty] session ended: detached\r\n", pty)
    return 0  # 不可达：_hard_exit 不返回；仅为类型/静态检查保留


# ============================ CLI ============================

def build_parser() -> argparse.ArgumentParser:
    # 选项名一律由文件前面的 alias_xxx 别名表 + client_mqtt._cli_opts 生成，
    # 与本地魔术栏命令、rpc(**ka) 别名三处永远一致；禁止再硬编码 "--xxx"。
    # dest 保持历史字段名（args.interval/args.port 等），调用方无需改动。
    p = argparse.ArgumentParser(
        prog="pty_client_mqtt",
        description="PTY over MQTT：SSH 式远程交互终端（常驻 shell，逐键过 broker）")
    # 连接参数（topic/key/allow/timeout）：直接复用 client_mqtt 别名表
    add_connection_args(p, default_timeout=30.0)
    # PTY 会话参数
    p.add_argument(*_cm._cli_opts(*alias_shell), dest="shell", default="",
                   help="远端 shell，默认服务端用户登录 shell（$SHELL/passwd）")
    p.add_argument(*_cm._cli_opts(*alias_term), dest="term", default="",
                   help="TERM，默认本地 $TERM 或 xterm-256color")
    p.add_argument(*_cm._cli_opts(*alias_cwd), dest="cwd", default=None,
                   help="启动目录，默认远端 HOME")
    p.add_argument(*_cm._cli_opts(*alias_interval), dest="interval",
                   type=float, default=0.0,
                   help="服务端主动推送最小间隔秒：0=实时（默认），>0 攒批")
    p.add_argument(*_cm._cli_opts(*alias_no_login), dest="no_login",
                   action="store_true",
                   help="不使用 login shell（默认 argv0 带 - 前缀）")
    p.add_argument(*_cm._cli_opts(*alias_ttl), dest="ttl",
                   type=float, default=DEFAULT_PTY_TTL,
                   help="孤儿会话最长存活秒（默认 12h，上限 24h）")
    p.add_argument(*_cm._cli_opts(*alias_heartbeat), dest="heartbeat",
                   type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument(*_cm._cli_opts(*alias_dead_timeout), dest="dead_timeout",
                   type=float, default=15.0,
                   help="多久收不到任何远端帧（输出/心跳）即判定服务器已死"
                        "并直接退出（默认 15s，实际不小于 3 倍心跳；"
                        "0=不检测，心跳关闭时自动失效）")
    p.add_argument(*_cm._cli_opts(*alias_size), dest="size", default=None,
                   help="强制窗口 ROWSxCOLS，如 24x100；默认取本地终端大小")
    p.add_argument(*_cm._cli_opts(*alias_detach_key), dest="detach_key",
                   default="ctrl-]",
                   help="本地脱离键：只断开 client，远端 tmux/shell 继续运行"
                        "（默认 ctrl-]，沿用 telnet 惯例，不与 tmux 前缀冲突；"
                        "可写 ctrl-a/f9/insert 等名字，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_menu_key), dest="menu_key",
                   default="ctrl-alt-insert",
                   help="本地命令栏热键：detach、status、热调 interval/"
                        "heartbeat/ttl/dead，不会把按键发到远端（默认 "
                        "ctrl-alt-insert；名字写法同脱离键，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_rpc_port), dest="port",
                   type=int, default=1188,
                   help="本地 AI 控制口 HTTP RPC 端口（默认 1188）；"
                        "外部进程经它调用 ai_bridge 复用本 PTY，不再重连 broker；0=关闭")
    p.add_argument(*_cm._cli_opts(*alias_rpc_host), dest="host",
                   default="0.0.0.0",
                   help="本地 AI 控制口绑定地址（默认 0.0.0.0）；仅本机调用建议 127.0.0.1")
    # SSH 式可选位置参数：连接成功后自动敲进常驻 shell 的前置命令，如
    # ``pty_client_mqtt.py -t q -k *** "tmux at"``；REMAINDER 保证命令自身
    # 的 -x 选项（tmux attach -d）不会被本客户端解析。命令结束后会话继续，
    # 人仍留在远端 shell / 全屏程序里。
    p.add_argument(alias_command[0], nargs=argparse.REMAINDER,
                   help="可选：连接后自动执行的前置命令（SSH 式），如 \"tmux at\"")

    return p


def main(argv=None) -> int:
    global _LOG_MIRROR_STDERR,transport
    args = build_parser().parse_args(argv)

    if args.size:
        try:
            r_txt, c_txt = args.size.lower().split("x", 1)
            rows, cols = int(r_txt), int(c_txt)
        except ValueError:
            sys.stderr.write("[ERROR] --size 格式应为 ROWSxCOLS，如 24x100\n")
            return 2
    else:
        sz = shutil.get_terminal_size((80, 24))
        rows, cols = sz.lines, sz.columns

    if not sys.stdin.isatty():
        sys.stderr.write("[ERROR] PTY 需要一个交互式本地终端（stdin 不是 TTY）\n")
        return 2
    
    _enable_output_vt()
    signed = bool(str(args.key or "").strip())
    t_conn = time.monotonic()
    _info("正在并发连接 %d 个公共 MQTT broker（多路径冗余，首个连上即继续，通常不足 1 秒）..."
          % len(BROKER_LIST))
    _info("request_topic=%s  reply_topic=%s  请求签名=%s  允许未验签回包=%s"
          % (args.request_topic, args.reply_topic,
             "是" if signed else "否", "是" if args.allow else "否"))
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)
    online, total, hosts = _broker_status(transport)
    _info("MQTT 就绪：在线 broker %d/%d，建连耗时 %.1f 秒"
      % (online, total, time.monotonic() - t_conn)
      + ("；在线节点：" + ", ".join(hosts) if hosts else ""))
    # if hosts:
        # _info("在线节点：" + ", ".join(hosts))
    # el
    if not hosts and total:
        _emit_local("[WARN] 当前没有任何 broker 在线，握手大概率超时\n")
        
    if args.port:
        import server_http
        # 注意：持久命名空间在此刻快照一次，ai_bridge/get_log 都是模块级
        # 对象引用，会话建立后 attach/feed 对 HTTP 调用方立即生效。
        # 起口成功即切换为"本地日志只进环形缓冲"：包括下面的启动 _info、
        # 每笔 [RPC] 请求行和 root 上的 MultiMQTT/paho logging，终端零写入；
        # 起口失败则保留 stderr 镜像，致命错误用户照样看得见。
        _LOG_MIRROR_STDERR = False
        try:
            ghs = server_http.start_rpc_server(
                port=args.port, ip=args.host, globals=globals(), locals=locals(),
                log_sink=_emit_local,
                websocket_handler=_LOG_HUB.serve, websocket_path="/wslog",
                redirect_root="/log_html(p)")
        except Exception:
            _LOG_MIRROR_STDERR = True
            raise
        # 起口成功后再接管 stdout/stderr/logging：浏览器日志台与 RPC 取日志
        # 两个查看通道都已就绪，PTY 终端从此只渲染远端 shell。
        _install_log_capture()
        lan = _lan_ip()
        _info(f"本地 AI 控制口已开启：http://{lan}:{args.port}/"
              f"根路径自动跳转 /log_html(p)")
    
    try:
        return run_session(transport, args, rows, cols)
    except RemoteError as exc:
        _stderr_write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        # main 即将 return 退出进程，给 stderr 写线程 1s 把错误落盘
        if _ERR_WRITER is not None:
            _ERR_WRITER.close(timeout=1.0)
        return 2
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
