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

MCP 工具服务（Model Context Protocol，AI 客户端标准工具协议）
====================================================
pip install "mcp[cli]" 后，常驻 PTY 自动以标准 MCP 工具形式持续暴露：
默认**复用 --port 的 HTTP 口**（路径 /mcp，与 RPC/浏览器日志台同口并存，
不新开端口；新版 SDK 走 streamable-http，旧版退回 /sse+/messages）：

    claude mcp add --transport http pty http://192.168.1.3:1188/mcp

工具集：pty_run 跑命令回收 rc/输出、pty_tail 查长命令实时进度、
pty_send 喂交互键、pty_pane 查询/设置 AI 命令的 tmux 窗口、pty_status
会话与 broker 状态、pty_log 本地诊断日志、pty_redraw 写卡滞自愈+整屏
重绘；另有 9 个远端文件工具：fs_read/fs_ls/fs_glob/fs_grep 只读，
fs_write/fs_edit/fs_mkdir/fs_move/fs_delete 默认允许写任意路径
（pty_run 本就能执行任意代码，写入根限制没有实际意义；--fs-read-only 可
整体禁写）。文件工具走 ai_bridge.transport.request 把 Python 源码发到
远端 server_mqtt.py 进程里 exec，作用于**远端机器**的文件系统（路径语义
是远端的，如 /root/...），既不经过交互 shell、也不占 busy 锁，与
client_mqtt.rpc 同一条通道。pty_* 与 HTTP RPC 口同源同锁。
--no-mcp 关闭；--mcp-port>0 改为另起独立端口（--port 0 关闭 RPC 口时仍
要 MCP 的场景）。未装 mcp 包只记日志，PTY 照常工作。

AI 命令的三条投递通道（run() 自动选择，逐级回落）
==================================================
① tmux 推送（默认）：客户端一次 RPC 让远端装 pipe-pane + send-keys，
远端起后台线程本地增量读输出并逐帧 publish，客户端只订阅收流——没有
"每读一次一往返"，长命令输出可以边产生边到达（pty_tail 即看这份实时
缓冲）。② tmux 轮询：远端不认识推送 op（旧版本）时退回客户端按 offset
增量读。③ 原生 PTY：tmux 不可用时直写本 PTY，行为与改造前一致。

每次 MCP 工具调用都会分配一个 6 位调用 id（os.urandom(3).hex()），开始行
带完整参数，完成行整行缩进、带参数短摘要 + 状态 + 耗时，两行各自可独立
阅读，并发交错也不影响归属：
    [MCP] 3f9a2b 开始 fs_read file_path='/tmp/a.txt' offset=0 limit=2000
       [MCP] 3f9a2b 完成 fs_read file_path='/tmp/a.txt' offset=0 limit=2000 ok 0.36s
业务失败是 FAIL + 原因，异常是 FAIL + 异常类型；参数按 repr 转义成单行、
开始行单值超 400 字符截断、完成行单值只留前 60 字符。不用等远端返回，
远端卡死/超时时也能从日志台看出是哪个调用没收尾。/mcp 的 2xx HTTP 访问行
（每次调用伴随多条 POST、既无工具名也无参数）已被过滤，只保留 4xx/5xx；
无状态 SDK 的 "Terminating session: None" INFO 已压到 WARNING，避免冲散日志。

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
import shlex
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
    RemotePty, RemoteShell, RemoteError, DEFAULT_PTY_TTL,
    RemoteOpError, _PtyReorderBuffer,
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
# MCP 工具服务（pip install "mcp[cli]" 后默认挂在 --port 的 RPC 口上）
alias_mcp_port     = ('mcp_port', 'mcp')
alias_mcp_transport= ('mcp_transport', 'mcptrans')
alias_no_mcp       = ('no_mcp', 'nomcp')
# MCP 文件工具的全局只读开关（默认关闭，写家族作用于任意路径）
alias_fs_read_only = ('fs_read_only', 'fsro')
# AI 目标窗口：默认「session 内 window_index 最小的窗口」，可 CLI/RPC 指定
alias_ai_pane     = ('ai_pane', 'aipane', 'aip')
# AI 命令编码：raw=窗口里直接看到命令原文（默认）；base64=旧严格编码兜底
alias_ai_encoding = ('ai_encoding', 'aienc')
alias_command      = ('command', 'cmd')
# 本地命令栏查看诊断日志（只读，不碰会话参数）
alias_log          = ('log', 'logs')
# 整屏重绘 / 写线程卡滞自愈（不会把命令发给远端 shell，只发控制帧）
alias_redraw       = ('redraw', 'refresh', 'rd')

# 魔术栏的"脱离"：语义等同退出，exit/quit 直接复用 client_mqtt 的别名表，
# PTY 语境再补 detach 系列（顺序无所谓，匹配一律用 in）。
alias_detach = tuple(dict.fromkeys(
    ('detach', 'd', 'bye', 'q', 'x') + tuple(_cm.alias_exit)))


# ==================== 版本自检：单文件 vs pty/parts 分片是否同步 ====================
#
# 单文件由 generate_single_file.py 把 parts/ 全部分片按序字节拼接而成，
# 「单文件字节 == 分片拼接」时两侧 sha256 必然相同——不需要在文件里嵌
# 任何版本标记（嵌标记反而破坏逐字节拼接的不变量）。main() 启动时自动
# 比对一次：分片改了没重组（或有人手改了单文件）会打 WARN 并给出重组
# 命令；结果也可随时经 HTTP RPC 查询：
#
#     curl "http://127.0.0.1:1188/r=check_parts_sync()"
#
# 部署机上没有 parts 目录时自动跳过，纯单文件场景不告警。


def check_parts_sync(verbose: bool = True) -> dict:
    """比对运行中的单文件与 client/pty/parts 分片是否逐字节同步。

    返回 {"ok": True/False/None(无法判定), "single", "parts",
    "sha_single", "sha_parts"}；verbose=True 时把结论写本地日志。
    """
    import hashlib

    here = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(here) == "pty":     # 包模式 client/pty/__init__.py
        parts_dir = os.path.join(here, "parts")
        single = os.path.join(os.path.dirname(here), "pty_client_mqtt.py")
    else:                                    # 单文件模式（含 _generated 预览）
        parts_dir = os.path.join(here, "pty", "parts")
        single = os.path.abspath(__file__)
    try:
        names = sorted(n for n in os.listdir(parts_dir) if n.endswith(".py"))
    except OSError:
        names = []
    if not names:
        res = {"ok": None, "reason": "parts 目录不存在（纯部署场景），跳过",
               "single": single, "parts": parts_dir}
        if verbose:
            _info("[pty] 版本自检：%s" % res["reason"])
        return res
    h = hashlib.sha256()
    for n in names:
        with open(os.path.join(parts_dir, n), "rb") as f:
            h.update(f.read())
    sha_parts = h.hexdigest()
    try:
        with open(single, "rb") as f:
            sha_single = hashlib.sha256(f.read()).hexdigest()
    except OSError:
        sha_single = None
    if sha_single is None:
        res = {"ok": False, "single": single, "parts": parts_dir,
               "sha_single": None, "sha_parts": sha_parts[:12]}
        if verbose:
            _emit_local("[pty][WARN] 版本自检：单文件 %s 不存在"
                        "（parts 才是真相源）\n" % single)
        return res
    res = {"ok": sha_single == sha_parts, "single": single, "parts": parts_dir,
           "sha_single": sha_single[:12], "sha_parts": sha_parts[:12]}
    if verbose and res["ok"]:
        _info("[pty] 版本自检：与 pty/parts 分片同步（sha256=%s）" % sha_parts[:12])
    elif verbose:
        _emit_local("[pty][WARN] 版本自检：当前文件与 client/pty/parts 分片不一致"
                    "（文件 %s / 分片 %s）！分片改动后请重组："
                    "python client/pty/generate_single_file.py "
                    "-o client/pty_client_mqtt.py -f\n"
                    % (res["sha_single"], res["sha_parts"]))
    return res


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
_LOG_HUB = globals().setdefault('_LOG_HUB', LogHub(_LOCAL_LOG))  # setdefault 防 reload 日志分脑：120_cli 已把 .serve 按值捕获进服务器配置、浏览器订阅全挂在旧 hub 上，reload 普通赋值会重绑到新 hub——新代码往新 hub 发、旧 WS 订阅者收不到，快照/增量各走一半；LogHub 构造只存 ring 引用无副作用，reload 时重复求值仅多一个即弃对象


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


# ==================== MCP 服务：把常驻 PTY 挂成标准 MCP 工具服务器 ====================
#
# 背景：本进程已通过 ai_bridge + 本地 HTTP RPC 口把常驻 PTY 暴露给外部进程，
# 但 AI 客户端侧还缺一个标准工具协议入口。MCP（Model Context Protocol）是
# 各 AI 客户端的通用工具协议；装好 ``pip install "mcp[cli]"`` 后，main 默认
# 把 MCP 端点**挂在 --port 已起的 RPC 服务器上**（路径 /mcp，与 RPC / 浏览器
# 日志台同口复用，不新开端口），AI 客户端即可把远端 shell 当普通 MCP 工具
# 集持续使用：
#
#     claude mcp add --transport http pty http://192.168.1.3:1188/mcp
#
# 同口复用的做法（参考 HuggingFace app.py 的保留路径分流中间件）：给
# server_http.RPCRequestHandler 包一层前置分发——命中 /mcp（sse 退回模式下
# 是 /sse、/messages）的请求桥接给 MCP 的 ASGI 应用，其余路径原样走既有
# RPC / WebSocket / 日志台逻辑。桥接要点：
# - mcp 1.x/2.x 双兼容：2.x 把 FastMCP 改名 MCPServer，ASGI 应用从
#   streamable_http_app()/sse_app() 取，SDK 不支持时逐级退回；
# - ASGI 应用跑在本模块专用事件循环线程上；lifespan（session manager
#   等启动上下文）手动驱动，等价 uvicorn 的启动阶段；
# - MCP 工具只是 ai_bridge 的薄封装：与 HTTP RPC 口共用同一把 busy 锁与
#   下行分流队列，MCP / RPC / 本地窗口三路调用看到的会话状态完全一致；
# - mcp 包懒导入：没装包只记一条本地日志，PTY 本体功能零影响；
# - 服务不碰主线程：桥接在 server_http 的请求线程 + 专用循环线程里完成。
#
# --mcp-port>0 时退回独立端口模式（自起 uvicorn，供 --port 0 关闭 RPC 口
# 但仍要 MCP 的场景）；--no-mcp 完全关闭。

import asyncio
import functools
import inspect
import logging
import time

try:  # mcp 2.x：FastMCP 改名 MCPServer，Context 在同名包
    from mcp.server.mcpserver import Context as _MCP_CTX
except ImportError:  # mcp 1.x
    try:
        from mcp.server.fastmcp import Context as _MCP_CTX
    except ImportError:  # 未安装 mcp：MCP 服务不会启动，工具也不会注册
        _MCP_CTX = None

_MCP_LOG_ARG_MAX = 400  # 开始行单个参数原始值最多保留的字符数
_MCP_DONE_ARG_MAX = 60  # 完成行单个参数值最多保留的字符数
_MCP_DONE_INDENT = "   "  # 完成行整行左缩进，视觉上从属于同 id 的开始行
_MCP_CALL_PARAMS_MAX = 200  # 在途调用参数缓存上限，防止异常中断的调用无限堆积
_MCP_CALL_PARAMS = globals().setdefault('_MCP_CALL_PARAMS', {})  # call_id -> 原始参数；跨 reload 保活，旧 wrapper 的完成行仍能取到
_MCP_PROGRESS_TICK = 0.5  # 有 progressToken 时的进度上报间隔（秒）
_MCP_PROGRESS_TAIL = 200  # 进度 message 里取实时缓冲尾部多少字节找最后一行
_MCP_WAIT_START_GRACE = 0.2  # wait>=0 时给执行线程的启动宽限（秒），让 busy/未连接这类秒回错误先暴露
_MCP_PARTIAL_OUT = 8000  # wait 到期提前返回时，out 取实时缓冲尾部多少字节



def _mcp_short_arg(value) -> str:
    text = repr(value)
    if len(text) > _MCP_LOG_ARG_MAX:
        return "%s…（共 %d 字符）" % (text[:_MCP_LOG_ARG_MAX], len(text))
    return text


def _mcp_done_arg(value) -> str:
    """完成行的参数值：只留前 60 字符，保证整行紧凑。"""
    text = repr(value)
    if len(text) <= _MCP_DONE_ARG_MAX:
        return text
    clipped = text[:_MCP_DONE_ARG_MAX]
    quote = text[0] if text[:1] in ("'", '"') else ""
    if quote and text.endswith(quote) and not clipped.endswith(quote):
        clipped = clipped[:-1] + "…" + quote  # 补回引号，避免截断值看起来没收尾
    else:
        clipped += "…"
    return clipped


def _mcp_log_begin(name, params) -> str:
    """工具调用一开始就落一条完整参数的日志，返回本次调用 id。

    id 由 os.urandom(3).hex() 生成，6 位十六进制，只用于把完成行和开始行
    关联起来；两行各自都带工具名与参数，可独立阅读：
        [MCP] 926fff 开始 fs_grep pattern='sherpa' path='/root/...' head_limit=30
           [MCP] 926fff 完成 fs_grep pattern='sherpa' path='/root/...' ok 0.21s
    记录的是客户端传进来的原始参数，不等远端返回；远端卡死/超时也能从
    日志台看出这次调用要做什么。
    """
    call_id = os.urandom(3).hex()
    try:
        _MCP_CALL_PARAMS[call_id] = dict(params or {})
        if len(_MCP_CALL_PARAMS) > _MCP_CALL_PARAMS_MAX:
            # dict 保序：批量丢弃最早登记的在途调用，避免缓存无限增长
            for stale in list(_MCP_CALL_PARAMS)[:_MCP_CALL_PARAMS_MAX // 2]:
                _MCP_CALL_PARAMS.pop(stale, None)
        parts = " ".join("%s=%s" % (key, _mcp_short_arg(value))
                         for key, value in (params or {}).items())
        _emit_local("[MCP] %s 开始 %s%s\n"
                    % (call_id, name, (" " + parts) if parts else ""))
    except Exception:
        pass
    return call_id


def _mcp_log_end(call_id, name, elapsed, result=None, exc=None) -> None:
    """与开始行同 id 的完成行：整行缩进，带参数短摘要、状态与耗时。"""
    params = _MCP_CALL_PARAMS.pop(call_id, None)
    try:
        if exc is not None:
            status = "FAIL %s: %s" % (type(exc).__name__, str(exc)[:200])
        elif isinstance(result, dict) and result.get("ok") is False:
            status = "FAIL %s" % str(result.get("error", ""))[:200]
        else:
            status = "ok"
        args = " ".join("%s=%s" % (key, _mcp_done_arg(value))
                        for key, value in (params or {}).items())
        _emit_local("%s[MCP] %s 完成 %s%s %s %.2fs\n"
                    % (_MCP_DONE_INDENT, call_id, name,
                       (" " + args) if args else "", status, elapsed))
    except Exception:
        pass


def _mcp_tool(fn):
    """给工具函数包一层「收到即记 + 同 id 收尾」的结构化日志。

    functools.wraps 保留 __wrapped__/__annotations__，FastMCP 仍能按原签名
    生成 schema；日志发生在调用线程、进入 ai_bridge 之前，所以不受远端耗时影响。
    同时支持同步与 async 工具：async 版本是新加的 pty_run 需要的（它要在等远端
    命令期间 await 进度上报，不能把事件循环卡死）。
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        sig = None

    def _bind(args, kwargs):
        if sig is None:
            return dict(kwargs)
        try:
            return dict(sig.bind_partial(*args, **kwargs).arguments)
        except Exception:
            return dict(kwargs)

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            call_id = _mcp_log_begin(fn.__name__, _bind(args, kwargs))
            started = time.monotonic()
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                _mcp_log_end(call_id, fn.__name__,
                             time.monotonic() - started, exc=exc)
                raise
            _mcp_log_end(call_id, fn.__name__,
                         time.monotonic() - started, result=result)
            return result

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        call_id = _mcp_log_begin(fn.__name__, _bind(args, kwargs))
        started = time.monotonic()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            _mcp_log_end(call_id, fn.__name__, time.monotonic() - started, exc=exc)
            raise
        _mcp_log_end(call_id, fn.__name__, time.monotonic() - started, result=result)
        return result

    return wrapper


def _mcp_progress_token(ctx):
    """本次调用请求里的 ``_meta.progressToken``：有 = 客户端愿收进度通知。

    MCP 客户端能力（initialize.capabilities）里没有 progress 项，进度是**按
    单次请求**协商的：只有请求带 progressToken，服务端才允许回
    ``notifications/progress`` 并由 SDK 绑定到该 token；不带 token 时 SDK 的
    ``report_progress`` 本身就是 no-op，这里直接判空可以连轮询都省掉。
    """
    if ctx is None:
        return None
    try:
        meta = ctx.request_context.meta
    except Exception:  # 没有请求上下文（本地直调）或 SDK 版本字段不同
        return None
    if meta is None:
        return None
    if isinstance(meta, dict):
        # mcp 2.x 收进来的是 RequestParamsMeta（TypedDict），SDK 用
        # RequestParams.model_validate(..., by_name=False) 归一化，线上键
        # progressToken 落成 progress_token；1.x/裸 dict 则保持原样。
        for key in ("progressToken", "progress_token"):
            if key in meta:
                return meta.get(key)
        return None
    return (getattr(meta, "progressToken", None)
            or getattr(meta, "progress_token", None))


def _mcp_last_line(text: str, limit: int = 80) -> str:
    """进度 message 里附带的最后一行输出：取最后一段非空文本并截断。"""
    lines = [ln.strip() for ln in (text or "").replace("\r", "\n").split("\n")]
    lines = [ln for ln in lines if ln]
    if not lines:
        return ""
    return lines[-1][:limit]


def _mcp_partial_result(st, wait) -> dict:
    """``wait`` 到期但命令未结束时的返回：任何字段都不允许被误读成"跑完了"。

    ``out`` 只是实时缓冲的最新一段，不是完整结果；``rc`` 必然是 None，
    ``completed/partial/running/wait_exceeded`` 四个标志互相印证，再加
    一条 ``next_action`` 指明唯一正确的后续动作（pty_tail 轮询到 END 标记）。
    """
    st = st or {}
    out = st.get("tail") or ""
    # 与完整结果的 _pack_result 对齐：从最后一条 BEGIN 标记行之后取正文，
    # 否则 tmux 原始回显里的 "echo __AI_BEGIN__; ... __AI_END_...:$__ai_rc"
    # 会留在 out 开头，容易被误读成"已有结束标记"。
    idx = out.rfind("__AI_BEGIN_")
    if idx >= 0:
        nl = out.find("\n", idx)
        if nl >= 0:
            out = out[nl + 1:]
    out = out.strip("\r\n")
    return {
        "ok": False,
        "completed": False,
        "partial": True,
        "running": True,
        "wait_exceeded": True,
        "wait_seconds": wait,
        "rc": None,
        "out": out,
        "elapsed": st.get("elapsed"),
        "total": st.get("total"),
        "channel": st.get("channel"),
        "error": ("wait %gs 到期但命令未结束：out 只是不完整片段（%d 字节），"
                  "rc 未知，命令仍在远端继续执行" % (wait, len(out))),
        "next_action": ("用 pty_tail(max_bytes=4000) 反复轮询查看最新输出，"
                        "直到 tail 中出现 __AI_END_<时间戳>__:<rc> 标记才表示命令结束；"
                        "在此之前再调 pty_run 会返回 busy。"),
    }


def _mcp_run_orphan(fut) -> None:
    """wait 提前返回后，执行线程还在跑：挂回调消费结果/异常，只落本地日志。

    不消费的话 executor future 的异常会变成 "exception was never retrieved"
    噪音；消费了还能在日志台看到这条命令最终跑成什么样。
    """
    def _done(f):
        try:
            res = f.result()
        except Exception as exc:
            _emit_local("[MCP] wait 提前返回的任务最终异常: %s: %s\n"
                        % (type(exc).__name__, str(exc)[:200]))
            return
        if isinstance(res, dict):
            _emit_local(
                "[MCP] wait 提前返回的任务最终完成 rc=%s timed_out=%s out=%d 字节\n"
                % (res.get("rc"), res.get("timed_out"),
                   len(res.get("out") or "")))
    try:
        fut.add_done_callback(_done)
    except Exception:
        pass


async def _mcp_await_run(fut, wait, token, ctx, tail_fn=None):
    """等执行线程的 future，按 ``wait`` 决定最多等多久。

    返回 ``(result, st)``：``result`` 非 None = 命令已结束（含 ai_bridge
    自己的 timeout 到达，此时 result 里 ``timed_out=True``）；``result`` 为
    None = ``wait`` 到期、命令仍在跑，``st`` 是 tail() 快照。

    ``wait < 0``：等到 future 完成（原阻塞行为）；``wait = 0``：立即返回；
    ``wait > 0``：最多等这么多秒。带 progressToken 时每 0.5s 上报一条进度。
    """
    wait = float(wait)
    tail = tail_fn or ai_bridge.tail
    loop = asyncio.get_running_loop()

    async def _report():
        st = tail(_MCP_PROGRESS_TAIL)
        last = _mcp_last_line(st.get("tail") or "")
        try:
            await ctx.report_progress(
                float(st.get("total") or 0), None,
                "已运行 %ss，已收 %d 字节%s"
                % (st.get("elapsed"), st.get("total") or 0,
                   (": " + last) if last else ""))
        except Exception:
            pass  # 客户端断开/不支持时不影响命令本身

    if wait < 0:
        if token is None:
            await asyncio.wait([fut])
            return fut.result(), None
        while True:
            await asyncio.wait([fut], timeout=_MCP_PROGRESS_TICK)
            if fut.done():
                return fut.result(), None
            await _report()

    # wait>=0 也留一小段启动宽限：不然 busy / 未连接这类秒回错误会被当成
    # "已启动并在跑"，白白返回一个空的不完整快照。
    deadline = loop.time() + max(wait, _MCP_WAIT_START_GRACE)
    while True:
        remain = deadline - loop.time()
        if remain <= 0:
            break
        step = min(_MCP_PROGRESS_TICK, remain) if token is not None else remain
        await asyncio.wait([fut], timeout=step)
        if fut.done():
            return fut.result(), None
        if token is not None:
            await _report()
    # 到点边界上刚好完成也算完成，避免把成功结果误报成不完整
    if fut.done():
        return fut.result(), None
    return None, tail(_MCP_PARTIAL_OUT)



def _mcp_quiet_sdk_logs() -> None:
    """压掉无状态 streamable-http 的 INFO 噪音。

    stateless 模式每请求新建一个临时 session，SDK 在拆 session 时打
    "Terminating session: None"，既没有 id 也没法跟工具调用对应，只会把
    真正的日志冲散。只降这一条 logger 的级别，WARNING 以上照常输出。
    """
    logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)


_MCP_SERVER = globals().setdefault('_MCP_SERVER', None)  # 启动/挂载成功后保存 MCP server 实例（诊断/测试用）；setdefault 防 reload 重绑丢引用
_MCP_ASGI_APP = globals().setdefault('_MCP_ASGI_APP', None)  # 挂 1188 口模式下的 ASGI 应用。最要命的一个：已打进 RPCRequestHandler 的前置分发 wrapper 运行期按模块全局名查本变量，reload 把名字重绑回 None 后 /mcp 请求静默回落成普通 RPC 执行——setdefault 让旧钩子跨 reload 继续拿到活应用
_MCP_HTTP_PATHS = globals().setdefault('_MCP_HTTP_PATHS', frozenset())  # 需在 RPC 口上拦截分流的路径集合；与 _MCP_ASGI_APP 同生共死，reload 丢集合同样让分流失效
_MCP_LOOP = globals().setdefault('_MCP_LOOP', None)  # ASGI 专用事件循环（后台线程）；reload 丢引用会让 _mcp_ensure_loop 再起第二条循环线程
_MCP_LOOP_LOCK = globals().setdefault('_MCP_LOOP_LOCK', threading.Lock())  # 循环线程启动互斥锁；reload 重造新锁 + _MCP_LOOP 名字回 None = 判空双起循环线程，锁必须与引用一起跨 reload 保活

_MCP_INSTRUCTIONS = (
    "远端服务器的常驻交互 PTY。pty_run 跑非交互命令并回收 rc/输出"
    "（wait>=0 会提前返回不完整结果，此时 completed=false，用 pty_tail 续看）；"
    "交互提示（密码/y/n）用 pty_send 喂键；pty_status 看会话与 broker "
    "状态；pty_log 看本地诊断日志；画面冻结时 pty_redraw 自愈。"
    "同一 shell 串行执行，重叠调用返回 busy。"
    "文件工具经 MQTT RPC 在远端 server 进程里执行，路径语义是远端机器的"
    "路径（不是 pty 客户端本机），默认允许任意路径：fs_read/fs_ls/"
    "fs_glob/fs_grep 只读；fs_write/fs_edit/fs_mkdir/fs_move/fs_delete 可写，"
    "仅 --fs-read-only 时整体禁用写操作。")


def _mcp_pick_transport(server, prefer: str) -> str:
    """选 HTTP transport：显式指定优先；自动时按 SDK 能力选。"""
    if prefer in ("streamable-http", "sse"):
        return prefer
    return "streamable-http" if hasattr(server, "streamable_http_app") else "sse"


def _new_mcp_server(host=None, port=None):
    """构造 MCP server 实例（mcp 1.x/2.x 双兼容），返回 (server, kind)。

    mcp 2.x 把 FastMCP 改名 MCPServer 且 host/port 移进 run() 参数；1.x
    的 host/port 在构造器 Settings 里。host/port 仅独立端口模式需要。
    不可用时返回 (None, None)。
    """
    try:
        try:  # mcp 2.x
            from mcp.server.mcpserver import MCPServer as _K
            kind = "2"
        except ImportError:  # mcp 1.x
            from mcp.server.fastmcp import FastMCP as _K
            kind = "1"
        try:
            if kind == "1" and port is not None:
                return _K("pty", instructions=_MCP_INSTRUCTIONS,
                          host=str(host), port=int(port)), kind
            return _K("pty", instructions=_MCP_INSTRUCTIONS), kind
        except TypeError:  # 构造器不收 instructions/host/port 的老版本
            server = _K("pty")
            if kind == "1" and port is not None:
                try:
                    server.settings.instructions = _MCP_INSTRUCTIONS
                    server.settings.host = str(host)
                    server.settings.port = int(port)
                except Exception:
                    pass
            return server, kind
    except Exception as exc:
        _emit_local('[pty][MCP] mcp 包不可用，跳过（pip install "mcp[cli]" 启用）: %s\n' % exc)
        return None, None


def _mcp_register_tools(server) -> None:
    """注册 MCP 工具：7 个 PTY 工具（直通 ai_bridge）+ 9 个文件工具（远端执行）。"""
    _mcp_quiet_sdk_logs()

    @server.tool()
    @_mcp_tool
    async def pty_run(cmd: str, timeout: float = 60.0,
                      acquire_timeout: float = 2.0, pane: str = "",
                      wait: float = -1.0, encoding: str = "",
                      ctx: _MCP_CTX = None) -> dict:
        """在常驻远程 shell 里跑一条命令，等结束标记并回收输出与退出码。

        返回 {"ok", "rc", "out", "timed_out", "pane", "channel", ...}；
        命令串行执行，上一条未结束时立刻返回 {"ok": False, "busy": True}
        （可加大 acquire_timeout 或稍后重试）。超时只停止回收，远端命令继续
        跑，可用 pty_send 干预或再 pty_run 收尾。

        ``wait``：本次调用最多阻塞多久（秒）。默认 -1 = 一直等到命令结束或
        ``timeout`` 上限（原行为）；0 = 启动后立即返回；正数 = 最多等这么久。
        提前返回时**结果一定不完整**，返回里明确标注
        ``completed=False / partial=True / running=True / wait_exceeded=True``
        且 ``rc=None``，``out`` 只是最新一段输出；此时按 ``next_action`` 用
        ``pty_tail`` 轮询到 ``__AI_END_<时间戳>__:<rc>`` 标记为止（这之前再调
        pty_run 会 busy）。不要把它当成命令失败或已完成。

        投递窗口：默认用会话启动时固化的 tmux 窗口（session 内 window_index
        最小的窗口，可用 pty_pane 或 --ai-pane 改）；pane 传 "%2"/"0:2"/窗口名
        可临时改本次目标。tmux 不可用时自动回落为直写本 PTY。

        ``encoding``：留空 = 自动（无控制字符原文直发；有裸换行且会话 shell
        是 bash/zsh 时用 ``$'...'`` 保持窗口可读；否则 base64）；``dollar``
        强制 ANSI-C 引用；``base64`` 强制严格编码。选择只看字符构成，与命令
        长短无关；多行命令在 Windows/未知 shell 上会自动走 base64，窗口里
        看到的是密文，这是预期行为。

        进度上报：客户端在本次请求里带 ``_meta.progressToken`` 时，命令执行
        期间每 0.5 秒回一条 notifications/progress，message 带已运行时长、
        已收字节数和最后一行输出；没带 token 就完全不开轮询（零额外开销）。
        注意进度只是**中途预览**，完整结果仍以本调用最终返回的 out/rc 为准。
        """
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(
            None,
            functools.partial(
                ai_bridge.run, str(cmd), timeout=float(timeout),
                acquire_timeout=float(acquire_timeout),
                pane=(str(pane) if pane else None),
                encoding=(str(encoding) if encoding else None)))
        token = _mcp_progress_token(ctx)
        result, st = await _mcp_await_run(fut, wait, token, ctx)
        if result is None:
            # 调用先返回，后台线程继续跑：挂回调消费结果/异常，避免
            # "exception was never retrieved" 并且日志台仍能看到最终 rc。
            _mcp_run_orphan(fut)
            return _mcp_partial_result(st, float(wait))
        return result

    @server.tool()
    @_mcp_tool
    def pty_pane(pane: str = "") -> dict:
        """查询或设置 AI 命令的默认 tmux 窗口。

        pane 留空 = 只查询当前值；"auto" = 清空固化值并按「index 最小的窗口」
        重新解析；其他值 = 固化为该 target（%2 最稳，也接受 0:2 / 窗口名）。
        返回 {"ok", "pane", "pane_source", "tmux_ok", "encoding"}。
        """
        if not str(pane or "").strip():
            return ai_bridge.get_pane()
        return ai_bridge.set_pane(pane)

    @server.tool()
    @_mcp_tool
    def pty_tail(max_bytes: int = 8000) -> dict:
        """查看**正在跑**的长命令已产出的输出（不占 busy 锁，可与 pty_run 并发）。

        pty_run 要等命令结束才返回；长任务期间用本工具反复调用即可看到最新
        进度（服务端推送通道边产生边累积）。返回 {"ok", "tail", "cmd",
        "channel", "elapsed", "total"}；channel 为 pty / tmux / tmux+push。
        """
        return ai_bridge.tail(max_bytes=int(max_bytes))

    @server.tool()
    @_mcp_tool
    def pty_send(data: str) -> dict:
        """向常驻 PTY 原样喂键，不回收输出：回答密码/y/n、发 Ctrl-C 等。

        例："y\\n" 确认提示、"\\x03" Ctrl-C、"vim file\\r" 打开编辑器。
        """
        return ai_bridge.send(data)

    @server.tool()
    @_mcp_tool
    def pty_status() -> dict:
        """查询常驻会话状态：是否 attached/已结束、是否忙、broker 在线数、
        终端写线程卡滞秒数（>2 建议 pty_redraw）等。"""
        return ai_bridge.status()

    @server.tool()
    @_mcp_tool
    def pty_log(n: int = 100) -> str:
        """读最近 n 行本地诊断日志（broker 连接/RPC 请求/告警），n 夹在 1~500。"""
        return get_log(max(1, min(int(n), 500)))

    @server.tool()
    @_mcp_tool
    def pty_redraw() -> dict:
        """终端写线程卡滞自愈：必要时切换本地写线程，并通知远端整屏重绘。"""
        steps = []
        if _OUT_WRITER is not None and _OUT_WRITER.stall_seconds() > 2.0:
            steps.append(reset_out_writer())
        pty = getattr(ai_bridge, "pty", None)
        if pty is None or getattr(pty, "end_reason", None):
            return {"ok": False, "error": "pty 未连接或已结束", "steps": steps}
        try:
            pty.configure(redraw=True)
        except Exception as exc:
            steps.append({"redraw": False, "error": str(exc)})
            return {"ok": False, "steps": steps}
        steps.append({"redraw": True})
        return {"ok": True, "steps": steps}

    # 文件工具（098_mcp_files）：经 transport.request 在远端执行，不经过 PTY shell
    _mcp_register_fs_tools(server)


def _mcp_loop_main(loop):
    """ASGI 专用事件循环线程体。"""
    global _MCP_LOOP
    asyncio.set_event_loop(loop)
    _MCP_LOOP = loop
    loop.run_forever()


def _mcp_ensure_loop():
    """懒启动专用事件循环线程，返回 loop（桥接与 lifespan 共用）。"""
    global _MCP_LOOP
    with _MCP_LOOP_LOCK:
        if _MCP_LOOP is None or _MCP_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=_mcp_loop_main, args=(loop,),
                             name="pty-mcp-loop", daemon=True).start()
            while _MCP_LOOP is None:
                time.sleep(0.01)
    return _MCP_LOOP


def _mcp_drive_lifespan(app, loop) -> None:
    """手动驱动 ASGI lifespan（等价 uvicorn 启动阶段）：起 session manager 等。

    只发 startup 不发 shutdown：常驻服务随进程退出（线程为 daemon）。
    """
    q = asyncio.Queue()

    async def receive():
        return await q.get()

    async def send(message):
        if message.get("type") == "lifespan.startup.failed":
            _emit_local("[pty][MCP] MCP lifespan 启动失败: %s\n"
                        % message.get("message", "?"))

    async def _drive():
        await q.put({"type": "lifespan.startup"})
        await app({"type": "lifespan", "asgi": {"version": "3.0",
                                                "spec_version": "2.0"},
                   "state": {}}, receive, send)

    asyncio.run_coroutine_threadsafe(_drive(), loop)


class _MCPHttpBridge:
    """1188 口 /mcp 请求 -> MCP ASGI 应用的同步桥。

    server_http 每请求一线程；ASGI 应用跑在专用事件循环线程上：请求体
    一次性读齐，receive 首次返回后即挂起；send 的响应字节由循环线程直接
    写回（此刻请求线程正阻塞等待，单写者安全）。无 content-length 的流式
    响应按 Connection: close 收尾；SSE 长连接由客户端断开，写失败即整条
    请求结束。
    """

    def __init__(self, handler):
        self.h = handler

    def run(self, app) -> None:
        h = self.h
        path, _, query = h.path.partition("?")
        try:
            length = int(h.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = h.rfile.read(length) if length > 0 else b""
        body_state = {"sent": False}
        state = {"started": False, "streaming": False}

        async def receive():
            if not body_state["sent"]:
                body_state["sent"] = True
                return {"type": "http.request", "body": body,
                        "more_body": False}
            await asyncio.Event().wait()  # 请求已完整读入：挂起等断开

        async def send(message):
            if message["type"] == "http.response.start":
                state["started"] = True
                h.log_request(message["status"])
                h.send_response_only(message["status"])
                has_len = False
                for k, v in message.get("headers", []):
                    name = bytes(k).decode("latin-1")
                    value = bytes(v).decode("latin-1")
                    low = name.lower()
                    if low in ("connection", "transfer-encoding"):
                        continue  # 由桥统一决定：流式时补 Connection: close
                    if low == "content-length":
                        has_len = True
                    h.send_header(name, value)
                state["streaming"] = not has_len
                if state["streaming"]:
                    h.send_header("Connection", "close")
                h.end_headers()
            elif message["type"] == "http.response.body":
                chunk = message.get("body", b"") or b""
                if chunk:
                    h.wfile.write(chunk)
                    h.wfile.flush()
                if state["streaming"] and not message.get("more_body"):
                    h.close_connection = True

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": h.request_version.rsplit("/", 1)[-1] or "1.1",
            "method": h.command,
            "scheme": "http",
            "path": path,
            "query_string": query.encode("latin-1"),
            "root_path": "",
            "server": (h.server.server_address[0], h.server.server_address[1]),
            "client": (h.client_address[0], h.client_address[1]),
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                        for k, v in h.headers.items()],
        }
        try:
            fut = asyncio.run_coroutine_threadsafe(app(scope, receive, send),
                                                   _mcp_ensure_loop())
            fut.result()
        except (ConnectionError, BrokenPipeError, OSError):
            h.close_connection = True  # 客户端断开：正常收尾
        except Exception as exc:
            _emit_local("[pty][MCP] /mcp 桥接异常: %r\n" % exc)
            h.close_connection = True
            if not state["started"]:
                try:
                    h.send_error(502, "MCP bridge error")
                except Exception:
                    pass


def _mcp_hook_handler(Handler) -> None:
    """给 RPCRequestHandler 打前置分发钩子（幂等）：保留路径分流给 ASGI 桥。"""
    if not hasattr(Handler, "do_DELETE"):
        # BaseHTTPRequestHandler 对未实现方法回 501；补上让 /mcp 的 DELETE
        # （MCP 会话结束信号）有机会被钩子接管，其余 DELETE 维持 501 原状。
        def _do_delete(self):
            self.send_error(501, "Unsupported method (%r)" % self.command)
        Handler.do_DELETE = _do_delete

    def _wrap(fn):
        def wrapper(self, *args, **kwargs):
            if (_MCP_ASGI_APP is not None
                    and self.path.split("?", 1)[0] in _MCP_HTTP_PATHS):
                _MCPHttpBridge(self).run(_MCP_ASGI_APP)
                return None
            return fn(self, *args, **kwargs)
        wrapper.__name__ = getattr(fn, "__name__", "handler")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        wrapper._mcp_wrapped = True  # 幂等键挂在被包装函数上而非模块级 _MCP_HOOKED 标志：标志在 reload 后重置 False 会把同一个类再包一层（请求链上叠双层路径检查），而函数对象自带标记跨 reload 永不失真
        return wrapper

    for name in ("do_GET", "do_POST", "do_DELETE"):
        base = getattr(Handler, name, None)
        # 已带 _mcp_wrapped 的（含 reload 前包过的旧类）直接跳过；server_http 被
        # reload 出新类时此处自然给新类补钩子，旧类旧钩子经 setdefault 保住的
        # _MCP_ASGI_APP 继续工作，两边都不脱靶
        if base is not None and not getattr(base, "_mcp_wrapped", False):
            setattr(Handler, name, _wrap(base))


_MCP_LOG_FILTER_NAME = "pty-mcp"  # server_http 注册表里的名字；同名覆盖 = reload 幂等


def _mcp_access_log_off(handler) -> bool:
    """server_http 日志过滤器：MCP 端点（/mcp，sse 退回模式是 /sse+/messages）静音。

    一次 MCP 工具调用在 HTTP 层会伴随 initialize / notifications / tools/call
    多条 POST，既无工具名也无参数，只会把日志冲散；真正的调用轨迹由 pty 侧
    ``[MCP] <id> 开始/完成`` 首尾配对承担。判定只认实际挂载的路径集合
    （_MCP_HTTP_PATHS，与前置分发钩子同一个集合）：未挂载/已卸载时集合为空，
    一律照常打日志，不会误伤其它 RPC / 日志台 URL。
    """
    if not _MCP_HTTP_PATHS:
        return False
    try:
        path = getattr(handler, "path", "") or ""
    except Exception:
        return False
    return path.split("?", 1)[0] in _MCP_HTTP_PATHS


def _mcp_register_access_log_filter() -> None:
    """把 MCP 端点静音规则注册到 server_http（同名覆盖，失败只降级）。

    规则属于 pty 的 MCP 服务自身，不硬编码在 server_http 里：server_http 只
    提供 add_log_filter() 挂点（rdp 页「服务端日志」走同一机制）。注册失败
    最多多打几行访问日志，绝不影响 PTY 会话。
    """
    try:
        import server_http
        server_http.add_log_filter(_MCP_LOG_FILTER_NAME, _mcp_access_log_off)
    except Exception as exc:
        _emit_local("[pty][MCP][WARN] 访问行静音规则注册失败，照常打日志: %r\n"
                    % exc)


def mount_mcp_on_rpc(prefer: str = "", host: str = "0.0.0.0") -> dict:
    """把 MCP 端点挂到 --port 已起的 HTTP RPC 服务器上（同口复用，不新开端口）。

    1188 一个口上 RPC / 浏览器日志台 / MCP 三种服务并存；返回启动结果
    dict；任何失败只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER, _MCP_ASGI_APP, _MCP_HTTP_PATHS  # 原 _MCP_HOOKED 模块标志已删除：幂等改由被包装函数上的 _mcp_wrapped 标记承担，标志在 reload 后重置 False 会失真
    if _MCP_ASGI_APP is not None:
        return {"ok": True, "already": True, "paths": sorted(_MCP_HTTP_PATHS)}
    try:
        import server_http
    except Exception as exc:
        return {"ok": False, "error": "server_http 不可用: %s" % exc}
    server, kind = _new_mcp_server()
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)

    app, paths = _mcp_build_asgi(server, prefer, host)
    if app is None or not paths:
        return {"ok": False, "error": "当前 mcp SDK 未提供可挂载的 HTTP 应用"}

    _mcp_hook_handler(server_http.RPCRequestHandler)  # 无条件调用：钩子按 fn._mcp_wrapped 自幂等，不再依赖会被 reload 重置的模块标志
    loop = _mcp_ensure_loop()
    _mcp_drive_lifespan(app, loop)
    _MCP_SERVER = server
    _MCP_HTTP_PATHS = frozenset(paths)
    _MCP_ASGI_APP = app
    _mcp_register_access_log_filter()  # 静音 /mcp 的 2xx 访问行：规则由本模块自注册，失败只多打日志
    _emit_local("[pty][MCP] MCP 工具服务已挂到 RPC 口：路径 %s（与 RPC/日志台同口复用）"
                "  工具：pty_run/pty_send/pty_status/pty_log/pty_redraw + "
                "fs_read/fs_write/fs_edit/fs_glob/fs_grep/fs_ls/fs_mkdir/fs_move/fs_delete\n"
                % "、".join(sorted(paths)))
    return {"ok": True, "paths": sorted(paths)}


def _mcp_build_asgi(server, prefer: str, host: str):
    """按 SDK 能力取 ASGI 应用与它占用的 HTTP 路径（优先 streamable-http）。"""
    if prefer != "sse" and hasattr(server, "streamable_http_app"):
        try:
            try:
                # 无状态模式：每请求新建 transport，不校验 Mcp-Session-Id。
                # 有状态会话只存在进程内存里，pty 重启即整体失效；客户端断线后
                # 往往只重连 SSE、不重新 initialize，会持续拿旧 session id 请求
                # 并被 404 "Session not found" 拒绝，表现为「list tools failed」。
                # 无状态下旧 id 直接忽略，重启不再断链。
                app = server.streamable_http_app(host=str(host), stateless_http=True)
            except TypeError:  # 旧版 SDK 不认 stateless_http：退回有状态模式
                try:
                    app = server.streamable_http_app(host=str(host))
                except TypeError:
                    app = server.streamable_http_app()
            base = str(getattr(getattr(server, "settings", None),
                               "streamable_http_path", None) or "/mcp").rstrip("/")
            return app, {base, base + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] streamable-http 应用创建失败，改试 sse: %r\n" % exc)
    if hasattr(server, "sse_app"):
        try:
            try:
                app = server.sse_app(host=str(host))
            except TypeError:
                app = server.sse_app()
            st = getattr(server, "settings", None)
            sse = str(getattr(st, "sse_path", None) or "/sse").rstrip("/")
            msg = str(getattr(st, "message_path", None) or "/messages").rstrip("/")
            return app, {sse, sse + "/", msg, msg + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] sse 应用创建失败: %r\n" % exc)
    return None, set()


def start_mcp_server(port, host="0.0.0.0", prefer: str = "") -> dict:
    """独立端口模式：在 daemon 线程里自起 MCP HTTP 服务（--mcp-port>0 时用）。

    挂 1188 口的常规路径见 mount_mcp_on_rpc。返回启动结果 dict；任何失败
    只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER
    port = int(port)
    if port <= 0:
        return {"ok": False, "error": "mcp_port<=0，独立 MCP 服务未开启"}
    if _MCP_SERVER is not None:
        return {"ok": True, "already": True, "port": port}
    server, kind = _new_mcp_server(host=host, port=port)
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)
    _MCP_SERVER = server

    transport = _mcp_pick_transport(server, (prefer or "").strip().lower())

    def _serve():
        try:
            if kind == "2":  # 2.x：host/port 是 run() 的 transport 参数
                server.run(transport=transport, host=str(host), port=port)
            else:
                server.run(transport=transport)
        except Exception as exc:
            _emit_local("[pty][MCP] MCP 服务异常退出: %s\n" % exc)

    threading.Thread(target=_serve, name="pty-mcp", daemon=True).start()
    path = "/mcp" if transport == "streamable-http" else "/sse"
    _emit_local("[pty][MCP] MCP 工具服务已开启（%s 独立端口）：http://%s:%d%s  "
                "工具：pty_run/pty_send/pty_status/pty_log/pty_redraw + 9 个 fs_* 文件工具\n"
                % (transport, host, port, path))
    return {"ok": True, "transport": transport, "port": port, "path": path}


# ==================== MCP 文件工具：经 MQTT RPC 在「远端」读写/编辑 ====================
#
# 结构说明（与 pty_* 工具的区别）：
#   - pty 通道：ai_bridge.run() 把命令行敲进远端交互 shell（占 busy 锁、
#     受 TTY 行规程与引号影响）；
#   - rpc 通道：ai_bridge.transport.request(code) 把 Python 源码发到远端
#     server_mqtt.py，在远端进程里 exec 并回传 r。不经过 shell、不占 busy
#     锁，就是 client_mqtt.rpc(code, topic) 使用的同一条路；
#   - 本文件的 fs_* 工具走第二条：实现函数（_fs_op_*）由客户端打包成源码，
#     首次调用在远端 exec 并缓存，之后每次只发少量的调用参数。因此路径
#     语义完全是**远端机器**的路径（例如 /root/build_xime_home/...），
#     而不是 pty 客户端本机。
#
# 权限模型：
#   - 读写默认都可作用于远端任意路径。pty_run 本就能执行任意代码，给写操作
#     加根限制没有实际意义；
#   - --fs-read-only：可选的全局只读开关（默认关闭），写家族全部禁用；
#   - 单次读写上限 5MB；grep 有文件数与耗时上限，避免拖死远端进程。

import base64
import fnmatch
import glob as _globmod
import hashlib
import inspect
import json

_FS_READ_ONLY = globals().setdefault('_FS_READ_ONLY', False)  # 只读模式：写家族一律拒绝
_FS_LOCK = globals().setdefault('_FS_LOCK', threading.RLock())  # 客户端串行化打包/配置
_FS_READ_MAX_BYTES = 5 * 1024 * 1024
_FS_WRITE_MAX_BYTES = 5 * 1024 * 1024
_FS_READ_MAX_LINES = 5000
_FS_LINE_MAX_CHARS = 2000
_FS_GREP_MAX_BYTES = 2 * 1024 * 1024
_FS_GREP_MAX_FILES = 20000
_FS_GREP_MAX_SECONDS = 5.0
_FS_GREP_SKIP_DIRS = frozenset((
    ".git", ".hg", ".svn", "__pycache__", ".venv", "venv", "node_modules",
    ".gradle", "build", "dist", ".idea", ".mypy_cache", ".pytest_cache",
))
# 单次远端请求的超时（含 grep 的 5 秒扫描窗口与网络往返）
_FS_CALL_TIMEOUT = globals().setdefault('_FS_CALL_TIMEOUT', 60.0)
# 远端源码包缓存：第一次调用时打包并缓存，避免每次 inspect 重新拼
_FS_SRC_CACHE = globals().setdefault('_FS_SRC_CACHE', None)


def _fs_fail(msg):
    return {"ok": False, "error": str(msg)}


def _fs_abs(path, base=None):
    """相对路径按 base（默认进程 cwd）解析成绝对路径。

    这段代码在远端执行，所以 cwd 就是远端 server_mqtt.py 进程的工作目录。
    """
    p = str(path or "").strip() or "."
    p = os.path.expanduser(p)
    if not os.path.isabs(p):
        p = os.path.join(base or os.getcwd(), p)
    return os.path.abspath(p)


def _fs_configure(read_only=False):
    """配置只读开关（main 在挂载 MCP 前调用；也可 RPC 热调）。"""
    global _FS_READ_ONLY
    _FS_READ_ONLY = bool(read_only)
    return {"ok": True, "read_only": _FS_READ_ONLY}


def _fs_require_writable():
    """写家族统一入口：只读模式返回错误文本，否则 None。"""
    if _FS_READ_ONLY:
        return "只读模式（--fs-read-only）：写操作已禁用"
    return None


def _fs_require_file(path):
    real = os.path.realpath(_fs_abs(path))
    if not os.path.exists(real):
        return "文件不存在: %s" % real
    if not os.path.isfile(real):
        return "不是普通文件: %s" % real
    return None


def _fs_decode(data, path):
    if b"\x00" in data[:8192]:
        return None, "看起来是二进制文件（含 NUL 字节），拒绝按文本处理: %s" % path
    try:
        return data.decode("utf-8"), None
    except UnicodeDecodeError as exc:
        return None, "不是 UTF-8 文本，拒绝按文本处理: %s (%s)" % (path, exc)


def _fs_clip_line(line):
    if len(line) <= _FS_LINE_MAX_CHARS:
        return line
    return line[:_FS_LINE_MAX_CHARS] + "…[本行超长已截断]"


def _fs_count_lines(text):
    if not text:
        return 0
    return len(text.splitlines())


def _fs_walk_files(base, name_glob=""):
    """遍历待搜索文件：单文件直接返回，目录按跳过表剪枝。"""
    if os.path.isfile(base):
        yield base
        return
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in _FS_GREP_SKIP_DIRS]
        for name in filenames:
            full = os.path.join(dirpath, name)
            if name_glob:
                rel = os.path.relpath(full, base).replace(os.sep, "/")
                if not (fnmatch.fnmatch(name, name_glob)
                        or fnmatch.fnmatch(rel, name_glob)):
                    continue
            yield full


# ---------------- 实现层（在远端 exec 执行，勿依赖客户端状态） ----------------

def _fs_op_read(file_path, offset=0, limit=2000):
    err = _fs_require_file(file_path)
    if err:
        return _fs_fail(err)
    real = os.path.realpath(_fs_abs(file_path))
    try:
        size = os.path.getsize(real)
        if size > _FS_READ_MAX_BYTES:
            return _fs_fail("文件过大（%d 字节，上限 %d）：请用 fs_grep 定位后分段读取"
                            % (size, _FS_READ_MAX_BYTES))
        with open(real, "rb") as f:
            data = f.read(_FS_READ_MAX_BYTES + 1)
    except OSError as exc:
        return _fs_fail("读取失败: %s" % exc)
    text, err = _fs_decode(data, real)
    if err:
        return _fs_fail(err)
    lines = text.splitlines()
    total = len(lines)
    off = max(0, int(offset))
    lim = max(1, min(int(limit), _FS_READ_MAX_LINES))
    chunk = lines[off:off + lim]
    content = "\n".join("%6d→%s" % (off + i + 1, _fs_clip_line(line))
                        for i, line in enumerate(chunk))
    return {
        "ok": True,
        "file_path": real,
        "content": content,
        "start_line": off + 1 if chunk else 0,
        "total_lines": total,
        "returned_lines": len(chunk),
        "truncated": off + lim < total,
        "bytes": len(data),
    }


def _fs_op_ls(path=".", show_hidden=True):
    root = _fs_abs(path)
    if not os.path.isdir(root):
        return _fs_fail("不是目录或不存在: %s" % root)
    entries = []
    try:
        with os.scandir(root) as it:
            for entry in it:
                if not show_hidden and entry.name.startswith("."):
                    continue
                try:
                    st = entry.stat(follow_symlinks=False)
                    size, mtime = st.st_size, st.st_mtime
                except OSError:
                    size, mtime = None, None
                if entry.is_symlink():
                    kind = "link"
                elif entry.is_dir():
                    kind = "dir"
                elif entry.is_file():
                    kind = "file"
                else:
                    kind = "other"
                entries.append({"name": entry.name, "type": kind,
                                "size": size, "mtime": mtime})
    except OSError as exc:
        return _fs_fail("读取目录失败: %s" % exc)
    entries.sort(key=lambda x: (x["type"] != "dir", x["name"].lower()))
    text = "\n".join(e["name"] + ("/" if e["type"] == "dir" else "") for e in entries)
    return {"ok": True, "path": root, "total": len(entries),
            "entries": entries, "text": text}


def _fs_op_glob(pattern, path=".", max_results=200):
    if not str(pattern or "").strip():
        return _fs_fail("pattern 不能为空")
    root = _fs_abs(path)
    pat = str(pattern)
    full = pat if os.path.isabs(pat) else os.path.join(root, pat)
    cap = max(1, min(int(max_results), 1000))
    try:
        found = _globmod.glob(full, recursive=True)
    except OSError as exc:
        return _fs_fail("查找失败: %s" % exc)
    items = []
    for p in found:
        if not os.path.isfile(p):
            continue
        try:
            st = os.stat(p)
        except OSError:
            continue
        items.append({"path": os.path.abspath(p),
                      "size": st.st_size, "mtime": st.st_mtime})
    items.sort(key=lambda x: x["mtime"], reverse=True)
    total = len(items)
    items = items[:cap]
    return {"ok": True, "path": root, "pattern": pat, "total": total,
            "matches": items, "truncated": total > cap,
            "text": "\n".join(x["path"] for x in items)}


def _fs_op_grep(pattern, path=".", glob="", ignore_case=False,
                output_mode="content", context=0, head_limit=100, offset=0):
    mode = str(output_mode or "content").strip().lower()
    if mode not in ("content", "files_with_matches", "count"):
        return _fs_fail("output_mode 只能是 content / files_with_matches / count")
    try:
        rx = re.compile(str(pattern), re.IGNORECASE if ignore_case else 0)
    except re.error as exc:
        return _fs_fail("正则无效: %s" % exc)
    base = _fs_abs(path)
    if not os.path.exists(base):
        return _fs_fail("路径不存在: %s" % base)
    cap = max(1, min(int(head_limit), 500))
    skip = max(0, int(offset))
    ctx = max(0, min(int(context), 20))
    deadline = time.monotonic() + _FS_GREP_MAX_SECONDS
    rows = []
    files_hit = []
    counts = []
    scanned = 0
    truncated = False

    for fp in _fs_walk_files(base, str(glob or "")):
        if scanned >= _FS_GREP_MAX_FILES or time.monotonic() > deadline:
            truncated = True
            break
        scanned += 1
        try:
            if os.path.getsize(fp) > _FS_GREP_MAX_BYTES:
                continue
            with open(fp, "rb") as f:
                data = f.read(_FS_GREP_MAX_BYTES + 1)
        except OSError:
            continue
        if b"\x00" in data[:4096]:
            continue
        text = data.decode("utf-8", "replace")
        lines = text.splitlines()
        hit_lines = [i for i, line in enumerate(lines) if rx.search(line)]
        if not hit_lines:
            continue
        real = os.path.abspath(fp)
        files_hit.append(real)
        counts.append((real, len(hit_lines)))
        if mode != "content":
            continue
        hit_set = set(hit_lines)
        keep = set()
        for i in hit_lines:
            keep.update(range(max(0, i - ctx), min(len(lines), i + ctx + 1)))
        last = None
        for i in sorted(keep):
            if last is not None and i != last + 1:
                rows.append("--")
            mark = ":" if i in hit_set else "-"
            rows.append("%s%s%d%s%s" % (real, mark, i + 1, mark, lines[i]))
            last = i

    if mode == "files_with_matches":
        rows = files_hit
    elif mode == "count":
        rows = ["%s:%d" % (p, n) for p, n in counts]
    total = len(rows)
    picked = rows[skip:skip + cap]
    return {
        "ok": True,
        "mode": mode,
        "pattern": pattern,
        "path": base,
        "scanned_files": scanned,
        "matched_files": len(files_hit),
        "total": total,
        "returned": len(picked),
        "truncated": truncated or total > skip + cap,
        "text": "\n".join(picked),
    }


def _fs_op_write(file_path, content):
    target = _fs_abs(file_path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not isinstance(content, str):
        return _fs_fail("content 必须是字符串")
    data = content.encode("utf-8")
    if len(data) > _FS_WRITE_MAX_BYTES:
        return _fs_fail("内容过大（%d 字节，上限 %d）" % (len(data), _FS_WRITE_MAX_BYTES))
    parent = os.path.dirname(target) or os.getcwd()
    if not os.path.isdir(parent):
        return _fs_fail("父目录不存在: %s" % parent)
    if os.path.isdir(target):
        return _fs_fail("目标是目录，不能整体写入: %s" % target)
    created = not os.path.exists(target)
    try:
        with _FS_LOCK:
            with open(target, "wb") as f:
                f.write(data)
    except OSError as exc:
        return _fs_fail("写入失败: %s" % exc)
    return {"ok": True, "file_path": target, "bytes": len(data),
            "created": created, "total_lines": _fs_count_lines(content)}


def _fs_op_edit(file_path, old_string, new_string, replace_all=False):
    err = _fs_require_file(file_path)
    if err:
        return _fs_fail(err)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not isinstance(old_string, str) or old_string == "":
        return _fs_fail("old_string 不能为空")
    if not isinstance(new_string, str):
        return _fs_fail("new_string 必须是字符串")
    real = os.path.realpath(_fs_abs(file_path))
    try:
        with open(real, "rb") as f:
            data = f.read(_FS_READ_MAX_BYTES + 1)
    except OSError as exc:
        return _fs_fail("读取失败: %s" % exc)
    text, err = _fs_decode(data, real)
    if err:
        return _fs_fail(err)
    count = text.count(old_string)
    if count == 0 and "\r\n" in text:
        # 文件是 CRLF、调用方给的是 LF：先改写成 CRLF 再匹配，写回仍保持 CRLF
        alt_old = old_string.replace("\r\n", "\n").replace("\n", "\r\n")
        if alt_old != old_string and text.count(alt_old):
            old_string = alt_old
            new_string = new_string.replace("\r\n", "\n").replace("\n", "\r\n")
            count = text.count(alt_old)
    if count == 0:
        return _fs_fail("未找到 old_string（文件 %d 字节；空白、缩进、换行必须完全一致）"
                        % len(data))
    if count > 1 and not replace_all:
        return _fs_fail("old_string 命中 %d 处，未开 replace_all 拒绝替换：请补充上下文使其唯一"
                        % count)
    idx = text.find(old_string)
    first_line = text.count("\n", 0, idx) + 1
    if replace_all:
        new_text = text.replace(old_string, new_string)
    else:
        new_text = text.replace(old_string, new_string, 1)
    out = new_text.encode("utf-8")
    try:
        with _FS_LOCK:
            with open(real, "wb") as f:
                f.write(out)
    except OSError as exc:
        return _fs_fail("写回失败: %s" % exc)
    return {"ok": True, "file_path": real,
            "replacements": count if replace_all else 1,
            "first_line": first_line, "bytes": len(out)}


def _fs_op_mkdir(path, parents=True):
    target = _fs_abs(path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if os.path.isdir(target):
        return {"ok": True, "path": target, "created": False}
    if os.path.exists(target):
        return _fs_fail("已存在同名非目录: %s" % target)
    try:
        with _FS_LOCK:
            if parents:
                os.makedirs(target, exist_ok=True)
            else:
                os.mkdir(target)
    except OSError as exc:
        return _fs_fail("创建目录失败: %s" % exc)
    return {"ok": True, "path": target, "created": True}


def _fs_op_move(src, dst, overwrite=False):
    s = _fs_abs(src)
    d = _fs_abs(dst)
    if not os.path.exists(s) and not os.path.islink(s):
        return _fs_fail("源不存在: %s" % s)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    try:
        with _FS_LOCK:
            if os.path.exists(d) or os.path.islink(d):
                if not overwrite:
                    return _fs_fail("目标已存在（overwrite=true 可覆盖）: %s" % d)
                if os.path.isdir(d) and not os.path.islink(d):
                    shutil.rmtree(d)
                else:
                    os.remove(d)
            shutil.move(s, d)
    except OSError as exc:
        return _fs_fail("移动失败: %s" % exc)
    return {"ok": True, "src": s, "dst": d}


def _fs_op_delete(path, recursive=False):
    target = _fs_abs(path)
    err = _fs_require_writable()
    if err:
        return _fs_fail(err)
    if not os.path.lexists(target):
        return _fs_fail("不存在: %s" % target)
    is_dir = os.path.isdir(target) and not os.path.islink(target)
    if is_dir and not recursive:
        return _fs_fail("目标是目录，需 recursive=true 才能删除: %s" % target)
    try:
        with _FS_LOCK:
            if is_dir:
                shutil.rmtree(target)
            else:
                os.remove(target)
    except OSError as exc:
        return _fs_fail("删除失败: %s" % exc)
    return {"ok": True, "path": target, "type": "dir" if is_dir else "file"}


# ---------------- 远端源码打包与调用 ----------------

# 远端执行的源码包含：固定 import 前导 + 上述常量/函数源码。注意不能包含
# 客户端专有的东西（_FS_SRC_CACHE、_FS_CALL_TIMEOUT、MCP 注册、日志出口）。
_FS_REMOTE_PREAMBLE = (
    "import fnmatch, os, re, shutil, threading, time\n"
    "import glob as _globmod\n"
    "_FS_LOCK = threading.RLock()\n"
)
_FS_REMOTE_NAMES = (
    "_FS_READ_ONLY",
    "_FS_READ_MAX_BYTES", "_FS_WRITE_MAX_BYTES",
    "_FS_READ_MAX_LINES", "_FS_LINE_MAX_CHARS",
    "_FS_GREP_MAX_BYTES", "_FS_GREP_MAX_FILES", "_FS_GREP_MAX_SECONDS",
    "_FS_GREP_SKIP_DIRS",
    "_fs_fail", "_fs_abs", "_fs_require_writable", "_fs_require_file",
    "_fs_decode", "_fs_clip_line", "_fs_count_lines", "_fs_walk_files",
    "_fs_op_read", "_fs_op_ls", "_fs_op_glob", "_fs_op_grep",
    "_fs_op_write", "_fs_op_edit", "_fs_op_mkdir", "_fs_op_move",
    "_fs_op_delete",
)


def _fs_remote_source():
    """把实现层源码打包成可远端 exec 的字符串，返回 (源码, 哈希)。"""
    cached = globals().get('_FS_SRC_CACHE')
    if cached:
        return cached
    with _FS_LOCK:
        cached = globals().get('_FS_SRC_CACHE')
        if cached:
            return cached
        chunks = [_FS_REMOTE_PREAMBLE]
        for name in _FS_REMOTE_NAMES:
            obj = globals().get(name)
            if obj is None:
                raise RuntimeError("远端文件工具源码缺少 %s" % name)
            if callable(obj):
                chunks.append(inspect.getsource(obj))
            else:
                chunks.append("%s = %r" % (name, obj))
        src = "\n\n".join(chunks)
        cached = (src, hashlib.sha256(src.encode("utf-8")).hexdigest())
        globals()['_FS_SRC_CACHE'] = cached
        return cached


def _fs_remote_code(op, params):
    """构造发往远端的 Python：按需 exec 源码，再调用指定实现并回传 JSON。"""
    src, src_hash = _fs_remote_source()
    b64_src = base64.b64encode(src.encode("utf-8")).decode("ascii")
    payload = json.dumps(params, ensure_ascii=False)
    b64_params = base64.b64encode(payload.encode("utf-8")).decode("ascii")
    return (
        "import base64 as _b64, json as _json\n"
        "if globals().get('_FS_SRC_HASH') != %r:\n"
        "    exec(_b64.b64decode(%r).decode('utf-8'), globals())\n"
        "    _FS_SRC_HASH = %r\n"
        "_fs_out = globals()[%r](**_json.loads(_b64.b64decode(%r).decode('utf-8')))\n"
        "r = _b64.b64encode(_json.dumps(_fs_out, ensure_ascii=False, default=str)"
        " .encode('utf-8')).decode('ascii')\n"
        % (src_hash, b64_src, src_hash, op, b64_params)
    )


def _fs_remote(op, params, write=False, timeout=None):
    """把一次文件操作发到远端执行，返回远端返回的 dict。

    走 ai_bridge.transport.request：与 client_mqtt.rpc 同一条 MQTT 一问一答
    通道，不占用 PTY 交互 shell 的 busy 锁，也不受 TTY 行规程影响。
    """
    if write:
        err = _fs_require_writable()
        if err:
            return _fs_fail(err)
    transport = getattr(ai_bridge, "transport", None)
    if transport is None:
        return _fs_fail("PTY 会话未建立：文件工具需要经 MQTT 在远端执行，请先连上远端")
    try:
        resp = transport.request(_fs_remote_code(op, params),
                                 timeout=timeout or _FS_CALL_TIMEOUT)
    except Exception as exc:
        return _fs_fail("远端请求异常: %r" % (exc,))
    if not resp:
        return _fs_fail("远端无响应（超时 %g 秒）" % (timeout or _FS_CALL_TIMEOUT))
    err = resp.get("error") or ""
    if err:
        return _fs_fail("远端执行失败: %s" % err)
    raw = resp.get("r")
    if not raw:
        return _fs_fail("远端返回为空")
    try:
        result = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception:
        return _fs_fail("远端返回值无法解析: %.200r" % (raw,))
    if write and isinstance(result, dict) and result.get("ok"):
        if op == "_fs_op_move":
            _emit_local("[fs] move %s -> %s\n" % (result.get("src"), result.get("dst")))
        else:
            _emit_local("[fs] %s %s\n" % (
                op[len("_fs_op_"):],
                result.get("file_path") or result.get("path") or ""))
    return result


# ---------------- MCP 工具层（参数校验 + 发远端） ----------------

def fs_read(file_path: str, offset: int = 0, limit: int = 2000) -> dict:
    """读取远端 UTF-8 文本文件的一段，返回带行号前缀的内容（只读）。

    :param file_path: 远端机器上的文件路径（绝对路径，或相对远端进程工作目录）
    :param offset: 起始行，0 基（默认 0，即第一行）
    :param limit: 最多返回行数（默认 2000，上限 5000）
    返回 {ok, file_path, content, start_line, total_lines, returned_lines,
    truncated, bytes}；二进制或非 UTF-8 文件拒绝按文本读取。
    """
    return _fs_remote("_fs_op_read", {"file_path": file_path,
                                      "offset": offset, "limit": limit})


def fs_ls(path: str = ".", show_hidden: bool = True) -> dict:
    """列出远端目录条目（只读）：名称/类型/大小/修改时间，目录排在前面。

    :param path: 远端目录路径（默认远端进程工作目录）
    :param show_hidden: 是否包含点开头条目（默认 true）
    """
    return _fs_remote("_fs_op_ls", {"path": path, "show_hidden": show_hidden})


def fs_glob(pattern: str, path: str = ".", max_results: int = 200) -> dict:
    """在远端按 glob 模式查找文件（只读，支持 ** 递归），按修改时间倒序返回。

    :param pattern: 模式，如 "**/*.py"；也可传绝对路径模式
    :param path: 远端基准目录（默认远端进程工作目录）
    :param max_results: 最多返回条数（默认 200，上限 1000）
    """
    return _fs_remote("_fs_op_glob", {"pattern": pattern, "path": path,
                                      "max_results": max_results})


def fs_grep(pattern: str, path: str = ".", glob: str = "",
            ignore_case: bool = False, output_mode: str = "content",
            context: int = 0, head_limit: int = 100, offset: int = 0) -> dict:
    """在远端按正则搜索文件内容（只读），自动跳过 .git/__pycache__ 与二进制。

    :param pattern: Python 正则表达式
    :param path: 远端文件或目录（默认远端进程工作目录）
    :param glob: 可选文件名过滤，如 "*.py"（对文件名与相对路径都匹配）
    :param ignore_case: 忽略大小写
    :param output_mode: content=逐行结果；files_with_matches=仅文件路径；count=每文件命中数
    :param context: content 模式每处命中前后附加的行数（默认 0，上限 20）
    :param head_limit: 最多返回结果条数（默认 100，上限 500）
    :param offset: 跳过前面多少条结果
    """
    return _fs_remote("_fs_op_grep", {
        "pattern": pattern, "path": path, "glob": glob,
        "ignore_case": ignore_case, "output_mode": output_mode,
        "context": context, "head_limit": head_limit, "offset": offset,
    })


def fs_write(file_path: str, content: str) -> dict:
    """整文件覆盖写入远端 UTF-8 文本（父目录必须已存在，不自动建目录）。

    :param file_path: 远端目标文件路径
    :param content: 完整文件内容
    """
    return _fs_remote("_fs_op_write", {"file_path": file_path,
                                       "content": content}, write=True)


def fs_edit(file_path: str, old_string: str, new_string: str,
            replace_all: bool = False) -> dict:
    """在远端精确替换字符串（old_string 必须唯一，除非 replace_all=true）。

    :param file_path: 远端目标文件路径
    :param old_string: 要替换的原文（不能为空；必须与文件内容逐字符一致）
    :param new_string: 替换后的文本
    :param replace_all: 是否替换全部命中（默认 false，多命中会报错）
    CRLF 文件允许传 LF 版本的 old_string，写回时自动保持 CRLF。
    """
    return _fs_remote("_fs_op_edit", {
        "file_path": file_path, "old_string": old_string,
        "new_string": new_string, "replace_all": replace_all,
    }, write=True)


def fs_mkdir(path: str, parents: bool = True) -> dict:
    """在远端创建目录（默认递归创建父目录；已存在视为成功）。

    :param path: 远端目录路径
    :param parents: true=递归建父目录，false=父目录必须已存在
    """
    return _fs_remote("_fs_op_mkdir", {"path": path, "parents": parents},
                      write=True)


def fs_move(src: str, dst: str, overwrite: bool = False) -> dict:
    """在远端移动/重命名文件或目录。

    :param src: 远端源路径
    :param dst: 远端目标路径
    :param overwrite: 目标已存在时是否覆盖（默认 false）
    """
    return _fs_remote("_fs_op_move", {"src": src, "dst": dst,
                                      "overwrite": overwrite}, write=True)


def fs_delete(path: str, recursive: bool = False) -> dict:
    """删除远端文件或目录（目录需 recursive=true）。

    :param path: 远端目标路径
    :param recursive: 是否递归删除目录
    """
    return _fs_remote("_fs_op_delete", {"path": path, "recursive": recursive},
                      write=True)


# ---------------- MCP 注册 ----------------

_FS_TOOLS = (
    fs_read, fs_write, fs_edit, fs_glob, fs_grep, fs_ls,
    fs_mkdir, fs_move, fs_delete,
)


def _mcp_register_fs_tools(server) -> None:
    """把 9 个文件工具注册到 MCP server（与 pty_* 工具同一个 server 实例）。

    统一包一层 _mcp_tool：请求到达时先写本地日志（原始参数），不等执行完。
    """
    for fn in _FS_TOOLS:
        try:
            server.tool()(_mcp_tool(fn))
        except Exception as exc:  # 老版本 SDK 没有 tool() 工厂时退回 add_tool
            try:
                server.add_tool(_mcp_tool(fn))
            except Exception:
                _emit_local("[pty][MCP] 文件工具注册失败 %s: %r\n"
                            % (getattr(fn, "__name__", "?"), exc))
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
        lines = ["%s <- %s 秒（已通知服务端热调）"
                 % (canon, applied[remote_key])]
        if canon == "heartbeat":
            # 与会话建立时（run_session 开局）同一规则联动 dead_timeout：
            # hb<=0 → 停止判死（静默 shell 与服务器已死无法区分，不误杀）；
            # hb>0 且判死已开启 → 下限 3×心跳。旧实现不联动：热调
            # heartbeat 0 或调大后，静默 shell 超过陈旧的 dead_timeout 即
            # 被误判"服务器已死"强制退出，退出路径还顺手发 stop 杀远端。
            _hb = live["heartbeat"]
            if _hb <= 0:
                live["dead_timeout"] = 0.0
                lines.append("心跳已关闭：dead_timeout 自动置 0（不再判死）")
            elif live["dead_timeout"] > 0:
                live["dead_timeout"] = max(live["dead_timeout"], _hb * 3)
                lines.append("dead_timeout 联动 <- %g 秒（不小于 3×心跳）"
                             % live["dead_timeout"])
        return False, lines
    return False, ["未知本地命令: %s（输入 help 查看）" % cmd]


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


def run_session(transport: MqttTransport, args, rows: int, cols: int) -> int:
    global _OUT_WRITER
    pty = RemotePty(transport, timeout=args.timeout)
    # 挂到 AI 桥：外部经本地 HTTP RPC 调用 ai_bridge 即复用本 PTY，
    # attach 放在 open 之前，保证最早的握手回显也能被 feed 分流到。
    ai_bridge.attach(pty, transport)
    # 注入 AI 投递配置：--ai-pane 为空时保持 auto——这里**不能**立刻解析默认
    # 窗口，因为 attach 发生在 pty.open 之前，远端 tmux 还没被前置命令
    # （如 "tmux at"）启动；首次 run() 时才解析并按需固化。
    # --ai-encoding 默认 raw：命令原文直接在窗口里可见。
    ai_bridge.configure(getattr(args, "ai_pane", ""),
                        getattr(args, "ai_encoding", "raw"))
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

    # 会话启动时把远端登录 shell 记入 AI 桥：多行命令能否用可读的 $'...'
    # 编码取决于它，探测不到（dash/未知）就由 _pick_encoding 退回 base64。
    ai_bridge.set_shell(env.get("shell") if env else None)

    banner = (
        f"[{stime()}] connected shell={env['shell']} pid={env['pid']} "
        f"({env['rows']}x{env['cols']}, interval={env['flush_interval']})\t"
        f"{env['in_topic']}\t{env['out_topic']}\n"
        f"[pty] {_describe_key(detach_key)} 本地脱离（远端 shell/tmux 继续运行）；"
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
    # 通道用 _ConsoleSink（Win32 WriteConsoleW + 自有锁），彻底绕开
    # sys.stdout.buffer 那个共享 BufferedWriter——它的 flush 在遇到半截
    # UTF-8 时会零进度死循环并占死锁（2026-10-25 实证）。
    if _bin_stdout() is not None:
        _OUT_WRITER = _TerminalWriter(_new_console_sink("pty-out"),
                                      name="pty-out")

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
    last_unstick_ts = [0.0]  # 上次尝试自动解除的时间（每 30s 重试直到成功）
    last_writer_reset_ts = [0.0]  # 上次自动切换写线程的时间（每 60s 一次）
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
                    _snap = _writer_sink_snapshot()
                    _emit_local(
                        "[pty][WARN] 终端写卡滞 %.0fs（通道=%s 零进度=%s次 "
                        "carry=%sB 持锁线程=%s）：窗口选择/标记模式请先点"
                        "本窗口再按 ESC；否则 60s 后自动换新输出通道\n"
                        % (_stall, _snap.get("kind"),
                           _snap.get("zero_progress"),
                           _snap.get("carried_bytes"),
                           _snap.get("holder")))
                # 长时间冻结（通常是选了文本忘记退出标记模式）：前台时
                # 合成 ESC 自动解除，鼠标左键选择/右键粘贴照常保留。
                # 每 30s 重试直到成功：解除仅在窗口前台时生效，用户切到
                # 别的窗口时一次尝试必然失败，旧的一次性设计会让冻结
                # 一直挂到手工干预（实证：卡滞 7 分钟未自愈）。
                if _stall >= _SELECTION_STALL_AUTO_UNSTICK \
                        and (time.monotonic() - last_unstick_ts[0]) > 30:
                    last_unstick_ts[0] = time.monotonic()
                    if _cancel_console_selection():
                        _emit_local(
                            "[pty] 窗口选择模式冻结输出超过 %.0f 秒，"
                            "已自动解除\n" % _SELECTION_STALL_AUTO_UNSTICK)
                # 写线程活锁兜底（2026-10-25 实证）：源头是半截 UTF-8 让
                # BufferedWriter.flush 零进度死循环并占死共享锁。现在新通道
                # 换的是新锁 + 新 CONOUT$ 句柄，旧实例卡死也拖不住新写线程。
                if _stall >= 60.0 \
                        and (time.monotonic() - last_writer_reset_ts[0]) > 60:
                    last_writer_reset_ts[0] = time.monotonic()
                    # 换新通道（新锁 + 新 CONOUT$ 句柄）才真正有效；次数
                    # 封顶防止"换了也没用"的极端场景无限堆线程。
                    if _WRITER_RESETS >= _AUTO_RESET_MAX:
                        _emit_local("[pty] 写卡滞 %.0fs，自愈已尝试 %d 次，"
                                    "停止自动换通道（需人工介入）\n"
                                    % (_stall, _WRITER_RESETS))
                    else:
                        _r = reset_out_writer()
                        _emit_local("[pty] 写卡滞 %.0fs，自动换输出通道: "
                                    "ok=%s resets=%s零进度=%s\n"
                                    % (_stall, _r.get("ok"),
                                       _r.get("resets"),
                                       (_r.get("old") or {}).get("sink")))
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

    # input_loop 结束：Ctrl-] 本地脱离 / 本地 stdin 关闭。
    # leave=True：只清本地订阅不发 stop，远端 shell/tmux 继续运行
    # （与 banner/帮助文案的承诺一致；裸 shell 前台作业不再被误杀）。
    _hard_exit(console, 0, "\r\n[pty] session ended: detached\r\n", pty,
               leave=True)
    return 0  # 不可达：_hard_exit 不返回；仅为类型/静态检查保留


# ============================ CLI ============================

def _pre_command_index(parser, argv):
    """SSH 式前置命令的起始下标：跳过选项及其值后的第一个非选项 token。

    返回 None = 整行都是选项（没有前置命令）；返回下标 i 表示 argv[i] 起
    都该按位置参数处理。``--`` 是显式的命令分隔符，返回它后面的下标。
    选项值识别规则：``--opt=value`` 自带值；``store_true`` 等 nargs=0 的
    动作不带走下一个 token；其余动作（nargs=None/int>0）认为值在下一个
    token。认不出的选项（未知名或歧义缩写）按“不取值”处理，把下一个非
    选项 token 当命令起点，让 argparse 自己报选项错误——扫描阶段绝不替
    argparse 猜测，更不吞掉用户真实的命令。
    """
    i, n = 0, len(argv)
    while i < n:
        tok = argv[i]
        if tok == "--":
            return i + 1
        if not tok or tok == "-" or not tok.startswith("-"):
            return i                       # 第一个位置参数 = 命令起点
        if tok[1:2].isdigit():
            return i                       # 负数字面量，argparse 同样当位置参数
        i += 1
        takes_value = _option_takes_value(parser, tok)
        if takes_value:
            i += 1                         # 跳过该选项的值（可能不存在，越界即到头）
    return None


def _option_takes_value(parser, token) -> bool:
    """该选项是否把下一个 token 当自己的值（只读 argparse 已注册的动作表）。"""
    action = parser._option_string_actions.get(token)
    explicit_arg = None
    if action is None:  # 缩写/歧义：用 argparse 自己的前缀匹配结果判定
        try:
            tuples = parser._get_option_tuples(token)
        except Exception:
            return False
        if len(tuples) != 1:
            return False
        action, _opt, explicit_arg = tuples[0]
    if "=" in token or explicit_arg is not None:
        return False                       # --opt=value 已内联，不再吃下一个
    nargs = action.nargs
    if nargs in (None, "?", "+"):
        return True
    return isinstance(nargs, int) and nargs > 0


class _PreCommandParser(argparse.ArgumentParser):
    """自动为 SSH 式前置命令插入 ``--`` 的 ArgumentParser。

    本客户端的选项别名里有单横线长名（-dir/-dead_timeout/-detach_key），
    argparse 会把命令自身的 ``-d``（如 ``tmux attach -d``）当成这些长名的
    缩写并报 ``ambiguous option: -d``（allow_abbrev=False 只关双横线长名的
    缩写，管不住单横线长名）。在命令起点前插入 ``--`` 后，选项解析到此为止，
    其后每个 token（含 -d/-t 和它们的值）都按位置参数交给 nargs=REMAINDER，
    与 ssh 的“命令放最后”语义一致；用户在命令前主动写的 ``--`` 不会被重复插入。
    """

    def parse_known_args(self, args=None, namespace=None):
        argv = list(sys.argv[1:] if args is None else args)
        idx = _pre_command_index(self, argv)
        if idx is not None and (idx == 0 or argv[idx - 1] != "--"):
            argv.insert(idx, "--")
        return super().parse_known_args(argv, namespace)


def build_parser() -> argparse.ArgumentParser:
    # 选项名一律由文件前面的 alias_xxx 别名表 + client_mqtt._cli_opts 生成，
    # 与本地魔术栏命令、rpc(**ka) 别名三处永远一致；禁止再硬编码 "--xxx"。
    # dest 保持历史字段名（args.interval/args.port 等），调用方无需改动。
    p = _PreCommandParser(
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
    p.add_argument(*_cm._cli_opts(*alias_mcp_port), dest="mcp_port",
                   type=int, default=0,
                   help="MCP 工具服务：0=自动（默认，挂在 --port 的 RPC 口 /mcp 路径，"
                        "不新开端口）；>0=另起独立端口（--port 0 关闭 RPC 口时仍要 "
                        "MCP 用）；需 pip install \"mcp[cli]\"，未装包仅记日志")
    p.add_argument(*_cm._cli_opts(*alias_no_mcp), dest="no_mcp",
                   action="store_true",
                   help="完全关闭 MCP 工具服务（默认开启并复用 --port 的口）")
    p.add_argument(*_cm._cli_opts(*alias_mcp_transport), dest="mcp_transport",
                   default="",
                   help="MCP 传输类型：留空自动（优先 streamable-http，旧版 mcp 包 "
                        "退回 sse），可显式 streamable-http / sse")
    p.add_argument(*_cm._cli_opts(*alias_fs_read_only), dest="fs_read_only",
                   action="store_true",
                   help="MCP 文件工具全只读：禁用 fs_write/fs_edit/fs_mkdir/"
                        "fs_move/fs_delete（默认关闭，允许写任意路径）")
    p.add_argument(*_cm._cli_opts(*alias_ai_pane), dest="ai_pane",
                   default="",
                   help="AI 命令默认投递到哪个 tmux 窗口：留空 = 自动按「session "
                        "内 window_index 最小的窗口」解析并固化；也可传 pane_id "
                        "（百分号前缀，如 2）或 0:2 / 窗口名（RPC 的 pty_pane 可运行时改）")
    p.add_argument(*_cm._cli_opts(*alias_ai_encoding), dest="ai_encoding",
                   default="raw",
                   help="AI 命令编码：raw = 自动（默认，无控制字符原文直发；有裸"
                        "换行且 shell 为 bash/zsh 时用 $'...' 保持可读；否则 "
                        "base64）；dollar = 强制 ANSI-C 引用；base64 = 强制严格"
                        "编码")
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
    # 启动自检：本文件是否仍是 pty/parts 分片重组出来的最新版（不同步只告警）
    check_parts_sync()
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

    # MCP 文件工具权限：默认读写任意路径，--fs-read-only 才全禁写。
    # 必须在挂载 MCP 前配置好。
    _fs_configure(getattr(args, "fs_read_only", False))

    # MCP 工具服务：默认复用 --port 的 HTTP 口（路径 /mcp，与 RPC/日志台
    # 同口并存，不新开端口）；--mcp-port>0 才另起独立端口（--port 0 时仍要
    # MCP 的场景）；--no-mcp 全关。没装 mcp 包只记日志，PTY 照常工作。
    if not getattr(args, "no_mcp", False):
        _prefer = getattr(args, "mcp_transport", "") or ""
        if int(getattr(args, "mcp_port", 0) or 0) > 0:
            start_mcp_server(port=int(args.mcp_port), host=args.host,
                             prefer=_prefer)
        elif args.port:
            _mcp = mount_mcp_on_rpc(prefer=_prefer, host=args.host)
            if _mcp.get("ok") and not _mcp.get("already"):
                _info(f"MCP 工具服务已挂到 RPC 口：http://{_lan_ip()}:{args.port}/mcp  "
                      f"AI 客户端：claude mcp add --transport http pty "
                      f"http://{_lan_ip()}:{args.port}/mcp")

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
