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
    python pty_client_mqtt.py                    # 交互式 PTY（默认参数）
    python pty_client_mqtt.py -i 0.2             # 服务端最多 0.2s 攒批
    python pty_client_mqtt.py --shell /bin/bash --cwd /root

断开
====
- 在远端 shell 里 ``exit`` / Ctrl-D（会话自然结束，客户端立即退出）；
- 本地强制脱离：Ctrl-]（默认，可用 ``--detach-key`` 修改）；
- 服务器进程被关 / 网络中断：心跳超时（``--heartbeat`` / ``--dead-timeout``）
  后客户端自动立即退出，绝不在 broker 仍在线时无限干等。

所有退出路径都学 client_mqtt.py 直接 ``os._exit``：先恢复本地终端模式、
fire-and-forget 发一帧 stop，不做 pty.close/transport.stop 那套慢清理。
"""
from __future__ import annotations

import argparse
import os
import queue
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cmd_client_mqtt import (           # noqa: E402
    MqttTransport, DEFAULT_REQUEST_TOPIC, DEFAULT_REPLY_TOPIC, DEFAULT_KEY,
)
from remote_cmd import (                # noqa: E402
    RemotePty, RemoteError, DEFAULT_PTY_TTL,
)


# ============================ 本地终端：输出 ANSI 支持 ============================

def _enable_output_vt() -> bool:
    """让本地终端能渲染远端 ANSI 输出。POSIX 原生支持；Windows 打开 VT 处理 + UTF-8。"""
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

    def enter(self):
        import ctypes
        mode = ctypes.c_uint32()
        if self.k.GetConsoleMode(self.h, ctypes.byref(mode)):
            self.old_mode = mode.value
            if self.k.SetConsoleMode(self.h, self.ENABLE_VIRTUAL_TERMINAL_INPUT):
                self.use_vt = True
        self.k.SetConsoleOutputCP(65001)

    def read(self) -> bytes:
        if self.use_vt:
            return os.read(0, 4096)
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


# ============================ PTY 会话 ============================

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
    if message:
        try:
            sys.stderr.write(message)
            sys.stderr.flush()
        except Exception:
            pass
    try:
        sys.stdout.buffer.flush()
    except Exception:
        pass
    os._exit(code)


def run_session(transport: MqttTransport, args, rows: int, cols: int) -> int:
    pty = RemotePty(transport, timeout=args.timeout)
    outq: "queue.Queue[bytes]" = queue.Queue()
    stop_ev = threading.Event()
    term = args.term or os.environ.get("TERM") or "xterm-256color"

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

    def on_heartbeat(_ts):
        signal_state["last"] = time.monotonic()

    # 协商：订阅 out topic + 下发 PTY 启动代码（shell 在服务端只启动这一次）
    env = pty.open(rows, cols, shell=(args.shell or None), term=term,
                   cwd=args.cwd, login=not args.no_login,
                   flush_interval=args.interval, ttl=args.ttl,
                   heartbeat=heartbeat, on_heartbeat=on_heartbeat,
                   on_data=on_data)

    banner = (
        f"[pty] connected shell={env['shell']} pid={env['pid']} "
        f"({env['rows']}x{env['cols']}, interval={env['flush_interval']})\n"
        f"[pty] in : {env['in_topic']}\n[pty] out: {env['out_topic']}\n"
        f"[pty] Ctrl-] 本地脱离；远端 exit/Ctrl-D 结束会话\n")
    if dead_timeout > 0:
        banner += (f"[pty] 心跳 {heartbeat:g}s：服务器关闭/断连后最多 "
                   f"{dead_timeout:g}s 自动退出\n")
    sys.stderr.write(banner)
    sys.stderr.flush()

    console = _make_raw_console()
    detach_key = args.detach_key.encode("latin-1", "ignore") or None

    def input_loop():
        try:
            console.enter()
            while not stop_ev.is_set():
                data = console.read()
                if not data:
                    break
                if detach_key and data == detach_key:
                    sys.stderr.write("\r\n[pty] 本地脱离\r\n")
                    break
                pty.send(data)
        except OSError:
            pass
        finally:
            stop_ev.set()

    def resize_watch():
        last = (rows, cols)
        while not stop_ev.wait(0.5):
            cur_sz = shutil.get_terminal_size((80, 24))
            cur = (cur_sz.lines, cur_sz.columns)
            if cur != last:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                last = cur

    t_in = threading.Thread(target=input_loop, name="pty-input", daemon=True)
    t_resize = threading.Thread(target=resize_watch, name="pty-resize",
                                daemon=True)
    t_in.start()
    t_resize.start()

    # 主线程：远端输出原样渲染；任何退出路径都走 _hard_exit 立即收场
    try:
        while not stop_ev.is_set():
            try:
                chunk = outq.get(timeout=0.3)
            except queue.Empty:
                if pty.end_reason is not None:
                    _hard_exit(
                        console, 0,
                        f"\r\n[pty] session ended: {pty.end_reason}\r\n", pty)
                if dead_timeout > 0 and (time.monotonic()
                                         - signal_state["last"]) > dead_timeout:
                    _hard_exit(
                        console, 3,
                        f"\r\n[pty] {dead_timeout:g}s 收不到任何远端帧"
                        f"（输出/心跳），服务器可能已关闭，直接断开\r\n", pty)
                continue
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
    except KeyboardInterrupt:
        _hard_exit(console, 130, "\r\n[pty] interrupted\r\n", pty)

    # input_loop 结束：Ctrl-] 本地脱离 / 本地 stdin 关闭
    _hard_exit(console, 0, "\r\n[pty] session ended: detached\r\n", pty)
    return 0  # 不可达：_hard_exit 不返回；仅为类型/静态检查保留


# ============================ CLI ============================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pty_client_mqtt",
        description="PTY over MQTT：SSH 式远程交互终端（常驻 shell，逐键过 broker）")
    # 连接参数（与 cmd_client_mqtt 一致）
    p.add_argument("--request-topic", "--topic", "-t",
                   default=DEFAULT_REQUEST_TOPIC)
    p.add_argument("--reply-topic", "--reply", default=DEFAULT_REPLY_TOPIC)
    p.add_argument("--key", "-k", default=DEFAULT_KEY,
                   help="私钥：整数表达式/PEM/文件路径；空串不签名（默认）")
    p.add_argument("--allow", "-a", dest="allow", action="store_true",
                   default=True)
    p.add_argument("--no-allow", dest="allow", action="store_false")
    p.add_argument("--timeout", type=float, default=30.0,
                   help="握手/问答等待秒数（默认 30）")
    # PTY 参数
    p.add_argument("--shell", default="",
                   help="远端 shell，默认服务端用户登录 shell（$SHELL/passwd）")
    p.add_argument("--term", default="",
                   help="TERM，默认本地 $TERM 或 xterm-256color")
    p.add_argument("--cwd", default=None, help="启动目录，默认远端 HOME")
    p.add_argument("--interval", "-i", type=float, default=0.0,
                   help="服务端主动推送最小间隔秒：0=实时（默认），>0 攒批")
    p.add_argument("--no-login", action="store_true",
                   help="不使用 login shell（默认 argv0 带 - 前缀）")
    p.add_argument("--ttl", type=float, default=DEFAULT_PTY_TTL,
                   help="孤儿会话最长存活秒（默认 12h，上限 24h）")
    p.add_argument("--heartbeat", type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument("--dead-timeout", type=float, default=15.0,
                   help="多久收不到任何远端帧（输出/心跳）即判定服务器已死"
                        "并直接退出（默认 15s，实际不小于 3 倍心跳；"
                        "0=不检测，心跳关闭时自动失效）")
    p.add_argument("--size", default=None,
                   help="强制窗口 ROWSxCOLS，如 24x100；默认取本地终端大小")
    p.add_argument("--detach-key", default="\x1d",
                   help="本地强制脱离键（默认 Ctrl-]）")
    return p


def main(argv=None) -> int:
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
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)
    try:
        return run_session(transport, args, rows, cols)
    except RemoteError as exc:
        sys.stderr.write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        return 2
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
