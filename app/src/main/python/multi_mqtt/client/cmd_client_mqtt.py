#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""cmd_client_mqtt —— remote_cmd 的 **MQTT 网络层绑定** + 终端入口。

分层
====
- :mod:`remote_cmd` —— 与网络层无关的操作封装（把命令包装成 Python 代码、
  解析回包、远端 grep/编辑、分块、1MiB 传输红线），换网络层不用动它；
- 本文件 —— 只是众多可能的 ``Transport`` 之一：复用 client_mqtt.MQTTClientNode
  实现「一发一收」，另含 CLI / 交互 REPL。

以后换成 HTTP / TCP / WebSocket 时：新写一个文件实现
``remote_cmd.Transport.request(code, timeout) -> dict`` 即可，所有上层 API
（RemoteShell 的 run/grep/edit_replace/upload…）原样可用。

默认连接
========
request_topic ``sys/device/request`` / reply_topic ``sys/device/response``
私钥 （也接受 ``k=py int expr`` 或 PEM/文件路径），allow_no_pub=True
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import posixpath
import sys
import threading
import time

# 本文件已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/cmd_client_mqtt.py」直接运行与包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# 直接按脚本启动时没有包上下文，相对导入会失败，显式补上。
if not __package__:
    __package__ = "client"

logger = logging.getLogger("cmd_client_mqtt")

from multi_mqtt import get_standard_pem_bytes  # noqa: E402
from . import client_mqtt as _cm               # noqa: E402  复用 REPL 基建
from .remote_cmd import (                      # noqa: E402
    Transport, RemoteShell as _RemoteShell, CmdResult,
    RemoteError, RemoteTimeout, RemoteRpcError, RemoteOpError, TransferTooLarge,
    DirArchiveTooLarge,
    DEFAULT_TIMEOUT, MAX_TRANSFER, WIRE_BUDGET,
    MAX_DIR_ARCHIVE, DEFAULT_DIR_ARCHIVE_MAX,
)

DEFAULT_REQUEST_TOPIC = "sys/device/request"
DEFAULT_REPLY_TOPIC = "sys/device/response"
DEFAULT_KEY = ""

# 向后兼容：从本模块 import 这些名字仍然可用
__all__ = [
    "MqttTransport", "MqttRemoteShell", "RemoteShell", "CmdResult",
    "RemoteError", "RemoteTimeout", "RemoteRpcError", "RemoteOpError",
    "TransferTooLarge", "connect", "get_shell", "run", "stop",
]


# ============================ MQTT 一问一答通道 ============================

class MqttTransport(Transport):
    """把 MQTTClientNode 适配成 remote_cmd.Transport。

    topic / key / allow_no_pub 都是可变属性，REPL 里的 %topic %key 等
    magic 直接改这里即可即时生效。
    """

    def __init__(self, node=None, request_topic=DEFAULT_REQUEST_TOPIC,
                 reply_topic=DEFAULT_REPLY_TOPIC, private_key=DEFAULT_KEY,
                 allow_no_pub=True):
        self.request_topic = request_topic
        self.reply_topic = reply_topic
        self.allow_no_pub = allow_no_pub
        self.key = self._normalize_key(private_key)
        # topic -> [handler]：服务端主动推送帧的本地监听器
        self._stream_handlers = {}
        self._owns_node = node is None
        if node is None:
            node = _cm.MQTTClientNode(
                client_private_key_bytes=self.key,
                allow_no_server_pubkey_response=allow_no_pub,
            )
            node.start()
        self.node = node

    @staticmethod
    def _normalize_key(key):
        if key is None:
            return None
        if isinstance(key, (bytes, bytearray)):
            return bytes(key)
        text = str(key).strip()
        if not text:
            return None
        if text.startswith("k="):
            text = text[2:].strip()
        return get_standard_pem_bytes(text)

    def request(self, code: str, timeout: float = DEFAULT_TIMEOUT) -> dict | None:
        return self.node.request(
            code,
            request_topic=self.request_topic,
            reply_topic=self.reply_topic,
            timeout=timeout,
            client_private_key_bytes=self.key,
            allow_no_server_pubkey_response=self.allow_no_pub,
        )

    # 首包未回前向新连上的 broker 补发同一握手请求的时间点（秒）。
    # 启动瞬间通常只有 1 个 broker 在线，单路径被公共 broker 限流时首包要
    # 等数秒；补发让握手随 broker 陆续上线不断扩大多路径竞速。同一 req_id
    # 由服务端 dedup_cache 去重、同 sid 由会话注册表幂等，安全。
    HANDSHAKE_REPUBLISH_DELAYS = (0.4, 0.9, 1.8)
    # 握手前等待多路径就绪：实测冷启动 0.6s 时已有 6~9 个 broker 在线。
    # QoS0 首发只投递给当前在线 broker、且应答也是一次性 QoS0，单 broker
    # 起步时握手 RTT 完全赌那一条路径的当下延迟；等到 6 路再首发，请求/
    # 应答双向竞速，首包稳定在亚秒级。已在线 broker 足够时立即返回。
    HANDSHAKE_MIN_BROKERS = 6
    HANDSHAKE_READY_TIMEOUT = 0.7

    def _handshake_ready(self):
        net = getattr(self.node, "mqtt_net", None)
        wait = getattr(net, "wait_connected", None) if net is not None else None
        if callable(wait):
            try:
                wait(min_count=self.HANDSHAKE_MIN_BROKERS,
                     timeout=self.HANDSHAKE_READY_TIMEOUT)
            except Exception:
                logger.exception("握手多路径就绪等待异常，按现状继续")

    def _node_request_kwargs(self, timeout):
        return dict(
            request_topic=self.request_topic,
            reply_topic=self.reply_topic,
            timeout=timeout,
            client_private_key_bytes=self.key,
            allow_no_server_pubkey_response=self.allow_no_pub,
        )

    def request_many(self, code: str, timeout: float = DEFAULT_TIMEOUT,
                     gather: float = 0.8, on_extras=None):
        """PTY/SOCKS5 握手专用可选能力，返回 ``(首包, 其余回包列表)``。

        一个握手请求会被每台在线设备各执行一次；第二个回包证明存在影子
        服务端（旧机器/旧容器/同机双进程），RemotePty 据此做归属仲裁。

        - 首包未回前按 HANDSHAKE_REPUBLISH_DELAYS 补发，覆盖迟到 broker；
        - ``on_extras`` 为 None（默认）：阻塞留 ``gather`` 秒收集迟到回包
          后返回（旧行为）；
        - ``on_extras`` 为回调：**首包一到立即返回**（extras 返回空），
          ``gather`` 秒收集窗在后台进行，结束后回调
          ``on_extras(extras: list)``，让首屏不再被收集窗阻塞。
        """
        self._handshake_ready()
        ka = self._node_request_kwargs(timeout)
        if on_extras is not None:
            resp = self.node.request_bg_gather(
                code, gather_window=gather,
                republish_delays=self.HANDSHAKE_REPUBLISH_DELAYS,
                on_gather=on_extras, **ka)
            return (resp or None), []
        resp = self.node.request(
            code,
            gather_window=gather,
            republish_delays=self.HANDSHAKE_REPUBLISH_DELAYS,
            **ka,
        )
        if not resp:
            return None, []
        extras = resp.pop("_extra_responses", None) or []
        return resp, extras

    # ---- 可选能力：向任意 topic 发一帧（RemotePty 的按键/控制帧用） ----

    def publish(self, topic: str, payload_dict: dict):
        """不走一问一答、不签名（帧内无 code），直接 publish 到指定 topic。"""
        self.node.mqtt_net.publish_broadcast(topic, payload_dict)

    # ---- 可选能力：服务端推送（RemoteShell.stream 用） ----

    def stream_subscribe(self, topic, handler):
        """订阅汇报 topic 并挂帧处理器。

        MQTTClientNode 构造时已把自己的 _on_message 注册成网络层唯一回调；
        这里在不影响正常一问一答的前提下，把回调链成「原 RPC 回调 + 流分发」。
        流帧没有 req_id，RPC 回调内部本来就会忽略，顺序无所谓。
        """
        net = self.node.mqtt_net
        if not getattr(net, "_cmq_stream_installed", False):
            orig = net.message_callback

            def chained(t, d, b):
                orig(t, d, b)
                self._dispatch_stream(t, d, b)

            net.set_on_message(chained)
            net._cmq_stream_installed = True
        self.node._subscribe_once(topic)
        # 需要 per-broker 到达统计的处理器可声明 handler(data, broker)；
        # 订阅时用签名嗅探记录参数个数，分发时按声明调用（兼容单参处理器）。
        try:
            import inspect as _inspect
            _n = len(_inspect.signature(handler).parameters)
        except (TypeError, ValueError):
            _n = 1
        self._stream_handlers.setdefault(topic, []).append((handler, _n >= 2))

    def stream_unsubscribe(self, topic, handler):
        lst = self._stream_handlers.get(topic)
        if lst:
            for _i, (_h, _) in enumerate(lst):
                if _h is handler:
                    lst.pop(_i)
                    break

    def _dispatch_stream(self, topic, data, broker):
        # "stream" 帧：周期汇报；"pty" 帧：PTY 下行输出；"s5" 帧：SOCKS5
        # 下行输出。三类共用分发链，按 topic 分发到各自会话的处理器。
        if not isinstance(data, dict) or \
                ("stream" not in data and "pty" not in data
                 and "s5" not in data):
            return
        for h, with_broker in list(self._stream_handlers.get(topic, [])):
            try:
                h(data, broker) if with_broker else h(data)
            except Exception:
                logger.exception("stream handler error topic=%s", topic)

    def close(self):
        if self._owns_node:
            self.node.stop()


class MqttRemoteShell(_RemoteShell):
    """:class:`remote_cmd.RemoteShell` 面向 MQTT 默认设备的便捷构造。

    也可直接 ``MqttRemoteShell()`` 无参使用；参数透传给 MqttTransport。
    """

    def __init__(self, request_topic=DEFAULT_REQUEST_TOPIC,
                 reply_topic=DEFAULT_REPLY_TOPIC, private_key=DEFAULT_KEY,
                 allow_no_pub=True, timeout=DEFAULT_TIMEOUT,
                 max_transfer=MAX_TRANSFER, wire_budget=WIRE_BUDGET, transport=None):
        if transport is None:
            transport = MqttTransport(
                request_topic=request_topic, reply_topic=reply_topic,
                private_key=private_key, allow_no_pub=allow_no_pub)
        super().__init__(transport, timeout=timeout,
                         max_transfer=max_transfer, wire_budget=wire_budget)


# 习惯用法：cmd_client_mqtt.RemoteShell(...) 即默认 MQTT 设备
RemoteShell = MqttRemoteShell


# ============================ 模块级单例 ============================

_default_shell = None
_default_shell_lock = threading.Lock()


def connect(private_key=DEFAULT_KEY, request_topic=DEFAULT_REQUEST_TOPIC,
            reply_topic=DEFAULT_REPLY_TOPIC, allow_no_pub=True,
            timeout=DEFAULT_TIMEOUT, **ka) -> MqttRemoteShell:
    """显式建立一个到默认设备的 RemoteShell（用完 .close()）。"""
    return MqttRemoteShell(
        private_key=private_key, request_topic=request_topic,
        reply_topic=reply_topic, allow_no_pub=allow_no_pub, timeout=timeout, **ka)


def get_shell(**ka) -> MqttRemoteShell:
    """获取（首次按参数创建）进程级共享 shell。"""
    global _default_shell
    with _default_shell_lock:
        if _default_shell is None:
            _default_shell = connect(**ka)
        return _default_shell


def run(cmd, **ka) -> CmdResult:
    """一行调用：``run('uname -a').text``。"""
    return get_shell().run(cmd, **ka)


def stop():
    global _default_shell
    with _default_shell_lock:
        shell = _default_shell
        _default_shell = None
    if shell is not None:
        shell.close()


# ============================ 交互式 REPL ============================

_REPL_HELP = """\
直接输入 shell 命令并回车即在远端执行（sh -c），和 SSH 一样；也可直接让远端装东西
（apt-get install / pip install / curl / tar 都在远端跑，文件不经过本机）。

会话内建：
  cd [dir] / pwd     切换/显示会话目录（跨命令保持）
  %info               远端环境信息
  %grep [opts] PAT [PATH...]  远端 grep（不回传文件）；opts: -i -F -r --include G
  %edit PATH          本机编辑器打开远端文件改完原子回传（仅适合 <1MiB 小文件）
  %replace PATH OLD == NEW [%] 远端就地精确替换（内容不离开远端；%=全部）
  %append PATH        stdin 文本追加到远端文件
  %cat <rpath>        查看远端小文本文件（<1MiB）
  %get <r> [local]    下载远端小文件（默认当前目录，>1MiB 默认拒绝）
  %put <l> [remote]   上传本地小文件（默认会话目录，>1MiB 默认拒绝）
  %getdir <r> [local] [--exclude P ...] [--tgz PATH]
                       整目录 tar.gz 打包拉取（纯 Python 打包，默认压缩包
                       ≤700KiB，硬顶 1MiB 超限拒绝并给出最大文件清单）
  %tmux [sess[:win[.pane]]] [-n 行数] [-r] [-S sock] [--args '-J -e']
                       抓取远端 tmux 窗格（capture-pane，默认回溯 9999 行）
  %py <code>          直接执行一行 Python（逃生舱）
  %topic [%t] / %reply / %key [%k] / %allow [%a] / %timeout / %his / %status [%s]
  %help (%?) / %exit (%quit)

实时：
  top / htop          内置监控（远端读 /proc，容器没装 top 也能用），秒级刷新，Ctrl-C 停
  watch [-n 秒] CMD   周期执行任意命令并刷新输出（如 watch -n 2 df -h）
  %monitor [秒] [帧]  滚屏式监控（默认 1 帧快照，适合管道/脚本）

红线：单报文 1MiB 是 broker 实测上限（1.2MiB 丢包）。大文件一律远端处理。
"""


# ============================ 实时帧渲染（top/watch） ============================

def _enable_win_vt():
    """Windows conhost 默认可能没开 ANSI VT，best-effort 打开；失败则用滚屏。"""
    if sys.platform != "win32":
        return True
    try:
        import ctypes
        k = ctypes.windll.kernel32
        h = k.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if not k.GetConsoleMode(h, ctypes.byref(mode)):
            return False
        return bool(k.SetConsoleMode(h, mode.value | 0x0004))
    except Exception:
        return False


def _fmt_stats(st, seq):
    """把一帧 /proc stats 渲染成纯 ASCII 面板（避免 Windows GBK 控制台编码问题）。"""
    cpu = float(st.get("cpu_pct", 0.0))
    bw = 20
    filled = int(min(100.0, cpu) / 100.0 * bw)
    bar = "[" + "#" * filled + "-" * (bw - filled) + "]"
    mt = st.get("mem_total_kb", 0) / 1024.0
    mu = (st.get("mem_total_kb", 0) - st.get("mem_avail_kb", 0)) / 1024.0
    load = " ".join(f"{float(x):.2f}" for x in st.get("load", []))
    lines = [
        f"CPU {bar} {cpu:5.1f}%   MEM {mu:6.0f}/{mt:.0f} MB "
        f"({st.get('mem_used_pct', 0):.0f}%)   LOAD {load}   frame #{seq}",
        f"{'PID':>7} {'%CPU':>6} {'RSS(MB)':>9}  COMMAND",
    ]
    for p in st.get("procs", [])[:8]:
        lines.append(f"{p['pid']:>7} {float(p['cpu']):>6.1f} "
                     f"{p['rss_kb'] / 1024.0:>9.1f}  {str(p['comm'])[:24]}")
    note = "注: /proc 为容器口径, 嵌套容器中进程列(尤其 RSS)可能失真" if seq == 1 else ""
    if note:
        lines.append(note)
    return "\n".join(lines)


def run_live(shell, *, stats, cmd=None, interval=1.0, count=0, ttl=600.0,
             out=None, redraw=None):
    """订阅远端周期汇报并实时显示。

    stats=True 走内置 /proc 监控；否则周期执行 cmd。
    redraw=None：out 是 TTY 且非定长采集时清屏重绘，否则滚屏（每帧分隔线）。
    返回收到的帧数。Ctrl-C 由底层 stream() 捕获并自动 stream_stop。
    """
    out = out or sys.stdout
    if redraw is None:
        redraw = bool(out.isatty()) and not count
    if redraw:
        redraw = _enable_win_vt()

    def cb(i, payload, raw):
        ts = time.strftime("%H:%M:%S")
        if stats:
            body = _fmt_stats(payload, i)
        else:
            body = payload["out"].decode("utf-8", "replace")
            err = payload["err"].decode("utf-8", "replace")
            if err.strip():
                body += "\n[stderr]\n" + err
            if payload.get("frame_error"):
                body += f"\n[frame_error] {payload['frame_error']}"
            elif payload.get("rc") not in (0, None):
                body += f"\n[rc={payload['rc']}]"
        if redraw:
            out.write("\x1b[H\x1b[2J" + body
                      + f"\n\x1b[2m{ts}  frame #{i}  Ctrl-C to stop\x1b[0m\x1b[0J")
        else:
            out.write(f"\n----- frame {i} @ {ts} -----\n{body}\n")
        out.flush()

    if redraw:
        out.write("\x1b[?25l")
        out.flush()
    try:
        if stats:
            return shell.monitor(cb, interval=interval, count=count, ttl=ttl)
        return shell.stream(cmd=cmd, interval=interval, count=count,
                            ttl=ttl, on_frame=cb)
    finally:
        if redraw:
            out.write("\x1b[?25h")
            out.flush()


def _parse_getdir_args(arg):
    """解析 `%getdir <远端目录> [本地目录] [--exclude P]... [--tgz PATH]`。

    返回 (remote, local, excludes, tgz)；解析失败返回 None。
    """
    import shlex
    try:
        toks = shlex.split(arg)
    except ValueError:
        return None
    if not toks:
        return None
    remote = toks[0]
    local = "."
    excludes = []
    tgz = None
    i = 1
    while i < len(toks):
        t = toks[i]
        if t == "--exclude" and i + 1 < len(toks):
            excludes.append(toks[i + 1]); i += 2
        elif t.startswith("--exclude="):
            excludes.append(t.split("=", 1)[1]); i += 1
        elif t == "--tgz" and i + 1 < len(toks):
            tgz = toks[i + 1]; i += 2
        elif t.startswith("--tgz="):
            tgz = t.split("=", 1)[1]; i += 1
        elif local == ".":
            local = t; i += 1
        else:
            return None
    return remote, local, excludes, tgz


def _do_pull_dir(shell, remote, local, excludes, tgz, max_bytes, print_fn):
    """CLI / REPL 共用的 pull-dir 执行体。"""
    if tgz:
        blob, meta = shell.pull_dir_bytes(remote, excludes=excludes,
                                          max_bytes=max_bytes)
        tgz_abs = os.path.abspath(tgz)
        os.makedirs(os.path.dirname(tgz_abs) or ".", exist_ok=True)
        tmp = tgz_abs + ".part"
        with open(tmp, "wb") as fh:
            fh.write(blob)
        os.replace(tmp, tgz_abs)
        print_fn(f"已保存压缩包 -> {tgz_abs} ({meta['arc_bytes']}B, "
                 f"md5={meta['md5']}, {meta['files']} 文件/{meta['dirs']} 目录, "
                 f"原始 {meta['raw_bytes']}B, 排除 {meta['excluded']} 项)",
                 color=_cm.C.GREEN)
    else:
        meta = shell.pull_dir(remote, local, excludes=excludes,
                              max_bytes=max_bytes)
        print_fn(f"已拉取目录 -> {meta['local']}（解压 {meta['extracted']} 条, "
                 f"压缩包 {meta['arc_bytes']}B, md5={meta['md5']}, "
                 f"{meta['files']} 文件/{meta['dirs']} 目录, "
                 f"原始 {meta['raw_bytes']}B, 排除 {meta['excluded']} 项, "
                 f"跳过链接 {meta['skipped_links']}/特殊 {meta['skipped_special']}）",
                 color=_cm.C.GREEN)


def _parse_tmux_args(arg):
    """解析 `%tmux [session] [-n 行数] [-r] [-S sock] [--args ARGS]`。

    返回 (session, max_lines, reverse, socket, capture_args)；失败返回 None。
    """
    import shlex
    try:
        toks = shlex.split(arg)
    except ValueError:
        return None
    session, max_lines, reverse, socket, cargs = "0", 9999, False, "", "-J"
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in ("-n", "--max-lines") and i + 1 < len(toks):
            try:
                max_lines = int(toks[i + 1])
            except ValueError:
                return None
            i += 2
        elif t in ("-r", "--reverse"):
            reverse = True; i += 1
        elif t in ("-S", "--socket") and i + 1 < len(toks):
            socket = toks[i + 1]; i += 2
        elif t == "--args" and i + 1 < len(toks):
            cargs = toks[i + 1]; i += 2
        elif t.startswith("--args="):
            cargs = t.split("=", 1)[1]; i += 1
        elif not t.startswith("-"):
            session = t; i += 1
        else:
            return None
    return session, max_lines, reverse, socket, cargs


def _parse_watch(line):
    """`watch [-n SEC] CMD...` -> (interval, cmd)；解析失败返回 None。"""
    toks = line.split(None, 2)
    if len(toks) < 2:
        return None
    interval = 1.0
    if toks[1] == "-n":
        if len(toks) < 3:
            return None
        try:
            interval = float(toks[2].split(None, 1)[0])
        except ValueError:
            return None
        rest = toks[2].split(None, 1)
        cmd = rest[1] if len(rest) > 1 else ""
    else:
        cmd = toks[1] if len(toks) == 2 else toks[2]
    if not cmd:
        return None
    return interval, cmd


def _build_shell_prompt(history_path):
    """返回 (prompt_fn, hist_ctl, has_pt)。优先 prompt_toolkit，异常则回退 input()。"""
    norm = _cm._normalize_history_path(history_path)

    def _make_session(path):
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.lexers import PygmentsLexer
        from prompt_toolkit.styles import Style
        from prompt_toolkit.key_binding import KeyBindings
        from prompt_toolkit.keys import Keys
        from pygments.lexers.shell import BashLexer
        kb = KeyBindings()

        @kb.add(Keys.BracketedPaste)
        def _paste(event):
            event.current_buffer.insert_text(
                event.data.replace("\r\n", "\n").replace("\r", "\n"))

        return PromptSession(
            lexer=PygmentsLexer(BashLexer),
            style=Style.from_dict({"prompt": "ansicyan bold"}),
            multiline=False,
            history=FileHistory(path) if path else None,
            key_bindings=kb,
        )

    try:
        import importlib
        importlib.import_module("prompt_toolkit")
        importlib.import_module("pygments")
        session0 = _make_session(norm)
    except Exception:
        # 无控制台（管道/重定向）或缺依赖时回退 input()
        state = {"path": norm}

        def prompt_fn(msg):
            return input(msg)

        return prompt_fn, {"get": lambda: state["path"],
                           "set": lambda p: state.__setitem__(
                               "path", _cm._normalize_history_path(p))}, False

    state = {"session": session0, "path": norm}

    def rebuild(path):
        path = _cm._normalize_history_path(path)
        state["session"] = _make_session(path)
        state["path"] = path
        return path

    def prompt_fn(msg):
        return state["session"].prompt(msg)

    return prompt_fn, {"get": lambda: state["path"], "set": rebuild}, True


def run_repl(shell: MqttRemoteShell, history_path=None):
    prompt, hist_ctl, has_pt = _build_shell_prompt(history_path)
    print_fn = _cm._make_print(has_pt)
    tr = shell.tr
    try:
        info = shell.info()
    except Exception as exc:
        print_fn(f"[WARN] 获取远端信息失败: {exc}", color=_cm.C.YELLOW)
        info = {"user": "?", "node": "?", "home": "/"}

    state = {
        "request_topic": tr.request_topic,
        "default_request_topic": DEFAULT_REQUEST_TOPIC,
        "reply_topic": tr.reply_topic,
        "timeout": shell.timeout,
        "key": tr.key,
        "allow_no_pub": tr.allow_no_pub,
        "history_path": hist_ctl["get"](),
        "hist_ctl": hist_ctl,
    }

    def sync():
        tr.request_topic = state["request_topic"]
        tr.reply_topic = state["reply_topic"]
        tr.key = state["key"]
        tr.allow_no_pub = state["allow_no_pub"]
        shell.timeout = float(state["timeout"])

    def short_p():
        p = shell.cwd
        h = info.get("home")
        if h and p == h:
            return "~"
        if h and p.startswith(h.rstrip("/") + "/"):
            return "~/" + p[len(h.rstrip("/")) + 1:]
        return p

    def do_command(line):
        sync()
        try:
            r = shell.run(line)
        except RemoteError as exc:
            print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            return
        if r.stdout:
            sys.stdout.buffer.write(r.stdout)
            sys.stdout.buffer.flush()
        if r.stderr:
            sys.stderr.buffer.write(r.stderr)
            sys.stderr.buffer.flush()
        tail = (["TIMEOUT"] if r.timed_out else []) + [f"rc={r.rc}", f"{r.duration:.2f}s"]
        print_fn("[" + " ".join(tail) + "]", color=_cm.C.GREEN if r.ok else _cm.C.RED)

    def do_grep(arg):
        # 极简解析：支持 -i/-F/-r 和 --include G；其余按 PAT [paths...]
        opts = {"ignore_case": False, "fixed": False, "recursive": True, "include": None}
        toks = arg.split()
        while toks and toks[0].startswith("-") and toks[0] != "-":
            t = toks.pop(0)
            if t == "-i":
                opts["ignore_case"] = True
            elif t == "-F":
                opts["fixed"] = True
            elif t == "--include":
                opts["include"] = toks.pop(0)
            elif t in ("-r", "-R"):
                opts["recursive"] = True
        if not toks:
            print_fn("用法: %grep [-i] [-F] [-r] [--include G] PAT [PATH...]",
                     color=_cm.C.YELLOW)
            return
        pat = toks.pop(0)
        try:
            ms = shell.grep(pat, paths=toks or ["."], **opts)
        except RemoteError as exc:
            print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            return
        for m in ms[:200]:
            print_fn(f"{m['path']}:{m['lineno']}: ", color=_cm.C.CYAN, end="")
            print_fn(m["text"])
        if len(ms) > 200:
            print_fn(f"... 仅显示前 200/{len(ms)} 条，请缩小范围", color=_cm.C.YELLOW)
        if not ms:
            print_fn("(无匹配)", color=_cm.C.GRAY)

    def do_replace(arg):
        # %replace PATH OLD == NEW [%]
        if "==" not in arg:
            print_fn("用法: %replace PATH OLD == NEW [%  (末尾 %% 表示全部替换)",
                     color=_cm.C.YELLOW)
            return
        head, new = arg.split("==", 1)
        all_flag = new.rstrip().endswith("%")
        if all_flag:
            new = new.rstrip()[:-1].rstrip()
        toks = head.split(None)
        if len(toks) < 2:
            print_fn("用法: %replace PATH OLD == NEW [%", color=_cm.C.YELLOW)
            return
        path, old = toks[0], " ".join(toks[1:])
        try:
            r = shell.edit_replace(path, old, new, count=(0 if all_flag else 1))
        except RemoteError as exc:
            print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            return
        print_fn(f"已替换 {r['replaced']}/{r['matches']} 处 -> {r['path']} "
                 f"({r['bytes']}B，备份 .bak)", color=_cm.C.GREEN)

    def do_magic(line):
        body = line[1:].strip()
        parts = body.split(None, 1)
        cmd = parts[0].lower() if parts else ""
        arg = parts[1].strip() if len(parts) > 1 else ""

        if cmd in ("help", "h", "?"):
            print_fn(_REPL_HELP, color=_cm.C.CYAN)
        elif cmd == "info":
            print_fn(json.dumps(shell.info(refresh=True), ensure_ascii=False, indent=2),
                     color=_cm.C.CYAN)
        elif cmd == "grep":
            do_grep(arg)
        elif cmd == "replace":
            do_replace(arg)
        elif cmd == "append":
            if not arg:
                print_fn("用法: %append PATH（随后输入文本，Ctrl-Z 结束）", color=_cm.C.YELLOW)
                return
            text = sys.stdin.read()
            r = shell.append_text(arg, text)
            print_fn(f"已追加 -> {r['path']} ({r['bytes']}B)", color=_cm.C.GREEN)
        elif cmd == "cat":
            if not arg:
                print_fn("用法: %cat <远端路径>", color=_cm.C.YELLOW)
            else:
                sys.stdout.buffer.write(shell.read(arg))
                sys.stdout.buffer.flush()
        elif cmd in ("get", "download"):
            ap = arg.split()
            if not ap:
                print_fn("用法: %get <远端路径> [本地路径]", color=_cm.C.YELLOW)
            else:
                r = shell.download(ap[0], ap[1] if len(ap) > 1 else ".")
                print_fn(f"已下载 -> {r['local']} ({r['bytes']}B, sha256={r['sha256'][:12]})",
                         color=_cm.C.GREEN)
        elif cmd in ("put", "upload"):
            ap = arg.split()
            if not ap:
                print_fn("用法: %put <本地路径> [远端路径]", color=_cm.C.YELLOW)
            else:
                rp = ap[1] if len(ap) > 1 else posixpath.join(
                    shell.cwd, os.path.basename(ap[0].rstrip("\\/")))
                r = shell.upload(ap[0], rp)
                print_fn(f"已上传 -> {r['path']} ({r['bytes']}B, {r['mode']})",
                         color=_cm.C.GREEN)
        elif cmd in ("getdir", "pulldir", "pull-dir"):
            parsed = _parse_getdir_args(arg)
            if parsed is None:
                print_fn("用法: %getdir <远端目录> [本地目录] [--exclude P]... "
                         "[--tgz PATH]", color=_cm.C.YELLOW)
                return
            try:
                _do_pull_dir(shell, *parsed, max_bytes=None, print_fn=print_fn)
            except DirArchiveTooLarge as exc:
                print_fn(f"[拒绝] {exc}", color=_cm.C.RED)
        elif cmd in ("tmux", "tmuxcap", "tmux-capture"):
            parsed = _parse_tmux_args(arg)
            if parsed is None:
                print_fn("用法: %tmux [sess[:win[.pane]]] [-n 行数] [-r] "
                         "[-S sock] [--args '-J -e']", color=_cm.C.YELLOW)
                return
            sys.stdout.write(shell.tmux_capture_pane(
                parsed[0], max_lines=parsed[1], reverse=parsed[2],
                socket=parsed[3], capture_args=parsed[4]))
        elif cmd == "edit":
            if not arg:
                print_fn("用法: %edit <远端路径>", color=_cm.C.YELLOW)
                return
            import tempfile
            rp = arg
            try:
                old = shell.read(rp)
            except RemoteOpError:
                old = b""
            fd, tmp = tempfile.mkstemp(suffix="_" + (posixpath.basename(rp) or "r.txt"))
            os.close(fd)
            with open(tmp, "wb") as fh:
                fh.write(old)
            try:
                editor = os.environ.get("EDITOR")
                if editor:
                    import subprocess
                    subprocess.run([editor, tmp])
                elif sys.platform == "win32":
                    os.startfile(tmp)  # noqa: S606
                    input("编辑完成后回到这里按 Enter 上传（Ctrl-C 取消）...")
                else:
                    import subprocess
                    subprocess.run(["vi", tmp])
                with open(tmp, "rb") as fh:
                    edited = fh.read()
            except KeyboardInterrupt:
                print_fn("已取消编辑", color=_cm.C.YELLOW)
                return
            finally:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
            if edited == old:
                print_fn("文件无变化，不上传", color=_cm.C.GRAY)
            else:
                r = shell.write(rp, edited, backup=True)
                print_fn(f"已保存 {r['path']} ({r['bytes']}B, sha256={r['sha256'][:12]}, "
                         f"旧文件 .bak)", color=_cm.C.GREEN)
        elif cmd in ("py", "python"):
            if not arg:
                print_fn("用法: %py <python 代码>", color=_cm.C.YELLOW)
            else:
                resp = shell.py(arg)
                if resp.get("stdout"):
                    print_fn(resp["stdout"], end="", color=_cm.C.CYAN)
                if resp.get("ok"):
                    if resp.get("r") not in (None, ""):
                        print_fn(resp["r"])
                else:
                    print_fn(resp.get("error", "remote python failed"), color=_cm.C.RED)
        elif cmd == "monitor":
            # %monitor [间隔秒] [帧数]：滚屏，默认只采 1 帧（快照）
            ap = arg.split()
            try:
                iv = float(ap[0]) if ap else 1.0
                cnt = int(ap[1]) if len(ap) > 1 else 1
            except ValueError:
                print_fn("用法: %monitor [间隔秒] [帧数]", color=_cm.C.YELLOW)
                return
            try:
                run_live(shell, stats=True, interval=iv, count=cnt,
                         ttl=max(60.0, iv * max(cnt, 1) + 30.0), redraw=False)
            except RemoteError as exc:
                print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
        else:
            _cm._handle_magic(line, state, print_fn)
            sync()

    print_fn(f"远端 Shell 已连接: {info.get('user')}@{info.get('node')} "
             f"({info.get('system')} {info.get('release')} {info.get('machine')}, "
             f"python {info.get('python')})", color=_cm.C.GREEN)
    print_fn("和 SSH 一样直接输命令；%help 查看帮助（含 1MiB 红线），%exit 退出。",
             color=_cm.C.BLUE)

    while True:
        try:
            line = prompt(f"{info.get('user', '?')}@{info.get('node', '?')}:{short_p()}$ ")
        except KeyboardInterrupt:
            print_fn("")
            continue
        except EOFError:
            print_fn("\nCtrl-D 退出", color=_cm.C.YELLOW)
            return
        stripped = line.strip()
        if not stripped or stripped in ("exit", "quit"):
            if stripped:
                return
            continue
        if stripped == "pwd" or (stripped.startswith("cd") and
                                 (len(stripped) == 2 or stripped[2] in (" ", "\t"))):
            ap = stripped.split(None, 1)
            if ap[0] == "pwd":
                print_fn(shell.cwd, color=_cm.C.CYAN)
            elif len(ap) < 2:
                shell._cwd = None
                print_fn(shell.cwd, color=_cm.C.CYAN)
            else:
                try:
                    print_fn(shell.cd(ap[1].strip()), color=_cm.C.CYAN)
                except RemoteError as exc:
                    print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            continue
        if stripped.startswith("%"):
            try:
                do_magic(stripped)
            except RemoteError as exc:
                print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            continue
        # 实时类命令：不是一问一答，而是订阅远端周期推送（top/watch 模型）
        if stripped in ("top", "htop"):
            sync()
            try:
                run_live(shell, stats=True, interval=1.0, count=0, ttl=600.0)
            except RemoteError as exc:
                print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            continue
        if stripped == "watch" or stripped.startswith("watch ") \
                or stripped.startswith("watch\t"):
            pw = _parse_watch(stripped)
            if pw is None:
                print_fn("用法: watch [-n 秒] <命令>", color=_cm.C.YELLOW)
            else:
                sync()
                try:
                    run_live(shell, stats=False, cmd=pw[1],
                             interval=pw[0], count=0, ttl=600.0)
                except RemoteError as exc:
                    print_fn(f"[ERROR] {exc}", color=_cm.C.RED)
            continue
        do_command(stripped)


# ============================ CLI ============================

def add_connection_args(p, default_timeout=DEFAULT_TIMEOUT):
    """注册连接类参数。

    选项名一律引用 client_mqtt 的统一别名表（alias_xxx）+ _cli_opts 生成，
    与 client_mqtt.py 自身 CLI、rpc(**ka) 别名、%magic 命令保持完全一致；
    pty_client_mqtt 也复用本函数，禁止再各写一份硬编码选项。
    dest 仍沿用本模块历史名称（key/allow），_shell_from_args 无需改动。
    """
    p.add_argument(
        *_cm._cli_opts(*_cm.alias_request_topic),
        dest="request_topic", default=DEFAULT_REQUEST_TOPIC,
        help=f"request topic（默认 {DEFAULT_REQUEST_TOPIC}）",
    )
    p.add_argument(
        *_cm._cli_opts(*_cm.alias_reply_topic),
        dest="reply_topic", default=DEFAULT_REPLY_TOPIC,
        help=f"reply topic（默认 {DEFAULT_REPLY_TOPIC}）",
    )
    p.add_argument(
        *_cm._cli_opts(*_cm.alias_private_key),
        dest="key", default=DEFAULT_KEY,
        help="私钥：整数表达式/PEM/文件路径，默认 DEFAULT_KEY；空串不签名",
    )
    p.add_argument(
        *_cm._cli_opts(*_cm.alias_allow_no_pub),
        dest="allow", action="store_true", default=True,
        help="允许接收未验签服务端的回包（默认允许）",
    )
    p.add_argument("--no-allow", dest="allow", action="store_false",
                   help="私钥模式下拦截未验签服务端的回包")
    p.add_argument(
        *_cm._cli_opts(*_cm.alias_timeout),
        dest="timeout", type=float, default=default_timeout,
        help=f"问答等待秒数（默认 {default_timeout}）",
    )


# 向后兼容旧名字
_add_common = add_connection_args


def _shell_from_args(args, wire_budget=WIRE_BUDGET):
    return MqttRemoteShell(
        request_topic=args.request_topic,
        reply_topic=args.reply_topic,
        private_key=args.key or None,
        allow_no_pub=args.allow,
        timeout=args.timeout,
        wire_budget=wire_budget,
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="cmd_client_mqtt",
        description="通过 MQTT RPC 远程执行命令 / 就地编辑文件（远端无需改动）")
    add_connection_args(parser)
    sub = parser.add_subparsers(dest="action")

    p_run = sub.add_parser("run", help="执行一条 shell 命令（和 ssh 一样）")
    p_run.add_argument("cmd")
    p_run.add_argument("--cwd")
    p_run.add_argument("--cmd-timeout", type=float)
    p_run.add_argument("-e", "--env", action="append", default=[])

    sub.add_parser("info", help="远端环境（JSON）")

    p_ls = sub.add_parser("ls", help="列目录（JSON）")
    p_ls.add_argument("path", nargs="?", default=".")
    p_stat = sub.add_parser("stat", help="路径元信息（JSON）")
    p_stat.add_argument("path")

    p_grep = sub.add_parser("grep", help="远端 grep（结构化 JSON，不拉文件）")
    p_grep.add_argument("pattern")
    p_grep.add_argument("paths", nargs="*", default=["."])
    p_grep.add_argument("-i", dest="ignore_case", action="store_true")
    p_grep.add_argument("-F", dest="fixed", action="store_true")
    p_grep.add_argument("--include")
    p_grep.add_argument("--exclude")

    p_edit = sub.add_parser("edit", help="远端就地精确替换（无需引号地狱）")
    p_edit.add_argument("path")
    p_edit.add_argument("--old", required=True)
    p_edit.add_argument("--new", required=True)
    p_edit.add_argument("--count", type=int, default=1, help="替换几处，0=全部")
    p_edit.add_argument("--no-backup", action="store_true")

    p_cat = sub.add_parser("cat", help="原样输出远端小文件（<1MiB）")
    p_cat.add_argument("path")
    p_cat.add_argument("--allow-large", action="store_true")

    p_get = sub.add_parser("get", help="下载远端小文件")
    p_get.add_argument("remote")
    p_get.add_argument("local", nargs="?", default=".")
    p_get.add_argument("--allow-large", action="store_true")

    p_put = sub.add_parser("put", help="上传本地小文件")
    p_put.add_argument("local")
    p_put.add_argument("remote", nargs="?")
    p_put.add_argument("--mode", default=None)
    p_put.add_argument("--allow-large", action="store_true")

    p_pd = sub.add_parser(
        "pull-dir", aliases=["pulldir", "getdir"],
        help="整目录 tar.gz 打包拉取并解压（纯 Python 打包；支持 --exclude、"
             "md5 校验；压缩包硬顶 1MiB，默认 700KiB，超限返回诊断）")
    p_pd.add_argument("remote", help="远端目录")
    p_pd.add_argument("local", nargs="?", default=".", help="本地解压目录（默认 .）")
    p_pd.add_argument("--exclude", action="append", default=[], metavar="PAT",
                      help="fnmatch 排除模式，可重复，如 --exclude build "
                           "--exclude '*.pyc'（对任意层级目录/文件名组件匹配）")
    p_pd.add_argument("--max-bytes", type=int, default=DEFAULT_DIR_ARCHIVE_MAX,
                      help=f"压缩包字节上限（默认 {DEFAULT_DIR_ARCHIVE_MAX}，"
                           f"硬顶 {MAX_DIR_ARCHIVE}）")
    p_pd.add_argument("--tgz", metavar="PATH",
                      help="不解压，只把 tar.gz 压缩包保存到该路径")

    p_write = sub.add_parser("write", help="内容写入远端文件（--data 或 stdin）")
    p_write.add_argument("path")
    p_write.add_argument("--data")
    p_write.add_argument("--mode", default=None)
    p_write.add_argument("--backup", action="store_true")
    p_write.add_argument("--allow-large", action="store_true")

    p_mkdir = sub.add_parser("mkdir"); p_mkdir.add_argument("path")
    p_rm = sub.add_parser("rm"); p_rm.add_argument("path")
    p_rm.add_argument("-r", "--recursive", action="store_true")
    p_apt = sub.add_parser("apt", help="远端 apt-get 非交互安装")
    p_apt.add_argument("packages", nargs="+")
    p_apt.add_argument("--no-update", action="store_true")
    p_pip = sub.add_parser("pip", help="远端 pip 安装")
    p_pip.add_argument("packages", nargs="+")

    def _add_live(p):
        p.add_argument("-i", "--interval", type=float, default=1.0,
                       help="帧间隔秒数（默认 1）")
        p.add_argument("-n", "--count", type=int, default=0,
                       help="收 N 帧后退出；0=持续到 Ctrl-C/TTL")
        p.add_argument("--ttl", type=float, default=600.0,
                       help="远端最长跑多久自动停（秒，上限 1800）")
        p.add_argument("--scroll", action="store_true",
                       help="强制滚屏（默认 TTY 清屏重绘）")

    p_top = sub.add_parser(
        "top", help="内置实时监控（远端读 /proc，无需安装 top，Ctrl-C 停）")
    _add_live(p_top)
    p_watch = sub.add_parser(
        "watch", help="周期执行命令并实时刷新输出，如: watch -i 2 df -h")
    p_watch.add_argument("cmd", help="远端 shell 命令（建议引号包起来）")
    _add_live(p_watch)

    p_tmux = sub.add_parser(
        "tmux-capture", aliases=["tmuxcap", "tmux"],
        help="抓取远端 tmux 窗格内容（capture-pane + show-buffer）")
    p_tmux.add_argument("session", nargs="?", default="0",
                        help="目标窗格（默认 0；支持 sess / sess:win / sess:win.pane）")
    p_tmux.add_argument("-n", "--max-lines", type=int, default=9999,
                        help="向上回溯行数（默认 9999）")
    p_tmux.add_argument("-r", "--reverse", action="store_true",
                        help="行序倒转（最新一行在最上）")
    p_tmux.add_argument("-S", "--socket", default="",
                        help="tmux -S 套接字路径（多 server 时用）")
    p_tmux.add_argument("--args", default="-J", metavar="ARGS",
                        help="透传给 capture-pane 的额外参数（默认 -J 合并折行；"
                             "保留 ANSI 颜色用 '-J -e'）")

    sub.add_parser("repl", help="交互式 shell（无参数时默认）")

    args = parser.parse_args(argv)
    action = args.action or "repl"

    if action == "repl":
        history = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "cmd_client_history.db")
        sh = _shell_from_args(args)
        try:
            run_repl(sh, history_path=history)
        finally:
            sh.close()
        return 0

    sh = _shell_from_args(args)
    try:
        if action == "run":
            envd = None
            if args.env:
                envd = {}
                for kv in args.env:
                    k, sep, v = kv.partition("=")
                    envd[k] = v if sep else None
            r = sh.run(args.cmd, cwd=args.cwd, timeout=args.cmd_timeout, env=envd)
            if r.stdout:
                sys.stdout.buffer.write(r.stdout)
            if r.stderr:
                sys.stderr.buffer.write(r.stderr)
            sys.stderr.write(f"[rc={r.rc} {r.duration:.2f}s"
                             f"{' TIMEOUT' if r.timed_out else ''}]\n")
            return r.rc if 0 <= r.rc <= 255 else 1
        if action == "info":
            print(json.dumps(sh.info(refresh=True), ensure_ascii=False, indent=2))
            return 0
        if action == "ls":
            print(json.dumps(sh.ls(args.path), ensure_ascii=False, indent=2))
            return 0
        if action == "stat":
            print(json.dumps(sh.stat(args.path), ensure_ascii=False, indent=2))
            return 0
        if action == "grep":
            ms = sh.grep(args.pattern, paths=args.paths, ignore_case=args.ignore_case,
                         fixed=args.fixed, include=args.include and [args.include],
                         exclude=args.exclude and [args.exclude])
            print(json.dumps(ms, ensure_ascii=False, indent=2))
            return 0 if ms else 1
        if action == "edit":
            r = sh.edit_replace(args.path, args.old, args.new, count=args.count,
                                backup=not args.no_backup)
            sys.stderr.write(f"OK replaced={r['replaced']}/{r['matches']} "
                             f"{r['path']} {r['bytes']}B\n")
            return 0
        if action == "cat":
            sys.stdout.buffer.write(sh.read(args.path, allow_large=args.allow_large))
            return 0
        if action == "get":
            r = sh.download(args.remote, args.local, allow_large=args.allow_large)
            sys.stderr.write(f"OK {r['local']} {r['bytes']}B sha256={r['sha256']}\n")
            return 0
        if action == "put":
            remote = args.remote or posixpath.join(
                sh.cwd, os.path.basename(os.path.abspath(args.local.rstrip("\\/"))))
            mode = int(args.mode, 8) if args.mode else None
            r = sh.upload(args.local, remote, mode=mode, allow_large=args.allow_large)
            sys.stderr.write(f"OK {r['path']} {r['bytes']}B mode={r['mode']} "
                             f"sha256={r['sha256']}\n")
            return 0
        if action in ("pull-dir", "pulldir", "getdir"):
            def _cli_print(msg, **_ka):
                sys.stderr.write(str(msg) + "\n")
            try:
                _do_pull_dir(sh, args.remote, args.local, args.exclude,
                             args.tgz, args.max_bytes, _cli_print)
            except DirArchiveTooLarge as exc:
                sys.stderr.write(f"[ERROR] DirArchiveTooLarge:\n{exc}\n")
                return 2
            return 0
        if action == "write":
            data = args.data.encode("utf-8") if args.data is not None \
                else sys.stdin.buffer.read()
            mode = int(args.mode, 8) if args.mode else None
            r = sh.write(args.path, data, mode=mode, backup=args.backup,
                         allow_large=args.allow_large)
            sys.stderr.write(f"OK {r['path']} {r['bytes']}B mode={r['mode']} "
                             f"sha256={r['sha256']}\n")
            return 0
        if action == "mkdir":
            sh.mkdir(args.path); return 0
        if action == "rm":
            sh.rm(args.path, recursive=args.recursive); return 0
        if action == "apt":
            r = sh.apt_install(args.packages, update=not args.no_update)
            if r.stdout:
                sys.stdout.buffer.write(r.stdout)
            return r.rc
        if action == "pip":
            r = sh.pip_install(args.packages)
            if r.stdout:
                sys.stdout.buffer.write(r.stdout)
            return r.rc
        if action in ("top", "watch"):
            n = run_live(
                sh, stats=(action == "top"),
                cmd=(None if action == "top" else args.cmd),
                interval=args.interval, count=args.count, ttl=args.ttl,
                redraw=(False if args.scroll else None))
            return 0 if n > 0 else 3
        if action in ("tmux-capture", "tmuxcap", "tmux"):
            sys.stdout.write(sh.tmux_capture_pane(
                args.session, max_lines=args.max_lines, reverse=args.reverse,
                socket=args.socket, capture_args=args.args))
            return 0
    except RemoteError as exc:
        # 红线/超时/远端错误：CLI 边界打一行人话，不抛 traceback
        sys.stderr.write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        return 2
    finally:
        sh.close()
    return 1


if __name__ == "__main__":
    sys.exit(main())
