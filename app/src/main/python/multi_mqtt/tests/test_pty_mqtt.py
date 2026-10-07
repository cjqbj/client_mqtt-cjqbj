#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt / RemotePty / _PTY_START_TEMPLATE 的详细测试。

分三部分：
1. RemotePtyClientTests —— 全平台可跑：用 FakeTransport 测客户端协商逻辑
   （iseq 打号、下行 seq 去重/乱序重排/丢帧跳号、心跳/end 帧、握手失败
   清理、close 行为）。不连任何 broker，不需要 POSIX。
2. PtyTemplateStaticTests —— 全平台可跑：模板编译与关键防护标记的静态检查。
3. PtyTemplateLiveTests —— 仅 POSIX：在 PythonExecutor 的持久命名空间里
   真实执行 _PTY_START_TEMPLATE（真 openpty + fork /bin/sh + 三个线程），
   用 FakeNet 模拟服务端 mqtt_net，覆盖：
     - 按键回显 / iseq 单调闸门（多副本去重 + 旧 winsz 乱序迟到不得生效）
       / winsz / stop→end
     - 心跳帧
     - cwd 不存在回退 HOME（不报错、不退出）
     - 重复握手路由幂等（RecursionError 回归测试）
     - 旧版自裹闭环的自愈
     - 非 PTY topic 原样透传到服务端 handle_message

运行：
    cd multi_mqtt && python -m unittest tests.test_pty_mqtt -v
"""
import io
import json
import os
import struct
import sys
import threading
import time
import unittest
from unittest import mock
import urllib.request
import urllib.error

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.remote_cmd import (  # noqa: E402
    RemotePty, Transport, RemoteError, RemoteTimeout, RemoteOpError,
    build_pty_start_code, PTY_FRAME_MAX,
)
from rpc_executor import PythonExecutor  # noqa: E402
from multi_mqtt import BROKER_LIST  # noqa: E402
from client import pty_client_mqtt as pcm  # noqa: E402

IS_POSIX = os.name == "posix"


# ============================ 通用 Fake ============================

def _canned_env(sid, **kw):
    env = {
        "ok": True, "sid": sid,
        "in_topic": "pty/%s/in" % sid, "out_topic": "pty/%s/out" % sid,
        "shell": "/bin/sh", "pid": 4242, "term": "xterm",
        "rows": 24, "cols": 80, "cwd": "/root", "cwd_warning": None,
        "flush_interval": 0.0, "ttl": 43200, "login": True, "heartbeat": 0.0,
    }
    env.update(kw)
    return env


class FakeTransport(Transport):
    """RemotePty 的内存传输：request 回罐头，publish/订阅全记录。"""

    def __init__(self, response=None):
        self.response = response
        self.extra_responses = []  # 其他服务端的迟到握手回包
        self.prefetch = []         # request_many 期间同步下发的帧（模拟首屏）
        self.requests = []         # [(code, timeout)]，记录调用顺序
        self.published = []        # [(topic, payload)]
        self.events = []           # 订阅/退订事件，保序
        self._handlers = {}

    def request(self, code, timeout=60):
        self.requests.append((code, timeout))
        return self.response

    def request_many(self, code, timeout=60, gather=0.8):
        self.requests.append((code, timeout, gather))
        for topic, data in list(self.prefetch):
            self.emit(topic, data)
        self.prefetch = []
        return self.response, list(self.extra_responses)

    def publish(self, topic, payload):
        self.published.append((topic, dict(payload)))

    def stream_subscribe(self, topic, handler):
        self.events.append(("sub", topic))
        self._handlers.setdefault(topic, []).append(handler)

    def stream_unsubscribe(self, topic, handler):
        self.events.append(("unsub", topic))
        self._handlers.get(topic, []).remove(handler)

    def emit(self, topic, data):
        """模拟 broker 推送一帧给本 topic 的所有订阅者。"""
        for h in list(self._handlers.get(topic, [])):
            h(data)


def _wait_until(pred, timeout=2.0, step=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(step)
    return False


def _open_pty(transport, sid="pty-test-1", env_extra=None, **open_kw):
    """用罐头回包完成一次 RemotePty.open，返回 (pty, env, 收到的数据列表)。"""
    env = _canned_env(sid)
    if env_extra:
        env.update(env_extra)
    transport.response = {"r": json.dumps(env), "ok": True}
    received = []
    heartbeats = []
    pty = RemotePty(transport, timeout=5)
    ret = pty.open(
        24, 80, sid=sid,
        in_topic=env["in_topic"], out_topic=env["out_topic"],
        heartbeat=5.0,
        on_data=received.append, on_heartbeat=heartbeats.append,
        **open_kw)
    return pty, ret, received, heartbeats


# ============================ 1. 客户端 RemotePty 逻辑（全平台） ============================

class RemotePtyClientTests(unittest.TestCase):

    def test_open_subscribes_out_topic_before_sending_request(self):
        tr = FakeTransport()
        pty, env, _, _ = _open_pty(tr, "pty-order-1")
        self.addCleanup(lambda: pty.close())
        self.assertEqual(env["sid"], "pty-order-1")
        # 必须先订阅再发握手代码，否则最早的 shell 输出会丢
        self.assertEqual(tr.events[0], ("sub", "pty/pty-order-1/out"))
        self.assertEqual(len(tr.requests), 1)
        self.assertIn("_cmq_pty_start", tr.requests[0][0])
        self.assertEqual(pty.sid, "pty-order-1")

    def test_downlink_seq_dedup_keeps_first_frame_only(self):
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-seq-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-seq-1/out"
        # seq=0 来 3 份（多 broker 重复），seq=1 一份，乱序迟到的 seq=0 再一份
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 1, "d": "B"})
        tr.emit(out, {"pty": "pty-seq-1", "seq": 0, "d": "A"})
        self.assertEqual(b"".join(got), b"AB")

    def test_downlink_reorders_out_of_order_frames(self):
        # 不同 broker 路径延迟抖动让后发帧先到：必须按 seq 重排后再渲染，
        # 否则跨 chunk 的 CSI 转义序列会把终端状态机打坏（进度条错位）。
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-reorder-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-reorder-1/out"
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 1, "d": "B"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 0, "d": "A"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 3, "d": "D"})
        tr.emit(out, {"pty": "pty-reorder-1", "seq": 2, "d": "C"})
        self.assertEqual(b"".join(got), b"ABCD")
        # 全部连续补齐后不应留下等待跳号的定时器
        self.assertIsNone(pty._reorder._timer)
        self.assertEqual(pty._reorder._pending, {})

    def test_downlink_gap_skips_after_timeout_instead_of_stalling(self):
        # 中间帧真被 broker 丢掉：短暂等待后跳号放行，输出不能永久卡死
        from client.remote_cmd import _PtyReorderBuffer
        _PtyReorderBuffer.GAP_TIMEOUT = 0.2
        self.addCleanup(
            setattr, _PtyReorderBuffer, "GAP_TIMEOUT", 0.75)
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-gap-1")
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-gap-1/out"
        tr.emit(out, {"pty": "pty-gap-1", "seq": 10, "d": "X"})
        self.assertEqual(got, [])  # 缺口未补齐前先缓存
        self.assertTrue(_wait_until(lambda: bool(got), timeout=2.0))
        self.assertEqual(b"".join(got), b"X")
        # 跳号后迟到的 seq=10 副本必须丢弃
        tr.emit(out, {"pty": "pty-gap-1", "seq": 10, "d": "X"})
        tr.emit(out, {"pty": "pty-gap-1", "seq": 11, "d": "Y"})
        self.assertEqual(b"".join(got), b"XY")

    def test_heartbeat_frames_update_liveness_without_data(self):
        tr = FakeTransport()
        pty, _, got, hbs = _open_pty(tr, "pty-hb-1")
        self.addCleanup(lambda: pty.close())
        tr.emit("pty/pty-hb-1/out", {"pty": "pty-hb-1", "hb": 1000})
        tr.emit("pty/pty-hb-1/out", {"pty": "pty-hb-1", "hb": 2000})
        self.assertEqual(hbs, [1000, 2000])
        self.assertEqual(got, [])  # 心跳不带数据

    def test_end_frame_sets_end_reason(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-end-1")
        self.assertIsNone(pty.end_reason)
        tr.emit("pty/pty-end-1/out",
                {"pty": "pty-end-1", "end": True, "reason": "exit", "rc": 0})
        self.assertEqual(pty.end_reason, "exit")
        self.assertTrue(pty.wait_end(timeout=1))

    def test_uplink_frames_carry_strict_monotonic_iseq(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-iseq-1")
        self.addCleanup(lambda: pty.close())
        pty.send(b"x")
        pty.resize(30, 100)
        pty.send(b"\xff\x00\x7f")  # 任意二进制经 latin-1 无损承载
        pty.detach()
        in_topic = "pty/pty-iseq-1/in"
        self.assertEqual([p[0] for p in tr.published],
                         [in_topic] * 4)
        self.assertEqual([p[1]["iseq"] for p in tr.published], [0, 1, 2, 3])
        self.assertEqual(tr.published[0][1]["k"], "x")
        self.assertEqual(tr.published[1][1]["winsz"], [30, 100])
        self.assertEqual(tr.published[2][1]["k"],
                         b"\xff\x00\x7f".decode("latin-1"))
        self.assertTrue(tr.published[3][1]["stop"])
        # 所有上行帧都没有 req_id：PTY 数据面不走 RPC 签名/去重通道
        for _, frame in tr.published:
            self.assertNotIn("req_id", frame)
            self.assertNotIn("code", frame)

    # ---- 多应答者归属仲裁（owner/claim） ----

    def test_open_sends_claim_and_all_uplink_frames_carry_owner(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-owner-1",
                                 env_extra={"owner": "aaaaaa", "host": "h1",
                                            "pid": 111})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(pty.owner, "aaaaaa")
        # open 一结束立即发 claim（iseq=0），影子服务端在首个按键前就自杀
        first = tr.published[0][1]
        self.assertEqual(first, {"pty": "pty-owner-1", "owner": "aaaaaa",
                                 "iseq": 0, "claim": True})
        pty.send(b"x")
        pty.resize(30, 100)
        self.assertEqual([f["owner"] for _, f in tr.published[1:]],
                         ["aaaaaa", "aaaaaa"])
        self.assertEqual([f["iseq"] for _, f in tr.published], [0, 1, 2])

    def test_foreign_owner_downlink_frames_dropped_including_end(self):
        # 影子 PTY 的输出帧和它的 end 帧都不能影响本视图
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-owner-2",
                                   env_extra={"owner": "win1"})
        self.addCleanup(lambda: pty.close())
        out = "pty/pty-owner-2/out"
        tr.emit(out, {"pty": "pty-owner-2", "owner": "shadow-x",
                      "seq": 0, "d": "GHOST"})
        tr.emit(out, {"pty": "pty-owner-2", "owner": "shadow-x",
                      "end": True, "reason": "claim_lost"})
        self.assertEqual(got, [])
        self.assertIsNone(pty.end_reason)
        self.assertEqual(pty.foreign_frames, 2)
        # 赢家的流照常渲染
        tr.emit(out, {"pty": "pty-owner-2", "owner": "win1",
                      "seq": 0, "d": "ok"})
        self.assertEqual(b"".join(got), b"ok")

    def test_ownerless_old_server_backward_compatible(self):
        # 旧服务端回包/帧不带 owner：不发 claim，下行照旧放行
        tr = FakeTransport()
        pty, _, got, _ = _open_pty(tr, "pty-ownerless-1")
        self.addCleanup(lambda: pty.close())
        self.assertIsNone(pty.owner)
        self.assertFalse(any(f.get("claim") for _, f in tr.published))
        tr.emit("pty/pty-ownerless-1/out",
                {"pty": "pty-ownerless-1", "seq": 0, "d": "legacy"})
        self.assertEqual(b"".join(got), b"legacy")

    def test_multiple_handshake_responders_parsed_winner_first(self):
        tr = FakeTransport()
        tr.extra_responses = [{
            "r": json.dumps(_canned_env(
                "pty-multi-1", owner="bbbbbb", host="host-b", pid=222)),
            "ok": True}]
        pty, _, _, _ = _open_pty(
            tr, "pty-multi-1",
            env_extra={"owner": "aaaaaa", "host": "host-a", "pid": 111})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(
            [(r["owner"], r["host"], r["pid"], r["winner"])
             for r in pty.responders],
            [("aaaaaa", "host-a", 111, True),
             ("bbbbbb", "host-b", 222, False)])

    def test_configure_publishes_clamped_set_frame(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-set-1",
                                 env_extra={"owner": "win-set"})
        self.addCleanup(lambda: pty.close())
        applied = pty.configure(interval=999, heartbeat=-5, ttl=5)
        self.assertEqual(applied, {"interval": 60.0, "heartbeat": 0.0,
                                   "ttl": 60.0})
        fr = tr.published[-1][1]
        self.assertEqual(fr["set"], {"interval": 60.0, "heartbeat": 0.0,
                                     "ttl": 60.0})
        self.assertEqual(fr["owner"], "win-set")
        self.assertEqual(fr["iseq"], 1)  # claim 占 0，set 帧顺延
        self.assertEqual(pty.configure(), {})  # 无参数不下发

    def test_configure_redraw_frame(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-redraw-1",
                                 env_extra={"owner": "win-rd"})
        self.addCleanup(lambda: pty.close())
        applied = pty.configure(redraw=True)
        self.assertEqual(applied["redraw"], True)
        self.assertEqual(tr.published[-1][1]["set"], {"redraw": True})

    def test_prefetch_frames_buffered_until_owner_then_ghosts_filtered(self):
        # 握手回包到达前（属主未定）就已经在飞的首屏帧：先缓存，定主后重放，
        # 其中影子 PTY 的早期帧必须被滤掉，不进屏幕/不制造 seq 缺口。
        tr = FakeTransport()
        sid = "pty-prefetch-1"
        out = "pty/%s/out" % sid
        tr.prefetch = [
            (out, {"pty": sid, "owner": "shadow", "seq": 0, "d": "G"}),
            (out, {"pty": sid, "owner": "win", "seq": 0, "d": "W"}),
        ]
        pty, _, got, _ = _open_pty(
            tr, sid, env_extra={"owner": "win"})
        self.addCleanup(lambda: pty.close())
        self.assertEqual(b"".join(got), b"W")
        self.assertEqual(pty.foreign_frames, 1)
        self.assertEqual(pty._pending_frames, [])

    def test_send_before_open_raises(self):
        pty = RemotePty(FakeTransport(), timeout=2)
        with self.assertRaises(RemoteError):
            pty.send(b"\r")
        # resize/detach 在未 open 时静默忽略，不能炸
        pty.resize(24, 80)
        pty.detach()

    def test_open_timeout_unsubscribes(self):
        tr = FakeTransport(response=None)
        pty = RemotePty(tr, timeout=2)
        with self.assertRaises(RemoteTimeout):
            pty.open(24, 80, sid="pty-timeout-1",
                     in_topic="pty/pty-timeout-1/in",
                     out_topic="pty/pty-timeout-1/out", req_timeout=2)
        self.assertIn(("unsub", "pty/pty-timeout-1/out"), tr.events)
        self.assertIsNone(pty.sid)

    def test_open_remote_op_error_unsubscribes(self):
        tr = FakeTransport(response={
            "r": json.dumps({"ok": False, "error": "boom traceback"}),
            "ok": True})
        pty = RemotePty(tr, timeout=2)
        with self.assertRaises(RemoteOpError):
            pty.open(24, 80, sid="pty-operr-1",
                     in_topic="pty/pty-operr-1/in",
                     out_topic="pty/pty-operr-1/out", req_timeout=2)
        self.assertIn(("unsub", "pty/pty-operr-1/out"), tr.events)

    def test_close_sends_stop_and_unsubscribes_without_stopping_transport(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-close-1")
        pty.close()
        in_topic = "pty/pty-close-1/in"
        self.assertTrue(any(f.get("stop") for _, f in tr.published
                            if _ == in_topic))
        self.assertIn(("unsub", "pty/pty-close-1/out"), tr.events)
        self.assertIsNone(pty.sid)

    def test_broker_status_without_node_falls_back_to_config_count(self):
        online, total, hosts = pcm._broker_status(FakeTransport())
        self.assertEqual((online, hosts), (0, []))
        self.assertEqual(total, len(BROKER_LIST))

    def test_broker_status_counts_only_connected_clients(self):
        from types import SimpleNamespace

        class _Cli:
            def __init__(self, up):
                self._up = up

            def is_connected(self):
                return self._up

        class _Boom:
            def is_connected(self):
                raise RuntimeError("paho 内部异常也不能炸统计")

        net = SimpleNamespace(clients={
            "up-a": _Cli(True), "down-b": _Cli(False), "boom": _Boom()})
        tr = SimpleNamespace(node=SimpleNamespace(mqtt_net=net))
        online, total, hosts = pcm._broker_status(tr)
        self.assertEqual((online, total), (1, 3))
        self.assertEqual(hosts, ["up-a"])


class PtyCliPreCommandTests(unittest.TestCase):
    """SSH 式前置命令：位置参数解析与拼接。"""

    def test_no_command_defaults_empty(self):
        args = pcm.build_parser().parse_args([])
        self.assertEqual(args.command, [])
        self.assertEqual(pcm._join_pre_command(args.command), "")

    def test_quoted_command_after_options(self):
        args = pcm.build_parser().parse_args(
            ["-t", "q", "-k", "123+456", "tmux at"])
        self.assertEqual(pcm._join_pre_command(args.command), "tmux at")

    def test_unquoted_words_joined_like_ssh(self):
        args = pcm.build_parser().parse_args(["tmux", "at"])
        self.assertEqual(pcm._join_pre_command(args.command), "tmux at")

    def test_command_own_dash_options_are_not_consumed_by_client(self):
        # REMAINDER：tmux attach -d 里的 -d 是给远端命令的，不能报无法识别
        args = pcm.build_parser().parse_args(["tmux", "attach", "-d", "-t", "x"])
        self.assertEqual(pcm._join_pre_command(args.command),
                         "tmux attach -d -t x")

    def test_double_dash_separator_stripped(self):
        self.assertEqual(pcm._join_pre_command(["--", "tmux at"]), "tmux at")
        self.assertEqual(pcm._join_pre_command(None), "")

    def test_legacy_option_names_still_parse(self):
        # 改名前的硬编码选项一个都不能丢
        a = pcm.build_parser().parse_args(
            ["--shell", "/bin/sh", "--term", "xterm", "--cwd", "/root",
             "--interval", "1", "--no-login", "--ttl", "3600",
             "--heartbeat", "3", "--dead-timeout", "9", "--size", "24x100",
             "--detach-key", "f9", "--menu-key", "f10",
             "--port", "0", "--host", "127.0.0.1"])
        self.assertEqual((a.shell, a.term, a.cwd, a.interval, a.no_login,
                          a.ttl, a.heartbeat, a.dead_timeout, a.size,
                          a.detach_key, a.menu_key, a.port, a.host),
                         ("/bin/sh", "xterm", "/root", 1.0, True, 3600.0,
                          3.0, 9.0, "24x100", "f9", "f10", 0, "127.0.0.1"))

    def test_alias_option_names_share_one_table(self):
        # 下划线/短横/短别名全部由 alias 表生成
        a = pcm.build_parser().parse_args(
            ["--sh", "/bin/bash", "--hb", "2", "--dead_timeout", "7",
             "--nologin", "--geometry", "20x80", "-i", "0.5", "-p", "0"])
        self.assertEqual((a.shell, a.heartbeat, a.dead_timeout, a.no_login,
                          a.size, a.interval, a.port),
                         ("/bin/bash", 2.0, 7.0, True, "20x80", 0.5, 0))


class BinaryStdoutGuardTests(unittest.TestCase):
    """RPC 执行期间 redirect_stdout(StringIO()) 不能再搞崩 PTY 渲染。"""

    def setUp(self):
        self._saved = pcm._BIN_STDOUT_CACHE
        pcm._BIN_STDOUT_CACHE = None

    def tearDown(self):
        pcm._BIN_STDOUT_CACHE = self._saved

    def test_falls_back_to_real_stdout_under_stringio_redirect(self):
        import io
        real = sys.stdout
        try:
            sys.stdout = io.StringIO()  # ai_bridge RPC 捕获 print 的情形
            buf = pcm._bin_stdout()
            self.assertIsNotNone(buf)
            # 必须是解释器原始 stdout 的二进制缓冲，而不是 StringIO
            self.assertIs(buf, getattr(real, "buffer", None)
                          or sys.__stdout__.buffer)
            # 复位序列此时也必须能写出，不抛异常
            pcm._terminal_cleanup()
        finally:
            sys.stdout = real

    def test_normal_stdout_resolves(self):
        pcm._BIN_STDOUT_CACHE = None
        self.assertIs(pcm._bin_stdout(), sys.stdout.buffer)


class PtyHotkeyParseTests(unittest.TestCase):
    """按键名 -> 终端字节序列，及反查显示名。"""

    def test_single_byte_control_keys(self):
        self.assertEqual(pcm._parse_key_spec("ctrl-]"), b"\x1d")
        self.assertEqual(pcm._parse_key_spec("ctrl-a"), b"\x01")
        self.assertEqual(pcm._parse_key_spec("ctrl-\\"), b"\x1c")
        # 默认值就是这个原始控制字符，必须保持兼容
        self.assertEqual(pcm._parse_key_spec("\x1d"), b"\x1d")

    def test_modified_insert_xterm_encoding(self):
        self.assertEqual(pcm._parse_key_spec("ctrl-alt-insert"),
                         b"\x1b[2;7~")
        self.assertEqual(pcm._parse_key_spec("shift+insert"), b"\x1b[2;2~")
        self.assertEqual(pcm._parse_key_spec("CTRL_ALT_INSERT"), b"\x1b[2;7~")
        self.assertEqual(pcm._parse_key_spec("ctrl-alt-shift-insert"),
                         b"\x1b[2;8~")

    def test_function_and_cursor_keys(self):
        self.assertEqual(pcm._parse_key_spec("f1"), b"\x1bOP")
        self.assertEqual(pcm._parse_key_spec("f9"), b"\x1b[20~")
        self.assertEqual(pcm._parse_key_spec("insert"), b"\x1b[2~")
        self.assertEqual(pcm._parse_key_spec("up"), b"\x1b[A")
        self.assertEqual(pcm._parse_key_spec("ctrl-left"), b"\x1b[1;5D")

    def test_disabled_and_unknown(self):
        self.assertEqual(pcm._parse_key_spec("none"), b"")
        self.assertEqual(pcm._parse_key_spec("off"), b"")
        self.assertEqual(pcm._parse_key_spec(None), b"")
        self.assertEqual(pcm._parse_key_spec("xyz"), b"xyz")

    def test_describe_roundtrip(self):
        self.assertEqual(pcm._describe_key(b"\x1d"), "ctrl-]")
        self.assertEqual(pcm._describe_key(b"\x1b[2;7~"), "ctrl-alt-insert")
        self.assertEqual(pcm._describe_key(b""), "禁用")
        self.assertEqual(pcm._parse_key_spec(
            "ctrl-alt-insert") in pcm._KEY_DISPLAY, True)


class PtyInputTranscodeTests(unittest.TestCase):
    """Windows VT 输入流按输入 CP 增量转 UTF-8：修右键粘贴中文乱码。"""

    def test_gbk_full_chunk(self):
        feed = pcm._new_input_transcoder(936)
        out = feed("导航Abc".encode("gbk") + b"\r")
        self.assertEqual(out, "导航Abc\r".encode("utf-8"))

    def test_gbk_split_across_reads(self):
        # 双字节汉字被两次 read 从中间切开时不能出 U+FFFD
        feed = pcm._new_input_transcoder(936)
        raw = "导航".encode("gbk")  # b5 bc ba bd
        self.assertEqual(feed(raw[:1]), b"")
        self.assertEqual(feed(raw[1:2]), "导".encode("utf-8"))
        self.assertEqual(feed(raw[2:3]), b"")
        self.assertEqual(feed(raw[3:]), "航".encode("utf-8"))

    def test_vt_sequences_ascii_transparent(self):
        # 热键/方向键等纯 ASCII VT 序列在 GBK 转码后必须逐字节不变
        feed = pcm._new_input_transcoder(936)
        raw = b"\x1b[2;2~" + "中文".encode("gbk") + b"\x1b[C\r"
        self.assertEqual(
            feed(raw),
            b"\x1b[2;2~" + "中文".encode("utf-8") + b"\x1b[C\r")

    def test_utf8_passthrough_and_split(self):
        feed = pcm._new_input_transcoder(65001)
        raw = "中文".encode("utf-8")  # e4 b8 ad e6 96 87
        self.assertEqual(feed(raw[:2]), b"")
        self.assertEqual(feed(raw[2:]), "中文".encode("utf-8"))

    def test_unknown_cp_fallback_utf8(self):
        feed = pcm._new_input_transcoder(99999)
        self.assertEqual(feed("A".encode("utf-8")), b"A")


class PtyClipboardPasteTests(unittest.TestCase):
    """Shift+Insert 本地粘贴：序列常量与文本规范化。"""

    def test_paste_seq_matches_conhost_emission(self):
        # conhost VT 输入下 Shift+Insert 上报的正是 xterm 的 CSI 2;2~
        self.assertEqual(pcm._PASTE_KEY_SEQ,
                         pcm._parse_key_spec("shift+insert"))
        # 普通 Insert 不能被当成粘贴
        self.assertNotEqual(pcm._PASTE_KEY_SEQ, b"\x1b[2~")

    def test_normalize_newlines_and_utf8(self):
        self.assertEqual(pcm._normalize_paste_text("a\r\nb\nc\rd"),
                         b"a\rb\rc\rd")
        self.assertEqual(pcm._normalize_paste_text("导航"),
                         "导航".encode("utf-8"))
        self.assertEqual(pcm._normalize_paste_text(""), b"")

    @unittest.skipUnless(sys.platform == "win32", "Windows only")
    def test_clipboard_roundtrip(self):
        import subprocess
        try:
            subprocess.run(
                ["powershell", "-NoProfile", "-Command",
                 "Set-Clipboard -Value '剪贴板中文Abc'"],
                check=True, capture_output=True, timeout=20)
        except Exception as exc:
            self.skipTest("Set-Clipboard unavailable: %r" % exc)
        self.assertEqual(pcm._read_clipboard_text_win(), "剪贴板中文Abc")


class LocalLogBufferTests(unittest.TestCase):
    """本地日志环形缓冲：只进内存缓冲，RPC 模式下绝不镜像写 stderr。"""

    def setUp(self):
        pcm._LOCAL_LOG.clear()
        self._mirror = pcm._LOG_MIRROR_STDERR
        pcm._LOG_MIRROR_STDERR = False

    def tearDown(self):
        pcm._LOG_MIRROR_STDERR = self._mirror
        pcm._LOCAL_LOG.clear()

    def test_ring_tail_and_clear(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)
        for i in range(5):
            ring.write("line%d\n" % i)
        self.assertEqual(ring.tail(2), "line3\nline4")
        self.assertEqual(ring.tail(0), "line0\nline1\nline2\nline3\nline4")
        self.assertEqual(len(ring), 5)
        ring.clear()
        self.assertEqual(ring.tail(10), "")
        self.assertEqual(len(ring), 0)

    def test_ring_eviction_by_chars_and_lines(self):
        ring = pcm._LogRing(max_chars=30, max_lines=1000)
        for i in range(20):
            ring.write("x%02d\n" % i)
        text = ring.tail(0)
        self.assertNotIn("x00", text)  # 超字符上限，最旧的已被丢掉
        self.assertIn("x19", text)
        ring2 = pcm._LogRing(max_chars=10 ** 6, max_lines=3)
        for i in range(5):
            ring2.write("y%d\n" % i)
        self.assertEqual(ring2.tail(0), "y2\ny3\ny4")

    def test_ring_accepts_bytes_and_splits_multiline(self):
        ring = pcm._LogRing()
        ring.write(b"a\nb\r\nc\rd")
        self.assertEqual(ring.tail(0), "a\nb\nc\nd")

    def test_long_line_is_clipped(self):
        ring = pcm._LogRing()
        ring.write("a" * (pcm._LOG_LINE_CLIP + 50) + "\n")
        out = ring.tail(1)
        self.assertIn("已截断", out)
        self.assertLessEqual(len(out), pcm._LOG_LINE_CLIP + 40)

    def test_get_log_and_clear_log_helpers(self):
        pcm._emit_local("hello-local-log\n")
        self.assertIn("hello-local-log", pcm.get_log())
        r = pcm.clear_log()
        self.assertTrue(r["ok"] and r["cleared"])
        self.assertEqual(pcm.get_log(), "")

    def test_emit_does_not_touch_stderr_in_rpc_mode(self):
        calls = []
        orig = pcm._stderr_write
        pcm._stderr_write = lambda text: calls.append(text)
        try:
            pcm._emit_local("quiet-line\n")
        finally:
            pcm._stderr_write = orig
        self.assertEqual(calls, [])  # 终端零写入
        self.assertIn("quiet-line", pcm.get_log())

    def test_emit_mirrors_stderr_without_rpc(self):
        pcm._LOG_MIRROR_STDERR = True
        calls = []
        orig = pcm._stderr_write
        pcm._stderr_write = lambda text: calls.append(text)
        try:
            pcm._emit_local("loud-line\n")
        finally:
            pcm._stderr_write = orig
        self.assertEqual(calls, ["loud-line\n"])
        self.assertIn("loud-line", pcm.get_log())

    def test_install_ring_logging_reroutes_and_idempotent(self):
        import logging as pylog
        root = pylog.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        probe = io.StringIO()
        root.addHandler(pylog.StreamHandler(probe))
        try:
            pcm._install_ring_logging()
            self.assertEqual(
                sum(isinstance(h, pcm._RingLogHandler) for h in root.handlers), 1)
            self.assertFalse(
                any(isinstance(h, pylog.StreamHandler) for h in root.handlers))
            pylog.getLogger("MultiMQTT.ring_test").info("ring-logging-marker-77")
            self.assertNotIn("ring-logging-marker-77", probe.getvalue())
            self.assertIn("ring-logging-marker-77", pcm.get_log())
            # 幂等：再装一次不得叠加 handler
            pcm._install_ring_logging()
            self.assertEqual(
                sum(isinstance(h, pcm._RingLogHandler) for h in root.handlers), 1)
        finally:
            for h in list(root.handlers):
                if h not in saved_handlers:
                    root.removeHandler(h)
            for h in saved_handlers:
                if h not in root.handlers:
                    root.addHandler(h)
            root.setLevel(saved_level)


class PtyMagicBarTests(unittest.TestCase):
    """本地魔术命令栏：只调时间参数/本地动作，不碰 topic。"""

    def setUp(self):
        # 单测里绝不允许 _emit_local 镜像写真实终端
        self._mirror = pcm._LOG_MIRROR_STDERR
        pcm._LOG_MIRROR_STDERR = False

    def tearDown(self):
        pcm._LOG_MIRROR_STDERR = self._mirror
        pcm._LOCAL_LOG.clear()

    def _ctx(self):
        tr = FakeTransport()
        pty, _, _, _ = _open_pty(tr, "pty-magic-1",
                                 env_extra={"owner": "win-magic"})
        self.addCleanup(lambda: pty.close())
        live = {"interval": 0.0, "heartbeat": 5.0, "ttl": 43200.0,
                "dead_timeout": 15.0}
        return tr, pty, {"pty": pty, "transport": tr, "live": live}

    def test_detach_aliases(self):
        # exit/quit 必须与 client_mqtt 是同一张表；x 是 PTY 侧自定义别名
        for word in ("detach", "exit", "quit", "q", "d", "x"):
            _, pty, ctx = self._ctx()
            do_detach, lines = pcm._run_magic(word, ctx)
            self.assertTrue(do_detach, word)
            self.assertTrue(lines)

    def test_help_and_blank(self):
        _, _, ctx = self._ctx()
        do_detach, lines = pcm._run_magic("help", ctx)
        self.assertFalse(do_detach)
        joined = "\n".join(lines)
        self.assertIn("detach", joined)
        self.assertIn("heartbeat", joined)
        self.assertNotIn("topic", joined)  # 刻意不提供 topic/key 设置
        self.assertEqual(pcm._run_magic("   ", ctx), (False, []))

    def test_shared_alias_tables_reuse_client_mqtt(self):
        # 能复用 client_mqtt 的别名不许在 PTY 侧搞第二份：
        # exit/quit、status/state/s、help/h/? 必须就是同一张表
        from client import client_mqtt as cm
        for word in cm.alias_exit:
            self.assertIn(word, pcm.alias_detach)
        self.assertIn("status", cm.alias_status)
        _, _, ctx = self._ctx()
        self.assertTrue(pcm._run_magic("state", ctx)[0] is False)
        self.assertIn("broker", "\n".join(pcm._run_magic("state", ctx)[1]))
        self.assertTrue(pcm._run_magic("?", ctx)[1])

    def test_dash_underscore_command_equivalence(self):
        _, _, ctx = self._ctx()
        pcm._run_magic("dead-timeout 7", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 7.0)
        pcm._run_magic("dead_timeout 0", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 0.0)

    def test_remote_params_publish_set_and_clamp(self):
        tr, pty, ctx = self._ctx()
        n0 = len(tr.published)
        do_detach, lines = pcm._run_magic("interval 999", ctx)
        self.assertFalse(do_detach)
        self.assertEqual(ctx["live"]["interval"], 60.0)  # 上限夹取
        self.assertEqual(tr.published[n0][1]["set"], {"interval": 60.0})
        pcm._run_magic("heartbeat 0", ctx)
        self.assertEqual(ctx["live"]["heartbeat"], 0.0)
        pcm._run_magic("ttl 10", ctx)
        self.assertEqual(ctx["live"]["ttl"], 60.0)  # 下限夹取

    def test_local_dead_timeout_no_frame(self):
        tr, _, ctx = self._ctx()
        n0 = len(tr.published)
        pcm._run_magic("dead 3", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 3.0)
        pcm._run_magic("dead 0", ctx)
        self.assertEqual(ctx["live"]["dead_timeout"], 0.0)
        # 纯本地参数不下发任何帧
        self.assertEqual(len(tr.published), n0)

    def test_bad_number_and_unknown_command(self):
        _, _, ctx = self._ctx()
        _, lines = pcm._run_magic("interval abc", ctx)
        self.assertTrue(any("数字" in x for x in lines))
        _, lines = pcm._run_magic("topic x", ctx)
        self.assertTrue(any("未知" in x for x in lines))

    def test_status_reports_session(self):
        _, pty, ctx = self._ctx()
        _, lines = pcm._run_magic("status", ctx)
        joined = "\n".join(lines)
        self.assertIn("pty-magic-1", joined)
        self.assertIn("broker", joined)

    def test_log_command_reads_ring_and_stays_local(self):
        _, _, ctx = self._ctx()
        pcm._emit_local("magic-ring-marker-42\n")
        do_detach, lines = pcm._run_magic("log", ctx)
        joined = "\n".join(lines)
        self.assertFalse(do_detach)
        self.assertIn("magic-ring-marker-42", joined)
        self.assertIn("get_log", joined)  # 提示 HTTP RPC 入口

    def test_log_aliases_and_bad_argument(self):
        _, _, ctx = self._ctx()
        for word in ("log", "logs"):
            do_detach, lines = pcm._run_magic(word, ctx)
            self.assertFalse(do_detach, word)
            self.assertTrue(lines, word)
        _, lines = pcm._run_magic("log abc", ctx)
        self.assertTrue(any("数字" in x for x in lines))

    def test_log_command_capped_at_100_lines(self):
        _, _, ctx = self._ctx()
        for i in range(150):
            pcm._emit_local("cap-line-%03d\n" % i)
        _, lines = pcm._run_magic("log 1000", ctx)  # 超上限夹到 100
        self.assertEqual(len(lines), 101)           # 1 行头 + 100 行日志
        self.assertIn("cap-line-149", "\n".join(lines))
        self.assertNotIn("cap-line-000", "\n".join(lines))

    def test_redraw_publishes_redraw_frame(self):
        tr, _, ctx = self._ctx()
        n0 = len(tr.published)
        do_detach, lines = pcm._run_magic("redraw", ctx)
        self.assertFalse(do_detach)
        self.assertTrue(any("重绘" in x for x in lines))
        # configure(redraw=True) 只发 set 控制帧，不向 shell 写命令
        self.assertTrue(any(
            pub[1].get("set", {}).get("redraw") is True
            for pub in tr.published[n0:]))

    def test_redraw_aliases(self):
        for word in ("redraw", "refresh", "rd"):
            _, _, ctx = self._ctx()
            do_detach, lines = pcm._run_magic(word, ctx)
            self.assertFalse(do_detach, word)
            self.assertTrue(lines, word)


# ============================ 2. 模板静态检查（全平台） ============================

class PtyTemplateStaticTests(unittest.TestCase):

    def setUp(self):
        self.code = build_pty_start_code({
            "sid": "pty-static-1",
            "in_topic": "pty/pty-static-1/in",
            "out_topic": "pty/pty-static-1/out",
            "rows": 24, "cols": 80, "heartbeat": 0.0})

    def test_template_compiles(self):
        compile(self.code, "<pty-template>", "exec")

    def test_payload_is_embedded(self):
        self.assertIn("pty-static-1", self.code)
        self.assertIn("pty/pty-static-1/in", self.code)
        self.assertIn("pty/pty-static-1/out", self.code)
        # __PAYLOAD__ 占位符必须已被完全替换
        self.assertNotIn("__PAYLOAD__", self.code)

    def test_router_idempotency_marker_present(self):
        # RecursionError 回归：必须用独立布尔标记，禁止再用空 dict 判空
        self.assertIn("_cmq_pty_installed", self.code)
        self.assertNotIn('if not getattr(_net, "_cmq_pty_router", None):',
                         self.code)

    def test_router_reentry_guard_present(self):
        self.assertIn("_rlocal", self.code)

    def test_cwd_fallback_present(self):
        self.assertIn("_cwd_warn", self.code)
        self.assertIn("cwd_warning", self.code)

    def test_uplink_iseq_monotonic_gate_present(self):
        # iseq 单调闸门：既去多 broker 重复，也拦乱序迟到的旧帧（旧 winsz
        # 晚到覆盖新尺寸会让远端 PTY 比本地窗口宽，进度条全线错位）
        self.assertIn("_iseq_last", self.code)
        self.assertIn("_iq <= _iseq_last", self.code)
        self.assertNotIn("_iseq_recent", self.code)
        self.assertIn('"iseq"', self.code.replace("'", '"'))

    def test_live_set_params_present(self):
        # 会话内热调：_live 共享 dict + set 帧夹取，只允许时间参数
        self.assertIn("_live", self.code)
        self.assertIn('_fr.get("set")', self.code)
        self.assertIn('float(_live["interval"])', self.code)
        self.assertIn('_live.get("heartbeat"', self.code)
        # redraw：set 帧主动给前台进程组发 SIGWINCH 强制全屏重绘
        self.assertIn("_sg.SIGWINCH", self.code)
        self.assertIn('_ss.get("redraw")', self.code)
        # 不允许借 set 帧改 topic / shell 等
        self.assertNotIn('"in_topic": _ss', self.code)

    def test_owner_arbitration_present(self):
        # 多应答者归属仲裁：进程稳定 uid、下行帧盖 owner、外来 owner 帧
        # 让影子 PTY 自杀、同进程同 sid 幂等注册表
        self.assertIn("_cmq_server_uid", self.code)
        self.assertIn("_cmq_pty_sessions", self.code)
        self.assertIn("claim_lost", self.code)
        self.assertIn("_ow != _uid", self.code)
        self.assertIn('"owner": _uid', self.code)
        self.assertIn('"owner": _uid, "host": _host', self.code)


# ============================ 3. 服务端模板真机测试（仅 POSIX） ============================

if IS_POSIX:

    class FakeNet:
        """模拟服务端 gms.mqtt_net：记录广播、保存唯一消息回调。"""

        def __init__(self):
            self.message_callback = None
            self.subscribed = []
            self.published = []
            self._lock = threading.Lock()

        def set_on_message(self, cb):
            self.message_callback = cb

        def subscribe(self, topic):
            self.subscribed.append(topic)

        def publish_broadcast(self, topic, payload):
            with self._lock:
                self.published.append((topic, dict(payload)))

        def deliver(self, topic, data):
            """模拟某个 broker 把帧送到分发线程。"""
            self.message_callback(topic, data, "fakebroker")

        def frames(self, topic):
            with self._lock:
                return [(t, dict(d)) for t, d in self.published if t == topic]

    class FakeServer:
        """模拟 server_mqtt.MQTTServer：持有 mqtt_net + handle_message。"""

        def __init__(self, net):
            self.mqtt_net = net
            self.base_calls = []

        def handle_message(self, topic, data, broker):
            self.base_calls.append((topic, data, broker))

    def _wait_for(pred, timeout=5.0, step=0.05):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(step)
        return False

    def _out_text(net, sid):
        return "".join(d.get("d", "")
                       for _, d in net.frames("pty/%s/out" % sid)
                       if "d" in d)

    def _end_frame(net, sid):
        for _, d in net.frames("pty/%s/out" % sid):
            if d.get("end"):
                return d
        return None

    @unittest.skipUnless(IS_POSIX, "openpty/fork 只能在 POSIX 上真机测试")
    class PtyTemplateLiveTests(unittest.TestCase):

        def setUp(self):
            # 每个用例一个全新的"服务端进程"：独立持久命名空间 + net + gms
            self.net = FakeNet()
            self.gms = FakeServer(self.net)
            self.ns = {"__name__": "__rpc_exec__", "gms": self.gms}
            self._sessions = []

        def tearDown(self):
            # 用例失败也要尽量收尸，避免残留 1h TTL 的 shell
            for i, sid in enumerate(self._sessions):
                try:
                    self._send(sid, {"iseq": 900000 + i, "stop": True})
                except Exception:
                    pass

        def _start(self, sid, **over):
            in_t, out_t = "pty/%s/in" % sid, "pty/%s/out" % sid
            payload = {
                "sid": sid, "in_topic": in_t, "out_topic": out_t,
                "rows": 24, "cols": 80, "shell": "/bin/sh", "term": "xterm",
                "cwd": None, "login": False, "flush_interval": 0.0,
                "ttl": 3600, "frame_max": PTY_FRAME_MAX, "heartbeat": 0.0,
            }
            payload.update(over)
            resp = PythonExecutor(globals=self.ns).execute(
                build_pty_start_code(payload))
            self.assertTrue(resp["ok"],
                            resp.get("stdout", "") + resp.get("error", ""))
            env = json.loads(resp["r"])
            self.assertTrue(env["ok"], env.get("error"))
            self._sessions.append(sid)
            return env

        def _send(self, sid, partial):
            frame = {"pty": sid}
            frame.update(partial)
            self.net.deliver("pty/%s/in" % sid, frame)

        def _type(self, sid, iseq, text, owner=None):
            fr = {"iseq": iseq, "k": text}
            if owner is not None:
                fr["owner"] = owner
            self._send(sid, fr)

        def _assert_echo(self, sid, marker, iseq, timeout=5.0, owner=None):
            self._type(sid, iseq, "echo %s\r" % marker, owner=owner)
            self.assertTrue(
                _wait_for(lambda: marker in _out_text(self.net, sid), timeout),
                "PTY 未在 %.0fs 内回显 %s，实际输出: %r"
                % (timeout, marker, _out_text(self.net, sid)))

        def _stop(self, sid, iseq=9999, owner=None):
            fr = {"iseq": iseq, "stop": True}
            if owner is not None:
                fr["owner"] = owner
            self._send(sid, fr)
            self.assertTrue(
                _wait_for(lambda: _end_frame(self.net, sid) is not None, 5.0),
                "stop 后未收到 end 帧")
            end = _end_frame(self.net, sid)
            self.assertEqual(end.get("reason"), "stopped")

        # ---- 基础数据面：回显、iseq 去重、winsz、stop ----

        def test_echo_roundtrip_and_stop_end_frame(self):
            sid = "pty-live-echo"
            env = self._start(sid)
            self.assertEqual(env["shell"], "/bin/sh")
            self.assertEqual(env["rows"], 24)
            self._assert_echo(sid, "ZZECHO42", iseq=0)
            self._stop(sid, iseq=1)

        def test_uplink_iseq_dedup_drops_broker_duplicates(self):
            sid = "pty-live-dedup"
            self._start(sid)
            self._assert_echo(sid, "ZZECHO43", iseq=0)
            # 同一帧（同 iseq）被 15 个 broker 各投递一次：只应执行一遍。
            # 执行一遍时，终端回显命令行 + 命令输出，marker 恰好出现 2 次。
            for _ in range(15):
                self._type(sid, 5, "echo ZZDUP42\r")
            self.assertTrue(
                _wait_for(lambda: _out_text(self.net, sid).count("ZZDUP42") >= 2),
                "首帧未放行")
            time.sleep(0.8)  # 给迟到副本留足作恶时间
            self.assertEqual(_out_text(self.net, sid).count("ZZDUP42"), 2,
                             "重复 iseq 帧被放行了：%r"
                             % _out_text(self.net, sid))
            self._stop(sid, iseq=6)

        def test_winsz_frame_is_accepted_and_session_keeps_working(self):
            sid = "pty-live-winsz"
            self._start(sid)
            self._send(sid, {"iseq": 1, "winsz": [30, 100]})
            time.sleep(0.3)
            # 改尺寸后会话必须仍然活着、能正常读写
            self._assert_echo(sid, "ZZSIZE42", iseq=2)
            self._stop(sid, iseq=3)

        def test_winsz_out_of_order_older_size_must_not_win(self):
            # 界面错乱回归：拖窗口时慢 broker 把旧的（更大）winsz 晚投递到，
            # 服务端 iseq 单调闸门必须丢掉它，PTY 最终尺寸以新帧为准。
            sid = "pty-live-winsz-reorder"
            self._start(sid)
            self._send(sid, {"iseq": 1, "winsz": [40, 200]})
            self._send(sid, {"iseq": 2, "winsz": [20, 100]})
            # iseq=1 的旧尺寸再经慢 broker 送达（重复/乱序迟到同一处理路径）
            self._send(sid, {"iseq": 1, "winsz": [40, 200]})
            time.sleep(0.3)
            self._type(sid, 3, "stty size\r")
            self.assertTrue(
                _wait_for(lambda: "20 100" in _out_text(self.net, sid)),
                "PTY 实际尺寸不是新帧的 20x100: %r"
                % _out_text(self.net, sid))
            self.assertNotIn("40 200", _out_text(self.net, sid),
                             "旧 winsz 帧乱序迟到后覆盖了新尺寸")
            self._stop(sid, iseq=4)

        # ---- 多应答者归属仲裁（owner/claim） ----

        def test_start_env_and_downlink_frames_carry_owner(self):
            sid = "pty-live-owner"
            env = self._start(sid)
            self.assertTrue(env.get("owner"))
            self.assertEqual(len(env["owner"]), 12)  # 6 字节 urandom.hex
            self.assertTrue(env.get("host"))
            self._assert_echo(sid, "ZZOWNER42", iseq=0)
            # 输出/心跳/end 全部盖 owner，客户端才能按属主过滤影子流
            frames = self.net.frames("pty/%s/out" % sid)
            self.assertTrue(frames)
            self.assertTrue(all(d.get("owner") == env["owner"]
                                for _, d in frames))
            self._stop(sid, iseq=1, owner=env["owner"])
            self.assertEqual(_end_frame(self.net, sid).get("owner"),
                             env["owner"])

        def test_winner_claim_is_harmless_foreign_claim_kills_shadow(self):
            sid = "pty-live-claim"
            env = self._start(sid)
            owner = env["owner"]
            # 赢家自己的 claim 帧绝不能误杀会话
            self._send(sid, {"iseq": 1, "owner": owner, "claim": True})
            time.sleep(0.3)
            self.assertIsNone(_end_frame(self.net, sid))
            self._assert_echo(sid, "ZZCLAIMOK", iseq=2, owner=owner)
            # 外来 owner（同时应答的另一进程）→ 本 PTY 判定自己是影子，
            # 立即 claim_lost 结束并停止往同一 out topic 推流
            self._send(sid, {"iseq": 3, "owner": "ffffffdeadbe",
                             "claim": True})
            self.assertTrue(
                _wait_for(lambda: _end_frame(self.net, sid) is not None),
                "影子 PTY 未在收到外来 owner 帧后自杀")
            self.assertEqual(_end_frame(self.net, sid).get("reason"),
                             "claim_lost")

        def test_set_frame_hot_tunes_heartbeat_without_reconnect(self):
            sid = "pty-live-set"
            env = self._start(sid)  # 初始 heartbeat=0
            self._send(sid, {"iseq": 0, "owner": env["owner"],
                             "set": {"heartbeat": 1}})
            try:
                self.assertTrue(
                    _wait_for(
                        lambda: any(d.get("hb") is not None
                                    for _, d in self.net.frames(
                                        "pty/%s/out" % sid)),
                        timeout=5.0),
                    "set 热调 heartbeat 后未收到心跳帧")
                # 非法值被忽略，会话照常工作
                self._send(sid, {"iseq": 1, "owner": env["owner"],
                                 "set": {"heartbeat": "abc"}})
                self._assert_echo(sid, "ZZSETOK", iseq=2, owner=env["owner"])
            finally:
                self._stop(sid, iseq=3, owner=env["owner"])

        def test_duplicate_handshake_same_sid_returns_existing_session(self):
            # 同进程内同一 sid 重复握手（任何重复执行兜底）：返回既有会话，
            # 不得再开第二个往同 topic 推流的 PTY
            sid = "pty-live-dup"
            in_t, out_t = "pty/%s/in" % sid, "pty/%s/out" % sid
            payload = {
                "sid": sid, "in_topic": in_t, "out_topic": out_t,
                "rows": 24, "cols": 80, "shell": "/bin/sh", "term": "xterm",
                "cwd": None, "login": False, "flush_interval": 0.0,
                "ttl": 3600, "frame_max": PTY_FRAME_MAX, "heartbeat": 0.0,
            }
            env1 = self._start(sid)
            resp2 = PythonExecutor(globals=self.ns).execute(
                build_pty_start_code(payload))
            self.assertTrue(resp2["ok"])
            env2 = json.loads(resp2["r"])
            self.assertTrue(env2["ok"])
            self.assertEqual(env2, env1)  # 同一个会话（含同一 pid/owner）
            # 会话照常工作
            self._assert_echo(sid, "ZZDUPHAND", iseq=0, owner=env1["owner"])
            self._stop(sid, iseq=1, owner=env1["owner"])

        def test_in_topic_is_subscribed_and_routed_off_rpc_path(self):
            sid = "pty-live-route"
            self._start(sid)
            self.assertIn("pty/%s/in" % sid, self.net.subscribed)
            # PTY topic 的帧绝不能漏到服务端 RPC 处理器
            self._type(sid, 0, "echo ZZROUTE\r")
            self.assertTrue(
                _wait_for(lambda: "ZZROUTE" in _out_text(self.net, sid)))
            time.sleep(0.3)
            self.assertEqual(self.gms.base_calls, [])
            self._stop(sid, iseq=1)

        # ---- 心跳 ----

        def test_heartbeat_frames_emitted_at_interval(self):
            sid = "pty-live-hb"
            self._start(sid, heartbeat=0.2)
            hb_count = lambda: sum(
                1 for _, d in self.net.frames("pty/%s/out" % sid)
                if d.get("hb") is not None)
            self.assertTrue(_wait_for(lambda: hb_count() >= 2, timeout=3.0),
                            "3s 内应至少收到 2 个心跳帧")
            self._stop(sid, iseq=1)

        # ---- cwd 容错 ----

        def test_missing_cwd_falls_back_instead_of_failing(self):
            sid = "pty-live-cwd"
            bad = "/no/such/cwd_xyz_%d" % os.getpid()
            env = self._start(sid, cwd=bad)
            # 会话正常建立，而不是 RemoteOpError 把客户端打退出
            self.assertTrue(env["ok"])
            self.assertTrue(env["cwd_warning"])
            self.assertIn(bad, env["cwd_warning"])
            self.assertTrue(os.path.isdir(env["cwd"]))
            # shell 真的跑在回退目录里
            self._type(sid, 0, "pwd\r")
            self.assertTrue(
                _wait_for(lambda: env["cwd"] in _out_text(self.net, sid)),
                "pwd 输出不含回退目录 %s: %r"
                % (env["cwd"], _out_text(self.net, sid)))
            self._stop(sid, iseq=1)

        # ---- RecursionError 回归：重复握手 ----

        def test_repeated_handshake_does_not_rewrap_callback(self):
            s1, s2 = "pty-live-restart-1", "pty-live-restart-2"
            self._start(s1)
            cb = self.net.message_callback
            router_dict = self.net._cmq_pty_router
            chain_base = self.net._cmq_pty_orig
            self.assertTrue(self.net._cmq_pty_installed)

            # 两个会话短暂共存：路由表里各占一项，回调仍是同一个
            self._start(s2)
            self.assertIs(self.net.message_callback, cb,
                          "第二次握手重新 set_on_message 包裹了回调")
            self.assertIs(self.net._cmq_pty_router, router_dict,
                          "路由 dict 被替换")
            self.assertIs(self.net._cmq_pty_orig, chain_base,
                          "回调链底被覆盖")
            self.assertIn("pty/%s/in" % s1, router_dict)
            self.assertIn("pty/%s/in" % s2, router_dict)

            # 两个会话互不干扰
            self._assert_echo(s1, "ZZONE42", iseq=0)
            self._assert_echo(s2, "ZZTWO42", iseq=0)

            # 非 PTY topic 必须沿链底到达服务端 RPC 处理器，且不递归
            self.net.deliver("sys/device/request",
                             {"req_id": "r1", "code": "1+1"})
            self.assertEqual(len(self.gms.base_calls), 1)
            self.assertEqual(self.gms.base_calls[0][0], "sys/device/request")

            self._stop(s1, iseq=1)
            # 旧会话收尸后路由项摘除，但回调身份依然不变
            self.assertTrue(
                _wait_for(lambda: "pty/%s/in" % s1 not in router_dict, 3.0))
            self.assertIs(self.net.message_callback, cb)
            self._stop(s2, iseq=1)
            self.net.deliver("sys/device/request",
                             {"req_id": "r2", "code": "2+2"})
            self.assertEqual(len(self.gms.base_calls), 2)

        # ---- 旧版坏链路自愈 ----

        def test_heals_legacy_self_wrapping_router_chain(self):
            sid = "pty-live-heal"

            # 手工复刻旧版 bug：空 dict 触发重装、orig 指向自己的闭环
            def buggy(topic, data, broker):
                q = self.net._cmq_pty_router.get(topic)
                if q is not None and isinstance(data, dict):
                    q.put(data)
                    return None
                return self.net._cmq_pty_orig(topic, data, broker)

            self.net.message_callback = buggy
            self.net._cmq_pty_router = {}   # 空 dict：旧版误判为"未安装"
            self.net._cmq_pty_orig = buggy  # 闭环：动态回读到自己
            # 先证明旧版故障确实可复现
            with self.assertRaises(RecursionError):
                self.net.deliver("sys/device/request",
                                 {"req_id": "old", "code": "1"})

            # 新模板握手：应一步把链底接回 gms.handle_message 并装布尔标记
            self._start(sid)
            self.assertTrue(self.net._cmq_pty_installed)
            self.assertEqual(self.net._cmq_pty_orig,
                             self.gms.handle_message)

            # 非 PTY 帧：恰好一次到达链底，不再递归
            self.net.deliver("sys/device/request",
                             {"req_id": "new", "code": "2+2"})
            self.assertEqual(len(self.gms.base_calls), 1)
            self.assertEqual(self.gms.base_calls[0][1]["req_id"], "new")

            # PTY 会话功能正常
            self._assert_echo(sid, "ZZHEAL42", iseq=0)
            self._stop(sid, iseq=1)


class TerminalWriterTests(unittest.TestCase):
    """_TerminalWriter：把原生控制台 IO 隔离到可牺牲线程，背压丢弃、
    close 有界；以及 RPCRequestHandler.log_message 的非阻塞 sink。"""

    def test_writes_in_order(self):
        stream = io.BytesIO()
        w = pcm._TerminalWriter(stream, max_bytes=1 << 20, name="tw-order")
        try:
            w.write(b"ab")
            w.write(b"cd")
            w.write(b"")  # 空串无害
        finally:
            w.close(timeout=2)
        self.assertEqual(stream.getvalue(), b"abcd")

    def test_backpressure_drops_but_never_blocks_caller(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockingStream:
            def write(self, data):
                entered.set()
                release.wait(5)  # 模拟终端读取方卡死：原生写无限阻塞
                return len(data)

            def flush(self):
                pass

        w = pcm._TerminalWriter(BlockingStream(), max_bytes=100,
                                name="tw-backpressure")
        try:
            w.write(b"x" * 100)
            self.assertTrue(entered.wait(2), "写线程未开始消费")
            # 确认写线程已卡在原生写上、字节记账已满
            time.sleep(0.1)
            t0 = time.monotonic()
            for _ in range(50):
                w.write(b"y" * 10)
            self.assertLess(time.monotonic() - t0, 1.0,
                            "队列满时 write() 绝不能阻塞调用方")
            self.assertEqual(w.dropped, 500)
        finally:
            release.set()
            w.close(timeout=2)

    def test_close_is_bounded_when_native_write_hangs(self):
        release = threading.Event()

        class HungStream:
            def write(self, data):
                release.wait(30)
                return len(data)

            def flush(self):
                pass

        w = pcm._TerminalWriter(HungStream(), max_bytes=1000,
                                name="tw-hung-close")
        w.write(b"z")
        t0 = time.monotonic()
        w.close(timeout=0.5)
        self.assertLess(time.monotonic() - t0, 2.0,
                        "写线程卡死时 close() 必须按时返回")
        release.set()

    def test_stream_error_does_not_kill_writer(self):
        target = io.BytesIO()
        calls = {"n": 0}

        class FlakyStream:
            def write(self, data):
                calls["n"] += 1
                if calls["n"] == 1:
                    raise OSError("boom")
                return target.write(data)

            def flush(self):
                target.flush()

        w = pcm._TerminalWriter(FlakyStream(), name="tw-flaky")
        try:
            w.write(b"a")
            w.write(b"b")
        finally:
            w.close(timeout=2)
        # 首段写报错但写线程存活，后续字节照样落盘
        self.assertEqual(target.getvalue(), b"b")

    def test_oversized_chunk_truncated_and_accounted(self):
        stream = io.BytesIO()
        w = pcm._TerminalWriter(stream, max_bytes=10, name="tw-oversize")
        try:
            w.write(b"abcdefghijklmn")  # 14 > 10
        finally:
            w.close(timeout=2)
        self.assertEqual(stream.getvalue(), b"abcdefghij")
        self.assertEqual(w.dropped, 4)

    def test_log_message_routes_to_nonblocking_sink(self):
        import server_http
        h = server_http.RPCRequestHandler.__new__(server_http.RPCRequestHandler)
        h.client_address = ("1.2.3.4", 5555)
        captured = []
        h.log_sink = captured.append  # 实例级 sink，不动类属性
        h.log_message("GET %s", "/")
        self.assertEqual(len(captured), 1)
        self.assertTrue(captured[0].startswith("[RPC]"))
        self.assertIn("1.2.3.4:5555", captured[0])

    def test_log_message_falls_back_when_sink_raises(self):
        import server_http
        h = server_http.RPCRequestHandler.__new__(server_http.RPCRequestHandler)
        h.client_address = ("1.2.3.4", 5555)

        def bad_sink(_line):
            raise RuntimeError("sink down")

        h.log_sink = bad_sink
        buf = io.StringIO()
        old = sys.stdout
        sys.stdout = buf
        try:
            h.log_message("code %s", 200)
        finally:
            sys.stdout = old
        # sink 异常时退回 print，日志本身不能打挂请求线程
        self.assertIn("[RPC]", buf.getvalue())

    def test_class_level_plain_function_sink_via_start_server(self):
        # 生产路径回归：start_rpc_server 把普通函数挂到类属性后，
        # self.log_sink 不得变成 bound method（否则 sink(line) 多收一个
        # handler 参数抛 TypeError 被吞，静默退回 print 直写终端）。
        import server_http
        h = server_http.RPCRequestHandler.__new__(server_http.RPCRequestHandler)
        h.client_address = ("1.2.3.4", 5555)

        plain_calls = []

        def plain_sink(line):
            plain_calls.append(line)

        class _BoundSink:
            def __init__(self):
                self.lines = []

            def sink(self, line):
                self.lines.append(line)

        bound = _BoundSink()
        old = server_http.RPCRequestHandler.log_sink
        buf = io.StringIO()
        try:
            server_http.start_rpc_server(listen=False, log_sink=plain_sink)
            self.assertIs(h.log_sink, plain_sink)  # 取回原函数，而非 bound method
            old_stdout = sys.stdout
            sys.stdout = buf
            try:
                h.log_message("GET %s", "/")
            finally:
                sys.stdout = old_stdout
            self.assertEqual(len(plain_calls), 1)
            self.assertEqual(buf.getvalue(), "")  # 不得触发 fallback print

            # bound method 形态的 sink 同样不能被二次绑定
            server_http.start_rpc_server(listen=False, log_sink=bound.sink)
            h.log_message("POST %s", "/x")
            self.assertEqual(len(bound.lines), 1)
        finally:
            server_http.RPCRequestHandler.log_sink = old

    @staticmethod
    def _blocked_writer(max_bytes=1 << 20):
        entered = threading.Event()
        release = threading.Event()

        class _BlockingStream:
            def write(self, data):
                entered.set()
                release.wait(5)  # 模拟控制台读取方冻结
                return len(data)

            def flush(self):
                pass

        w = pcm._TerminalWriter(_BlockingStream(), max_bytes=max_bytes,
                                name="tw-stall")
        return w, entered, release

    def test_stall_seconds_tracks_blocked_flush(self):
        w, entered, release = self._blocked_writer()
        try:
            w.write(b"hello")
            self.assertTrue(entered.wait(2), "写线程未开始写")
            time.sleep(0.15)
            self.assertGreaterEqual(w.stall_seconds(), 0.1)
        finally:
            release.set()
            w.close(timeout=2)
        # 积压排空后不再算 stall
        self.assertEqual(w.stall_seconds(), 0.0)

    def test_abandon_pending_drains_queue_and_thread_exits(self):
        w, entered, release = self._blocked_writer()
        try:
            w.write(b"first-block")
            self.assertTrue(entered.wait(2))
            for _ in range(5):
                w.write(b"queued" * 4)  # 排在阻塞块之后
            w.abandon_pending()
            release.set()
            self.assertTrue(w._thread.join(2) is None
                            and not w._thread.is_alive(),
                            "abandon 后写线程应在解除阻塞时退出")
            # 剩余积压全部丢弃且记账归零（不是 dropped 计数路径）
            self.assertEqual(w._queued_bytes, 0)
        finally:
            release.set()
            w.close(timeout=2)

    def test_writer_diag_reports_live_state(self):
        stream = io.BytesIO()
        w = pcm._TerminalWriter(stream, name="tw-diag")
        saved = pcm._OUT_WRITER
        pcm._OUT_WRITER = w
        try:
            d = pcm.writer_diag()
            self.assertIn("out", d)
            self.assertIn("err", d)
            self.assertEqual(d["out"]["queued_bytes"], 0)
            self.assertEqual(d["out"]["stall_seconds"], 0.0)
            self.assertIn("abandon", d["out"])
        finally:
            pcm._OUT_WRITER = saved
            w.close(timeout=2)

    def test_reset_out_writer_swaps_instance(self):
        stream = io.BytesIO()
        old_w = pcm._TerminalWriter(stream, name="tw-reset-old")
        saved = pcm._OUT_WRITER
        pcm._OUT_WRITER = old_w
        new_w = None
        try:
            res = pcm.reset_out_writer()
            self.assertTrue(res["ok"])
            new_w = pcm._OUT_WRITER
            self.assertIsNot(new_w, old_w)
            self.assertTrue(old_w._abandon)
            self.assertTrue(old_w._thread.join(2) is None
                            and not old_w._thread.is_alive(),
                            "旧 writer 应排空退出")
        finally:
            pcm._OUT_WRITER = saved
            if new_w is not None:
                new_w.close(timeout=2)

    def test_reset_out_writer_without_session(self):
        saved = pcm._OUT_WRITER
        pcm._OUT_WRITER = None
        try:
            res = pcm.reset_out_writer()
            self.assertFalse(res["ok"])
            self.assertIn("writer", res["error"])
        finally:
            pcm._OUT_WRITER = saved


class SelectionAutoUnstickTests(unittest.TestCase):
    """QuickEdit 保留（左键选择/右键粘贴）；长冻结由合成 ESC 自动解除，
    并有泄漏兜底过滤。"""

    def setUp(self):
        pcm._ESC_GUARD["until"] = 0.0

    def test_esc_guard_drops_single_esc_once(self):
        pcm._arm_esc_guard()
        self.assertEqual(pcm._filter_injected_esc(b"\x1b"), b"")
        # 只丢弃一次：后续 ESC 放行
        self.assertEqual(pcm._filter_injected_esc(b"\x1b"), b"\x1b")

    def test_esc_guard_passes_other_bytes_and_chunks(self):
        pcm._arm_esc_guard()
        self.assertEqual(pcm._filter_injected_esc(b"\x1b[A"), b"\x1b[A")
        self.assertEqual(pcm._filter_injected_esc(b"a"), b"a")

    def test_esc_guard_expires(self):
        pcm._arm_esc_guard(window=0.05)
        time.sleep(0.08)
        self.assertEqual(pcm._filter_injected_esc(b"\x1b"), b"\x1b")

    def test_cancel_only_when_console_is_foreground(self):
        import ctypes
        k = ctypes.windll.kernel32
        u = ctypes.windll.user32
        with mock.patch.object(k, "GetConsoleWindow", return_value=111), \
                mock.patch.object(u, "GetForegroundWindow",
                                  return_value=222), \
                mock.patch.object(u, "keybd_event") as kb:
            self.assertFalse(pcm._cancel_console_selection())
            kb.assert_not_called()  # 非前台绝不发 ESC（会打进别的程序）

    def test_cancel_sends_esc_down_up_and_arms_guard(self):
        import ctypes
        k = ctypes.windll.kernel32
        u = ctypes.windll.user32
        with mock.patch.object(k, "GetConsoleWindow", return_value=333), \
                mock.patch.object(u, "GetForegroundWindow",
                                  return_value=333), \
                mock.patch.object(u, "keybd_event") as kb:
            self.assertTrue(pcm._cancel_console_selection())
        self.assertEqual(kb.call_args_list, [
            mock.call(0x1B, 0, 0, 0),        # ESC down
            mock.call(0x1B, 0, 0x0002, 0),   # KEYEVENTF_KEYUP
        ])
        self.assertGreater(pcm._ESC_GUARD["until"], 0.0)

    def test_cancel_non_win32(self):
        with mock.patch.object(pcm.sys, "platform", "linux"):
            self.assertFalse(pcm._cancel_console_selection())

    def test_enable_vt_does_not_touch_input_mode(self):
        # 回归：QuickEdit 必须保留——_enable_output_vt 只许操作输出句柄
        import ctypes
        k = ctypes.windll.kernel32
        with mock.patch.object(k, "SetConsoleOutputCP") as cp, \
                mock.patch.object(k, "GetStdHandle",
                                  return_value=777) as geth, \
                mock.patch.object(k, "GetConsoleMode",
                                  return_value=True), \
                mock.patch.object(k, "SetConsoleMode",
                                  return_value=True) as setm:
            self.assertTrue(pcm._enable_output_vt())
        cp.assert_called_once_with(65001)
        self.assertEqual(geth.call_args_list, [mock.call(-11)])  # 无 -10
        # 读到的模式值为 0（c_uint32 初值），只补 VT 位
        self.assertEqual(setm.call_args_list,
                         [mock.call(777, 0x0004)])


class LogRingSubscribeTests(unittest.TestCase):
    """_LogRing.subscribe：快照 + 增量不重不漏、退订即停、坏监听不反噬。"""

    def test_snapshot_then_incremental_no_dup_no_gap(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)
        ring.write("old-1\n"); ring.write("old-2\n")
        got = []
        snapshot, unsubscribe = ring.subscribe(lambda seq, line: got.append(line))
        try:
            self.assertEqual([line for _s, line in snapshot], ["old-1", "old-2"])
            ring.write("new-3\n"); ring.write("new-4\n")
            deadline = time.time() + 2
            while len(got) < 2 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(got, ["new-3", "new-4"])  # 只收快照之后的，不重不漏
        finally:
            unsubscribe()
        ring.write("after-unsub\n")
        time.sleep(0.05)
        self.assertNotIn("after-unsub", got)

    def test_listener_exception_never_breaks_ring_or_other_listeners(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)

        def bad_cb(_seq, _line):
            raise RuntimeError("listener boom")

        good = []
        snapshot, unsub_bad = ring.subscribe(bad_cb)
        _snap2, unsub_good = ring.subscribe(lambda seq, line: good.append(line))
        try:
            ring.write("survive-1\n")
            deadline = time.time() + 2
            while len(good) < 1 and time.time() < deadline:
                time.sleep(0.01)
            self.assertEqual(good, ["survive-1"])
            self.assertIn("survive-1", ring.tail(0))
        finally:
            unsub_bad(); unsub_good()

    def test_concurrent_subscribers_each_get_full_tail(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)
        for i in range(50):
            ring.write("c%02d\n" % i)
        results = []
        unsubs = []
        try:
            for _ in range(4):
                snap, unsub = ring.subscribe(lambda *_: None)
                results.append(len(snap))
                unsubs.append(unsub)
            self.assertEqual(results, [50, 50, 50, 50])
        finally:
            for u in unsubs:
                u()


class RingTextIOTests(unittest.TestCase):
    """_RingTextIO：print/stdout/stderr 导流进日志环，且无 buffer 可被渲染侧误用。"""

    def setUp(self):
        pcm._LOCAL_LOG.clear()
        self._mirror = pcm._LOG_MIRROR_STDERR
        pcm._LOG_MIRROR_STDERR = False

    def tearDown(self):
        pcm._LOG_MIRROR_STDERR = self._mirror
        pcm._LOCAL_LOG.clear()

    def test_line_splitting_and_tail_fragment(self):
        rio = pcm._RingTextIO("<test-out>")
        n = rio.write("a\nb\r\nc\rd")
        self.assertEqual(n, len("a\nb\r\nc\rd"))
        self.assertEqual(pcm.get_log(0), "a\nb\nc")  # 残片 d 等换行才入环
        rio.write("\n")
        self.assertEqual(pcm.get_log(0), "a\nb\nc\nd")

    def test_accepts_bytes_and_print(self):
        rio = pcm._RingTextIO("<test-bytes>")
        rio.write(b"bytes-line\n")
        print("print-line", file=rio)
        text = pcm.get_log(0)
        self.assertIn("bytes-line", text)
        self.assertIn("print-line", text)

    def test_no_buffer_and_terminal_shims(self):
        rio = pcm._RingTextIO("<test-shim>")
        self.assertFalse(hasattr(rio, "buffer"))  # 强制 _bin_stdout 回退 sys.__stdout__
        self.assertTrue(rio.writable())
        self.assertFalse(rio.readable())
        self.assertFalse(rio.isatty())
        with self.assertRaises(OSError):
            rio.fileno()
        rio.flush()  # no-op，不报错

    def test_install_log_capture_reroutes_stdout_stderr_and_is_idempotent(self):
        import logging as pylog
        old_out, old_err = sys.stdout, sys.stderr
        root = pylog.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        try:
            pcm._install_log_capture()
            self.assertIsInstance(sys.stdout, pcm._RingTextIO)
            self.assertIsInstance(sys.stderr, pcm._RingTextIO)
            self.assertFalse(hasattr(sys.stdout, "buffer"))
            print("capture-stdout-51")
            sys.stderr.write("capture-stderr-52\n")
            pylog.getLogger("MultiMQTT.capture_test").info("capture-logging-53")
            text = pcm.get_log(0)
            self.assertIn("capture-stdout-51", text)
            self.assertIn("capture-stderr-52", text)
            self.assertIn("capture-logging-53", text)
            # 幂等：不得二次包裹
            pcm._install_log_capture()
            self.assertEqual(sum(isinstance(h, pcm._RingLogHandler)
                                 for h in root.handlers), 1)
        finally:
            sys.stdout, sys.stderr = old_out, old_err
            for h in list(root.handlers):
                if h not in saved_handlers:
                    root.removeHandler(h)
            for h in saved_handlers:
                if h not in root.handlers:
                    root.addHandler(h)
            root.setLevel(saved_level)


class _FakeWebSocket:
    """LogHub.serve 用的假 WebSocket：脚本化入站消息，记录所有出站帧。"""

    def __init__(self, incoming=(), stop_after_drain=False):
        self._incoming = list(incoming)
        self.sent = []
        self.closed = False
        self._stop = threading.Event()
        self._send_gate = None  # 可选：第一次 lines 发送前阻塞（测积压丢行）
        self._send_blocked = None  # 已进入 lines 阻塞的通知事件

    def block_first_lines_until_released(self):
        """返回 (放行闸门, 已阻塞事件)：调用方等 blocked 即可确认 writer
        已堵在第一次 lines 发送上，无需靠固定 sleep 猜时序。"""
        ev = threading.Event()
        blocked = threading.Event()
        self._send_gate = ev
        self._send_blocked = blocked
        return ev, blocked

    def release_stop(self):
        self._stop.set()

    def send(self, message):
        self.sent.append(message)
        if self._send_gate is not None:
            try:
                msg = json.loads(message)
            except ValueError:
                return
            if msg.get("type") == "lines":
                if self._send_blocked is not None:
                    self._send_blocked.set()
                self._send_gate.wait(5)

    def receive(self):
        if self._incoming:
            return self._incoming.pop(0)
        self._stop.wait(5)
        return None

    def close(self):
        self.closed = True


class LogHubTests(unittest.TestCase):
    """LogHub：snapshot 先行、增量 lines、ping/pong、积压丢最旧并插提示。"""

    def _frames(self, ws, timeout=3.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                return [json.loads(s) for s in ws.sent]
            except ValueError:
                pass
            time.sleep(0.02)
        return [json.loads(s) for s in ws.sent]

    def test_snapshot_then_lines_and_pong(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)
        ring.write("snap-a\n"); ring.write("snap-b\n")
        hub = pcm.LogHub(ring)
        ws = _FakeWebSocket(incoming=("ping",))
        t = threading.Thread(target=hub.serve, args=(ws, None), daemon=True)
        t.start()
        time.sleep(0.2)  # 等 snapshot 发出
        ring.write("live-1\n"); ring.write("live-2\n")
        deadline = time.time() + 3
        while time.time() < deadline:
            frames = self._frames(ws)
            blob = "\n".join(f.get("text", "") for f in frames
                             if f.get("type") == "lines")
            if "live-1" in blob and "live-2" in blob:
                break
            time.sleep(0.02)
        ws.release_stop()
        t.join(timeout=3)
        frames = self._frames(ws)
        types = [f["type"] for f in frames]
        self.assertEqual(types[0], "snapshot")  # 快照必须在最前
        snap = frames[0]
        self.assertIn("snap-a", snap["text"])
        self.assertIn("snap-b", snap["text"])
        self.assertEqual(snap["lines"], 2)
        blob = "\n".join(f.get("text", "") for f in frames
                         if f.get("type") == "lines")
        self.assertIn("live-1", blob)
        self.assertIn("live-2", blob)
        self.assertIn("pong", types)
        self.assertTrue(ws.closed)

    def test_slow_consumer_drops_oldest_with_notice(self):
        ring = pcm._LogRing(max_chars=10 ** 9, max_lines=10 ** 9)
        hub = pcm.LogHub(ring)
        ws = _FakeWebSocket()
        gate, blocked = ws.block_first_lines_until_released()
        t = threading.Thread(target=hub.serve, args=(ws, None), daemon=True)
        t.start()
        # 先只喂 1 条：writer 第一次合批必然只抽到这 1 条就堵在 send 上，
        # 与线程调度快慢完全无关（旧写法靠 sleep 抢这个窗口，高负载必 flaky）
        ring.write("seed-0\n")
        self.assertTrue(blocked.wait(3), "writer 未堵在第一次 lines 发送")
        # writer 已堵：再灌 1100 条，队列容量 1000，恰好丢 100 旧行
        for i in range(pcm._LOG_WS_QUEUE + 100):
            ring.write("burst-%04d\n" % i)
        gate.set()  # 放行：恢复后首帧应带积压丢弃提示
        deadline = time.time() + 3
        while time.time() < deadline:
            frames = self._frames(ws)
            blob = "\n".join(f.get("text", "") for f in frames
                             if f.get("type") == "lines")
            if "burst-1099" in blob:
                break
            time.sleep(0.02)
        ws.release_stop()
        t.join(timeout=3)
        frames = self._frames(ws)
        notices = [f["text"] for f in frames
                   if "已丢弃" in f.get("text", "")]
        self.assertTrue(notices, "慢消费积压时必须插入丢弃提示")
        self.assertIn("100", notices[0])
        self.assertIn("burst-1099",
                      "\n".join(f.get("text", "") for f in frames))


class LogHtmlPageTests(unittest.TestCase):
    """log_html：整页就是全屏控制台，无工具栏，直连 /wslog。"""

    def test_page_fullscreen_and_wslog_only(self):
        class FakeResponse:
            def __init__(self):
                self.headers = {}
                self.data = None

            def set_header(self, key, value):
                self.headers[key] = value

            def set_data(self, data):
                self.data = data

        resp = FakeResponse()
        pcm.log_html(resp)
        self.assertIn("text/html", resp.headers["Content-Type"])
        self.assertEqual(resp.headers["Cache-Control"], "no-store")
        page = resp.data
        self.assertIs(page, pcm._LOG_PAGE)
        self.assertIn("/wslog", page)
        self.assertIn("100vh", page)       # 整个视口即控制台
        self.assertIn("snapshot", page)
        self.assertIn("appendBlock", page)
        self.assertNotIn("<toolbar", page)
        self.assertNotIn('role="toolbar"', page)


class LogConsoleEndToEndTests(unittest.TestCase):
    """真实 HTTP 口端到端：页面/302/取日志 RPC + 裸 WebSocket 收 snapshot/lines。"""

    @classmethod
    def setUpClass(cls):
        import logging as pylog
        import server_http
        cls._server_http = server_http
        cls._old_out, cls._old_err = sys.stdout, sys.stderr
        cls._root = pylog.getLogger()
        cls._saved_handlers = list(cls._root.handlers)
        cls._saved_level = cls._root.level
        cls._mirror = pcm._LOG_MIRROR_STDERR
        pcm._LOCAL_LOG.clear()
        pcm._LOG_MIRROR_STDERR = False
        cls.server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={"log_html": pcm.log_html,
                                             "get_log": pcm.get_log,
                                             "clear_log": pcm.clear_log},
            log_sink=pcm._emit_local,
            websocket_handler=pcm._LOG_HUB.serve, websocket_path="/wslog",
            redirect_root="/log_html(p)")
        cls.port = cls.server.server_address[1]
        pcm._install_log_capture()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.server.shutdown()
        finally:
            sys.stdout, sys.stderr = cls._old_out, cls._old_err
            for h in list(cls._root.handlers):
                if h not in cls._saved_handlers:
                    cls._root.removeHandler(h)
            for h in cls._saved_handlers:
                if h not in cls._root.handlers:
                    cls._root.addHandler(h)
            cls._root.setLevel(cls._saved_level)
            pcm._LOG_MIRROR_STDERR = cls._mirror
            pcm._LOCAL_LOG.clear()

    def _get(self, path):
        return urllib.request.urlopen(
            "http://127.0.0.1:%d%s" % (self.port, path), timeout=5)

    def test_page_rpc_and_root_redirect(self):
        with self._get("/log_html(p)") as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers["Content-Type"])
            body = resp.read().decode("utf-8", "replace")
            self.assertIn("/wslog", body)
            self.assertIn("100vh", body)

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            opener.open("http://127.0.0.1:%d/" % self.port, timeout=5)
            self.fail("根路径应 302 跳转日志台")
        except urllib.error.HTTPError as e:
            self.assertEqual(e.code, 302)
            self.assertEqual(e.headers["Location"], "/log_html(p)")
            e.close()

    def test_wslog_snapshot_then_stdout_and_logging_frames(self):
        import socket as _socket
        import logging as pylog
        key = b"1234567890abcdef"
        sock = _socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            handshake = (
                b"GET /wslog HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n"
                b"Connection: Upgrade\r\nSec-WebSocket-Key: "
                + __import__("base64").b64encode(key)
                + b"\r\nSec-WebSocket-Version: 13\r\n\r\n")
            sock.sendall(handshake)
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = sock.recv(4096)
                self.assertTrue(chunk, "握手无响应")
                resp += chunk
            self.assertIn(b"101 Switching Protocols", resp)

            def recv_frame():
                def rd(n):
                    data = b""
                    while len(data) < n:
                        chunk = sock.recv(n - len(data))
                        if not chunk:
                            raise ConnectionError("websocket 已断开")
                        data += chunk
                    return data
                h0, h1 = rd(2)
                opcode = h0 & 0x0F
                length = h1 & 0x7F
                if length == 126:
                    length = struct.unpack("!H", rd(2))[0]
                elif length == 127:
                    length = struct.unpack("!Q", rd(8))[0]
                payload = rd(length) if length else b""
                return opcode, payload.decode("utf-8", "replace")

            opcode, payload = recv_frame()
            self.assertEqual(opcode, 0x1)
            self.assertEqual(json.loads(payload)["type"], "snapshot")

            print("e2e-stdout-marker-91")
            pylog.getLogger("MultiMQTT.e2e").info("e2e-logging-marker-92")
            blob = ""
            deadline = time.time() + 5
            while time.time() < deadline and not (
                    "e2e-stdout-marker-91" in blob
                    and "e2e-logging-marker-92" in blob):
                _op, frame = recv_frame()
                if _op == 0x1:
                    blob += json.loads(frame).get("text", "") + "\n"
            self.assertIn("e2e-stdout-marker-91", blob)
            self.assertIn("e2e-logging-marker-92", blob)

            # 应用层心跳：发 mask 文本帧 ping，LogHub 回 JSON 文本帧 pong
            mask = b"\x01\x02\x03\x04"
            ping = b"ping"
            masked = bytes(b ^ mask[i % 4] for i, b in enumerate(ping))
            sock.sendall(b"\x81" + bytes((0x80 | len(ping),)) + mask + masked)
            pong_seen = False
            pong_deadline = time.time() + 5
            while time.time() < pong_deadline and not pong_seen:
                opcode, payload = recv_frame()
                if opcode == 0x1:
                    try:
                        pong_seen = json.loads(payload).get("type") == "pong"
                    except ValueError:
                        pass
            self.assertTrue(pong_seen)
        finally:
            sock.close()

        with self._get("/r=get_log()") as resp:
            text = resp.read().decode("utf-8", "replace")
            self.assertIn("e2e-stdout-marker-91", text)
            self.assertIn("e2e-logging-marker-92", text)


if __name__ == "__main__":
    unittest.main()
