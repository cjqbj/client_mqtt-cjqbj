#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""socks5_client_mqtt / _SOCKS5_START_TEMPLATE 服务端模板测试。

全平台可跑（只用本机 socket + 线程，不连任何 broker、不需要 POSIX）：

1. Socks5TemplateStaticTests —— 模板渲染/编译与关键防护的静态检查。
2. Socks5TemplateLiveTests —— 在 PythonExecutor 的持久命名空间里真实执行
   服务端模板，FakeNet 模拟 gms.mqtt_net，重点覆盖线程/注册表生命周期：
     - 目标拒连（connect_failed）：opened(ok=False)+closed 必须发出，
       s5-conn/s5-wr 线程必须退出，_conns 条目必须摘除（线程泄漏回归）；
     - 目标 EOF + 客户端只读不 close（WebSocket/SSE 典型形态）：writer
       不得永久阻塞在 wq.get()，连接条目必须回收（线程泄漏回归）；
     - 上行数据经 wq 送达目标、回显数据下行，EOF 后同样干净收尾；
     - 连续多次失败连接不得堆积线程/条目；
     - stop 帧触发会话结束并广播 end。

运行：
    cd multi_mqtt && python -m unittest tests.test_socks5_mqtt -v
"""
import json
import os
import socket
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rpc_executor import PythonExecutor  # noqa: E402
from client import socks5_client_mqtt as scm  # noqa: E402


# ============================ 1. 静态检查 ============================

class Socks5TemplateStaticTests(unittest.TestCase):

    def test_render_compiles(self):
        code = scm.build_socks5_start_code(
            {"sid": "x", "in_topic": "a", "out_topic": "b",
             "frame_max": 16384})
        compile(code, "<socks5-template>", "exec")

    def test_payload_rendered_once_and_is_json(self):
        code = scm.build_socks5_start_code(
            {"sid": "sid-中文", "in_topic": "i", "out_topic": "o"})
        self.assertNotIn("__PAYLOAD__", code)
        self.assertIn("_cmq_socks5_start()", code)

    def test_failure_and_eof_paths_wake_writer(self):
        """回归守卫：两条读侧结束路径都必须唤醒 writer，否则线程泄漏。"""
        code = scm._SOCKS5_START_TEMPLATE
        # connect 失败路径：r_done 之后、return 之前必须唤醒 writer
        self.assertIn('"connect_failed"', code)
        # EOF 正常结束路径同样需要
        self.assertIn('"reason": "eof"', code)
        # 两条路径共用唤醒助手，且助手确实向 wq 投 None
        self.assertIn("_wake_writer", code)
        self.assertIn('put_nowait(None)', code)


# ============================ 2. 服务端模板真机测试 ============================

def _wait_for(pred, timeout=5.0, step=0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


def _free_tcp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class _OneShotServer(threading.Thread):
    """accept 一条连接：echo 数据，最后按策略关闭（制造目标 EOF）。"""

    def __init__(self, echo=True, close_delay=0.1, read_delay=0.0):
        super().__init__(daemon=True)
        self.echo = echo
        self.close_delay = close_delay
        self.read_delay = read_delay
        self.got = b""
        self.ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", 0))
        self.ls.listen(1)
        self.port = self.ls.getsockname()[1]

    def run(self):
        try:
            self.ls.settimeout(10)
            conn, _ = self.ls.accept()
        except OSError:
            return
        try:
            conn.settimeout(5)
            if self.read_delay:
                time.sleep(self.read_delay)
            if self.echo:
                while True:
                    try:
                        data = conn.recv(4096)
                    except OSError:
                        break
                    if not data:
                        break
                    self.got += data
                    try:
                        conn.sendall(data)
                    except OSError:
                        break
            if self.close_delay:
                time.sleep(self.close_delay)
        finally:
            conn.close()

    def shutdown(self):
        try:
            self.ls.close()
        except OSError:
            pass


class FakeNet:
    """模拟服务端 gms.mqtt_net：记录广播、保存唯一消息回调。"""

    def __init__(self):
        self.message_callback = None
        self.subscribed = []
        self.published = []
        self.lock = threading.Lock()

    def set_on_message(self, cb):
        self.message_callback = cb

    def subscribe(self, topic):
        self.subscribed.append(topic)

    def publish_broadcast(self, topic, payload):
        with self.lock:
            self.published.append((topic, dict(payload)))

    def deliver(self, topic, data):
        self.message_callback(topic, data, "fakebroker")

    def frames(self, topic):
        with self.lock:
            return [(t, dict(d)) for t, d in self.published if t == topic]


class FakeServer:
    """模拟 server_mqtt.MQTTServer：持有 mqtt_net + handle_message。"""

    def __init__(self, net):
        self.mqtt_net = net
        self.base_calls = []

    def handle_message(self, topic, data, broker):
        self.base_calls.append((topic, data, broker))


class Socks5TemplateLiveTests(unittest.TestCase):

    HB = 0.2

    def setUp(self):
        self.net = FakeNet()
        self.gms = FakeServer(self.net)
        # 模拟服务端持久 executor 命名空间
        self.ns = {"__name__": "__rpc_exec__", "gms": self.gms}
        self.sid = "s5test-%d-%d" % (id(self), time.time() * 1000)
        self.in_t = "s5/%s/in" % self.sid
        self.out_t = "s5/%s/out" % self.sid
        self._started = False

    def tearDown(self):
        if self._started:
            try:
                self._send({"stop": True})
            except Exception:
                pass

    # ---- 辅助 ----

    def _start(self, **over):
        payload = {
            "sid": self.sid, "in_topic": self.in_t, "out_topic": self.out_t,
            "frame_max": 16384, "ttl": 300, "heartbeat": self.HB,
            "connect_timeout": 2.0, "gap_timeout": 15.0,
        }
        payload.update(over)
        resp = PythonExecutor(globals=self.ns).execute(
            scm.build_socks5_start_code(payload))
        self.assertTrue(resp["ok"],
                        resp.get("stdout", "") + resp.get("error", ""))
        env = json.loads(resp["r"])
        self.assertTrue(env["ok"], env.get("error"))
        self._started = True
        return env

    def _send(self, partial, seq=None, cid=None):
        frame = {"s5": self.sid}
        if cid is not None:
            frame["cid"] = cid
        if seq is not None:
            frame["seq"] = seq
        frame.update(partial)
        self.net.deliver(self.in_t, frame)

    def _open(self, cid, port, host="127.0.0.1"):
        # 每条连接首帧 seq=0（open），与客户端 _send 打号一致
        self._send({"open": {"host": host, "port": int(port)}},
                   seq=0, cid=cid)

    def _cid_frames(self, cid):
        return [d for _, d in self.net.frames(self.out_t)
                if d.get("cid") == cid]

    def _wait_frame(self, cid, key, timeout=5.0):
        ok = _wait_for(
            lambda: any(key in d for d in self._cid_frames(cid)),
            timeout=timeout)
        self.assertTrue(ok, "cid=%d 超时未等到含 %r 的下行帧" % (cid, key))
        return [d for d in self._cid_frames(cid) if key in d]

    def _s5_threads(self, cid):
        names = ("s5-conn-%d" % cid, "s5-wr-%d" % cid)
        return [t for t in threading.enumerate() if t.name in names]

    def _wait_threads_gone(self, cid, timeout=3.0):
        ok = _wait_for(
            lambda: not any(t.is_alive() for t in self._s5_threads(cid)),
            timeout=timeout, step=0.02)
        alive = [t.name for t in self._s5_threads(cid) if t.is_alive()]
        self.assertTrue(ok, "cid=%d 线程未退出: %r" % (cid, alive))

    def _hb_conns(self):
        """取最新一帧心跳捎带的服务端 per-cid 水位字典。"""
        snap = None
        for _, d in self.net.frames(self.out_t):
            if d.get("hb") is not None and isinstance(d.get("c"), dict):
                snap = d["c"]
        return snap

    def _wait_conn_unregistered(self, cid, timeout=3.0):
        key = str(cid)
        ok = _wait_for(
            lambda: (self._hb_conns() or {}).get(key) is None,
            timeout=timeout, step=0.05)
        self.assertTrue(ok, "cid=%d 在心跳水位中未摘除: %r"
                        % (cid, self._hb_conns()))

    # ---- 用例 ----

    def test_connect_refused_no_thread_or_entry_leak(self):
        """高优先级回归：目标拒连后 conn/writer 线程与 _conns 条目都要回收。"""
        self._start()
        cid = 0
        dead_port = _free_tcp_port()  # 无人监听 → ConnectionRefused
        self._open(cid, dead_port)
        opened = self._wait_frame(cid, "opened")[0]["opened"]
        self.assertFalse(opened.get("ok"), opened)
        closed = self._wait_frame(cid, "closed")[0]
        self.assertEqual(closed.get("reason"), "connect_failed")
        # bug 表现：s5-wr 永久阻塞在 wq.get()，条目留在 _conns
        self._wait_threads_gone(cid)
        self._wait_conn_unregistered(cid)

    def test_target_eof_readonly_client_releases_writer(self):
        """高优先级回归：目标 EOF + 客户端不发 close（WS/SSE），writer 不得挂死。"""
        srv = _OneShotServer(echo=False, close_delay=0.1)
        srv.start()
        try:
            self._start()
            cid = 1
            self._open(cid, srv.port)
            opened = self._wait_frame(cid, "opened")[0]["opened"]
            self.assertTrue(opened.get("ok"), opened)
            # 故意不发任何 close 帧（模拟只读长连接）
            closed = self._wait_frame(cid, "closed", timeout=6.0)[0]
            self.assertEqual(closed.get("reason"), "eof")
            self._wait_threads_gone(cid)
            self._wait_conn_unregistered(cid)
        finally:
            srv.shutdown()
            srv.join(timeout=3)

    def test_uplink_data_echo_then_eof_cleanup(self):
        """上行数据必须送达目标；回显下行；EOF 后连接干净回收。"""
        srv = _OneShotServer(echo=True, close_delay=0.2)
        srv.start()
        try:
            self._start()
            cid = 2
            self._open(cid, srv.port)
            self._wait_frame(cid, "opened")
            self._send({"d": "ping-echo-123"}, seq=1, cid=cid)
            ok = _wait_for(lambda: srv.got == b"ping-echo-123", timeout=5)
            self.assertTrue(ok, "目标未收到上行数据: %r" % srv.got)
            ok = _wait_for(
                lambda: any(d.get("d") == "ping-echo-123"
                            for d in self._cid_frames(cid)),
                timeout=5)
            self.assertTrue(ok, "未收到回显下行帧")
            self._wait_frame(cid, "closed", timeout=6.0)
            self._wait_threads_gone(cid)
            self._wait_conn_unregistered(cid)
        finally:
            srv.shutdown()
            srv.join(timeout=3)

    def test_repeated_failed_connects_do_not_accumulate(self):
        """连续失败连接不得堆积线程/注册表条目。"""
        self._start()
        for cid in range(5):
            dead_port = _free_tcp_port()
            self._open(cid, dead_port)
            self._wait_frame(cid, "closed", timeout=6.0)
        # 给全部收尾线程一点时间
        _wait_for(lambda: False, timeout=0.5)
        for cid in range(5):
            self._wait_threads_gone(cid, timeout=3.0)
            self._wait_conn_unregistered(cid, timeout=3.0)
        leftover = [t.name for t in threading.enumerate()
                    if t.name.startswith(("s5-conn-", "s5-wr-"))]
        self.assertEqual(leftover, [])

    def test_stop_session_broadcasts_end(self):
        self._start()
        self._send({"stop": True})
        ok = _wait_for(
            lambda: any(d.get("end") for _, d in self.net.frames(self.out_t)),
            timeout=5)
        self.assertTrue(ok, "stop 后未广播 end 帧")


if __name__ == "__main__":
    unittest.main(verbosity=2)
