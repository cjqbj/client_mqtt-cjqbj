#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""server_mqtt global_topic_list 定期报告测试。

与 stats.get_report 网络质量报告不同，本功能定期发布服务端监听 topic 列表到
global_topic_list，tick 并入 _dispatch_loop 周期任务槽，不额外创建线程。

默认运行离线桩测试（不连真实 broker）。
真实 broker 端到端测试（调小间隔，验证 Retain/QoS1/空闲阈值/指令抑制）：
    set MM_LIVE=1
    C:\QGB\miniforge3\python.exe tests\test_global_topic_list.py -v
"""
import json
import os
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from multi_mqtt import BROKER_LIST, ConnectionQualityStats, MultiMQTTManager
from server_mqtt import DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL, MQTTServer
from rpc_executor import utc_ms

LIVE_BROKER_CANDIDATES = [                    # 按序尝试，选第一个可连上的公共 broker
    ("broker.codenow.cn", 1883),
    ("broker.mqtt-dashboard.com", 1883),
    ("broker-cn.emqx.io", 1883),
    ("broker.emqx.io", 1883),
    ("test.mosquitto.org", 1883),
    ("mqtt.touchsocket.net", 1883),
]


def make_stub_server(**kw):
    """构造未连接的 MQTTServer，桩掉网络层的连接快照与广播发布。"""
    s = MQTTServer(**kw)
    calls = []
    s.mqtt_net.publish_broadcast = lambda topic, payload, **ka: calls.append((topic, payload, ka))
    s.calls = calls
    remote = []
    # 远端 topics 追加走真实 HTTPS 会受 qmm 证书过期影响，这里整体打桩避免测试联网。
    s._append_global_topic_list_remote = lambda payload: remote.append(payload)
    s.remote_appends = remote
    return s


def stub_snapshot(server, mapping):
    server.mqtt_net.get_connection_snapshot = lambda: dict(mapping)


class StubbedGlobalTopicListTests(unittest.TestCase):
    """离线验证 tick 核心判定逻辑（24h 空闲阈值语义）。"""

    def test_startup_publishes_retain_qos1_report(self):
        s = make_stub_server()
        s.mqtt_net.subscribe(s.request_topic)          # 模拟 start() 已完成订阅
        stub_snapshot(s, {"brokerA": True})
        s.global_topic_list_tick()
        self.assertEqual(len(s.calls), 1)
        topic, payload, ka = s.calls[0]
        self.assertEqual(topic, "global_topic_list")
        self.assertEqual(ka, {"qos": 1, "retain": True})
        self.assertEqual(payload["reason"], "startup")
        self.assertNotIn("type", payload)
        self.assertIn(s.request_topic, payload["listen_topics"])

    def test_recent_command_suppresses_publish(self):
        s = make_stub_server()
        stub_snapshot(s, {"brokerA": True})
        s.last_command_ts = time.time()                # 刚收到指令，未超过 24h 阈值
        s.global_topic_list_tick()
        self.assertEqual(s.calls, [])

    def test_idle_beyond_configurable_threshold_resumes(self):
        s = make_stub_server(global_topic_list_idle_threshold=60.0, global_topic_list_interval=5.0)
        stub_snapshot(s, {"brokerA": True})
        s.last_command_ts = time.time() - 59           # 阈值内：不发布
        s._last_gtl_publish_ts = time.time() - 100
        s.global_topic_list_tick()
        self.assertEqual(s.calls, [])
        s.last_command_ts = time.time() - 61           # 超过阈值：恢复发布
        s.global_topic_list_tick()
        self.assertEqual(len(s.calls), 1)
        self.assertEqual(s.calls[0][1]["reason"], "idle_no_command")

    def test_custom_payload_fn(self):
        s = make_stub_server(
            global_topic_list="my/global_topic_list",
            global_topic_list_payload_fn=lambda sv: {"type": "custom", "topics": list(sv.mqtt_net.subscribed_topics)},
        )
        stub_snapshot(s, {"brokerA": True})
        s.global_topic_list_tick()
        self.assertEqual(len(s.calls), 1)
        self.assertEqual(s.calls[0][0], "my/global_topic_list")
        self.assertEqual(s.calls[0][1]["type"], "custom")

    def test_custom_publish_kwargs_passthrough(self):
        # 自定义发布参数原样透传，但 qos 恒被钉在 1：报告的 broker 侧保留
        # 语义依赖 QoS1，自定义值不允许把报告跌回低 QoS（见 MQTTServer.__init__）
        s = make_stub_server(global_topic_list_publish_kwargs={"qos": 2, "retain": True})
        stub_snapshot(s, {"brokerA": True})
        s.global_topic_list_tick()
        self.assertEqual(s.calls[0][2], {"qos": 1, "retain": True})

    def test_default_check_interval_reuses_ping_interval(self):
        """默认 tick 粒度复用 ConnectionQualityStats.PING_INTERVAL，与 ping 同节拍统一时间驱动。"""
        self.assertEqual(DEFAULT_GLOBAL_TOPIC_LIST_CHECK_INTERVAL, ConnectionQualityStats.PING_INTERVAL)

    def test_start_config_maps_all_global_topic_list_keys(self):
        """start(config) 必须把全部 mqtt_global_topic_list_* 配置键透传给 MQTTServer 构造。

        此前 global_topic_list_check_interval 在 start(config) 中被静默丢弃，
        本用例锁定 config->构造参数的完整映射，防止同类遗漏。
        """
        from unittest import mock
        from server_mqtt import start as module_start

        cfg = {
            "mqtt_global_topic_list": "cfg/topic_list",
            "mqtt_global_topic_list_interval": 30.0,
            "mqtt_global_topic_list_idle_threshold": 60.0,
            "mqtt_global_topic_list_check_interval": 7.5,
            "mqtt_global_topic_list_publish_kwargs": {"qos": 0, "retain": False},
        }
        with mock.patch("server_mqtt.MQTTServer") as MockServer:
            MockServer.return_value.start.return_value = "started"
            self.assertEqual(module_start(config=cfg, globals={}), "started")
            _, kwargs = MockServer.call_args
            self.assertEqual(kwargs["global_topic_list"], "cfg/topic_list")
            self.assertEqual(kwargs["global_topic_list_interval"], 30.0)
            self.assertEqual(kwargs["global_topic_list_idle_threshold"], 60.0)
            self.assertEqual(kwargs["global_topic_list_check_interval"], 7.5)
            self.assertEqual(kwargs["global_topic_list_publish_kwargs"], {"qos": 0, "retain": False})

        # 缺省 config：check_interval 回落到默认值（复用 PING_INTERVAL）
        with mock.patch("server_mqtt.MQTTServer") as MockServer:
            module_start(config={}, globals={})
            _, kwargs = MockServer.call_args
            self.assertEqual(kwargs["global_topic_list_check_interval"],
                             ConnectionQualityStats.PING_INTERVAL)

    def test_tick_runs_in_dispatch_loop_periodic_slot(self):
        """tick 注册进 _dispatch_loop 周期任务槽，与 stats Ping/报告同级，不另开线程。"""
        s = make_stub_server(global_topic_list_check_interval=7.5)
        before = threading.active_count()
        s._register_global_topic_list_tick()
        self.assertEqual(s.mqtt_net._periodic_fn, s.global_topic_list_tick)
        self.assertEqual(s.mqtt_net._periodic_interval, 7.5)
        s._unregister_global_topic_list_tick()
        self.assertIsNone(s.mqtt_net._periodic_fn)
        self.assertEqual(threading.active_count(), before)   # 注册/清除不创建线程

    def test_publish_now_appends_remote_and_returns_ack_status(self):
        """立即发布接口：自定义 reason、等待 QoS1 确认、默认上报全部 URL。"""
        s = make_stub_server()
        confirmed = []
        s.mqtt_net.publish_broadcast_confirmed = lambda topic, payload, **ka: (
            confirmed.append((topic, payload, ka))
            or {"queued": 2, "acked": 1, "timeout": True}
        )
        remote = s.remote_appends                 # fixture 默认已打桩远端上报
        before = time.time()
        status = s.publish_global_topic_list_now(reason="ime_feedback")
        self.assertEqual(confirmed[0][0], "global_topic_list")
        self.assertEqual(confirmed[0][1]["reason"], "ime_feedback")
        self.assertEqual(confirmed[0][1]["request_topic"], s.request_topic)
        self.assertEqual(confirmed[0][2]["qos"], 1)
        self.assertEqual(confirmed[0][2]["retain"], True)
        self.assertEqual(confirmed[0][2]["confirm_timeout"], 2.0)
        self.assertTrue(status["ok"])
        self.assertEqual(status["queued"], 2)
        self.assertEqual(status["acked"], 1)
        self.assertTrue(status["sent"])          # 至少一个 broker 收到 PUBACK
        self.assertTrue(status["timeout"])       # 仍有 broker 未确认
        self.assertEqual(len(remote), 1)          # 交互路径也上报远端（异步不阻塞）
        self.assertEqual(remote[0]["reason"], "ime_feedback")
        self.assertGreaterEqual(s._last_gtl_publish_ts, before)

    def test_publish_now_without_confirm_falls_back_to_queued_count(self):
        """confirm_timeout=0 走普通广播，sent 以入队数为准，acked 保持 None。"""
        s = make_stub_server()
        s.mqtt_net.publish_broadcast = lambda topic, payload, **ka: 3
        status = s.publish_global_topic_list_now(reason="ime_feedback", confirm_timeout=0)
        self.assertEqual(status["queued"], 3)
        self.assertIsNone(status["acked"])
        self.assertTrue(status["sent"])
        self.assertFalse(status["timeout"])

    def test_publish_now_no_ack_marks_unsent(self):
        """入队但无 PUBACK 时必须返回未成功，供调用方挂重试任务。"""
        s = make_stub_server()
        s.mqtt_net.publish_broadcast_confirmed = lambda *a, **ka: {
            "queued": 1, "acked": 0, "timeout": True,
        }
        status = s.publish_global_topic_list_now(reason="ime_feedback")
        self.assertFalse(status["sent"])
        self.assertTrue(status["timeout"])

    def test_publish_now_custom_payload_not_dict_returns_error(self):
        s = make_stub_server(global_topic_list_payload_fn=lambda sv: ["bad"])
        status = s.publish_global_topic_list_now(reason="ime_feedback")
        self.assertFalse(status["ok"])
        self.assertEqual(status["error"], "payload_not_dict")
        self.assertFalse(status["sent"])

    def test_tick_still_appends_remote_by_default(self):
        """周期 tick 行为不变：仍然上报远端 topics。"""
        s = make_stub_server()
        stub_snapshot(s, {"brokerA": True})
        s.global_topic_list_tick()
        self.assertEqual(len(s.remote_appends), 1)

    def test_remote_report_fans_out_to_all_url_templates(self):
        """每个模板一个线程：REPORT_BODY 替换为编码正文，无占位符的模板原样发送。"""
        s = make_stub_server(global_topic_url_list=[
            "https://a.example.net/log/REPORT_BODY",
            "https://b.example.net/fixed",
        ])
        sent = []
        s._send_global_topic_report = sent.append
        MQTTServer._append_global_topic_list_remote(s, {"reason": "ime_feedback"})
        self.assertTrue(_wait_until(lambda: len(sent) == 2, 5), f"上报线程未发齐: {sent}")
        self.assertTrue(sent[0].startswith("https://a.example.net/log/%7B%22reason%22"))
        self.assertTrue(sent[0].endswith("%0A"))   # 末尾换行，远端一行一条
        self.assertEqual(sent[1], "https://b.example.net/fixed")

    def test_remote_report_is_skipped_when_url_list_empty(self):
        """global_topic_url_list=[] 即关闭远端上报，不产生任何线程/请求。"""
        s = make_stub_server(global_topic_url_list=[])
        sent = []
        s._send_global_topic_report = sent.append
        MQTTServer._append_global_topic_list_remote(s, {"reason": "ime_feedback"})
        self.assertEqual(sent, [])

    def test_remote_report_ignores_tls_verify_errors(self):
        """上报故意跳过证书校验（curl -k）：部分机型证书库残留过期根证书。"""
        import ssl
        from unittest import mock

        s = make_stub_server()
        response = mock.MagicMock()
        response.__enter__.return_value = response
        with mock.patch("urllib.request.urlopen", return_value=response) as urlopen:
            s._send_global_topic_report("https://reports.example.net/log/x")
        context = urlopen.call_args.kwargs["context"]
        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 9)
        response.read.assert_called_once()


class BroadcastConfirmedTests(unittest.TestCase):
    """publish_broadcast_confirmed：QoS1 ACK 统计与超时语义（不连真实 broker）。"""

    class _FakeInfo:
        def __init__(self, rc=0, raise_timeout=False):
            self.rc = rc
            self._published = False
            self._raise_timeout = raise_timeout

        def wait_for_publish(self, timeout=None):
            if self._raise_timeout:      # 旧版 paho 超时抛 ValueError
                raise ValueError("timed out")
            self._published = True

        def is_published(self):
            return self._published

    class _FakeClient:
        def __init__(self, info, connected=True):
            self.info = info
            self.connected = connected
            self.published = []

        def is_connected(self):
            return self.connected

        def publish(self, topic, payload, **ka):
            self.published.append((topic, payload, ka))
            return self.info

    @staticmethod
    def _manager(clients):
        manager = object.__new__(MultiMQTTManager)
        manager.clients = clients
        manager.lock = threading.RLock()
        manager.enable_crypto = False
        manager.client_sk = None
        return manager

    def test_counts_only_acked_brokers(self):
        ack_client = self._FakeClient(self._FakeInfo())
        slow_client = self._FakeClient(self._FakeInfo(raise_timeout=True))
        manager = self._manager({"a": ack_client, "b": slow_client})
        result = manager.publish_broadcast_confirmed(
            "t", {"reason": "ime_feedback"}, confirm_timeout=0.05, qos=1
        )
        self.assertEqual(result["queued"], 2)
        self.assertEqual(result["acked"], 1)
        self.assertTrue(result["timeout"])

    def test_skips_disconnected_and_failed_publish(self):
        failed = self._FakeClient(self._FakeInfo(rc=1))
        offline = self._FakeClient(self._FakeInfo(), connected=False)
        manager = self._manager({"a": failed, "b": offline})
        result = manager.publish_broadcast_confirmed(
            "t", {"reason": "x"}, confirm_timeout=0.01
        )
        self.assertEqual(result, {"queued": 0, "acked": 0, "timeout": False})

    def test_same_payload_and_kwargs_as_broadcast(self):
        client = self._FakeClient(self._FakeInfo())
        manager = self._manager({"a": client})
        manager.publish_broadcast_confirmed(
            "t", {"reason": "x"}, confirm_timeout=0.01, qos=1, retain=True
        )
        topic, payload_str, ka = client.published[0]
        self.assertEqual(topic, "t")
        self.assertEqual(ka, {"qos": 1, "retain": True})
        self.assertIsInstance(payload_str, str)
        self.assertIn("x", payload_str)


def _wait_until(cond, timeout, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return cond()


@unittest.skipUnless(os.environ.get("MM_LIVE") == "1",
                     "真实 broker 端到端测试需设置环境变量 MM_LIVE=1")
class LiveBrokerGlobalTopicListTests(unittest.TestCase):
    """真实 broker 端到端：tick 由 _dispatch_loop 驱动，验证 Retain 补发、QoS1、空闲阈值、指令抑制。"""

    @classmethod
    def setUpClass(cls):
        import paho.mqtt.client as paho_mqtt

        cls.tag = uuid.uuid4().hex[:6]
        # 独立随机 topic，避免污染线上 global_topic_list 的 retained 消息
        cls.report_topic = f"test/multi_mqtt/{cls.tag}/global_topic_list"
        cls.request_topic = f"test/multi_mqtt/{cls.tag}/request"
        cls.reply_topic = f"test/multi_mqtt/{cls.tag}/reply"

        # 服务端：所有间隔调小（模拟 24h 阈值与每小时间隔），tick 由 _dispatch_loop 驱动
        cls.server = MQTTServer(
            request_topic=cls.request_topic,
            reply_topic=cls.reply_topic,
            global_topic_list=cls.report_topic,
            global_topic_list_interval=5.0,            # 模拟"每小时"
            global_topic_list_idle_threshold=10.0,     # 模拟"距上次指令 24 小时"
            global_topic_list_check_interval=1.0,
            global_topic_url_list=[],                 # 端到端测试不上报远端站点
        )
        started = cls.server.start(block=False)
        if started is None:
            raise RuntimeError("MQTTServer 启动失败")
        if cls.server.mqtt_net._periodic_fn != cls.server.global_topic_list_tick:
            raise RuntimeError("tick 未注册进 _dispatch_loop 周期任务槽")

        # 订阅端后上线：验证 broker Retain 消息补发；subscribe(qos=1) 即 **ka 透传
        cls.rx = paho_mqtt.Client(
            callback_api_version=paho_mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"mmtest-rx-{cls.tag}",
        )
        cls.rx_messages = []
        cls.rx.on_message = lambda c, u, m: cls.rx_messages.append(m)
        cls.rx.loop_start()
        last_err = None
        for host, port in LIVE_BROKER_CANDIDATES:
            try:
                cls.rx.connect(host, port, keepalive=30)
                if _wait_until(lambda: cls.rx.is_connected(), 10):
                    cls.live_broker = (host, port)
                    break
            except Exception as exc:   # 换下一个候选 broker
                last_err = exc
                try:
                    cls.rx.loop_stop()
                    cls.rx.loop_start()
                except Exception:
                    pass
        else:
            raise RuntimeError(f"订阅端无法连接任何候选 broker: {last_err}")
        cls.rx.subscribe(cls.report_topic, qos=1)   # 要求 broker 以 QoS1 下发
        cls.rx.subscribe(cls.reply_topic)

    @classmethod
    def tearDownClass(cls):
        try:
            # 清除公共 broker 上的测试 retained 消息：发布空 payload retain=True
            cls.rx.publish(cls.report_topic, b"", qos=1, retain=True).wait_for_publish(timeout=10)
        except Exception:
            pass
        try:
            cls.server._unregister_global_topic_list_tick()
            cls.server.mqtt_net.stop()
        finally:
            cls.rx.loop_stop()
            cls.rx.disconnect()

    def _reports(self):
        return [m for m in self.rx_messages if m.topic == self.report_topic and m.payload]

    def test_live_report_flow(self):
        # 1) 订阅端上线即收到 Retain 补发，QoS1（publish qos=1 + subscribe qos=1）
        self.assertTrue(_wait_until(lambda: len(self._reports()) >= 1, 20), "未收到 retained 报告")
        first = self._reports()[0]
        self.assertEqual(first.retain, True, "首条应为 Retain 补发")
        self.assertEqual(first.qos, 1, "下发 QoS 应为 1")
        payload = json.loads(first.payload.decode("utf-8"))
        self.assertEqual(payload["reason"], "startup")
        self.assertEqual(payload["type"], "global_topic_list")
        self.assertIn(self.request_topic, payload["listen_topics"])

        # 2) 空闲期持续发布（interval=5s，观察 12s 应 ≥3 条；tick 由 _dispatch_loop 驱动）
        time.sleep(12)
        reports = self._reports()
        self.assertGreaterEqual(len(reports), 3, f"空闲期报告不足: {len(reports)}")
        for m in reports:
            p = json.loads(m.payload.decode("utf-8"))
            self.assertIn(p["reason"], ("startup", "idle_no_command"))
            self.assertIn(self.request_topic, p["listen_topics"])

        # 3) 下发指令刷新 last_command_ts：空闲报告暂停，指令响应正常
        cmd_id = f"cmd-{self.tag}"
        self.server.mqtt_net.publish_broadcast(
            self.request_topic,
            {"req_id": cmd_id, "code": "21*2", "timestamp": utc_ms(), "reply_topic": self.reply_topic},
        )
        self.assertTrue(
            _wait_until(lambda: any(
                m.topic == self.reply_topic
                and json.loads(m.payload.decode("utf-8")).get("req_id") == cmd_id
                and json.loads(m.payload.decode("utf-8")).get("ok") is True
                for m in self.rx_messages
            ), 15),
            "未收到指令执行响应",
        )
        t_cmd = time.time()
        n_before = len(self._reports())
        time.sleep(7)   # > interval(5s)，若未正确暂停必有新报告
        self.assertEqual(len(self._reports()), n_before, "指令后空闲报告未暂停")

        # 4) 距上次指令超过 idle_threshold(10s) 后自动恢复发布
        self.assertTrue(
            _wait_until(lambda: any(
                time.time() - t_cmd >= 10
                and m.payload
                and json.loads(m.payload.decode("utf-8")).get("reason") == "idle_no_command"
                for m in self._reports()[n_before:]
            ), 20),
            "空闲阈值过后未恢复报告",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
