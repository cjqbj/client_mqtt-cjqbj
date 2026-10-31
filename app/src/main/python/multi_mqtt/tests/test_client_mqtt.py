import builtins
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import client_mqtt
from multi_mqtt import MultiMQTTManager, get_standard_pem_bytes


class ClientMqttTests(unittest.TestCase):
    def test_request_waits_for_minimum_brokers_and_checks_publish_count(self):
        node = client_mqtt.MQTTClientNode()
        calls = []

        class _FakeNet:
            client_private_key_bytes = None

            def wait_connected(self, min_count, timeout):
                calls.append(("wait", min_count, timeout))
                return min_count

            def publish_broadcast(self, topic, payload, **_kwargs):
                calls.append(("publish", topic))
                node._on_message(
                    client_mqtt.REPLY_TOPIC,
                    {"req_id": payload["req_id"], "ok": True},
                    "broker-test",
                )
                return 5

        node.mqtt_net = _FakeNet()
        response = node.request("3", timeout=0.2, min_broker_count=5)

        self.assertTrue(response["ok"])
        self.assertEqual(calls, [
            ("wait", 5, 0.2),
            ("publish", client_mqtt.REQUEST_TOPIC),
        ])

    def test_request_does_not_publish_when_minimum_brokers_are_unavailable(self):
        node = client_mqtt.MQTTClientNode()
        published = []

        class _FakeNet:
            client_private_key_bytes = None

            def wait_connected(self, min_count, timeout):
                return min_count - 1

            def publish_broadcast(self, *_args, **_kwargs):
                published.append(True)

        node.mqtt_net = _FakeNet()
        response = node.request("3", timeout=0.01, min_broker_count=5)

        self.assertEqual(response, {})
        self.assertEqual(published, [])

    def test_request_does_not_wait_for_response_if_publish_count_is_too_low(self):
        node = client_mqtt.MQTTClientNode()

        class _FakeNet:
            client_private_key_bytes = None

            def wait_connected(self, min_count, timeout):
                return min_count

            def publish_broadcast(self, *_args, **_kwargs):
                return 4

        node.mqtt_net = _FakeNet()
        response = node.request("3", timeout=0.01, min_broker_count=5)

        self.assertEqual(response, {})
        self.assertEqual(node.pending_requests, {})

    def test_fallback_input_ignores_invalid_python(self):
        answers = [
            "/workspaces/build_xime_home/git.py push https://example.com/repo -m",
            "print(123)",
        ]
        original_input = builtins.input
        builtins.input = lambda prompt="": answers.pop(0)
        try:
            result = client_mqtt._fallback_code_input()
            self.assertEqual(result, "print(123)")
        finally:
            builtins.input = original_input

    def test_request_uses_configured_private_key_by_default(self):
        node = client_mqtt.MQTTClientNode(client_private_key_bytes="2333")
        self.assertIsNotNone(node.mqtt_net.client_private_key_bytes)

    def test_expression_private_key_is_supported(self):
        pem = get_standard_pem_bytes("1+1")
        self.assertIn(b"BEGIN EC PRIVATE KEY", pem)

    def test_stats_intervals_are_applied_to_mqtt_net(self):
        """client_mqtt 的统计周期参数必须透传到 MultiMQTTManager.stats。"""
        node = client_mqtt.MQTTClientNode(
            stats_print_interval=75,
            stats_ping_interval=25,
        )
        self.assertEqual(node.mqtt_net.stats.print_interval, 75.0)
        self.assertEqual(node.mqtt_net.stats.ping_interval, 25.0)

        default_node = self._node_with_client_rpc({})
        self.assertEqual(
            default_node.mqtt_net.stats.print_interval,
            default_node.mqtt_net.stats.PRINT_INTERVAL,
        )
        self.assertEqual(
            default_node.mqtt_net.stats.ping_interval,
            default_node.mqtt_net.stats.PING_INTERVAL,
        )

    def test_stats_interval_cli_aliases_are_generated(self):
        """CLI 同时支持新键名与历史 report/print 别名。"""
        options = client_mqtt._cli_opts(*client_mqtt.alias_stats_print_interval)
        self.assertIn("--stats_print_interval", options)
        self.assertIn("--report_interval", options)
        self.assertIn("--print_interval", options)
        ping_options = client_mqtt._cli_opts(*client_mqtt.alias_stats_ping_interval)
        self.assertIn("--stats_ping_interval", ping_options)
        self.assertIn("--ping_interval", ping_options)

    def _node_with_client_rpc(self, document, **kwargs):
        """用临时 rpc.json 构造客户端，避免读仓库根目录的真实配置。"""
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "rpc.json")
            with open(path, "w", encoding="utf-8") as file:
                json.dump(document, file)
            with patch.object(client_mqtt, "CLIENT_RPC_CONFIG_PATH", path):
                return client_mqtt.MQTTClientNode(**kwargs)

    def test_client_stats_print_interval_prefers_client_specific_key(self):
        """client_stats_print_interval 优先于 stats_print_interval。"""
        node = self._node_with_client_rpc({
            "client_stats_print_interval": 90,
            "stats_print_interval": 120,
        })
        self.assertEqual(node.mqtt_net.stats.print_interval, 90.0)

    def test_client_stats_print_interval_falls_back_to_shared_key(self):
        """客户端专属键缺失/null 时回退 stats_print_interval。"""
        for document in (
            {"stats_print_interval": 120},
            {"client_stats_print_interval": None, "stats_print_interval": 120},
            {"client_stats_print_interval": "", "stats_print_interval": 120},
        ):
            node = self._node_with_client_rpc(document)
            self.assertEqual(node.mqtt_net.stats.print_interval, 120.0)

    def test_client_stats_print_interval_falls_back_to_original_default(self):
        """两个键都没有时回退 Python 原有默认行为（3600s）。"""
        node = self._node_with_client_rpc({})
        self.assertEqual(
            node.mqtt_net.stats.print_interval,
            node.mqtt_net.stats.PRINT_INTERVAL,
        )

    def test_client_stats_print_interval_explicit_argument_wins(self):
        """CLI/rpc(...)/构造参数显式传入时优先于 rpc.json。"""
        node = self._node_with_client_rpc(
            {"client_stats_print_interval": 90},
            stats_print_interval=30,
        )
        self.assertEqual(node.mqtt_net.stats.print_interval, 30.0)

    def test_client_stats_print_interval_invalid_values_fall_back(self):
        """非法客户端值不阻塞启动：回退 stats_print_interval，再回退默认。"""
        node = self._node_with_client_rpc({
            "client_stats_print_interval": "bad",
            "stats_print_interval": 120,
        })
        self.assertEqual(node.mqtt_net.stats.print_interval, 120.0)

        node = self._node_with_client_rpc({
            "client_stats_print_interval": -1,
            "stats_print_interval": "bad",
        })
        self.assertEqual(
            node.mqtt_net.stats.print_interval,
            node.mqtt_net.stats.PRINT_INTERVAL,
        )

    def test_signed_request_is_blocked_when_server_pubkey_is_missing(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123|deadbeef"
        node.pending_requests["req-123"] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertFalse(event.set.called)
        self.assertIsNone(node.pending_requests["req-123"]["response"])

    def test_late_duplicate_response_goes_to_extras_without_overwriting_first(self):
        # 旧协议里客户端节点持有 server_public_key_bytes；现协议客户端不再保存
        # 服务端公钥（公钥只在服务端侧验客户端请求签名），"服务端已验签"由
        # 回包 req_id 不带签名后缀（干净 req_id）表示，该接受路径已由
        # test_signed_request_is_accepted_when_response_req_id_is_clean 覆盖。
        # 本用例改测多应答者仲裁：首包落定后，持相同 key 的其他服务端的迟到
        # 同 req_id 回包必须进 extras，不得覆盖首包。
        node = client_mqtt.MQTTClientNode(client_private_key_bytes="2333")
        event = Mock()
        req_id = "req-123"
        ctx = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "extras": [],
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }
        node.pending_requests[req_id] = ctx

        node._on_message(
            "sys/device/response",
            {"req_id": req_id, "ok": True, "node": "first"},
            "mqtt.emqx.io",
        )
        first = ctx["response"]
        self.assertEqual(first["node"], "first")
        self.assertEqual(event.set.call_count, 1)

        node._on_message(
            "sys/device/response",
            {"req_id": req_id, "ok": True, "node": "late"},
            "broker.emqx.io",
        )

        self.assertIs(ctx["response"], first, "迟到回包不能覆盖首包")
        self.assertEqual(len(ctx["extras"]), 1)
        self.assertEqual(ctx["extras"][0]["node"], "late")
        self.assertEqual(ctx["extras"][0]["req_id"], req_id)
        self.assertEqual(event.set.call_count, 1, "迟到回包不应再次 set 事件")

    def test_signed_request_is_accepted_when_response_req_id_is_clean(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123"
        node.pending_requests[req_id] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": False,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertTrue(event.set.called)
        self.assertIsNotNone(node.pending_requests[req_id]["response"])

    def test_signed_request_with_allow_flag_accepts_no_pubkey_response(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
        )
        event = Mock()
        req_id = "req-123|deadbeef"
        node.pending_requests["req-123"] = {
            "event": event,
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": True,
        }

        node._on_message("sys/device/response", {"req_id": req_id, "ok": True}, "mqtt.emqx.io")

        self.assertTrue(event.set.called)
        self.assertEqual(node.pending_requests["req-123"]["response"]["req_id"], "req-123")

    def test_request_uses_client_allow_flag_by_default(self):
        node = client_mqtt.MQTTClientNode(
            client_private_key_bytes="2333",
            allow_no_server_pubkey_response=True,
        )
        node.mqtt_net.publish_broadcast = Mock()

        req_id = "req-999"
        node.pending_requests[req_id] = {
            "event": Mock(),
            "start_time": 0.0,
            "response": None,
            "client_private_key_bytes": "1+1",
            "allow_no_server_pubkey_response": True,
        }

        node._on_message(
            "sys/device/response",
            {"req_id": "req-999|deadbeef", "ok": True},
            "mqtt.emqx.io",
        )

        self.assertTrue(node.pending_requests[req_id]["response"]["req_id"] == "req-999")


class WaitConnectedTests(unittest.TestCase):
    """wait_connected：首个 broker 上线即放行，替代写死的 sleep(2)。"""

    def _manager_with_fakes(self, statuses):
        """statuses: list[bool|Exception]，构造同数量的假 paho client。"""
        class _FakeClient:
            def __init__(self, state):
                self._state = state

            def is_connected(self):
                if isinstance(self._state, Exception):
                    raise self._state
                return self._state

        mgr = MultiMQTTManager(brokers=[], enable_stats=False)
        mgr.clients = {"b%d" % i: _FakeClient(s) for i, s in enumerate(statuses)}
        return mgr

    def test_returns_immediately_when_one_already_connected(self):
        mgr = self._manager_with_fakes([True, False, False])
        t0 = time.monotonic()
        online = mgr.wait_connected(min_count=1, timeout=2.0)
        self.assertEqual(online, 1)
        self.assertLess(time.monotonic() - t0, 0.1,
                        "已有连接时必须立即放行，不该再 sleep")

    def test_times_out_returning_zero_when_none_connects(self):
        mgr = self._manager_with_fakes([False, False])
        t0 = time.monotonic()
        online = mgr.wait_connected(min_count=1, timeout=0.3,
                                    poll_interval=0.02)
        self.assertEqual(online, 0)
        self.assertGreaterEqual(time.monotonic() - t0, 0.25)

    def test_unblocks_as_soon_as_threshold_is_reached(self):
        class _UpClient:
            def __init__(self, up):
                self.up = up

            def is_connected(self):
                return self.up

        mgr = MultiMQTTManager(brokers=[], enable_stats=False)
        late = _UpClient(False)
        mgr.clients = {"fast": _UpClient(True), "late": late}

        def flip():
            time.sleep(0.2)
            late.up = True

        threading.Thread(target=flip, daemon=True).start()
        t0 = time.monotonic()
        online = mgr.wait_connected(min_count=2, timeout=3.0,
                                    poll_interval=0.02)
        elapsed = time.monotonic() - t0
        self.assertEqual(online, 2)
        self.assertGreaterEqual(elapsed, 0.15)
        self.assertLess(elapsed, 1.0,
                        "必须在第二个连接一上线就放行，而不是等满超时")

    def test_is_connected_exception_counts_as_offline(self):
        mgr = self._manager_with_fakes([RuntimeError("paho boom"), True])
        online = mgr.wait_connected(min_count=1, timeout=0.2,
                                    poll_interval=0.02)
        self.assertEqual(online, 1)


class PublishBroadcastTests(unittest.TestCase):
    def test_returns_number_of_brokers_that_accepted_publish(self):
        class _FakeClient:
            def __init__(self, rc):
                self.rc = rc

            def is_connected(self):
                return True

            def publish(self, *_args, **_kwargs):
                return SimpleNamespace(rc=self.rc)

        mgr = MultiMQTTManager(brokers=[], enable_stats=False)
        mgr.clients = {
            "accepted": _FakeClient(0),
            "rejected": _FakeClient(15),
            "also-accepted": _FakeClient(0),
        }

        self.assertEqual(mgr.publish_broadcast("topic", {"value": 1}), 2)


class TimestampMsInjectionTests(unittest.TestCase):
    """[救援通道] timestamp_ms 注入：req_id/timestamp 同源于注入的服务器时钟。

    服务端验签开启且时钟偏差超窗时，"按服务器钟签名"是唯一 MQTT 侧自救
    手段（签名覆盖 timestamp，可签任意时间）；本组锁定注入链路本身。
    """

    def _node_with_capture_net(self, captured):
        node = client_mqtt.MQTTClientNode()

        class _FakeNet:
            client_private_key_bytes = None

            def publish_broadcast(self, topic, payload, **_ka):
                captured.update(payload)
                node._on_message(client_mqtt.REPLY_TOPIC,
                                 {"req_id": payload["req_id"], "ok": True},
                                 "broker-test")
                return 5

        node.mqtt_net = _FakeNet()
        return node

    def test_request_uses_injected_timestamp_and_derives_req_id(self):
        captured = {}
        node = self._node_with_capture_net(captured)
        server_ms = 1666622867720   # 模拟慢钟服务器时间（2022）
        resp = node.request("1+1", timeout=0.5, timestamp_ms=server_ms)
        self.assertTrue(resp["ok"])
        self.assertEqual(captured["timestamp"], server_ms,
                         "报文 timestamp 必须用注入值（穿窗的关键）")
        self.assertEqual(captured["req_id"], client_mqtt.get_req_id(server_ms),
                         "req_id 应由同一注入值派生（保持与 timestamp 同源的既有风格）")

    def test_request_default_uses_local_clock(self):
        from rpc_executor import utc_ms
        captured = {}
        node = self._node_with_capture_net(captured)
        t0, t1 = utc_ms(), None
        resp = node.request("1+1", timeout=0.5)
        t1 = utc_ms()
        self.assertTrue(resp["ok"])
        self.assertGreaterEqual(captured["timestamp"], t0)
        self.assertLessEqual(captured["timestamp"], t1)

    def test_rpc_passes_ts_alias_through(self):
        stub = SimpleNamespace(request=Mock(return_value={"ok": True}))
        orig = client_mqtt._default_client
        client_mqtt._default_client = stub
        try:
            out = client_mqtt.rpc("1+1", "t/x", timeout=1,
                                  ts=1666622867720)
            self.assertEqual(out, {"ok": True})
            self.assertEqual(stub.request.call_args.kwargs.get("timestamp_ms"),
                             1666622867720, "rpc 的 ts 别名必须透传为 timestamp_ms")
            client_mqtt.rpc("1+1", "t/x", timeout=1)
            self.assertIsNone(stub.request.call_args.kwargs.get("timestamp_ms"),
                              "未传时透传 None（走默认本机时钟路径）")
        finally:
            client_mqtt._default_client = orig


if __name__ == "__main__":
    unittest.main()
