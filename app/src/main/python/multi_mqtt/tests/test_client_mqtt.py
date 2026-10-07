import builtins
import os
import sys
import threading
import time
import unittest
from unittest.mock import Mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import client_mqtt
from multi_mqtt import MultiMQTTManager, get_standard_pem_bytes


class ClientMqttTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
