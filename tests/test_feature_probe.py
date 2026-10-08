import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import client_service
import feature_probe


class ProbeParseTests(unittest.TestCase):
    def test_probe_code_compiles_to_tuple_assignment(self):
        # PROBE_CODE 的唯一实现已收敛到 client_service（probe_online 用）。
        compile(client_service.PROBE_CODE, "remote_probe.py", "exec")
        namespace = {}
        # 模拟目标执行：代码应能独立跑通并给 r 赋 4-tuple（本机平台无关字段）。
        exec(client_service.PROBE_CODE, namespace)
        self.assertIsInstance(namespace["r"], tuple)
        self.assertEqual(len(namespace["r"]), 4)

    def test_parse_tuple_accepts_repr_text(self):
        values = client_service._parse_probe_tuple("('/usr/bin/python3', 'node1', 'arm64', '5.10.0')")
        self.assertEqual(values, ("/usr/bin/python3", "node1", "arm64", "5.10.0"))

    def test_parse_tuple_rejects_junk_and_non_tuple(self):
        self.assertIsNone(client_service._parse_probe_tuple(""))
        self.assertIsNone(client_service._parse_probe_tuple("   "))
        self.assertIsNone(client_service._parse_probe_tuple(None))
        self.assertIsNone(client_service._parse_probe_tuple("not a tuple"))
        self.assertIsNone(client_service._parse_probe_tuple("['a', 'b']"))


class ProbeRunTests(unittest.TestCase):
    SELECTED_TOPIC = "sys/device/k12"

    def _patch_rpc(self, response=None, side_effect=None):
        fake_mqtt = mock.Mock()
        if side_effect is not None:
            fake_mqtt.rpc.side_effect = side_effect
        else:
            fake_mqtt.rpc.return_value = response
        return (
            mock.patch.object(
                feature_probe.client_service,
                "_device_config",
                return_value={"request_topic": self.SELECTED_TOPIC},
            ),
            mock.patch.object(feature_probe.client_service, "_mqtt_client_module", return_value=fake_mqtt),
            fake_mqtt,
        )

    def test_run_success_reports_platform_fields(self):
        raw_repr = "('/data/data/com.qgb.client/files/python/bin/python', 'angler', 'aarch64', '3.10.108')"
        config_patch, mqtt_patch, fake_mqtt = self._patch_rpc(
            {"ok": True, "r": raw_repr, "elapsed_ms": 128}
        )
        with config_patch as device_config, mqtt_patch:
            # 显式传 topic 仍可覆盖默认目标。
            result = json.loads(feature_probe.run("q", 2.0))

        self.assertTrue(result["ok"])
        self.assertEqual(result["topic"], "q")
        self.assertEqual(result["timeout"], 2.0)
        # elapsed_ms 由探测入口实测（不再透传目标自报值），应为非负毫秒数。
        self.assertIsInstance(result["elapsed_ms"], (int, float))
        self.assertGreaterEqual(result["elapsed_ms"], 0)
        self.assertEqual(result["raw"], raw_repr)
        self.assertEqual(result["executable"], "/data/data/com.qgb.client/files/python/bin/python")
        self.assertEqual(result["node"], "angler")
        self.assertEqual(result["machine"], "aarch64")
        self.assertEqual(result["release"], "3.10.108")
        # 显式绕过全局 10s 超时：topic/timeout 必须按参数透传。
        kwargs = fake_mqtt.rpc.call_args.kwargs
        self.assertEqual(kwargs["request_topic"], "q")
        self.assertEqual(kwargs["timeout"], 2.0)
        device_config.assert_called_with(None)

    def test_run_without_topic_follows_selected_target(self):
        raw_repr = "('/usr/bin/python3', 'k12', 'x86_64', '5.15.0')"
        config_patch, mqtt_patch, fake_mqtt = self._patch_rpc(
            {"ok": True, "r": raw_repr, "elapsed_ms": 88}
        )
        with config_patch, mqtt_patch:
            result = json.loads(feature_probe.run())
        # 无参 run()：topic 取当前选中目标的 request_topic，不再写死 "q"。
        self.assertTrue(result["ok"])
        self.assertEqual(result["topic"], self.SELECTED_TOPIC)
        self.assertEqual(fake_mqtt.rpc.call_args.kwargs["request_topic"], self.SELECTED_TOPIC)

    def test_run_timeout_returns_structured_offline_error(self):
        config_patch, mqtt_patch, _ = self._patch_rpc({"r": None, "elapsed_ms": 2000})
        with config_patch, mqtt_patch:
            result = json.loads(feature_probe.run())
        self.assertFalse(result["ok"])
        self.assertEqual(result["topic"], self.SELECTED_TOPIC)
        self.assertIn("timeout", result["error"].lower())

    def test_run_rpc_exception_returns_json_error(self):
        config_patch, mqtt_patch, _ = self._patch_rpc(side_effect=TimeoutError("mqtt timeout"))
        with config_patch, mqtt_patch:
            result = json.loads(feature_probe.run())
        self.assertFalse(result["ok"])
        self.assertIn("TimeoutError", result["error"])

    def test_run_non_tuple_answer_is_flagged_but_raw_kept(self):
        config_patch, mqtt_patch, _ = self._patch_rpc({"ok": True, "r": "'plain string'"})
        with config_patch, mqtt_patch:
            result = json.loads(feature_probe.run())
        self.assertFalse(result["ok"])
        self.assertEqual(result["raw"], "'plain string'")
        self.assertIn("4-tuple", result["error"])

    def test_probe_success_writes_online_health(self):
        raw_repr = "('/usr/bin/python3', 'k12', 'x86_64', '5.15.0')"
        config_patch, mqtt_patch, _ = self._patch_rpc({"ok": True, "r": raw_repr})
        with config_patch, mqtt_patch:
            client_service.probe_online()
            health = json.loads(client_service.target_health())
        self.assertTrue(health["last_probe_ok"])
        self.assertGreater(health["last_probe_at_ms"], 0)
        self.assertEqual(health["node"], "k12")
        self.assertEqual(health["machine"], "x86_64")
        self.assertEqual(health["inflight_count"], 0)

    def test_probe_timeout_marks_offline_with_long_error_but_keeps_timestamp(self):
        config_patch, mqtt_patch, _ = self._patch_rpc(None)
        with config_patch, mqtt_patch:
            result = client_service.probe_online()
            health = json.loads(client_service.target_health())
        self.assertFalse(result["ok"])
        self.assertFalse(health["last_probe_ok"])
        # 探针自身的失败保留时间戳（按正常间隔复探），区别于业务超时的强制清零。
        self.assertGreater(health["last_probe_at_ms"], 0)
        self.assertEqual(health["inflight_count"], 0)


if __name__ == "__main__":
    unittest.main()
