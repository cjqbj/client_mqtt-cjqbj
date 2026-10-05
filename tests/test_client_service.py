import ast
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import client_service
import feature_camera
import feature_files
import feature_wifi


class ClientServiceTests(unittest.TestCase):
    def test_general_online_probe_settings_have_defaults_and_bounds(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                defaults = json.loads(client_service.general_settings())
                self.assertTrue(defaults["online_probe_enabled"])
                self.assertEqual(defaults["online_probe_interval"], 30)

                updated = json.loads(client_service.update_general_settings({
                    "online_probe_enabled": False,
                    "online_probe_interval": 99999,
                }))
                self.assertFalse(updated["online_probe_enabled"])
                self.assertEqual(updated["online_probe_interval"], 3600)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_mqtt_loader_uses_flat_modules_not_package_submodules(self):
        previous_file = client_service.__file__
        previous_path = list(sys.path)
        previous_modules = {
            name: module for name, module in sys.modules.items()
            if name == "multi_mqtt" or name.startswith("multi_mqtt.") or name == "client_mqtt"
        }
        with tempfile.TemporaryDirectory() as directory:
            module_dir = Path(directory) / "multi_mqtt"
            module_dir.mkdir()
            (module_dir / "multi_mqtt.py").write_text("class MultiMQTTManager: pass\n", encoding="utf-8")
            (module_dir / "client_mqtt.py").write_text(
                "from multi_mqtt import MultiMQTTManager\ndef rpc(*args, **kwargs): return {'ok': True}\n",
                encoding="utf-8",
            )
            try:
                client_service.__file__ = str(Path(directory) / "client_service.py")
                for name in tuple(sys.modules):
                    if name == "multi_mqtt" or name.startswith("multi_mqtt.") or name == "client_mqtt":
                        sys.modules.pop(name, None)

                module = client_service._mqtt_client_module()

                self.assertEqual(module.__name__, "client_mqtt")
                self.assertEqual(Path(module.__file__), module_dir / "client_mqtt.py")
                self.assertTrue(module.rpc()["ok"])
            finally:
                client_service.__file__ = previous_file
                sys.path[:] = previous_path
                for name in tuple(sys.modules):
                    if name == "multi_mqtt" or name.startswith("multi_mqtt.") or name == "client_mqtt":
                        sys.modules.pop(name, None)
                sys.modules.update(previous_modules)

    def test_broken_runtime_feature_returns_json_instead_of_raising(self):
        import bootstrap

        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.pop("feature_broken", None)
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                Path(directory, "feature_broken.py").write_text(
                    "FEATURE = {'name': 'broken'}\ndef run():\n    raise RuntimeError('plugin failed')\n",
                    encoding="utf-8",
                )

                result = json.loads(client_service.call_feature("broken", "run"))

                self.assertFalse(result["ok"])
                self.assertEqual(result["feature"], "broken")
                self.assertEqual(result["error"], "RuntimeError: plugin failed")

                Path(directory, "feature_broken.py").write_text(
                    "FEATURE = {'name': 'broken'}\ndef run():\n    raise SystemExit('plugin exit')\n",
                    encoding="utf-8",
                )
                # 缓存策略：文件改写后不自动重载，行为仍为旧模块的 RuntimeError。
                result = json.loads(client_service.call_feature("broken", "run"))
                self.assertEqual(result["error"], "RuntimeError: plugin failed")
                # 显式 reload 后新代码才生效，SystemExit 同样被桥接成结构化错误。
                self.assertTrue(json.loads(client_service.reload_feature("broken"))["ok"])
                result = json.loads(client_service.call_feature("broken", "run"))
                self.assertEqual(result["error"], "SystemExit: plugin exit")
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                sys.modules.pop("feature_broken", None)
                if previous_module is not None:
                    sys.modules["feature_broken"] = previous_module

    def test_rpc_passes_private_key_expression_to_mqtt_normalizer(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/key-test",
                    "private_key": "233",
                })
                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.return_value = b"-----BEGIN EC PRIVATE KEY-----"
                mqtt_module.rpc.return_value = {"ok": True, "r": "{}"}
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    client_service.rpc("r = {}")

                self.assertEqual(mqtt_module.rpc.call_args.kwargs["client_private_key_bytes"], "233")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_private_key_standardization_uses_upstream_normalizer(self):
        mqtt_module = mock.Mock()
        mqtt_module.get_standard_pem_bytes.return_value = b"-----BEGIN EC PRIVATE KEY-----\nredacted\n"
        with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
            result = client_service.standardize_private_key("233")
        self.assertTrue(result.startswith("-----BEGIN EC PRIVATE KEY-----"))
        mqtt_module.get_standard_pem_bytes.assert_called_once_with("233")

    def test_private_key_standardization_converts_integer_to_pem(self):
        pem = client_service.standardize_private_key("233")
        self.assertTrue(pem.startswith("-----BEGIN EC PRIVATE KEY-----"))
        self.assertTrue(pem.rstrip().endswith("-----END EC PRIVATE KEY-----"))

    def test_private_key_standardization_accepts_exponent_expression(self):
        pem = client_service.standardize_private_key("233")
        self.assertTrue(pem.startswith("-----BEGIN EC PRIVATE KEY-----"))
        self.assertTrue(pem.rstrip().endswith("-----END EC PRIVATE KEY-----"))

    def test_rpc_timeout_logs_topic_elapsed_and_broker_state_without_secrets(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                    "private_key": "DO_NOT_LOG_THIS_KEY",
                    "timeout": 5,
                })
                broker_client = mock.Mock()
                broker_client.is_connected.return_value = False
                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.return_value = b"-----BEGIN EC PRIVATE KEY-----\nredacted\n"
                mqtt_module.rpc.return_value = None
                mqtt_module._default_client = mock.Mock(
                    mqtt_net=mock.Mock(clients={"broker.example": broker_client})
                )
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    result = client_service.rpc("wifi probe")

                self.assertEqual(result["error"], "RPC timeout")
                self.assertEqual(result["topic"], "sys/device/k12")
                self.assertEqual(result["broker_states"], ["broker.example:disconnected"])
                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("topic=sys/device/k12", logs)
                self.assertIn("brokers=[broker.example:disconnected]", logs)
                self.assertIn("private_key_configured=yes", logs)
                self.assertIn("key_format=raw-text", logs)
                self.assertIn("normalized_bytes=40", logs)
                self.assertNotIn("DO_NOT_LOG_THIS_KEY", logs)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rpc_success_log_shows_request_source_and_response_metadata(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                })
                mqtt_module = mock.Mock()
                mqtt_module.rpc.return_value = {
                    "ok": True,
                    "r": "{}",
                    "req_id": "server-req-1",
                    "server_time": 1790421445203,
                    "latency_ms": 329.5,
                    "server_from": "server-broker",
                    "client_from": "client-broker",
                }
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    response = client_service.rpc("DO_NOT_LOG_RPC_SOURCE")

                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("topic=sys/device/k12", logs)
                self.assertIn("phase=response", logs)
                self.assertIn("MQTT RESPONSE ENVELOPE", logs)
                self.assertIn('"r": "{}"', logs)
                self.assertIn("req_id=server-req-1", logs)
                self.assertIn("server_time=1790421445203", logs)
                self.assertIn("server=server-broker", logs)
                self.assertIn("client=client-broker", logs)
                self.assertIn("REQUEST CODE", logs)
                self.assertIn("DO_NOT_LOG_RPC_SOURCE", logs)
                parsed = client_service.parse_json_result(response)
                self.assertEqual(parsed["_rpc"]["req_id"], "server-req-1")
                health = json.loads(client_service.target_health("sys/device/k12"))
                self.assertTrue(health["last_probe_ok"])
                self.assertGreater(health["last_success_at_ms"], 0)
                self.assertEqual(health["inflight_count"], 0)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rpc_exception_logs_type_but_not_exception_payload(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                })
                mqtt_module = mock.Mock()
                mqtt_module.rpc.side_effect = ValueError("private key rejected: SECRET_VALUE")
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    result = client_service.rpc("wifi probe")

                self.assertIn("SECRET_VALUE", result["error"])
                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("exception_type=ValueError", logs)
                self.assertNotIn("SECRET_VALUE", logs)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rpc_logs_key_normalization_failure_without_key_value(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                    "private_key": "SECRET_BAD_KEY",
                })
                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.side_effect = ValueError("invalid private key SECRET_BAD_KEY")
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    result = client_service.rpc("wifi probe")

                self.assertEqual(result["topic"], "sys/device/k12")
                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("phase=key-normalization-failed", logs)
                self.assertIn("key_configured=yes", logs)
                self.assertIn("exception_type=ValueError", logs)
                self.assertNotIn("SECRET_BAD_KEY", logs)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_scan_code_is_bounded_and_json_based(self):
        code = feature_files.build_scan_code("/data/data", offset=10, limit=3)
        ast.parse(code)
        self.assertIn("has_more", code)
        self.assertIn("next_offset", code)
        self.assertIn("seen < start", code)

    def test_scan_returns_directories_first_and_pages_without_skipping(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "b-file.txt").write_text("b", encoding="utf-8")
            Path(root, "a-folder").mkdir()
            Path(root, "c-folder").mkdir()

            first_env = {}
            exec(feature_files.build_scan_code(root, offset=0, limit=2), first_env)
            first = json.loads(first_env["r"])
            self.assertEqual([item["kind"] for item in first["items"]], ["directory", "directory"])
            self.assertEqual(first["next_offset"], 2)
            self.assertTrue(first["has_more"])

            second_env = {}
            exec(feature_files.build_scan_code(root, offset=2, limit=2), second_env)
            second = json.loads(second_env["r"])
            self.assertEqual(len(second["items"]), 1)
            self.assertEqual(second["items"][0]["kind"], "file")
            self.assertEqual(second["next_offset"], 3)
            self.assertFalse(second["has_more"])

    def test_path_cannot_escape_root(self):
        self.assertEqual(
            client_service.normalize_relative_path("/data/data", "logs/today.txt"),
            "/data/data/logs/today.txt",
        )
        with self.assertRaises(ValueError):
            client_service.normalize_relative_path("/data/data", "../secret")

    def test_photo_code_has_no_mqtt_payload_or_aliyun_secret(self):
        code = feature_camera.build_photo_code(1)
        self.assertIn("bytes(data)", code)
        self.assertIn("aliyun_git.upload", code)
        self.assertNotIn('token=', code)
        self.assertNotIn('domain=', code)

    def test_feature_rpc_generators_are_owned_by_features(self):
        self.assertFalse(hasattr(client_service, "build_wifi_code"))
        self.assertFalse(hasattr(client_service, "scan_remote"))
        self.assertFalse(hasattr(client_service, "build_scan_code"))
        self.assertFalse(hasattr(client_service, "upload_remote"))
        wifi_code = feature_wifi.build_wifi_code()
        ast.parse(wifi_code)
        self.assertIn('"mac"', wifi_code)
        self.assertIn('"ip"', wifi_code)
        ast.parse(feature_files.build_upload_code("/data/file.txt"))

    def test_wifi_feature_returns_json_for_the_app_ui(self):
        response = {"r": json.dumps({"ok": True, "wifi": {"ip": "192.168.1.11"}})}
        with mock.patch.object(client_service, "rpc", return_value=response):
            result = json.loads(feature_wifi.info())
        self.assertEqual(result["wifi"]["ip"], "192.168.1.11")

        timeout = {"ok": False, "error": "RPC timeout"}
        with mock.patch.object(client_service, "rpc", return_value=timeout):
            result = json.loads(feature_wifi.info())
        self.assertEqual(result, timeout)

    def test_parse_json_result_preserves_structured_rpc_errors(self):
        response = {"ok": False, "error": "RPC timeout", "elapsed_ms": 5000}
        result = client_service.parse_json_result(response)
        self.assertEqual(result["ok"], False)
        self.assertEqual(result["error"], "RPC timeout")
        self.assertEqual(result["_rpc"]["elapsed_ms"], 5000)

    def test_server_python_traceback_is_summarized_and_key_redacted(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                    "private_key": "PRIVATE_KEY_SECRET",
                })
                response = {
                    "ok": False,
                    "req_id": "server-req-error",
                    "server_from": "broker.example",
                    "r": None,
                    "error": "Traceback (most recent call last):\n  File \"<rpc>\", line 1\nRuntimeError: failed PRIVATE_KEY_SECRET",
                }
                with mock.patch.object(client_service, "_mqtt_client_module") as loader:
                    loader.return_value.rpc.return_value = response
                    result = client_service.rpc("raise RuntimeError('PRIVATE_KEY_SECRET')")

                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("MQTT RESPONSE ENVELOPE", logs)
                self.assertIn('"r": null', logs)
                self.assertIn("Traceback (most recent call last)", logs)
                parsed = client_service.parse_json_result(result)
                self.assertIn("RuntimeError: failed <redacted>", logs)
                self.assertNotIn("PRIVATE_KEY_SECRET", logs)
                self.assertIn("REQUEST CODE", logs)
                self.assertIn("raise RuntimeError", logs)
                self.assertNotIn("PRIVATE_KEY_SECRET", logs)
                self.assertEqual(parsed["error"], response["error"])
                self.assertEqual(parsed["_rpc"]["server_from"], "broker.example")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_parse_json_result_never_returns_null_or_non_object(self):
        self.assertFalse(client_service.parse_json_result({"ok": True, "r": None})["ok"])
        self.assertFalse(client_service.parse_json_result({"ok": True, "r": "null"})["ok"])
        self.assertEqual(
            client_service.parse_json_result({"ok": True, "r": "[1,2]"})["error"],
            "target RPC result must be a JSON object, got list",
        )

    def test_files_feature_actions_use_shared_rpc_and_return_json(self):
        page = {"ok": True, "items": [], "has_more": False, "next_offset": 0}
        with mock.patch.object(client_service, "rpc", return_value={"r": json.dumps(page)}) as rpc_call:
            result = json.loads(feature_files.scan("/data", 0, 20))
        self.assertEqual(result, page)
        self.assertIn("os.walk", rpc_call.call_args.args[0])

        transfer = {"ok": True, "url": "https://example.invalid/file", "name": "file"}
        with mock.patch.object(client_service, "rpc", return_value={"r": json.dumps(transfer)}) as rpc_call:
            result = json.loads(feature_files.upload("/data/file"))
        self.assertEqual(result, transfer)
        self.assertIn("aliyun_git.upload", rpc_call.call_args.args[0])

    def test_camera_feature_returns_json_metadata_for_the_app_ui(self):
        capture = {"ok": True, "url": "https://example.invalid/photo.jpg", "facing": 1}
        with mock.patch.object(client_service, "rpc", return_value={"r": json.dumps(capture)}) as rpc_call:
            result = json.loads(feature_camera.capture(1))
        self.assertEqual(result, capture)
        self.assertIn("Camera.open(1)", rpc_call.call_args.args[0])

    def test_device_settings_are_isolated_by_request_topic(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/one",
                    "remote_root": "/data/one",
                })
                client_service.update_device_settings("", {
                    "request_topic": "sys/device/two",
                    "remote_root": "/data/two",
                })
                client_service.update_aliyun_settings({"aliyun": {"bucket": "shared"}})

                client_service.select_device("sys/device/one")
                first = json.loads(client_service.device_settings())
                client_service.select_device("sys/device/two")
                second = json.loads(client_service.device_settings())

                self.assertEqual(first["remote_root"], "/data/one")
                self.assertEqual(second["remote_root"], "/data/two")
                self.assertNotIn("aliyun", first)
                self.assertNotIn("aliyun", second)
                self.assertEqual(json.loads(client_service.aliyun_settings())["aliyun"], {"bucket": "shared"})
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_device_id_survives_topic_rename_and_external_file_edit(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                created = json.loads(client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/old",
                    "remote_root": "/data/old",
                    "aliyun": {"bucket": "before"},
                }))
                device_id = created["id"]
                renamed = json.loads(client_service.update_device_settings(device_id, {
                    "request_topic": "sys/device/new",
                    "remote_root": "/data/new",
                    "aliyun": {"bucket": "after"},
                }))
                self.assertEqual(renamed["id"], device_id)
                self.assertEqual(len(json.loads(client_service.device_catalog())), 1)

                config_path = client_service._STATE["config_path"]
                with open(config_path, "r", encoding="utf-8") as config_file:
                    config = json.load(config_file)
                config["devices"][0]["remote_root"] = "/data/external-edit"
                with open(config_path, "w", encoding="utf-8") as config_file:
                    json.dump(config, config_file)

                refreshed = json.loads(client_service.device_settings(device_id))
                self.assertEqual(refreshed["request_topic"], "sys/device/new")
                self.assertEqual(refreshed["remote_root"], "/data/external-edit")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_incomplete_aliyun_draft_keeps_last_valid_global_config(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_aliyun_settings({"aliyun": {"bucket": "valid"}})
                saved = json.loads(client_service.update_aliyun_settings({"aliyun_json_draft": "{"}))

                self.assertEqual(saved["aliyun"], {"bucket": "valid"})
                self.assertEqual(saved["aliyun_json_draft"], "{")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_initialize_migrates_legacy_per_topic_aliyun_to_shared_setting(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                path = Path(files_dir) / "client_mqtt.json"
                path.write_text(json.dumps({
                    "devices": [{
                        "request_topic": "sys/device/one",
                        "aliyun": {"bucket": "legacy"},
                    }]
                }), encoding="utf-8")

                client_service.initialize(files_dir)
                config = client_service.load_config()

                self.assertEqual(config["aliyun"], {"bucket": "legacy"})
                self.assertNotIn("aliyun", config["devices"][0])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_device_catalog_assigns_id_to_external_target(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                config_path = client_service._STATE["config_path"]
                with open(config_path, "r", encoding="utf-8") as config_file:
                    config = json.load(config_file)
                config["devices"].append({"request_topic": "sys/device/external"})
                with open(config_path, "w", encoding="utf-8") as config_file:
                    json.dump(config, config_file)

                devices = json.loads(client_service.device_catalog())
                added = next(device for device in devices if device["request_topic"] == "sys/device/external")
                self.assertTrue(added["id"])
                self.assertEqual(added["remote_root"], "/data/data")
                with open(config_path, "r", encoding="utf-8") as config_file:
                    saved = json.load(config_file)
                self.assertEqual(saved["devices"][1]["id"], added["id"])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_selected_device_persists_across_reinitialize(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/one",
                    "remote_root": "/data/one",
                })
                client_service.update_device_settings("sys/device/one", {
                    "request_topic": "sys/device/two",
                    "remote_root": "/data/two",
                })
                selected_two = json.loads(client_service.device_settings())
                self.assertEqual(selected_two["request_topic"], "sys/device/two")

                # 模拟应用被杀掉后重启：重新 initialize 必须恢复上次选择的 topic。
                client_service.initialize(files_dir)
                restored = json.loads(client_service.device_settings())
                self.assertEqual(restored["request_topic"], "sys/device/two")
                self.assertEqual(client_service._STATE["selected_topic"], "sys/device/two")

                with open(client_service._STATE["config_path"], "r", encoding="utf-8") as handle:
                    saved_config = json.load(handle)
                self.assertEqual(saved_config["selected_device_id"], restored["id"])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_selected_feature_is_persisted_per_device(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                first_id = json.loads(client_service.device_settings())["id"]
                second_id = json.loads(client_service.update_device_settings("sys/device/two", {
                    "request_topic": "sys/device/two",
                }))["id"]

                client_service.select_feature("camera", first_id)
                client_service.select_feature("wifi", second_id)
                self.assertEqual(client_service.selected_feature(first_id), "camera")
                self.assertEqual(client_service.selected_feature(second_id), "wifi")
                # device_ref 缺省时跟随当前选中设备（update_device_settings 选中了 second）。
                self.assertEqual(client_service.selected_feature(), "wifi")

                # 模拟重启：重新 initialize 后按设备恢复各自的 tab。
                client_service.initialize(files_dir)
                self.assertEqual(client_service.selected_feature(first_id), "camera")
                self.assertEqual(client_service.selected_feature(second_id), "wifi")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_feature_settings_are_isolated_by_device_and_feature(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                first_id = json.loads(client_service.device_settings())["id"]
                second_id = json.loads(client_service.update_device_settings("sys/device/two", {
                    "request_topic": "sys/device/two",
                }))["id"]

                merged = json.loads(client_service.update_feature_settings("camera", {"lens_facing": 1}, first_id))
                self.assertEqual(merged["lens_facing"], 1)
                # 浅合并保留已有键。
                merged = json.loads(client_service.update_feature_settings("camera", {"flash": "off"}, first_id))
                self.assertEqual(merged, {"lens_facing": 1, "flash": "off"})
                # 设备与 feature 双重隔离。
                self.assertEqual(json.loads(client_service.feature_settings("camera", second_id)), {})
                self.assertEqual(json.loads(client_service.feature_settings("wifi", first_id)), {})
                # feature_ 前缀归一化为裸名。
                self.assertEqual(json.loads(client_service.feature_settings("feature_camera", first_id))["lens_facing"], 1)

                client_service.initialize(files_dir)
                self.assertEqual(json.loads(client_service.feature_settings("camera", first_id))["lens_facing"], 1)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_feature_settings_reject_invalid_input(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                with self.assertRaises(ValueError):
                    client_service.update_feature_settings("camera", [1, 2])
                with self.assertRaises(ValueError):
                    client_service.select_feature("  ")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_parse_json_result_passes_raw_stdout_and_stderr_through(self):
        response = {
            "ok": True,
            "r": json.dumps({"ok": True, "value": 1}),
            "stdout": "printed line\n",
            "stderr": "warning line\n",
        }
        result = client_service.parse_json_result(response)
        self.assertEqual(result["_stdout"], "printed line\n")
        self.assertEqual(result["_stderr"], "warning line\n")

    def test_rpc_log_includes_raw_remote_stdout_section(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12",
                })
                mqtt_module = mock.Mock()
                mqtt_module.rpc.return_value = {
                    "ok": True,
                    "r": json.dumps({"ok": True}),
                    "stdout": "RAW PYTHON PRINT\n",
                    "req_id": "server-req-1",
                }
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    client_service.rpc("print('RAW PYTHON PRINT')")

                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("REMOTE STDOUT", logs)
                self.assertIn("--- begin remote stdout ---", logs)
                self.assertIn("RAW PYTHON PRINT", logs)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_builtin_feature_files_come_from_bootstrap_catalog(self):
        import bootstrap

        names = json.loads(client_service.builtin_feature_files())
        self.assertEqual(names, [f"feature_{name}.py" for name in bootstrap.BUILTIN_FEATURES])

    def test_reload_feature_bridge_returns_descriptor(self):
        import bootstrap

        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.get("feature_reloadable")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feature_reloadable.py"
            path.write_text(
                "FEATURE = {'name': 'reloadable', 'title': 'Reload', 'actions': ['run']}\n"
                "def run():\n    return 'v1'\n",
                encoding="utf-8",
            )
            try:
                bootstrap.init_env(directory)
                self.assertEqual(client_service.call_feature("reloadable"), "v1")
                path.write_text(
                    "FEATURE = {'name': 'reloadable', 'title': 'Reload', 'actions': ['run']}\n"
                    "def run():\n    return 'v2'\n",
                    encoding="utf-8",
                )
                reloaded = json.loads(client_service.reload_feature("reloadable"))
                self.assertTrue(reloaded["ok"])
                self.assertEqual(reloaded["descriptor"]["name"], "reloadable")
                self.assertEqual(client_service.call_feature("reloadable"), "v2")
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                sys.modules.pop("feature_reloadable", None)
                if previous_module is not None:
                    sys.modules["feature_reloadable"] = previous_module

    def test_builtin_feature_installer_writes_scripts_and_logs_progress(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as script_root:
            try:
                import bootstrap

                def install(url, filename, update_dir, progress, **kwargs):
                    Path(update_dir, filename).write_text(
                        f"FEATURE = {{'name': '{filename[8:-3]}'}}\n",
                        encoding="utf-8",
                    )
                    progress(f"{filename}: installed for test")
                    return {"filename": filename, "ok": True}

                with mock.patch.object(bootstrap, "install_feature", side_effect=install) as download:
                    result = json.loads(client_service.install_builtin_features(script_root))
                    self.assertTrue(result["ok"])
                    self.assertEqual(len(result["results"]), 3)
                    self.assertTrue(all(item["ok"] for item in result["results"]))
                    self.assertTrue(any("installed for test" in item for item in result["logs"]))
                    self.assertEqual(download.call_count, 3)

                    second = json.loads(client_service.install_builtin_features(script_root))
                    self.assertTrue(all(item.get("skipped") for item in second["results"]))
                    self.assertEqual(download.call_count, 3)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)


if __name__ == "__main__":
    unittest.main()
