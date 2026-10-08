import ast
import functools
import http.server
import json
import os
import socketserver
import sys
import tempfile
import threading
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

    def test_mqtt_loader_supports_migrated_client_subdirectory(self):
        previous_file = client_service.__file__
        previous_path = list(sys.path)
        previous_modules = {
            name: module for name, module in sys.modules.items()
            if name == "multi_mqtt" or name.startswith("multi_mqtt.") or name == "client_mqtt"
        }
        with tempfile.TemporaryDirectory() as directory:
            module_dir = Path(directory) / "multi_mqtt"
            client_dir = module_dir / "client"
            client_dir.mkdir(parents=True)
            (module_dir / "multi_mqtt.py").write_text(
                "class MultiMQTTManager: pass\n",
                encoding="utf-8",
            )
            (client_dir / "client_mqtt.py").write_text(
                "from multi_mqtt import MultiMQTTManager\n"
                "def rpc(*args, **kwargs): return {'ok': True}\n",
                encoding="utf-8",
            )
            try:
                client_service.__file__ = str(Path(directory) / "client_service.py")
                for name in tuple(sys.modules):
                    if name == "multi_mqtt" or name.startswith("multi_mqtt.") or name == "client_mqtt":
                        sys.modules.pop(name, None)

                module = client_service._mqtt_client_module()

                self.assertEqual(Path(module.__file__), client_dir / "client_mqtt.py")
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

    def test_describe_features_survives_systemexit_in_one_module(self):
        import bootstrap

        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.pop("feature_dying", None)
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                Path(directory, "feature_dying.py").write_text(
                    "raise SystemExit('[FATAL] no config')\n", encoding="utf-8"
                )
                # 一个 feature 导入期 SystemExit 不能炸掉整个目录枚举。
                described = bootstrap.describe_features()
                dying = next(item for item in described if item["name"] == "dying")
                self.assertIn("SystemExit", dying["error"])
                # 其余正常 feature 仍然在列（files 是内置常驻 feature）。
                self.assertIn("files", [item["name"] for item in described])
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                sys.modules.pop("feature_dying", None)
                if previous_module is not None:
                    sys.modules["feature_dying"] = previous_module

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

    def test_rpc_timeout_logs_topic_and_broker_state_without_secrets(self):
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
                self.assertNotIn("elapsed_ms", logs)
                self.assertNotIn("request_id", logs)
                self.assertNotIn("elapsed_ms", result)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def _init_k12(self, files_dir):
        client_service.initialize(files_dir)
        client_service.update_device_settings("sys/device/request", {
            "request_topic": "sys/device/k12",
            "private_key": "233",
        })

    def test_rpc_timeout_forces_immediate_reprobe_instead_of_stale_online(self):
        # 回归：feature 执行超时后在线状态仍停在 online。
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                self._init_k12(files_dir)
                # 先制造一次成功探针，目标处于 online。
                probe_client = mock.Mock()
                probe_client.rpc.return_value = {
                    "r": "('/usr/bin/python3', 'k12', 'x86_64', '5.15')"
                }
                with mock.patch.object(client_service, "_mqtt_client_module",
                                       return_value=probe_client):
                    self.assertTrue(client_service.probe_online()["ok"])
                health = json.loads(client_service.target_health())
                self.assertTrue(health["last_probe_ok"])
                self.assertGreater(health["last_probe_at_ms"], 0)

                # 业务 RPC 完全超时：探针时间戳必须清零，逼 UI 立即复探。
                broker_client = mock.Mock()
                broker_client.is_connected.return_value = False
                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.return_value = b"key"
                mqtt_module.rpc.return_value = None
                mqtt_module._default_client = mock.Mock(
                    mqtt_net=mock.Mock(clients={"broker.example": broker_client})
                )
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    self.assertEqual(client_service.rpc("slow feature")["error"], "RPC timeout")
                health = json.loads(client_service.target_health())
                self.assertFalse(health["last_probe_ok"])
                self.assertEqual(health["last_probe_at_ms"], 0)
                self.assertFalse(health["last_rpc_ok"])
                self.assertIn("RPC timeout", health["last_probe_error"])
                self.assertEqual(health["inflight_count"], 0)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rpc_remote_traceback_keeps_probe_state(self):
        # 目标应答了但代码抛错：在线但执行失败，探针状态不能被改成离线，
        # 也不能被刷成"刚刚 probe 成功"（业务 RPC 与探针彻底解耦）。
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                self._init_k12(files_dir)
                probe_at_before = 1234567890
                with client_service._STATE["lock"]:
                    entry = client_service._health_entry(
                        client_service._device_config("sys/device/k12")
                    )
                    entry["last_probe_at_ms"] = probe_at_before
                    entry["last_probe_ok"] = True

                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.return_value = b"key"
                mqtt_module.rpc.return_value = {"ok": False, "error": "Traceback: boom"}
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    response = client_service.rpc("1/0")
                self.assertFalse(response["ok"])
                health = json.loads(client_service.target_health())
                self.assertFalse(health["last_rpc_ok"])
                self.assertIn("boom", health["last_rpc_error"])
                # 探针字段原封不动。
                self.assertTrue(health["last_probe_ok"])
                self.assertEqual(health["last_probe_at_ms"], probe_at_before)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_online_delegates_to_lightweight_probe(self):
        with mock.patch.object(
            client_service, "probe_online",
            return_value={"ok": True, "topic": "sys/device/k12"},
        ) as probe:
            envelope = json.loads(client_service.online("sys/device/k12"))
        probe.assert_called_once_with("sys/device/k12")
        self.assertTrue(envelope["ok"])
        self.assertEqual(envelope["result"]["topic"], "sys/device/k12")

    def test_probe_online_never_raises_and_clamps_timeout(self):
        probe_client = mock.Mock()
        probe_client.rpc.side_effect = RuntimeError("broker down")
        with mock.patch.object(client_service, "_device_config",
                               return_value={"request_topic": "sys/device/k12"}), \
             mock.patch.object(client_service, "_mqtt_client_module",
                               return_value=probe_client):
            result = client_service.probe_online(timeout=999)
        self.assertFalse(result["ok"])
        self.assertIn("RuntimeError", result["error"])
        self.assertEqual(result["timeout"], client_service._PROBE_TIMEOUT_MAX)
        health = json.loads(client_service.target_health())
        self.assertFalse(health["last_probe_ok"])
        self.assertEqual(health["inflight_count"], 0)

    def _init_k12(self, files_dir):
        client_service.initialize(files_dir)
        client_service.update_device_settings("sys/device/request", {
            "request_topic": "sys/device/k12",
            "response_topic": "sys/device/response",
        })
        # 健康表按设备 uuid id 建档（不是 request_topic）。
        selected = client_service._device_config(None)
        return selected, selected.get("id") or selected["request_topic"]

    def test_stale_inflight_lease_self_heals_to_zero(self):
        # 复现"probe 已超时但 UI 卡 checking"：计数泄漏为 1 且租约已过期，
        # target_health 必须就地把 count/lease 归零。
        with tempfile.TemporaryDirectory() as files_dir:
            selected, key = self._init_k12(files_dir)
            try:
                stored = client_service._health_entry(selected)
                stored["inflight_count"] = 1
                stored["inflight_until_ms"] = int(client_service.time.time() * 1000) - 1
                health = json.loads(client_service.target_health())
                self.assertEqual(health["inflight_count"], 0)
                self.assertEqual(health["inflight_until_ms"], 0)
                self.assertEqual(stored["inflight_count"], 0)
                self.assertEqual(stored["inflight_until_ms"], 0)
            finally:
                client_service._STATE["rpc_health"].pop(key, None)

    def test_fresh_inflight_lease_stays_during_request_and_clears_on_end(self):
        with tempfile.TemporaryDirectory() as files_dir:
            selected, key = self._init_k12(files_dir)
            try:
                client_service._record_request_start(selected, timeout_seconds=2)
                health = json.loads(client_service.target_health())
                self.assertEqual(health["inflight_count"], 1)
                self.assertGreater(health["inflight_until_ms"],
                                   int(client_service.time.time() * 1000))
                client_service._record_request_end(selected, "probe", True)
                health = json.loads(client_service.target_health())
                self.assertEqual(health["inflight_count"], 0)
                self.assertEqual(health["inflight_until_ms"], 0)
            finally:
                client_service._STATE["rpc_health"].pop(key, None)

    # -- 持久化设置：/sdcard 按 topic 分目录，卸载/重装不丢 -------------------

    def _use_durable_root(self, durable_root):
        self._previous_env = os.environ.get("QGB_SETTINGS_ROOT")
        os.environ["QGB_SETTINGS_ROOT"] = durable_root

    def _restore_durable_root(self):
        if self._previous_env is None:
            os.environ.pop("QGB_SETTINGS_ROOT", None)
        else:
            os.environ["QGB_SETTINGS_ROOT"] = self._previous_env

    def test_durable_settings_survive_reinstall_in_per_topic_folders(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as durable_root, \
                tempfile.TemporaryDirectory() as install_a, \
                tempfile.TemporaryDirectory() as install_b:
            self._use_durable_root(durable_root)
            try:
                client_service.initialize(install_a)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/k12", "name": "k12",
                })
                client_service.update_aliyun_settings(
                    {"aliyun": {"token": "sek"}}, "sys/device/k12"
                )
                topic_file = (Path(durable_root) / "settings" / "topics"
                              / "sys_device_k12" / "device.json")
                self.assertTrue(topic_file.is_file())
                doc = json.loads(topic_file.read_text(encoding="utf-8"))
                self.assertEqual(doc["request_topic"], "sys/device/k12")
                self.assertEqual(doc["aliyun"], {"token": "sek"})
                self.assertTrue(
                    (Path(durable_root) / "settings" / "global.json").is_file()
                )

                # 模拟卸载重装：全新 App 私有目录，外置设置必须原样恢复。
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)
                client_service.initialize(install_b)
                self.assertEqual(
                    client_service._STATE["settings_backend"], "durable"
                )
                topics = [d["request_topic"]
                          for d in json.loads(client_service.device_catalog())]
                self.assertIn("sys/device/k12", topics)
                settings = json.loads(
                    client_service.aliyun_settings("sys/device/k12")
                )
                self.assertEqual(settings["aliyun"], {"token": "sek"})
            finally:
                self._restore_durable_root()
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_durable_topic_rename_moves_folder_and_aliyun_is_isolated(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as durable_root, \
                tempfile.TemporaryDirectory() as files_dir:
            self._use_durable_root(durable_root)
            try:
                client_service.initialize(files_dir)
                created = json.loads(client_service.update_device_settings(
                    "sys/device/request", {"request_topic": "sys/device/old"}
                ))
                client_service.update_aliyun_settings(
                    {"aliyun": {"bucket": "old-only"}}, "sys/device/old"
                )
                old_folder = Path(durable_root) / "settings" / "topics" / "sys_device_old"
                new_folder = Path(durable_root) / "settings" / "topics" / "sys_device_new"
                self.assertTrue(old_folder.is_dir())

                client_service.update_device_settings(created["id"], {
                    "request_topic": "sys/device/new",
                })
                self.assertTrue(new_folder.is_dir())
                self.assertFalse(old_folder.is_dir())

                # 第二个 topic 的 aliyun 与第一个互不干扰。
                client_service.update_device_settings("", {
                    "request_topic": "sys/device/two",
                })
                client_service.update_aliyun_settings(
                    {"aliyun": {"bucket": "two-only"}}, "sys/device/two"
                )
                self.assertEqual(
                    json.loads(client_service.aliyun_settings("sys/device/new"))["aliyun"],
                    {"bucket": "old-only"},
                )
                self.assertEqual(
                    json.loads(client_service.aliyun_settings("sys/device/two"))["aliyun"],
                    {"bucket": "two-only"},
                )
            finally:
                self._restore_durable_root()
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_legacy_backend_used_without_external_root(self):
        # 桌面/无外置存储：仍是单文件后端，老路径/老语义不变。
        previous_state = client_service._STATE.copy()
        previous_env = os.environ.get("QGB_SETTINGS_ROOT")
        os.environ.pop("QGB_SETTINGS_ROOT", None)
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                self.assertEqual(client_service._STATE["settings_backend"], "legacy")
                self.assertTrue(Path(files_dir, "client_mqtt.json").is_file())
            finally:
                if previous_env is not None:
                    os.environ["QGB_SETTINGS_ROOT"] = previous_env
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    # -- 下载服务器 feature 列表 -------------------------------------------

    def test_github_listing_urls_derived_from_ghfast_raw_root(self):
        proxy, real = client_service._split_proxied_url(client_service.DEFAULT_FEATURE_URL_ROOT)
        self.assertEqual(proxy, "https://ghfast.top")
        parts = client_service._github_parts(real)
        self.assertEqual(parts, ("cjqbj", "client_mqtt-cjqbj", "main",
                                 "app/src/main/python"))
        urls = client_service._server_listing_urls(client_service.DEFAULT_FEATURE_URL_ROOT)
        # 列表要最新：直连 Contents API 优先，代理兜底，root（文件服务索引）最后。
        self.assertEqual(
            urls[0],
            "https://api.github.com/repos/cjqbj/client_mqtt-cjqbj/contents/app/src/main/python?ref=main",
        )
        self.assertIn(
            "https://ghfast.top/https://api.github.com/repos/cjqbj/client_mqtt-cjqbj/contents/app/src/main/python?ref=main",
            urls,
        )
        self.assertIn(
            "https://ghfast.top/https://github.com/cjqbj/client_mqtt-cjqbj/tree/main/app/src/main/python",
            urls,
        )
        self.assertEqual(urls[-1], client_service.DEFAULT_FEATURE_URL_ROOT)
        # 自定义静态服务器：只 GET root，不乱加 GitHub API。
        self.assertEqual(
            client_service._server_listing_urls("http://10.0.0.1/files/"),
            ["http://10.0.0.1/files/"],
        )

    def test_parse_feature_listing_accepts_github_api_json(self):
        payload = json.dumps([
            {"type": "file", "name": "feature_audio.py", "size": 123,
             "download_url": "https://x/feature_audio.py"},
            {"type": "dir", "name": "feature_nope"},
            {"type": "file", "name": "README.md", "size": 9},
        ])
        files = client_service._parse_feature_listing(payload, "http://x/")
        self.assertEqual([item["name"] for item in files], ["feature_audio.py"])
        self.assertEqual(files[0]["size"], 123)

    def test_download_server_feature_files_lists_latest_http_index(self):
        # 普通 HTTP 文件服务：GET root 解析目录索引；服务器新增文件后再刷即见。
        state = {"extra": False}

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                links = [
                    "<a href=\"feature_audio.py\">feature_audio.py</a>",
                    "<a href=\"feature_dialer.py\">feature_dialer.py</a>",
                    "<a href=\"README.md\">README.md</a>",
                ]
                if state["extra"]:
                    links.append(
                        "<a href=\"feature_newly_uploaded.py\">feature_newly_uploaded.py</a>"
                    )
                body = ("<html><body>%s</body></html>" % "".join(links)).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        previous_state = client_service._STATE.copy()
        try:
            root = "http://127.0.0.1:%d/" % server.server_address[1]
            with tempfile.TemporaryDirectory() as files_dir:
                client_service.initialize(files_dir)
                client_service.update_feature_download_settings(
                    {"feature_url_root": root}
                )
                result = json.loads(
                    client_service.download_server_feature_files(timeout=5)
                )
                self.assertTrue(result["ok"], result)
                self.assertEqual(result["source_url"], root)
                self.assertEqual(
                    [item["name"] for item in result["files"]],
                    ["feature_audio.py", "feature_dialer.py"],
                )

                # 模拟往文件服务上传新 feature：再次刷新必须拿到最新列表。
                state["extra"] = True
                result = json.loads(
                    client_service.download_server_feature_files(timeout=5)
                )
                self.assertTrue(result["ok"], result)
                self.assertEqual(
                    [item["name"] for item in result["files"]],
                    ["feature_audio.py", "feature_dialer.py",
                     "feature_newly_uploaded.py"],
                )
        finally:
            server.shutdown()
            server.server_close()
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
                mqtt_module.utc_ms.return_value = 1790421445000
                mqtt_module.get_req_id.return_value = "client-req-1"
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

                # 只有一套请求 id：发送前日志用的 req_id 必须原样透传给竞速客户端。
                self.assertEqual(
                    mqtt_module.rpc.call_args.kwargs["req_id"], "client-req-1"
                )
                logs = "\n".join(json.loads(client_service.rpc_logs()))
                self.assertIn("req_id=client-req-1", logs)
                self.assertIn("topic=sys/device/k12", logs)
                self.assertIn("phase=response", logs)
                self.assertIn("MQTT RESPONSE ENVELOPE", logs)
                self.assertIn('"r": "{}"', logs)
                self.assertIn('"req_id": "server-req-1"', logs)
                self.assertIn("server_time=1790421445203", logs)
                self.assertIn("server=server-broker", logs)
                self.assertIn("client=client-broker", logs)
                self.assertIn("REQUEST CODE", logs)
                self.assertIn("DO_NOT_LOG_RPC_SOURCE", logs)
                # 不再展示内部 request_id / 耗时（需要时走 adb 动态插桩）。
                self.assertNotIn("request_id", logs)
                self.assertNotIn("elapsed_ms", logs)
                parsed = client_service.parse_json_result(response)
                self.assertEqual(parsed["_rpc"]["req_id"], "server-req-1")
                health = json.loads(client_service.target_health("sys/device/k12"))
                # 业务 RPC 成功只登记 last_rpc_*，绝不再覆写专用探针字段。
                self.assertTrue(health["last_rpc_ok"])
                self.assertGreater(health["last_rpc_at_ms"], 0)
                self.assertIsNone(health["last_probe_ok"])
                self.assertEqual(health["last_probe_at_ms"], 0)
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
        response = {"ok": False, "error": "RPC timeout", "req_id": "req-42"}
        result = client_service.parse_json_result(response)
        self.assertEqual(result["ok"], False)
        self.assertEqual(result["error"], "RPC timeout")
        self.assertEqual(result["_rpc"]["req_id"], "req-42")
        # 内部计时/重复请求 id 不再透传给 feature UI。
        self.assertNotIn("elapsed_ms", result["_rpc"])
        self.assertNotIn("request_id", result["_rpc"])

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

                builtin_count = len(bootstrap.BUILTIN_FEATURES)
                with mock.patch.object(bootstrap, "install_feature", side_effect=install) as download, \
                        mock.patch.object(bootstrap, "_purge_feature_pyc") as purge:
                    result = json.loads(client_service.install_builtin_features(script_root))
                    self.assertTrue(result["ok"])
                    self.assertFalse(result["force"])
                    self.assertEqual(len(result["results"]), builtin_count)
                    self.assertTrue(all(item["ok"] for item in result["results"]))
                    self.assertTrue(any("installed for test" in item for item in result["logs"]))
                    self.assertEqual(download.call_count, builtin_count)
                    # 非 force：已存在文件直接跳过，不重新下载、不清缓存。
                    purge.assert_not_called()

                    second = json.loads(client_service.install_builtin_features(script_root))
                    self.assertTrue(all(item.get("skipped") for item in second["results"]))
                    self.assertEqual(download.call_count, builtin_count)

                    single = json.loads(
                        client_service.install_builtin_feature(script_root, "feature_audio.py")
                    )
                    self.assertTrue(single["ok"])
                    self.assertEqual(single["result"]["filename"], "feature_audio.py")
                    self.assertEqual(download.call_count, builtin_count + 1)

                    invalid = json.loads(
                        client_service.install_builtin_feature(script_root, "feature_unknown.py")
                    )
                    self.assertFalse(invalid["ok"])
                    self.assertIn("unknown built-in feature file", invalid["result"]["error"])
                    self.assertEqual(download.call_count, builtin_count + 1)

                    # force（一键全部重新下载）：不跳过任何文件，全部重下，
                    # 每个成功项都弹出模块缓存并清 pyc，reinstall_ 入口等价于 force=True。
                    forced = json.loads(client_service.reinstall_builtin_features(script_root))
                    self.assertTrue(forced["ok"])
                    self.assertTrue(forced["force"])
                    self.assertEqual(len(forced["results"]), builtin_count)
                    self.assertTrue(all(not item.get("skipped") for item in forced["results"]))
                    self.assertTrue(all(item.get("reloaded") for item in forced["results"]))
                    self.assertEqual(download.call_count, 2 * builtin_count + 1)
                    self.assertEqual(purge.call_count, builtin_count)
                    purged = {call.args[0] for call in purge.call_args_list}
                    self.assertEqual(
                        purged,
                        {f"feature_{name}" for name in bootstrap.BUILTIN_FEATURES},
                    )
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_feature_download_settings_default_persist_normalize_and_reject(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                # 缺省即 ghfast 代理 GitHub main 根目录，结尾必须带 /。
                settings = json.loads(client_service.feature_download_settings())
                self.assertEqual(
                    settings["feature_url_root"],
                    client_service.DEFAULT_FEATURE_URL_ROOT,
                )
                self.assertTrue(settings["feature_url_root"].endswith("/"))

                # 自定义根：自动补尾斜杠并持久化。
                saved = json.loads(
                    client_service.update_feature_download_settings(
                        {"feature_url_root": "https://example.com/repo/python"}
                    )
                )
                self.assertEqual(
                    saved["feature_url_root"], "https://example.com/repo/python/"
                )
                self.assertEqual(
                    json.loads(client_service.feature_download_settings())["feature_url_root"],
                    "https://example.com/repo/python/",
                )
                with self.assertRaises(ValueError):
                    client_service.update_feature_download_settings(
                        {"feature_url_root": "not-a-url"}
                    )
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_feature_source_urls_use_configured_root_and_builtin_fallback(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_feature_download_settings(
                    {"feature_url_root": "https://mirror.test/py/"}
                )
                # 内置文件：配置根首选，追加官方镜像回退，列表无重复。
                urls = client_service._feature_source_urls("feature_wifi.py")
                self.assertEqual(urls[0], "https://mirror.test/py/feature_wifi.py")
                self.assertTrue(any(
                    item.startswith("https://raw.githubusercontent.com/") for item in urls
                ))
                self.assertEqual(len(set(urls)), len(urls))

                # 非内置自定义 feature：只有配置根，不做必然 404 的 GitHub 回退。
                custom = client_service._feature_source_urls(
                    "feature_mytool.py", allow_fallback=False
                )
                self.assertEqual(custom, ["https://mirror.test/py/feature_mytool.py"])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rpc_timeout_override_beats_device_default_and_clamps(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/to",
                    "private_key": "233",
                    "timeout": 5,
                })
                mqtt_module = mock.Mock()
                mqtt_module.get_standard_pem_bytes.return_value = b"-----BEGIN EC PRIVATE KEY-----"
                mqtt_module.rpc.return_value = {"ok": True, "r": "{}"}
                with mock.patch.object(client_service, "_mqtt_client_module", return_value=mqtt_module):
                    client_service.rpc("r={}")
                    self.assertEqual(mqtt_module.rpc.call_args.kwargs["timeout"], 5)
                    # feature 显式传秒数覆盖设备默认值。
                    client_service.rpc("r={}", timeout=45)
                    self.assertEqual(mqtt_module.rpc.call_args.kwargs["timeout"], 45)
                    # 裁剪到 [1, 600]。
                    client_service.rpc("r={}", timeout=0)
                    self.assertEqual(mqtt_module.rpc.call_args.kwargs["timeout"], 1.0)
                    client_service.rpc("r={}", timeout=9999)
                    self.assertEqual(mqtt_module.rpc.call_args.kwargs["timeout"], 600.0)
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_install_named_feature_uses_configured_root_without_github_fallback(self):
        previous_state = client_service._STATE.copy()
        previous_wifi = sys.modules.get("feature_wifi")
        with tempfile.TemporaryDirectory() as files_dir, \
                tempfile.TemporaryDirectory() as script_root:
            try:
                import bootstrap

                client_service.initialize(files_dir)
                client_service.update_feature_download_settings(
                    {"feature_url_root": "https://mirror.test/py/"}
                )
                calls = []

                def install(url, filename, update_dir, progress, **kwargs):
                    calls.append((url, filename, list(kwargs.get("fallback_urls") or [])))
                    Path(update_dir, filename).write_text(
                        "FEATURE = {'name': 'mytool'}\ndef run():\n    return 'ok'\n",
                        encoding="utf-8",
                    )
                    return {"filename": filename, "ok": True}

                with mock.patch.object(bootstrap, "install_feature", side_effect=install) as download, \
                        mock.patch.object(bootstrap, "_purge_feature_pyc") as purge:
                    # 裸名自动补 feature_ 前缀/.py 后缀；非内置只打配置根，无回退。
                    result = json.loads(
                        client_service.install_named_feature(script_root, "mytool")
                    )
                    self.assertTrue(result["ok"])
                    self.assertEqual(download.call_count, 1)
                    self.assertEqual(calls[0][0], "https://mirror.test/py/feature_mytool.py")
                    self.assertEqual(calls[0][2], [])
                    self.assertTrue(result["result"].get("reloaded"))
                    purge.assert_called_with("feature_mytool")
                    self.assertEqual(
                        result["update_dir"], str(Path(script_root, "py_updates"))
                    )

                    # 非法名返回 ok=false，不触发下载。
                    bad = json.loads(
                        client_service.install_named_feature(script_root, "bad-name!")
                    )
                    self.assertFalse(bad["ok"])
                    self.assertEqual(download.call_count, 1)

                    # 内置名允许走官方镜像回退。
                    client_service.install_named_feature(script_root, "wifi")
                    self.assertEqual(calls[-1][0], "https://mirror.test/py/feature_wifi.py")
                    self.assertTrue(calls[-1][2])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)
                sys.modules.pop("feature_mytool", None)
                if previous_wifi is not None:
                    sys.modules["feature_wifi"] = previous_wifi

    def test_enabled_features_default_to_all_and_isolate_per_target(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                first_id = json.loads(client_service.device_settings())["id"]
                second_id = json.loads(client_service.update_device_settings("sys/device/two", {
                    "request_topic": "sys/device/two",
                }))["id"]

                # 缺省：enabled_features 为 None（全选），任何 feature 都生效。
                self.assertIsNone(json.loads(client_service.device_settings())["enabled_features"])
                self.assertIsNone(json.loads(client_service.enabled_features(first_id)))
                self.assertTrue(client_service.is_feature_enabled("camera", first_id))
                self.assertTrue(client_service.is_feature_enabled("feature_files", second_id))

                # 白名单持久化：去 feature_ 前缀、去重、丢弃非法标识。
                saved = json.loads(client_service.update_device_settings(first_id, {
                    "request_topic": "sys/device/request",
                    "enabled_features": ["feature_camera", "camera", "wifi", "bad-name!", ""],
                }))
                self.assertEqual(saved["enabled_features"], ["camera", "wifi"])
                self.assertTrue(client_service.is_feature_enabled("camera", first_id))
                self.assertFalse(client_service.is_feature_enabled("files", first_id))
                self.assertEqual(
                    json.loads(client_service.enabled_features(first_id)), ["camera", "wifi"]
                )
                # 另一目标不受影响，仍是全选。
                self.assertTrue(client_service.is_feature_enabled("files", second_id))
                # 白名单字符串 JSON 也接受。
                saved = json.loads(client_service.update_device_settings(first_id, {
                    "request_topic": "sys/device/request",
                    "enabled_features": json.dumps(["files"]),
                }))
                self.assertEqual(saved["enabled_features"], ["files"])
                # 非列表/非空值拒绝。
                with self.assertRaises(ValueError):
                    client_service.update_device_settings(first_id, {
                        "request_topic": "sys/device/request",
                        "enabled_features": {"camera": True},
                    })
                # JSON null 显式回到全选。
                saved = json.loads(client_service.update_device_settings(first_id, {
                    "request_topic": "sys/device/request",
                    "enabled_features": None,
                }))
                self.assertIsNone(saved["enabled_features"])
                self.assertTrue(client_service.is_feature_enabled("files", first_id))

                # 重启后白名单仍在（重新落一份非空名单再模拟重启）。
                client_service.update_device_settings(first_id, {
                    "request_topic": "sys/device/request",
                    "enabled_features": ["camera"],
                })
                client_service.initialize(files_dir)
                self.assertTrue(client_service.is_feature_enabled("camera", first_id))
                self.assertFalse(client_service.is_feature_enabled("wifi", first_id))
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_make_progress_throttles_and_always_emits_first_and_last(self):
        calls = []
        report = client_service.make_progress(lambda got, total: calls.append((got, total)),
                                              interval=10)
        # 首次必发；随后同时间窗内被节流。
        report(100, 1000)
        report(200, 1000)
        report(300, 1000)
        self.assertEqual(calls, [(100, 1000)])
        # 到总量（完成）时即使在时间窗内也必发一次。
        report(1000, 1000)
        self.assertEqual(calls, [(100, 1000), (1000, 1000)])
        # 未知总大小（流式）每次都发，不被节流吞掉。
        calls.clear()
        report2 = client_service.make_progress(lambda got, total: calls.append((got, total)),
                                               interval=10)
        report2(10, 0)
        report2(20, 0)
        self.assertEqual(calls, [(10, 0), (20, 0)])
        # callback 抛错不外泄。
        def boom(got, total):
            raise RuntimeError("ui dead")
        client_service.make_progress(boom, interval=0)(5, 10)
        self.assertIsNone(client_service.make_progress(None))

    def test_download_progress_capability_detection_keeps_old_aliyun_git_working(self):
        # 其他 topic 上的老版本 aliyun_git：download 没有 progress 形参。
        class OldGit:
            __file__ = "<test-old-aliyun_git>"

            def download(self, url, save_to=None, max_show_bytes_size=0):
                self.kwargs = {"save_to": save_to, "max_show_bytes_size": max_show_bytes_size}
                return b"old"

        class NewGit:
            __file__ = "<test-new-aliyun_git>"

            def download(self, url, save_to=None, max_show_bytes_size=0, progress=None):
                self.progress_seen = progress
                if progress is not None:
                    progress(1, 1)
                return b"new"

        client_service._DOWNLOAD_PROGRESS_CAPABLE.discard("<test-old-aliyun_git>")
        client_service._DOWNLOAD_PROGRESS_CAPABLE.discard("<test-new-aliyun_git>")
        old = OldGit()
        self.assertFalse(client_service._download_supports_progress(old))
        # 老模块：不传 progress，下载照常完成（无速度显示而已）。
        self.assertEqual(
            client_service._aliyun_download(old, "u", lambda g, t: None, save_to=None),
            b"old",
        )
        self.assertNotIn("progress", old.kwargs)

        new = NewGit()
        self.assertTrue(client_service._download_supports_progress(new))
        ticks = []
        self.assertEqual(
            client_service._aliyun_download(new, "u", lambda g, t: ticks.append((g, t)),
                                            save_to=None),
            b"new",
        )
        self.assertEqual(ticks, [(1, 1)])

    def test_feature_back_handler_registry_consumes_and_isolates_errors(self):
        client_service.set_feature_back_handler("files", lambda: True)
        self.assertTrue(client_service.handle_feature_back("files"))
        client_service.set_feature_back_handler("files", lambda: False)
        self.assertFalse(client_service.handle_feature_back("files"))

        def boom():
            raise RuntimeError("view gone")

        client_service.set_feature_back_handler("files", boom)
        # 回调异常不能把返回键流程带崩：按未消费处理。
        self.assertFalse(client_service.handle_feature_back("files"))
        # 未注册的 feature 直接放行给宿主双击退出。
        self.assertFalse(client_service.handle_feature_back("camera"))
        client_service.set_feature_back_handler("files", None)
        self.assertFalse(client_service.handle_feature_back("files"))

    def test_new_features_adopt_into_custom_whitelists_without_reviving_disabled(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                config = client_service.load_config()
                # 模拟老用户：两个目标，一个白名单、一个全选。
                config["devices"][0]["enabled_features"] = ["files", "camera"]
                config["devices"][0]["request_topic"] = "sys/device/a"
                config["devices"][0]["id"] = "dev-a"
                config["devices"].append(dict(client_service._DEFAULT_DEVICE))
                config["devices"][1].update({
                    "id": "dev-b", "request_topic": "sys/device/b",
                    "enabled_features": None,
                })
                # 首次登记：只记录当前已知集合，不改动白名单。
                config["known_features"] = ["files", "camera"]
                client_service.save_config(config)

                # 新出现 dialer（APK 升级带来的新内置）+ 手动 push 的脚本。
                with mock.patch("bootstrap.list_features",
                                return_value=["files", "camera", "dialer"]):
                    client_service._sync_known_features()

                saved = client_service.load_config()
                self.assertEqual(
                    saved["devices"][0]["enabled_features"],
                    ["files", "camera", "dialer"],
                )
                # 全选目标保持 null，不会被落成列表。
                self.assertIsNone(saved["devices"][1]["enabled_features"])
                self.assertEqual(saved["known_features"], ["files", "camera", "dialer"])

                # 用户随后把 camera 勾掉；已知集合不变，再同步不会复活 camera。
                saved["devices"][0]["enabled_features"] = ["files", "dialer"]
                client_service.save_config(saved)
                with mock.patch("bootstrap.list_features",
                                return_value=["files", "camera", "dialer"]):
                    client_service._sync_known_features()
                again = client_service.load_config()
                self.assertEqual(again["devices"][0]["enabled_features"], ["files", "dialer"])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_delete_runtime_feature_removes_override_and_rejects_escape(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                with tempfile.TemporaryDirectory() as script_root:
                    update_dir = Path(script_root) / "py_updates"
                    update_dir.mkdir()
                    # 一个纯运行时 feature（无内置同名），一个内置同名覆盖。
                    (update_dir / "feature_extra.py").write_text("FEATURE={}\n", encoding="utf-8")
                    (update_dir / "feature_files.py").write_text(
                        "FEATURE={'name': 'files'}\n", encoding="utf-8"
                    )
                    removed = json.loads(
                        client_service.delete_runtime_feature(script_root, "extra")
                    )
                    self.assertTrue(removed["ok"])
                    self.assertTrue(removed["removed_file"])
                    self.assertFalse(removed["falls_back_to_builtin"])
                    self.assertFalse((update_dir / "feature_extra.py").exists())

                    fell_back = json.loads(
                        client_service.delete_runtime_feature(script_root, "feature_files.py")
                    )
                    self.assertTrue(fell_back["ok"])
                    self.assertTrue(fell_back["falls_back_to_builtin"])
                    self.assertFalse((update_dir / "feature_files.py").exists())

                    # 路径穿越/非法名拒绝。
                    bad = json.loads(
                        client_service.delete_runtime_feature(script_root, "../evil")
                    )
                    # ../evil 规范化后 isidentifier 不合法 -> invalid name
                    self.assertFalse(bad["ok"])
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_unnamed_target_stops_displaying_legacy_default_name(self):
        previous_state = client_service._STATE.copy()
        with tempfile.TemporaryDirectory() as files_dir:
            try:
                client_service.initialize(files_dir)
                # 老版本持久化里用户没起过名，记录沿用内置默认 "Target"。
                config_path = client_service._STATE["config_path"]
                with open(config_path, "r", encoding="utf-8") as config_file:
                    config = json.load(config_file)
                config["devices"][0]["name"] = "Target"
                with open(config_path, "w", encoding="utf-8") as config_file:
                    json.dump(config, config_file)

                # 任意一次设置保存（用户没传 name）都应回退成 topic 末段。
                saved = json.loads(client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/request",
                }))
                self.assertEqual(saved["name"], "request")
                # 显式自定义名保留。
                saved = json.loads(client_service.update_device_settings("sys/device/request", {
                    "request_topic": "sys/device/request",
                    "name": "My Phone",
                }))
                self.assertEqual(saved["name"], "My Phone")
            finally:
                client_service._STATE.clear()
                client_service._STATE.update(previous_state)

    def test_rescan_features_discovers_new_file_and_drops_deleted_runtime(self):
        import bootstrap

        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.get("feature_newscan")
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                feature_file = Path(directory, "feature_newscan.py")
                feature_file.write_text(
                    "FEATURE = {'name': 'newscan', 'title': 'New', 'actions': ['run']}\n"
                    "def run():\n    return 'scan-ok'\n",
                    encoding="utf-8",
                )
                catalog = json.loads(client_service.rescan_features())
                self.assertIn("newscan", {item["name"] for item in catalog})
                # 新文件按需导入立即可用。
                self.assertEqual(client_service.call_feature("newscan"), "scan-ok")
                self.assertIn("feature_newscan", sys.modules)

                # 删除运行时文件后重扫：该模块清出缓存；内置 feature 始终保留。
                feature_file.unlink()
                catalog_after = json.loads(client_service.rescan_features())
                names_after = {item["name"] for item in catalog_after}
                self.assertNotIn("newscan", names_after)
                self.assertNotIn("feature_newscan", sys.modules)
                self.assertIn("wifi", names_after)
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                sys.modules.pop("feature_newscan", None)
                if previous_module is not None:
                    sys.modules["feature_newscan"] = previous_module


if __name__ == "__main__":
    unittest.main()
