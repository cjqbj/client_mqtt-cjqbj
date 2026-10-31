import asyncio
import importlib.abc
import json
import sys
import textwrap
import types
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import feature_audio

TOPIC = "sys/device/14c"
BOOT_OK = {
    "ok": True,
    "tts_ready": True,
    "edge_tts": "7.3.1",
    "save_dir": "/sdcard/apm",
    "save_dir_ok": True,
}


def compile_remote(code):
    """目标代码含顶层 await 时，按 rpc_executor 的语义包一层 async 再编译。"""
    if "await" in code:
        compile(
            "async def __probe_async__():\n" + textwrap.indent(code, "    "),
            "<remote_code>",
            "exec",
        )
    else:
        compile(code, "<remote_code>", "exec")


class AudioCodeTests(unittest.TestCase):
    def test_play_code_uses_configured_audio_path_and_returns_playback_result(self):
        code = feature_audio.build_play_code("/sdcard/apm/custom.mp3")

        self.assertIn('AUDIO_PATH = "/sdcard/apm/custom.mp3"', code)
        self.assertIn('r["playing"] = bool(mp.isPlaying())', code)
        self.assertIn('r["errors"].append("file not found")', code)
        # 供 status/stop 共用：播放器与当前文件挂在持久 holder 上。
        self.assertIn('sys.modules["_mp_holder"].path = AUDIO_PATH', code)
        compile_remote(code)

    def test_bootstrap_code_defines_tts_and_reports_edge_tts_health(self):
        code = feature_audio.bootstrap_code()

        self.assertIn("async def tts(", code)
        self.assertIn("_qgb_tts_ready = True", code)
        self.assertNotIn("@SAVE_DIR@", code)  # 占位符已全部替换
        self.assertIn('_qgb_save_dir = "/sdcard/apm"', code)
        self.assertIn("_qgb_rpip.install(_qgb_pkg)", code)
        self.assertIn("def _qgb_missing_module(", code)
        compile_remote(code)

        # 注入假 edge_tts：本机没有该包时兜底会真的联网安装，测试里绝不能触发。
        fake = types.SimpleNamespace(__version__="7.4.0")
        with mock.patch.dict(sys.modules, {"edge_tts": fake}):
            namespace = {}
            exec(code, namespace)
            health = json.loads(namespace["r"])
        self.assertTrue(health["tts_ready"])
        self.assertEqual(health["save_dir"], "/sdcard/apm")
        self.assertTrue(namespace["_qgb_tts_ready"])
        # 清理桌面执行残留，避免影响其它用例（本机可能没有 /sdcard/apm）。
        self.assertIn("edge_tts", health)
        self.assertEqual(health["edge_tts"], "7.4.0")
        self.assertFalse(health["edge_tts_install"]["attempted"])

    def test_bootstrap_installs_edge_tts_when_missing(self):
        """edge_tts 不在（无缺失模块名）：补装 edge_tts 后能 import。"""
        code = feature_audio.bootstrap_code()
        install_calls = []

        def fake_install(name, *args, **kwargs):
            install_calls.append(name)
            sys.modules.pop("edge_tts", None)  # 依赖补齐后重新 import 成功
            sys.modules["edge_tts"] = types.SimpleNamespace(__version__="7.4.0")
            return {"ok": True, "method": "pypi", "version": "7.4.0",
                    "error": None, "log": "Successfully installed edge-tts"}

        fake_pip = types.ModuleType("runtime_pip")
        fake_pip.install = fake_install
        # sys.modules[name] = None 会让 `import name` 直接抛 ImportError（无 name）。
        with mock.patch.dict(
            sys.modules, {"edge_tts": None, "runtime_pip": fake_pip}
        ):
            namespace = {}
            exec(code, namespace)
            health = json.loads(namespace["r"])

        self.assertEqual(install_calls, ["edge_tts"])
        self.assertTrue(health["ok"])
        self.assertTrue(health["edge_tts_install"]["attempted"])
        self.assertTrue(health["edge_tts_install"]["ok"])
        self.assertEqual(health["edge_tts"], "7.4.0")
        self.assertEqual(health["edge_tts_error"], "")

    def test_bootstrap_installs_module_named_in_import_error(self):
        """k12 现场：错误点名 typing_extensions（只有 dist-info、缺 .py）→ 只补装它。"""
        code = feature_audio.bootstrap_code()
        install_calls = []

        class BoomFinder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "edge_tts":
                    raise ModuleNotFoundError(
                        "No module named 'typing_extensions'",
                        name="typing_extensions",
                    )
                return None

        def fake_install(name, *args, **kwargs):
            install_calls.append(name)
            sys.modules["edge_tts"] = types.SimpleNamespace(__version__="7.4.0")
            return {"ok": True, "method": "pip", "version": "4.16.0",
                    "error": None, "log": "Successfully installed typing_extensions"}

        fake_pip = types.ModuleType("runtime_pip")
        fake_pip.install = fake_install
        saved = sys.modules.pop("edge_tts", None)
        try:
            with mock.patch.object(
                sys, "meta_path", [BoomFinder()] + sys.meta_path
            ), mock.patch.dict(sys.modules, {"runtime_pip": fake_pip}):
                namespace = {}
                exec(code, namespace)
                health = json.loads(namespace["r"])
        finally:
            sys.modules.pop("edge_tts", None)
            if saved is not None:
                sys.modules["edge_tts"] = saved

        # 关键：不要去重装 edge_tts（metadata 已“满足”，装了也不补缺文件）
        self.assertEqual(install_calls, ["typing_extensions"])
        self.assertTrue(health["ok"])
        self.assertEqual(health["edge_tts"], "7.4.0")
        self.assertEqual(
            health["edge_tts_install"]["steps"][0]["pkg"], "typing_extensions"
        )
        self.assertTrue(health["edge_tts_install"]["ok"])

    def test_bootstrap_install_failure_degrades_with_diagnostics(self):
        code = feature_audio.bootstrap_code()
        fake_pip = types.ModuleType("runtime_pip")
        fake_pip.install = lambda *a, **k: {
            "ok": False, "method": None, "version": None,
            "error": "network unreachable", "log": "pip download failed",
        }
        with mock.patch.dict(
            sys.modules, {"edge_tts": None, "runtime_pip": fake_pip}
        ):
            namespace = {}
            exec(code, namespace)
            health = json.loads(namespace["r"])

        self.assertFalse(health["ok"])
        self.assertTrue(health["tts_ready"])  # 定义仍在，只是合成不可用
        self.assertTrue(health["edge_tts_install"]["attempted"])
        self.assertFalse(health["edge_tts_install"]["ok"])
        self.assertIn("network unreachable", health["edge_tts_install"]["error"])
        self.assertIn("edge_tts", health["edge_tts_error"])
        # 装不上就放弃，不无限重试（禁用合成即可，不入死循环）
        self.assertEqual(len(health["edge_tts_install"]["steps"]), 1)

    def test_speak_code_awaits_tts_and_starts_playback(self):
        code = feature_audio.build_speak_code("你好，世界", save_dir="/sdcard/apm")

        self.assertIn('TEXT = "你好，世界"', code)
        self.assertIn("await tts(TEXT", code)
        self.assertIn('SAVE_DIR = "/sdcard/apm"', code)
        self.assertIn("PROXY = None", code)  # 没配代理就直连（proxy=None）
        self.assertIn('r["playing"] = bool(mp.isPlaying())', code)
        self.assertIn("holder.path = path", code)
        self.assertIn('r["rebootstrap"] = True', code)
        for token in ("@TEXT@", "@VOICE@", "@RATE@", "@PITCH@", "@SAVE_DIR@", "@STREAM@", "@PROXY@"):
            self.assertNotIn(token, code)
        compile_remote(code)

    def test_speak_code_carries_proxy_when_configured(self):
        code = feature_audio.build_speak_code(
            "你好", save_dir="/sdcard/apm", proxy="http://192.168.1.2:7890"
        )
        self.assertIn('PROXY = "http://192.168.1.2:7890"', code)
        self.assertIn("proxy=(PROXY or None)", code)
        compile_remote(code)

    def test_status_and_stop_code_read_shared_player_holder(self):
        status_code = feature_audio.build_status_code(3)
        stop_code = feature_audio.build_stop_code(3)

        for code in (status_code, stop_code):
            self.assertIn('sys.modules.get("_mp_holder")', code)
            compile_remote(code)
        self.assertIn('getattr(holder, "path"', status_code)
        self.assertIn('r["stopped"] = True', stop_code)

    def test_volume_code_compiles_for_get_and_set(self):
        for action, level in (("get", 0), ("set", 15)):
            code = feature_audio.build_volume_code(action, level, 3)
            compile_remote(code)

    def test_tts_timeout_setting_is_clamped(self):
        with mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value=5
        ):
            self.assertEqual(feature_audio.tts_timeout(), 5.0)
        with mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value="junk"
        ):
            self.assertEqual(
                feature_audio.tts_timeout(), float(feature_audio.TTS_TIMEOUT)
            )
        with mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value=9999
        ):
            self.assertEqual(feature_audio.tts_timeout(), 600.0)

    def test_tts_proxy_setting_is_validated(self):
        """脏值一律按直连（边缘功能只降级），不要因为一个乱写的代理打断 TTS。"""
        cases = [
            ("http://192.168.1.2:7890", "http://192.168.1.2:7890"),
            ("  https://proxy.local:8443  ", "https://proxy.local:8443"),
            ("socks5://127.0.0.1:1080", ""),  # edge_tts/aiohttp 不支持 socks
            ("192.168.1.2:7890", ""),  # 没写协议
            ("", ""),
            (None, ""),
            (123, ""),
        ]
        for raw, expected in cases:
            with mock.patch.object(
                feature_audio.client_service, "feature_setting", return_value=raw
            ):
                self.assertEqual(feature_audio.tts_proxy(), expected, repr(raw))


class TTSRetryTests(unittest.TestCase):
    """k12 现场：微软 TTS 节点间歇性 reset 首次握手，重试即可成功。"""

    def _run_tts(self, fail_times, retries=None, connector_factory=None):
        state = {"constructed": 0, "streams": 0, "options": []}

        class FakeCommunicate:
            def __init__(self, text, voice, **options):
                state["constructed"] += 1
                self._fail = state["constructed"] <= fail_times
                self.options = options
                state["options"].append(options)

            def stream(self):
                return self._gen()

            async def _gen(self):
                state["streams"] += 1
                if self._fail:
                    raise ConnectionResetError("Connection reset by peer")
                yield {"type": "audio", "data": b"ID3"}
                yield {"type": "audio", "data": b"rest"}
                yield {"type": "WordBoundary", "text": "x", "offset": 1,
                       "duration": 1}

        fake = types.SimpleNamespace(__version__="7.3.1", Communicate=FakeCommunicate)
        code = feature_audio.bootstrap_code()
        with mock.patch.dict(sys.modules, {"edge_tts": fake}):
            namespace = {}
            exec(code, namespace)
            if retries is not None:
                namespace["TTS_RETRIES"] = retries
            if connector_factory is not None:
                namespace["_ipv4_connector"] = connector_factory
            result = asyncio.run(
                namespace["tts"]("\u4f60\u597d", "zh-CN-YunxiaNeural", save_dir="")
            )
        return result, state

    def test_tts_retries_after_connection_reset(self):
        result, state = self._run_tts(fail_times=1)

        self.assertEqual(result, b"ID3rest")
        self.assertEqual(state["constructed"], 2)
        self.assertEqual(state["streams"], 2)

    def test_tts_gives_up_after_configured_attempts(self):
        result, state = self._run_tts(fail_times=99, retries=2)

        self.assertIsInstance(result, str)
        self.assertIn("comm.stream", result)
        self.assertIn("attempts=2", result)
        self.assertEqual(state["constructed"], 2)

    def test_tts_forces_ipv4_connector_and_closes_it(self):
        """k12 现场：IPv6 优先会间歇 reset，合成时强制 IPv4 并在结束后关闭。"""
        created, closed = [], []

        class FakeConnector:
            def __init__(self):
                created.append(self)

            async def close(self):
                closed.append(self)

        result, state = self._run_tts(fail_times=0, connector_factory=FakeConnector)

        self.assertEqual(result, b"ID3rest")
        self.assertEqual(len(created), 1)
        self.assertIs(state["options"][0]["connector"], created[0])
        self.assertEqual(closed, created)

    def test_tts_falls_back_when_ipv4_connector_unavailable(self):
        result, state = self._run_tts(fail_times=0, connector_factory=lambda: None)

        self.assertEqual(result, b"ID3rest")
        self.assertNotIn("connector", state["options"][0])

    def test_tts_retry_never_retries_streaming_response(self):
        """HTTP 直出已写给客户端，重试会产生重复音频，所以只试一次。"""

        class FakeResponse:
            def __init__(self):
                self.chunks = []
                self.headers = {}

            def set_header(self, name, value):
                self.headers[name] = value

            def write(self, data):
                self.chunks.append(data)

        state = {"constructed": 0}

        class FakeCommunicate:
            def __init__(self, text, voice, **options):
                state["constructed"] += 1

            def stream(self):
                return self._gen()

            async def _gen(self):
                raise ConnectionResetError("Connection reset by peer")
                yield  # pragma: no cover

        fake = types.SimpleNamespace(__version__="7.3.1", Communicate=FakeCommunicate)
        code = feature_audio.bootstrap_code()
        with mock.patch.dict(sys.modules, {"edge_tts": fake}):
            namespace = {}
            exec(code, namespace)
            response = FakeResponse()
            result = asyncio.run(
                namespace["tts"]("\u4f60\u597d", "zh-CN-YunxiaNeural", response=response,
                                 save_dir="")
            )
        self.assertIsInstance(result, str)
        self.assertIn("attempts=1", result)
        self.assertEqual(state["constructed"], 1)


class TTSBootstrapTests(unittest.TestCase):
    def setUp(self):
        feature_audio._TTS_READY.clear()

    def tearDown(self):
        feature_audio._TTS_READY.clear()

    def _target(self):
        return mock.patch.object(
            feature_audio.client_service,
            "_device_config",
            return_value={"request_topic": TOPIC},
        )

    def test_definition_sent_once_then_reused(self):
        envelope = {"ok": True, "r": json.dumps(BOOT_OK)}
        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", return_value=envelope
        ) as rpc:
            first = feature_audio._ensure_tts()
            second = feature_audio._ensure_tts()

        self.assertTrue(first["ok"])
        self.assertEqual(rpc.call_count, 1)
        self.assertEqual(rpc.call_args.args[0], feature_audio.bootstrap_code())
        self.assertTrue(second["cached"])
        self.assertEqual(second["key"], TOPIC)
        self.assertTrue(feature_audio._TTS_READY[TOPIC])

    def test_bootstrap_failure_degrades_without_raising(self):
        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", side_effect=TimeoutError("mqtt timeout")
        ):
            result = feature_audio._ensure_tts()

        self.assertFalse(result["ok"])
        self.assertIn("TimeoutError", result["error"])
        self.assertNotIn(TOPIC, feature_audio._TTS_READY)

    def test_bootstrap_timeout_ignores_tts_setting(self):
        """bootstrap 可能要现装 edge_tts，超时不能被设置页调小的 tts_timeout 夹掉。"""
        envelope = {"ok": True, "r": json.dumps(BOOT_OK)}
        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", return_value=envelope
        ) as rpc, mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value=30
        ):
            feature_audio._ensure_tts()

        self.assertEqual(
            rpc.call_args.kwargs["timeout"], float(feature_audio.BOOTSTRAP_TIMEOUT)
        )

    def test_speak_bootstrap_failure_reports_install_diagnostics(self):
        boot = {
            "ok": False,
            "key": TOPIC,
            "edge_tts_error": "ImportError: No module named 'typing_extensions'",
            "edge_tts_install": {"attempted": True, "ok": False,
                                  "error": "network unreachable"},
            "save_dir": "/sdcard/apm",
            "save_dir_error": "",
        }
        with self._target(), mock.patch.object(
            feature_audio.client_service,
            "rpc",
            return_value={"ok": True, "r": json.dumps(boot)},
        ):
            data = json.loads(feature_audio.speak("你好"))

        self.assertFalse(data["ok"])
        self.assertEqual(data["stage"], "bootstrap")
        self.assertIn("install edge_tts failed", data["error"])
        self.assertTrue(data["edge_tts_install"]["attempted"])
        self.assertIn("typing_extensions", data["edge_tts_error"])

    def test_speak_empty_text_never_touches_target(self):
        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc"
        ) as rpc:
            data = json.loads(feature_audio.speak("   "))

        self.assertFalse(data["ok"])
        self.assertEqual(data["stage"], "input")
        rpc.assert_not_called()

    def test_speak_bootstraps_then_sends_short_code(self):
        boot_envelope = {"ok": True, "r": json.dumps(BOOT_OK)}
        spoken = {
            "ok": True,
            "stage": "playing",
            "playing": True,
            "path": "/sdcard/apm/你好.mp3",
            "duration_ms": 5000,
            "vol_before": 8,
            "vol_after": 15,
            "vol_max": 15,
        }

        def fake_rpc(code, timeout=None):
            if code == feature_audio.bootstrap_code():
                return boot_envelope
            return {"ok": True, "r": json.dumps(spoken)}

        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", side_effect=fake_rpc
        ) as rpc, mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value=None
        ):
            data = json.loads(feature_audio.speak("你好", save_dir="/sdcard/apm"))

        self.assertTrue(data["ok"])
        self.assertTrue(data["playing"])
        self.assertEqual(rpc.call_count, 2)  # bootstrap 定义 + 短 speak 代码
        self.assertEqual(rpc.call_args_list[0].args[0], feature_audio.bootstrap_code())
        self.assertIn("await tts(TEXT", rpc.call_args_list[1].args[0])
        self.assertEqual(rpc.call_args_list[1].kwargs["timeout"], 90.0)

    def test_speak_passes_configured_proxy_to_target_code(self):
        boot_envelope = {"ok": True, "r": json.dumps(BOOT_OK)}
        spoken = {"ok": True, "stage": "playing", "playing": True,
                  "path": "/sdcard/apm/x.mp3"}
        sent = []

        def fake_rpc(code, timeout=None):
            sent.append(code)
            if code == feature_audio.bootstrap_code():
                return boot_envelope
            return {"ok": True, "r": json.dumps(spoken)}

        def fake_setting(feature, key, default=None):
            if key == "tts_proxy":
                return "http://192.168.1.2:7890"
            return default

        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", side_effect=fake_rpc
        ), mock.patch.object(
            feature_audio.client_service, "feature_setting", side_effect=fake_setting
        ):
            data = json.loads(feature_audio.speak("你好"))

        self.assertTrue(data["ok"])
        self.assertEqual(data["proxy"], "http://192.168.1.2:7890")
        self.assertIn('PROXY = "http://192.168.1.2:7890"', sent[1])

    def test_speak_rebootstraps_once_when_namespace_lost(self):
        boot_envelope = {"ok": True, "r": json.dumps(BOOT_OK)}
        lost = {
            "ok": False,
            "stage": "tts",
            "rebootstrap": True,
            "errors": ["tts call: name 'tts' is not defined"],
        }
        spoken = {
            "ok": True,
            "stage": "playing",
            "playing": True,
            "path": "/sdcard/apm/x.mp3",
        }
        responses = [
            boot_envelope,
            {"ok": True, "r": json.dumps(lost)},
            boot_envelope,
            {"ok": True, "r": json.dumps(spoken)},
        ]
        calls = []

        def fake_rpc(code, timeout=None):
            calls.append(code)
            return responses.pop(0)

        with self._target(), mock.patch.object(
            feature_audio.client_service, "rpc", side_effect=fake_rpc
        ), mock.patch.object(
            feature_audio.client_service, "feature_setting", return_value=None
        ):
            data = json.loads(feature_audio.speak("你好"))

        self.assertTrue(data["ok"])
        self.assertEqual(len(calls), 4)  # bootstrap → speak → 补发定义 → 重试 speak
        self.assertEqual(calls[2], feature_audio.bootstrap_code())


class AudioActionWiringTests(unittest.TestCase):
    def test_play_calls_rpc_with_generated_code_and_parses_result(self):
        rpc_result = {"ok": True, "r": '{"ok": true, "playing": true}'}
        with mock.patch.object(
            feature_audio.client_service, "rpc", return_value=rpc_result
        ) as rpc, mock.patch.object(
            feature_audio.client_service,
            "parse_json_result",
            return_value={"ok": True, "playing": True},
        ) as parse_result:
            result = json.loads(feature_audio.play())

        self.assertTrue(result["ok"])
        self.assertTrue(result["playing"])
        self.assertEqual(rpc.call_args.args[0], feature_audio.build_play_code())
        parse_result.assert_called_once_with(rpc_result)

    def test_status_and_stop_wrap_rpc_results_as_json(self):
        rpc_result = {"ok": True, "r": '{"ok": true}'}
        for action in (feature_audio.status, feature_audio.stop):
            with mock.patch.object(
                feature_audio.client_service, "rpc", return_value=rpc_result
            ) as rpc, mock.patch.object(
                feature_audio.client_service,
                "parse_json_result",
                return_value={"ok": True},
            ):
                result = json.loads(action(3))
            self.assertTrue(result["ok"])
            self.assertIn("_mp_holder", rpc.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
