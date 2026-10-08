import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import bootstrap
import feature_repl


class ReplEvalTests(unittest.TestCase):
    def test_empty_code_is_rejected_without_rpc(self):
        with mock.patch.object(feature_repl.client_service, "rpc") as rpc:
            result = json.loads(feature_repl.eval("   "))
        self.assertFalse(result["ok"])
        self.assertIn("empty", result["error"])
        rpc.assert_not_called()

    def test_eval_passes_code_and_clamped_timeout(self):
        envelope = {
            "ok": True,
            "topic": "sys/device/k12",
            "elapsed_ms": 42.0,
            "r": "[1, 2, 3]",
            "stdout": "hello\n",
            "stderr": "",
            "error": "",
        }
        with mock.patch.object(feature_repl.client_service, "rpc", return_value=envelope) as rpc:
            result = json.loads(feature_repl.eval("print('hello')\n[1,2,3]", "3"))
        self.assertTrue(result["ok"])
        self.assertEqual(result["r"], "[1, 2, 3]")
        self.assertEqual(result["stdout"], "hello\n")
        self.assertEqual(result["timeout"], 3.0)
        rpc.assert_called_once_with("print('hello')\n[1,2,3]", timeout=3.0)

    def test_timeout_is_clamped_and_bad_value_falls_back_to_default(self):
        with mock.patch.object(feature_repl.client_service, "rpc",
                               return_value={"ok": True, "r": "0"}) as rpc:
            feature_repl.eval("1", "999")
            self.assertEqual(rpc.call_args.kwargs["timeout"], feature_repl._TIMEOUT_MAX)
        with mock.patch.object(feature_repl.client_service, "rpc",
                               return_value={"ok": True, "r": "0"}) as rpc:
            feature_repl.eval("1", "0")
            self.assertEqual(rpc.call_args.kwargs["timeout"], feature_repl._TIMEOUT_MIN)
        with mock.patch.object(feature_repl.client_service, "rpc",
                               return_value={"ok": True, "r": "0"}) as rpc:
            feature_repl.eval("1", "not-a-number")
            self.assertEqual(rpc.call_args.kwargs["timeout"], feature_repl.DEFAULT_TIMEOUT)

    def test_timeout_envelope_fields_pass_through(self):
        envelope = {"ok": False, "error": "RPC timeout", "elapsed_ms": 10000.0,
                    "topic": "sys/device/k12"}
        with mock.patch.object(feature_repl.client_service, "rpc", return_value=envelope):
            result = json.loads(feature_repl.eval("while True: pass", "10"))
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "RPC timeout")
        self.assertIsNone(result["r"])

    def test_format_includes_sections(self):
        text = feature_repl._format({
            "ok": True, "elapsed_ms": 12.5, "timeout": 10, "topic": "q",
            "stdout": "out", "stderr": "warn", "error": "", "r": "42",
        })
        self.assertIn("ok=True", text)
        self.assertIn("--- stdout ---\nout", text)
        self.assertIn("--- stderr ---\nwarn", text)
        self.assertIn("r = 42", text)

    def test_manifest_uses_filename_naming_and_python_ui(self):
        self.assertEqual(feature_repl.FEATURE["ui"], "python")
        self.assertEqual(feature_repl.FEATURE["actions"], ["eval"])
        # 新命名约定：manifest 不再带 name/title，一律以文件名 repl 为准。
        self.assertNotIn("name", feature_repl.FEATURE)
        self.assertNotIn("title", feature_repl.FEATURE)

    def test_repl_is_discovered_as_builtin_and_loads(self):
        names = bootstrap.BUILTIN_FEATURES
        self.assertIn("repl", names)
        module = bootstrap.load_feature("repl")
        self.assertEqual(module.__name__, "feature_repl")
        self.assertTrue(callable(module.eval))


if __name__ == "__main__":
    unittest.main()
