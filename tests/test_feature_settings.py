"""每 feature 参数机制（FEATURE["settings"]）单元测试。

覆盖链路：feature 脚本声明 -> 规范化 -> 设置页数据源 -> 按 scope 落盘 ->
运行时读取（feature_setting / feature_timeout）。
"""

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import bootstrap
import client_service


DEMO_SPECS = [
    {"key": "page_size", "label": "单次文件个数", "type": "int", "scope": "topic",
     "default": 100, "min": 1, "max": 10000, "help": "每次扫描读多少条"},
    {"key": "timeout", "label": "RPC 超时（秒）", "type": "int", "scope": "global",
     "default": None, "min": 1, "max": 600, "help": "留空 = 用动作内置默认"},
    {"key": "enabled", "label": "启用", "type": "bool", "scope": "global",
     "default": True, "min": None, "max": None, "help": ""},
]


class ConfigHarness(unittest.TestCase):
    """用内存 config 替换 load_config/save_config，避免触碰真实 settings 目录。"""

    def setUp(self):
        self.config = {
            "devices": [
                {"id": "dev-main", "request_topic": "sys/main/request"},
                {"id": "dev-alt", "request_topic": "sys/alt/request"},
            ],
        }
        self.saves = []
        self._patchers = [
            mock.patch.object(client_service, "load_config", side_effect=self._load_config),
            mock.patch.object(client_service, "save_config", side_effect=self._save_config),
            mock.patch.dict(client_service._STATE, {
                "selected_device_id": "dev-main",
                "selected_topic": "sys/main/request",
            }),
        ]
        for patcher in self._patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def _load_config(self):
        return json.loads(json.dumps(self.config))

    def _save_config(self, value):
        self.saves.append(json.loads(json.dumps(value)))
        self.config = json.loads(json.dumps(value))
        return {"ok": True}


class CoerceTests(unittest.TestCase):
    def test_int_clamps_to_range_and_rounds(self):
        spec = {"type": "int", "min": 1, "max": 10000}
        self.assertEqual(client_service._coerce_feature_setting_value(spec, "0"), 1)
        self.assertEqual(client_service._coerce_feature_setting_value(spec, "20000"), 10000)
        self.assertEqual(client_service._coerce_feature_setting_value(spec, "100.6"), 101)
        self.assertEqual(client_service._coerce_feature_setting_value(spec, 250), 250)

    def test_float_rounds_to_three_decimals(self):
        spec = {"type": "float", "min": 1, "max": 600}
        self.assertEqual(client_service._coerce_feature_setting_value(spec, "1.23456"), 1.235)
        self.assertEqual(client_service._coerce_feature_setting_value(spec, "900"), 600)

    def test_bool_accepts_common_spellings(self):
        spec = {"type": "bool"}
        self.assertIs(client_service._coerce_feature_setting_value(spec, True), True)
        self.assertIs(client_service._coerce_feature_setting_value(spec, "yes"), True)
        self.assertIs(client_service._coerce_feature_setting_value(spec, "OFF"), False)
        self.assertIs(client_service._coerce_feature_setting_value(spec, "0"), False)
        with self.assertRaises(ValueError):
            client_service._coerce_feature_setting_value(spec, "maybe")

    def test_rejects_blank_and_garbage_numbers(self):
        with self.assertRaises(ValueError):
            client_service._coerce_feature_setting_value({"type": "int"}, "  ")
        with self.assertRaises(ValueError):
            client_service._coerce_feature_setting_value({"type": "int"}, "abc")

    def test_text_keeps_raw_value(self):
        self.assertEqual(client_service._coerce_feature_setting_value({"type": "text"}, 12), "12")
        self.assertEqual(client_service._coerce_feature_setting_value({"type": "text"}, "jpeg"), "jpeg")


class SpecNormalizationTests(unittest.TestCase):
    def _module(self, settings):
        return SimpleNamespace(FEATURE={"settings": settings})

    def test_dedupes_and_falls_back_for_bad_type_and_scope(self):
        settings = [
            {"key": "page_size", "label": "单次文件个数", "type": "int", "scope": "topic",
             "default": 100, "min": 1, "max": 10000, "help": "h"},
            {"key": "page_size", "label": "重复定义", "type": "int"},
            {"label": "缺 key", "type": "int"},
            "not-a-dict",
            {"key": "weird", "type": "weird", "scope": "weird"},
            {"key": "flag", "type": "BOOL", "scope": "GLOBAL"},
            {"key": "   ", "type": "int"},
        ]
        with mock.patch.object(bootstrap, "load_feature", return_value=self._module(settings)):
            specs = client_service._feature_setting_specs("demo")

        self.assertEqual([item["key"] for item in specs], ["page_size", "weird", "flag"])
        self.assertEqual(specs[0]["label"], "单次文件个数")
        self.assertEqual(specs[0]["scope"], "topic")
        self.assertEqual(specs[1]["type"], "text")     # 非法 type 回退 text
        self.assertEqual(specs[1]["scope"], "topic")  # 非法 scope 回退 topic
        self.assertEqual(specs[1]["label"], "weird")  # 缺 label 用 key
        self.assertEqual(specs[2]["type"], "bool")
        self.assertEqual(specs[2]["scope"], "global")

    def test_accepts_json_string_declaration(self):
        payload = json.dumps([{"key": "timeout", "type": "int", "scope": "global"}])
        with mock.patch.object(bootstrap, "load_feature", return_value=self._module(payload)):
            specs = client_service._feature_setting_specs("demo")
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["label"], "timeout")

    def test_missing_settings_declaration_returns_empty(self):
        with mock.patch.object(bootstrap, "load_feature", return_value=SimpleNamespace(FEATURE={})):
            self.assertEqual(client_service._feature_setting_specs("demo"), [])

    def test_feature_load_failure_propagates_to_caller_guard(self):
        with mock.patch.object(bootstrap, "load_feature", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                client_service._feature_setting_specs("demo")


class StorageTests(ConfigHarness):
    def setUp(self):
        super().setUp()
        self._specs_patcher = mock.patch.object(
            client_service, "_feature_setting_specs",
            side_effect=lambda feature: [dict(item) for item in DEMO_SPECS],
        )
        self._specs_patcher.start()
        self.addCleanup(self._specs_patcher.stop)

    def test_update_topic_scope_writes_device_json(self):
        result = json.loads(client_service.update_feature_setting("demo", "page_size", "250"))
        self.assertTrue(result["ok"])
        self.assertEqual(result["scope"], "topic")
        self.assertEqual(result["scope_text"], "分 topic")
        self.assertEqual(result["value"], 250)
        self.assertTrue(result["stored"])
        self.assertEqual(self.config["feature_settings"]["dev-main"]["demo"]["page_size"], 250)
        self.assertNotIn("feature_global_settings", self.config)

    def test_update_global_scope_writes_global_json(self):
        result = json.loads(client_service.update_feature_setting("demo", "timeout", "45"))
        self.assertTrue(result["ok"])
        self.assertEqual(result["scope"], "global")
        self.assertEqual(result["scope_text"], "全局")
        self.assertEqual(self.config["feature_global_settings"]["demo"]["timeout"], 45)
        self.assertNotIn("feature_settings", self.config)

    def test_topic_scope_kept_per_device(self):
        client_service.update_feature_setting("demo", "page_size", "300")
        client_service.update_feature_setting("demo", "page_size", "50", device_ref="dev-alt")
        self.assertEqual(self.config["feature_settings"]["dev-main"]["demo"]["page_size"], 300)
        self.assertEqual(self.config["feature_settings"]["dev-alt"]["demo"]["page_size"], 50)

        main_values = json.loads(client_service.feature_settings_values("demo"))["values"]
        alt_values = json.loads(
            client_service.feature_settings_values("demo", device_ref="dev-alt"))["values"]
        self.assertEqual(main_values["page_size"], 300)
        self.assertEqual(alt_values["page_size"], 50)

    def test_global_scope_shared_across_devices(self):
        client_service.update_feature_setting("demo", "timeout", "45")
        schema = json.loads(client_service.feature_settings_schema("demo", device_ref="dev-alt"))
        timeout_row = next(row for row in schema["settings"] if row["key"] == "timeout")
        self.assertEqual(schema["topic"], "sys/alt/request")
        self.assertEqual(timeout_row["value"], 45)
        self.assertTrue(timeout_row["stored"])

    def test_reset_clears_override_and_falls_back_to_declared_default(self):
        client_service.update_feature_setting("demo", "page_size", "250")
        self.assertEqual(client_service.feature_setting("demo", "page_size"), 250)
        result = json.loads(client_service.reset_feature_setting("demo", "page_size"))
        self.assertTrue(result["ok"])
        self.assertTrue(result["cleared"])
        self.assertEqual(result["value"], 100)
        self.assertEqual(self.config["feature_settings"], {})
        values = json.loads(client_service.feature_settings_values("demo"))["values"]
        self.assertEqual(values["page_size"], 100)

    def test_blank_string_clears_override(self):
        client_service.update_feature_setting("demo", "page_size", "250")
        result = json.loads(client_service.update_feature_setting("demo", "page_size", "   "))
        self.assertTrue(result["cleared"])
        self.assertEqual(result["value"], 100)
        self.assertEqual(self.config["feature_settings"], {})

    def test_invalid_value_is_rejected_without_writing(self):
        result = json.loads(client_service.update_feature_setting("demo", "page_size", "abc"))
        self.assertFalse(result["ok"])
        self.assertIn("ValueError", result["error"])
        self.assertEqual(self.saves, [])
        self.assertNotIn("feature_settings", self.config)

    def test_unknown_key_is_rejected(self):
        result = json.loads(client_service.update_feature_setting("demo", "nope", "1"))
        self.assertFalse(result["ok"])
        self.assertIn("does not declare", result["error"])

    def test_schema_marks_scope_default_and_saved_source(self):
        schema = json.loads(client_service.feature_settings_schema("demo"))
        self.assertTrue(schema["ok"])
        self.assertEqual(schema["device_id"], "dev-main")
        self.assertEqual(schema["topic"], "sys/main/request")
        rows = {row["key"]: row for row in schema["settings"]}

        self.assertEqual(rows["page_size"]["scope"], "topic")
        self.assertEqual(rows["page_size"]["scope_text"], "分 topic")
        self.assertEqual(rows["page_size"]["value"], 100)
        self.assertEqual(rows["page_size"]["value_text"], "100")
        self.assertEqual(rows["page_size"]["source"], "default")
        self.assertFalse(rows["page_size"]["stored"])

        self.assertEqual(rows["timeout"]["scope"], "global")
        self.assertEqual(rows["timeout"]["scope_text"], "全局")
        self.assertIsNone(rows["timeout"]["value"])
        self.assertEqual(rows["timeout"]["value_text"], "")
        self.assertEqual(rows["timeout"]["source"], "default")

        self.assertEqual(rows["enabled"]["value"], True)
        self.assertEqual(rows["enabled"]["value_text"], "True")

    def test_schema_reports_saved_values_as_saved_source(self):
        client_service.update_feature_setting("demo", "page_size", "250")
        client_service.update_feature_setting("demo", "timeout", "45")
        schema = json.loads(client_service.feature_settings_schema("demo"))
        rows = {row["key"]: row for row in schema["settings"]}
        self.assertEqual(rows["page_size"]["value"], 250)
        self.assertEqual(rows["page_size"]["source"], "saved")
        self.assertTrue(rows["page_size"]["stored"])
        self.assertEqual(rows["timeout"]["value"], 45)
        self.assertEqual(rows["timeout"]["source"], "saved")

    def test_feature_setting_reads_declared_value_and_falls_back(self):
        self.assertEqual(client_service.feature_setting("demo", "page_size"), 100)
        # timeout 声明的 default 为空 -> 用调用点默认值
        self.assertEqual(client_service.feature_setting("demo", "timeout", 30), 30)
        self.assertEqual(client_service.feature_setting("demo", "missing", 7), 7)
        client_service.update_feature_setting("demo", "page_size", "250")
        self.assertEqual(client_service.feature_setting("demo", "page_size"), 250)

    def test_feature_setting_accepts_feature_prefix(self):
        self.assertEqual(client_service.feature_setting("feature_demo", "page_size"), 100)

    def test_feature_timeout_uses_config_then_clamps_to_rpc_range(self):
        self.assertEqual(client_service.feature_timeout("demo", 20), 20)
        client_service.update_feature_setting("demo", "timeout", "900")
        self.assertEqual(client_service.feature_timeout("demo", 20), 600)
        client_service.update_feature_setting("demo", "timeout", "0.5")
        self.assertEqual(client_service.feature_timeout("demo", 20), 1)

    def test_feature_timeout_survives_dirty_stored_value(self):
        self.config["feature_global_settings"] = {"demo": {"timeout": "abc"}}
        self.assertEqual(client_service.feature_timeout("demo", 20), 20)


class SchemaErrorTests(ConfigHarness):
    def test_schema_reports_load_failure_instead_of_raising(self):
        with mock.patch.object(client_service, "_feature_setting_specs",
                               side_effect=RuntimeError("boom")):
            schema = json.loads(client_service.feature_settings_schema("demo"))
        self.assertFalse(schema["ok"])
        self.assertIn("RuntimeError", schema["error"])
        self.assertEqual(schema["settings"], [])

    def test_update_reports_load_failure_instead_of_raising(self):
        with mock.patch.object(client_service, "_feature_setting_specs",
                               side_effect=RuntimeError("boom")):
            result = json.loads(client_service.update_feature_setting("demo", "timeout", "30"))
        self.assertFalse(result["ok"])
        self.assertIn("RuntimeError", result["error"])


class GlobalFieldTests(unittest.TestCase):
    def test_scope_stores_are_persisted_in_global_json(self):
        for key in ("feature_settings", "feature_global_settings"):
            self.assertIn(key, client_service._GLOBAL_FIELDS)


class BuiltinDeclarationTests(unittest.TestCase):
    EXPECTED = ("files", "audio", "camera", "dialer", "wifi", "lockscreen", "probe", "repl")

    def test_each_feature_declares_rpc_timeout_in_topic_scope(self):
        for name in self.EXPECTED:
            specs = {item["key"]: item for item in client_service._feature_setting_specs(name)}
            self.assertIn("timeout", specs, name)
            self.assertEqual(specs["timeout"]["scope"], "topic", name)
            self.assertIn(specs["timeout"]["type"], ("int", "float"), name)
            self.assertIsNone(specs["timeout"]["default"], name)

    def test_files_page_size_moved_out_of_main_view(self):
        specs = {item["key"]: item for item in client_service._feature_setting_specs("files")}
        page = specs["page_size"]
        self.assertEqual(page["type"], "int")
        self.assertEqual(page["scope"], "topic")
        self.assertEqual(page["default"], 100)
        self.assertEqual(page["min"], 1)
        self.assertEqual(page["max"], 10000)

    def test_camera_declares_lens_and_format_params(self):
        specs = {item["key"]: item for item in client_service._feature_setting_specs("camera")}
        self.assertEqual(specs["lens_facing"]["type"], "int")
        self.assertEqual(specs["lens_facing"]["scope"], "topic")
        self.assertEqual(specs["image_format"]["type"], "text")
        self.assertEqual(specs["image_format"]["default"], "jpeg")


if __name__ == "__main__":
    unittest.main()
