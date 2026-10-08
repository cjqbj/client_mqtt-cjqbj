import io
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock
from urllib.error import URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import bootstrap


class BootstrapTests(unittest.TestCase):
    def test_builtin_features_are_listed_without_python_source_files(self):
        previous_update_dir = bootstrap._UPDATE_DIR
        previous_file = bootstrap.__file__
        previous_path = list(sys.path)
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.__file__ = str(Path(directory) / "bootstrap.py")
                bootstrap.init_env(str(Path(directory) / "updates"))
                self.assertEqual(
                    set(bootstrap.list_features()),
                    {"files", "camera", "wifi", "audio", "probe"},
                )
            finally:
                bootstrap.__file__ = previous_file
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path

    def test_runtime_feature_replaces_cached_builtin_module(self):
        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.get("feature_camera")
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                cached_builtin = types.ModuleType("feature_camera")
                cached_builtin.__file__ = "/chaquopy/compiled/feature_camera.pyc"
                sys.modules["feature_camera"] = cached_builtin
                (Path(directory) / "feature_camera.py").write_text(
                    "FEATURE = {'name': 'camera'}\ndef run():\n    return 'external'\n",
                    encoding="utf-8",
                )

                self.assertEqual(bootstrap.call_feature("camera", "run"), "external")
            finally:
                if previous_module is None:
                    sys.modules.pop("feature_camera", None)
                else:
                    sys.modules["feature_camera"] = previous_module
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path

    def test_install_retries_and_reports_progress(self):
        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        content = b"FEATURE = {'name': 'demo'}\ndef run():\n    return 'ready'\n"
        progress = []
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                with mock.patch.object(
                    bootstrap.urllib.request,
                    "urlopen",
                    side_effect=[URLError("timed out"), io.BytesIO(content)],
                ) as open_url, mock.patch.object(bootstrap.time, "sleep") as sleep:
                    result = bootstrap.install_feature(
                        "https://github.com/example/feature_demo.py",
                        "feature_demo.py",
                        update_dir=directory,
                        retries=2,
                        timeout=7,
                        fallback_urls=("https://raw.example/feature_demo.py",),
                        progress=progress.append,
                    )
                self.assertTrue(result["ok"])
                self.assertEqual(result["attempts"], 2)
                self.assertEqual(open_url.call_args_list[0].kwargs["timeout"], 7.0)
                self.assertIn("raw.example", open_url.call_args_list[1].args[0].full_url)
                self.assertEqual(sleep.call_count, 1)
                self.assertTrue(any("attempt 1/2" in line for line in progress))
                self.assertTrue((Path(directory) / "feature_demo.py").is_file())
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path

    def test_writable_feature_stays_cached_until_manual_reload(self):
        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.get("feature_dynamic")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "feature_dynamic.py"
            path.write_text("def run(value):\n    return 'v1:' + value\n", encoding="utf-8")
            try:
                bootstrap.init_env(directory)
                self.assertEqual(bootstrap.call_feature("dynamic", "run", "ok"), "v1:ok")
                path.write_text("def run(value):\n    return 'v2:' + value\n", encoding="utf-8")
                # 已导入的 py_updates 模块保持缓存，文件被外部改写也不自动重载。
                self.assertEqual(bootstrap.call_feature("dynamic", "run", "ok"), "v1:ok")
                cached = sys.modules["feature_dynamic"]
                self.assertIs(bootstrap.load_feature("dynamic"), cached)
                # 只有显式 reload_feature（UI 长按）才清缓存重新导入。
                bootstrap.reload_feature("dynamic")
                self.assertEqual(bootstrap.call_feature("dynamic", "run", "ok"), "v2:ok")
                self.assertIsNot(sys.modules.get("feature_dynamic"), cached)
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                sys.modules.pop("feature_dynamic", None)
                if previous_module is not None:
                    sys.modules["feature_dynamic"] = previous_module

    def test_shadow_takeover_happens_only_once(self):
        previous_update_dir = bootstrap._UPDATE_DIR
        previous_path = list(sys.path)
        previous_module = sys.modules.get("feature_camera")
        with tempfile.TemporaryDirectory() as directory:
            try:
                bootstrap.init_env(directory)
                cached_builtin = types.ModuleType("feature_camera")
                cached_builtin.__file__ = "/chaquopy/compiled/feature_camera.pyc"
                sys.modules["feature_camera"] = cached_builtin
                (Path(directory) / "feature_camera.py").write_text(
                    "FEATURE = {'name': 'camera'}\ndef run():\n    return 'external'\n",
                    encoding="utf-8",
                )

                self.assertEqual(bootstrap.call_feature("camera", "run"), "external")
                shadow = sys.modules["feature_camera"]
                self.assertIsNot(shadow, cached_builtin)
                # 接管完成后后续调用直接复用缓存，不再重复重载。
                self.assertIs(bootstrap.load_feature("camera"), shadow)
            finally:
                bootstrap._UPDATE_DIR = previous_update_dir
                sys.path[:] = previous_path
                if previous_module is None:
                    sys.modules.pop("feature_camera", None)
                else:
                    sys.modules["feature_camera"] = previous_module

    def test_feature_name_is_restricted(self):
        with self.assertRaises(ValueError):
            bootstrap.load_feature("../unsafe")

    def test_all_builtin_features_render_with_python_ui(self):
        # 所有内置 feature 界面都必须由脚本自绘（ui=python + build_view），
        # APK 端不再保留任何 Compose 专属页面。
        import importlib

        self.assertEqual(
            set(bootstrap.BUILTIN_FEATURES),
            {"files", "camera", "wifi", "audio", "probe"},
        )
        for name in bootstrap.BUILTIN_FEATURES:
            module = importlib.import_module("feature_%s" % name)
            self.assertEqual(module.FEATURE.get("ui"), "python", name)
            self.assertTrue(callable(getattr(module, "build_view", None)), name)
        # 共享自绘工具包必须能在无 android 模块的桌面环境导入。
        import pyui_kit  # noqa: F401

        self.assertTrue(callable(pyui_kit.run_async))


if __name__ == "__main__":
    unittest.main()
