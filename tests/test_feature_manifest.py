"""Feature 版本清单（feature_manifest.json）的纯逻辑与安装路径测试。"""

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import bootstrap
import client_service


class FeatureManifestTests(unittest.TestCase):
    def test_parse_manifest_keeps_valid_entries(self):
        body = json.dumps({
            "schema": 1,
            "generated": "2026-10-29T00:00:00Z",
            "features": [
                {"name": "files", "version": 4, "sha256": "AABB", "size": 10},
                {"name": "camera", "filename": "feature_camera.py", "version": 3},
                {"name": "", "version": 1},
                {"filename": "notes.txt"},
                "broken",
            ],
        }).encode("utf-8")
        parsed = client_service._parse_feature_manifest(body, "http://x/m.json")
        self.assertEqual(parsed["source_url"], "http://x/m.json")
        self.assertEqual([item["name"] for item in parsed["features"]], ["files", "camera"])
        self.assertEqual(parsed["features"][0]["filename"], "feature_files.py")
        self.assertEqual(parsed["features"][0]["sha256"], "aabb")
        self.assertEqual(parsed["features"][1]["version"], 3)

    def test_parse_manifest_rejects_missing_features_list(self):
        with self.assertRaises(ValueError):
            client_service._parse_feature_manifest(b'{"schema": 1}')
        with self.assertRaises(ValueError):
            client_service._parse_feature_manifest(b"not json")

    def test_version_is_read_from_feature_literal(self):
        source = b'"""doc"""\nFEATURE = {"name": "x", "version": 7}\n'
        self.assertEqual(client_service._feature_version_from_source(source), 7)
        self.assertEqual(client_service._feature_version_from_source(b"FEATURE = {}"), 1)
        self.assertEqual(client_service._feature_version_from_source(b"broken" * 3), 0)

    def test_compare_marks_newer_version_and_missing_file(self):
        remote = {"version": 4, "sha256": "aaa"}
        self.assertEqual(
            client_service._compare_feature_entry(remote, {"present": False}),
            (True, "not installed"),
        )
        self.assertEqual(
            client_service._compare_feature_entry(
                remote, {"present": True, "version": 3, "sha256": "", "source": "builtin"}
            ),
            (True, "version 3 -> 4"),
        )
        self.assertEqual(
            client_service._compare_feature_entry(
                remote, {"present": True, "version": 4, "sha256": "bbb", "source": "py_updates"}
            ),
            (True, "sha256 differs"),
        )
        self.assertEqual(
            client_service._compare_feature_entry(
                remote, {"present": True, "version": 4, "sha256": "aaa", "source": "py_updates"}
            ),
            (False, "up to date"),
        )
        # 内置模块读的是 .pyc，sha 不同不算更新。
        self.assertEqual(
            client_service._compare_feature_entry(
                remote, {"present": True, "version": 4, "sha256": "bbb", "source": "builtin"}
            ),
            (False, "up to date"),
        )

    def test_feature_updates_reports_newer_and_missing(self):
        manifest = {
            "source_url": "http://x/feature_manifest.json",
            "generated": "2026-10-29T00:00:00Z",
            "cached": False,
            "features": [
                {"name": "files", "filename": "feature_files.py", "version": 4,
                 "sha256": "abc", "size": 10},
                {"name": "brandnew", "filename": "feature_brandnew.py", "version": 1,
                 "sha256": "def", "size": 5},
            ],
        }
        descriptors = [{"name": "files", "version": 3, "source": "builtin"}]
        file_info = {"module_file": "", "sha256": "", "size": None}
        with mock.patch.object(client_service, "fetch_feature_manifest",
                               return_value=manifest), \
                mock.patch.object(bootstrap, "describe_features", return_value=descriptors), \
                mock.patch.object(bootstrap, "feature_file_info", return_value=file_info):
            payload = json.loads(client_service.feature_updates(force=True))
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["available"], 2)
        rows = {row["name"]: row for row in payload["features"]}
        self.assertTrue(rows["files"]["update_available"])
        self.assertEqual(rows["files"]["local_version"], 3)
        self.assertEqual(rows["files"]["remote_version"], 4)
        self.assertTrue(rows["brandnew"]["update_available"])
        self.assertEqual(rows["brandnew"]["reason"], "not installed")

    def test_install_builtin_features_updates_outdated_file(self):
        with tempfile.TemporaryDirectory() as files_dir:
            update_dir = Path(files_dir, "py_updates")
            update_dir.mkdir()
            target = update_dir / "feature_audio.py"
            content = b'FEATURE = {"name": "audio", "version": 1}\n'
            target.write_bytes(content)
            manifest = {"features": [{
                "name": "audio", "filename": "feature_audio.py", "version": 2,
                "sha256": hashlib.sha256(b"new").hexdigest(), "size": 3,
            }]}
            calls = []

            def fake_install(bootstrap_module, filename, root, retries, timeout, sha256=""):
                calls.append({"filename": filename, "sha256": sha256})
                return {"ok": True, "filename": filename, "sha256": sha256}

            with mock.patch.object(client_service, "fetch_feature_manifest",
                                   return_value=manifest), \
                    mock.patch.object(client_service, "_builtin_feature_filenames",
                                      return_value=["feature_audio.py"]), \
                    mock.patch.object(client_service, "_install_builtin_feature_file",
                                      side_effect=fake_install):
                payload = json.loads(client_service.install_builtin_features(files_dir))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["results"][0]["action"], "updated")
            self.assertEqual(calls[0]["sha256"], hashlib.sha256(b"new").hexdigest())

    def test_install_builtin_features_skips_up_to_date_file(self):
        with tempfile.TemporaryDirectory() as files_dir:
            update_dir = Path(files_dir, "py_updates")
            update_dir.mkdir()
            content = b'FEATURE = {"name": "audio", "version": 2}\n'
            (update_dir / "feature_audio.py").write_bytes(content)
            manifest = {"features": [{
                "name": "audio", "filename": "feature_audio.py", "version": 2,
                "sha256": hashlib.sha256(content).hexdigest(), "size": len(content),
            }]}
            with mock.patch.object(client_service, "fetch_feature_manifest",
                                   return_value=manifest), \
                    mock.patch.object(client_service, "_builtin_feature_filenames",
                                      return_value=["feature_audio.py"]), \
                    mock.patch.object(client_service, "_install_builtin_feature_file") as install:
                payload = json.loads(client_service.install_builtin_features(files_dir))
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["results"][0]["skipped"])
            self.assertEqual(payload["results"][0]["action"], "skipped")
            install.assert_not_called()


if __name__ == "__main__":
    unittest.main()
