import json
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import feature_audio


class AudioFeatureTests(unittest.TestCase):
    def test_play_code_uses_configured_audio_path_and_returns_playback_result(self):
        code = feature_audio.build_play_code("/sdcard/apm/custom.mp3")

        self.assertIn('AUDIO_PATH = "/sdcard/apm/custom.mp3"', code)
        self.assertIn('r["playing"] = bool(mp.isPlaying())', code)
        self.assertIn('r["errors"].append("file not found")', code)
        compile(code, "remote_audio_play.py", "exec")

    def test_play_calls_rpc_with_generated_code_and_parses_result(self):
        rpc_result = {"ok": True, "r": '{"ok": true, "playing": true}'}
        with mock.patch.object(feature_audio.client_service, "rpc", return_value=rpc_result) as rpc, \
             mock.patch.object(
                 feature_audio.client_service,
                 "parse_json_result",
                 return_value={"ok": True, "playing": True},
             ) as parse_result:
            result = json.loads(feature_audio.play())

        self.assertTrue(result["ok"])
        self.assertTrue(result["playing"])
        self.assertEqual(rpc.call_args.args[0], feature_audio.build_play_code())
        parse_result.assert_called_once_with(rpc_result)


if __name__ == "__main__":
    unittest.main()
