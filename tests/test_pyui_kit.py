import sys
import threading
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app" / "src" / "main" / "python"))

import pyui_kit


class RunAsyncTests(unittest.TestCase):
    def test_failure_callback_keeps_exception_after_except_scope(self):
        # 回归：except ... as error 退出块时 CPython 会 del error；
        # 旧实现在 except 内定义引用 error 的闭包并 _post 延迟执行，
        # 主线程真正回调时 NameError 直接崩 Activity（拍照/下载崩溃根因）。
        posted = []
        done = threading.Event()
        seen = {}

        def fake_post(fn):
            posted.append(fn)
            done.set()

        def boom():
            raise RuntimeError("download failed")

        def on_error(error):
            seen["error"] = error

        with mock.patch.object(pyui_kit, "_post", fake_post):
            pyui_kit.run_async(boom, on_error=on_error)
            done.wait(2)

        self.assertEqual(len(posted), 1)
        # 延迟到"主线程"执行时异常对象必须仍可访问，且不抛 NameError。
        posted[0]()
        self.assertIsInstance(seen.get("error"), RuntimeError)
        self.assertEqual(str(seen["error"]), "download failed")

    def test_success_callback_receives_value(self):
        posted = []
        done = threading.Event()

        with mock.patch.object(pyui_kit, "_post", lambda fn: (posted.append(fn), done.set())):
            pyui_kit.run_async(lambda: 42, on_ok=lambda value: seen.update(value=value))

        seen = {}
        done.wait(2)
        posted[0]()
        self.assertEqual(seen["value"], 42)

    def test_guarded_swallows_every_exception_including_base_exception(self):
        # JVM 回调边界铁律：feature 回调任何异常都不能穿透到 Android 主线程。
        def fail_hard():
            raise SystemExit("feature died")

        pyui_kit.guarded("test callback", fail_hard)()  # 不抛即通过

        def normal():
            return "ok"

        self.assertEqual(pyui_kit.guarded("test callback", normal)(), "ok")


if __name__ == "__main__":
    unittest.main()
