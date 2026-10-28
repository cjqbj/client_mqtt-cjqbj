# -*- coding: utf-8 -*-
"""[R1] PythonExecutor.execute(timeout=999) 超时守卫测试。

背景：executor.execute 原先在调用线程内同步执行任意代码且整个执行
持有自锁——server_mqtt.handle_message 在单分发线程里调用（一行
``while True: pass`` 永久瘫痪整个 MQTT 服务端：消息/ping/统计全停），
server_http.handle_rpc 又包了一层模块级 _stdout_lock（一条慢请求冻结
全部 HTTP RPC）。现在 execute 把执行移到 daemon 工作线程并默认最多
等 999 秒：超时返回明确错误；仍在运行的后台代码让执行器进入 busy，
新请求快速失败、后台结束后自动恢复。
"""
import inspect
import sys
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rpc_executor import PythonExecutor


class ExecutorTimeoutTests(unittest.TestCase):
    def test_default_timeout_is_999(self):
        sig = inspect.signature(PythonExecutor.execute)
        self.assertEqual(sig.parameters["timeout"].default, 999)

    def test_quick_code_returns_normally(self):
        ex = PythonExecutor(globals={})
        r = ex.execute("1+1")
        self.assertTrue(r["ok"])
        self.assertEqual(r["r"], 2)
        r = ex.execute("print('hi'); 5*5")
        self.assertTrue(r["ok"])
        self.assertEqual(r["stdout"], "hi\n")

    def test_timeout_returns_error_and_busy_then_recovers(self):
        ex = PythonExecutor(globals={})
        t0 = time.monotonic()
        r = ex.execute("import time; time.sleep(1.5); 42", timeout=0.3)
        dt = time.monotonic() - t0
        self.assertFalse(r["ok"])
        self.assertIn("timed out", r["error"])
        self.assertLess(dt, 1.2, "超时必须立即返回，不能等代码跑完")

        # 后台仍在运行：新请求快速失败（busy），而不是挂住
        t0 = time.monotonic()
        r2 = ex.execute("1+1", timeout=30)
        dt2 = time.monotonic() - t0
        self.assertFalse(r2["ok"])
        self.assertIn("busy", r2["error"])
        self.assertLess(dt2, 1.0, "busy 必须快速失败")

        # 后台线程结束后自动恢复
        deadline = time.monotonic() + 5
        while ex._stuck and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertFalse(ex._stuck, "后台执行结束后 busy 必须自动清除")
        r3 = ex.execute("2+2")
        self.assertTrue(r3["ok"])
        self.assertEqual(r3["r"], 4)

    def test_queued_request_is_aborted_not_executed_late(self):
        """等锁超时的请求必须作废：调用方拿到错误后代码绝不能再执行。"""
        ex = PythonExecutor(globals={})
        ex.globals_dict["ran"] = []
        # 第一条占住执行器 1.2s；第二条排队且只肯等 0.2s
        first = threading.Thread(
            target=lambda: ex.execute(
                "import time; time.sleep(1.2)", timeout=30))
        first.start()
        time.sleep(0.3)   # 确保第一条已持锁运行
        r = ex.execute("ran.append(1)", timeout=0.2)
        self.assertFalse(r["ok"])
        self.assertIn("busy", r["error"])
        first.join(timeout=10)
        # 作废的请求不允许在调用方返回后才被执行
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and threading.enumerate():
            if not any(t.name == "rpc-exec" and t.is_alive()
                       for t in threading.enumerate()):
                break
            time.sleep(0.05)
        self.assertEqual(ex.globals_dict["ran"], [],
                         "被作废的排队请求不应被执行")

    def test_execution_still_serialized(self):
        """锁语义不变：并发请求仍严格串行，注入互不可见。"""
        ex = PythonExecutor(globals={})
        ex.globals_dict["n"] = 0
        errs = []

        def bump():
            r = ex.execute(
                "n = n + 1\n"
                "import time; time.sleep(0.05)\n"
                "n = n + 1\n"
                "r = n", timeout=30)
            if not r.get("ok"):
                errs.append(r.get("error"))

        ts = [threading.Thread(target=bump) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=30)
        self.assertEqual(errs, [])
        self.assertEqual(ex.globals_dict["n"], 8,
                         "串行执行下 n 必须恰好累加 8 次")

    def test_request_context_injected_and_restored(self):
        ex = PythonExecutor(globals={})
        r = ex.execute("ctx_var + 1", globals={"ctx_var": 41})
        self.assertTrue(r["ok"])
        self.assertEqual(r["r"], 42)
        self.assertNotIn("ctx_var", ex.globals_dict, "请求级注入必须还原")

    def test_system_exit_still_caught(self):
        ex = PythonExecutor(globals={})
        r = ex.execute("raise SystemExit(0)")
        self.assertFalse(r["ok"])
        self.assertIn("SystemExit", r["error"])

    def test_timeout_none_keeps_sync_semantics(self):
        ex = PythonExecutor(globals={})
        r = ex.execute("7*6", timeout=None)
        self.assertTrue(r["ok"])
        self.assertEqual(r["r"], 42)

    def test_alias_define_and_use_still_work(self):
        """alias 定义即执行的内部递归不能被工作线程模型破坏。"""
        ex = PythonExecutor(globals={})
        r = ex.execute("#alias abc\n1+2")
        self.assertTrue(r["ok"], r.get("error"))
        self.assertEqual(r["r"], 3)
        self.assertEqual(r["alias"]["name"], "abc")
        r2 = ex.execute("abc")
        self.assertTrue(r2["ok"], r2.get("error"))
        self.assertEqual(r2["r"], 3)


if __name__ == "__main__":
    unittest.main()
