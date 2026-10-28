# -*- coding: utf-8 -*-
"""[R3] rpc_tcp.py 适配现行 rpc_executor API 的回归测试。

背景：9 月 executor 重构（execute 只剩 globals 参数）后 rpc_tcp 从未跟
上——TCPServer.__init__ 里引用不存在的 locals_dict 直接 NameError，构造
即崩；_process_request 调 execute(locals_dict=...) 每请求必 TypeError；
__main__ 传的 globals_dict= 也不是合法 kwarg。本文件锁定适配后的
server_http/server_mqtt 同款模式：持久 globals + 请求级 client_sock 注入。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import rpc_tcp


class RpcTcpRoundTripTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = rpc_tcp.TCPServer(host="127.0.0.1", port=0,
                                       globals={})
        cls.server.start()
        cls.client = rpc_tcp.TCPClient("127.0.0.1", cls.server.port)
        cls.client.connect()

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        cls.server.stop()

    def test_constructor_no_longer_raises(self):
        # 回归：旧实现 __init__ 引用不存在的 locals_dict，NameError
        srv = rpc_tcp.TCPServer(host="127.0.0.1", port=0)
        self.assertIsNone(srv.server_socket)

    def test_simple_expression(self):
        r = self.client.call("r = 1 + 1")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["r"], 2)
        self.assertEqual(r["stdout"], "")

    def test_stdout_captured(self):
        r = self.client.call("print('hi'); r = 'x' * 3")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["stdout"], "hi\n")
        self.assertEqual(r["r"], "xxx")

    def test_client_sock_injected_per_request(self):
        r = self.client.call("r = type(client_sock).__name__")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["r"], "socket")

    def test_client_sock_not_leaked_into_namespace(self):
        # 注入只在执行期间可见（上一用例已用到 client_sock）；
        # 持久命名空间在请求前后都必须不含它
        self.assertNotIn("client_sock", self.server.executor.globals_dict)
        r = self.client.call("r = type(client_sock).__name__")
        self.assertTrue(r["ok"], r)
        self.assertNotIn("client_sock", self.server.executor.globals_dict,
                         "请求级注入必须还原，不得泄漏到持久命名空间")

    def test_persistent_namespace_across_requests(self):
        r = self.client.call("v = 42")
        self.assertTrue(r["ok"], r)
        r = self.client.call("r = v * 2")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["r"], 84)

    def test_error_propagates_without_killing_server(self):
        r = self.client.call("1/0")
        self.assertFalse(r["ok"])
        self.assertIn("ZeroDivisionError", r["error"])
        # 出错后服务照常可用
        r = self.client.call("r = 'still-alive'")
        self.assertTrue(r["ok"], r)
        self.assertEqual(r["r"], "still-alive")

    def test_req_id_round_trip(self):
        r1 = self.client.call("r = 1")
        r2 = self.client.call("r = 2")
        self.assertEqual(r2["req_id"], r1["req_id"] + 1)

    def test_non_serializable_r_falls_back_to_repr(self):
        # 旧 TODO：r=client_sock 曾因 json 序列化卡死；帧编码 default=repr 兜底
        r = self.client.call("r = client_sock")
        self.assertTrue(r["ok"], r)
        self.assertIsInstance(r["r"], str)
        self.assertIn("socket", r["r"])


if __name__ == "__main__":
    unittest.main()
