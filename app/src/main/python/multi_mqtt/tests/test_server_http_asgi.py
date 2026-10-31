import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ASGI 应用在 import 时就初始化执行器并恢复 rpc.json 里的别名：先把
# RPC_CONFIG_PATH 指到临时文件，本组用例的别名只落临时盘，不污染仓库
# rpc.json（持久化本身由 test_server_http 覆盖）。
_TEMP_CONFIG_DIR = tempfile.TemporaryDirectory()
import server_http
server_http.RPC_CONFIG_PATH = Path(_TEMP_CONFIG_DIR.name) / "rpc.json"
import server_http_asgi
from rpc_executor import PythonExecutor


def _http_scope(method, path, headers=None, query=b""):
    """构造最小 http scope。

    ASGI 的 scope['path'] 已是 percent-decode 后的 UTF-8 文本（uvicorn 负责
    解码），与 WSGI PATH_INFO 还原后的形态一致，因此直接给解码后的路径。
    """
    return {
        "type": "http", "http_version": "1.1", "method": method,
        "scheme": "http", "path": path,
        "raw_path": path.encode("utf-8"),
        "query_string": query, "root_path": "",
        "headers": [(k.encode("latin-1"), v.encode("latin-1"))
                    for k, v in (headers or [])],
        "client": ("127.0.0.1", 1234), "server": ("test", 80),
    }


def _call_asgi_path(path, method="GET", headers=None, body=b"", query=b""):
    """驱动一次 ASGI 请求，返回 (status:int, headers_dict, body_bytes)。

    ``query`` 是原始 query bytes（与 uvicorn 给 scope['query_string'] 的
    形态一致）；路径仍是已解码文本。
    """
    messages = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        messages.append(message)

    asyncio.run(server_http_asgi.application(
        _http_scope(method, path, headers, query), receive, send))

    start = next(m for m in messages if m["type"] == "http.response.start")
    out = b"".join(m.get("body") or b"" for m in messages
                   if m["type"] == "http.response.body")
    headers_dict = {k.decode("latin-1").title(): v.decode("latin-1")
                    for k, v in start["headers"]}
    return start["status"], headers_dict, out


def _call_asgi(code):
    """按线上 URL 形态 /<key><code> 驱动一次 ASGI 请求。"""
    return _call_asgi_path("/" + server_http_asgi.RPC_KEY + code)


class AsgiApplicationTests(unittest.TestCase):
    def test_plain_expression_returns_200_with_value(self):
        status, _headers, out = _call_asgi("r=1")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"1")

    def test_streaming_write_sets_octet_stream_without_content_length(self):
        status, headers, out = _call_asgi(
            "response.set_header('X-Test','1');"
            "response.write(b'ab');response.write(b'cd')"
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("X-Test"), "1")
        self.assertEqual(headers.get("Content-Type"), "application/octet-stream")
        self.assertNotIn("Content-Length", headers,
                         "流式响应长度未知，不能预置 Content-Length")
        self.assertEqual(out, b"abcd")

    def test_undefined_name_returns_500_with_traceback(self):
        status, _headers, out = _call_asgi("nope_undefined_name")
        self.assertEqual(status, 500)
        self.assertIn(b"NameError", out)

    def test_chinese_path_is_roundtripped_without_mojibake(self):
        status, _headers, out = _call_asgi("r='你好'")
        self.assertEqual(status, 200)
        self.assertEqual(out, "你好".encode("utf-8"))

    def test_path_without_key_prefix_is_forbidden(self):
        status, _headers, out = _call_asgi_path("/r=1")
        self.assertEqual(status, 403)
        self.assertIn(b"Forbidden", out)

    def test_registered_alias_works_without_key_prefix(self):
        # alias_url='raw'：线上 /add 免 key 前缀，等价 /-r=1+1
        _call_asgi("#alias add\nr=1+1")
        status, _headers, out = _call_asgi_path("/add")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"2")

    def test_query_params_injected_into_params_alias(self):
        # alias.md 8.1：ASGI 垫片从原始 query_string 解析一次交给执行器
        _call_asgi("#alias addi(x, y=3)\nint(x) + int(y)")
        status, _headers, out = _call_asgi_path("/-addi", query=b"x=4")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"7")

    def test_unknown_query_param_returns_500(self):
        _call_asgi("#alias addi(x, y=3)\nint(x) + int(y)")
        status, _headers, out = _call_asgi_path(
            "/-addi", query=b"x=4&z=9")
        self.assertEqual(status, 500)
        self.assertIn("未知参数", out.decode("utf-8", "replace"))

    def test_root_path_runs_root_alias(self):
        # 首页 / 执行 root 别名（定义后持久化到 rpc.json）
        _call_asgi("#alias root\nr='<h1>home</h1>'")
        status, _headers, out = _call_asgi_path("/")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"<h1>home</h1>")
        self.assertTrue(server_http.RPC_CONFIG_PATH.exists())

    def test_set_executor_keeps_persisted_aliases(self):
        # 宿主 app.py 绑自己的 globals 后，模块初始化时从 rpc.json 恢复的
        # 别名表必须一起带过去，否则免前缀别名会全部失效。
        _call_asgi("#alias hostcheck\nr='ok'")
        original = server_http.RPCRequestHandler.executor
        try:
            server_http_asgi.set_executor(
                PythonExecutor(globals={'__name__': 'host_namespace'}))
            status, _headers, out = _call_asgi_path("/hostcheck")
            self.assertEqual(status, 200)
            self.assertEqual(out, b"ok")
        finally:
            server_http_asgi.set_executor(original)

    def test_non_get_post_method_rejected(self):
        status, _headers, out = _call_asgi_path("/r=1", method="PUT")
        self.assertEqual(status, 405)
        self.assertEqual(out, b"Method Not Allowed")

    def test_post_body_readable_from_rfile(self):
        code = "r=request.rfile.read(int(request.headers.get('Content-Length',0))).decode()"
        status, _headers, out = _call_asgi_path(
            "/" + server_http_asgi.RPC_KEY + code, method="POST",
            headers=[("Content-Length", "5")], body=b"hello")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"hello")

    def test_post_body_readable_from_wsgi_input(self):
        # client_http 生成代码在 hasattr(request,'rfile') 为假时走 environ
        code = "r=request.environ['wsgi.input'].read(5).decode()"
        status, _headers, out = _call_asgi_path(
            "/" + server_http_asgi.RPC_KEY + code, method="POST",
            headers=[("Content-Length", "5")], body=b"hello")
        self.assertEqual(status, 200)
        self.assertEqual(out, b"hello")

    def test_basic_auth_challenge_and_pass(self):
        original = server_http.RPCRequestHandler.basic_auth
        server_http.RPCRequestHandler.basic_auth = "admin:pw"
        try:
            status, headers, out = _call_asgi("r=2")
            self.assertEqual(status, 401)
            self.assertIn("Basic", headers.get("Www-Authenticate", ""))
            self.assertIn(b"401", out)
            status, _headers, out = _call_asgi_path(
                "/" + server_http_asgi.RPC_KEY + "r=2",
                headers=[("Authorization", "Basic YWRtaW46cHc=")])
            self.assertEqual(status, 200)
            self.assertEqual(out, b"2")
        finally:
            server_http.RPCRequestHandler.basic_auth = original

    def test_range_request_returns_206_slice(self):
        code = "response.write(b'abcdef')"
        path = "/" + server_http_asgi.RPC_KEY + code
        status, headers, out = _call_asgi_path(
            path, headers=[("Range", "bytes=1-3")])
        self.assertEqual(status, 206)
        self.assertEqual(headers.get("Content-Range"), "bytes 1-3/6")
        self.assertEqual(out, b"bcd")

    def test_open_range_keeps_streaming_200(self):
        code = "response.write(b'abcdef')"
        status, _headers, out = _call_asgi_path(
            "/" + server_http_asgi.RPC_KEY + code,
            headers=[("Range", "bytes=0-")])
        self.assertEqual(status, 200)
        self.assertEqual(out, b"abcdef")


def _drive_websocket(path, handler):
    """驱动一次 websocket scope：接受 → 处理器发消息 → 喂 disconnect → 关闭。"""
    sent = []

    async def scenario():
        incoming = asyncio.Queue()

        async def receive():
            return await incoming.get()

        async def send(message):
            sent.append(message)

        server_http.RPCRequestHandler.websocket_handlers[path] = handler
        task = asyncio.create_task(server_http_asgi.application(
            {"type": "websocket", "path": path, "headers": [],
             "query_string": b""}, receive, send))
        try:
            for _ in range(100):
                if len(sent) >= 2:
                    break
                await asyncio.sleep(0.01)
            await incoming.put({"type": "websocket.disconnect"})
            await asyncio.wait_for(task, 2)
        finally:
            server_http.RPCRequestHandler.websocket_handlers.pop(path, None)

    asyncio.run(scenario())
    return sent


class AsgiWebSocketTests(unittest.TestCase):
    def test_registered_handler_receives_accept_and_messages(self):
        # 与 HF Space 场景一致：realtime_editor 运行期注册的处理器无需
        # 往宿主应用注册额外路由，ASGI 应用按路径表直接分发。
        received = []

        def handler(connection, _request):
            connection.send('{"type":"snapshot"}')
            received.append(connection.receive())

        messages = _drive_websocket("/ws/test", handler)
        self.assertEqual(messages[0]["type"], "websocket.accept")
        self.assertEqual(messages[1]["type"], "websocket.send")
        self.assertEqual(messages[1]["text"], '{"type":"snapshot"}')
        self.assertEqual(messages[-1]["type"], "websocket.close")
        self.assertEqual(received, [None], "对端断开时应投递 None 结束处理器")

    def test_unknown_path_closes_with_1008(self):
        messages = []

        async def scenario():
            async def receive():
                return {"type": "websocket.disconnect"}

            async def send(message):
                messages.append(message)

            await server_http_asgi.application(
                {"type": "websocket", "path": "/ws/missing", "headers": [],
                 "query_string": b""}, receive, send)

        asyncio.run(scenario())
        self.assertEqual(messages, [{"type": "websocket.close", "code": 1008}])


if __name__ == "__main__":
    unittest.main()
