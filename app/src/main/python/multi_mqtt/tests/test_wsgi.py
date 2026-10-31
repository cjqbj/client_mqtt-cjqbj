import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# WSGI 应用在 import 时就初始化执行器并恢复 rpc.json 里的别名：先把
# RPC_CONFIG_PATH 指到临时文件，本组用例的别名只落临时盘，不污染仓库
# rpc.json（持久化本身由 test_server_http 覆盖）。
_TEMP_CONFIG_DIR = tempfile.TemporaryDirectory()
import server_http
server_http.RPC_CONFIG_PATH = Path(_TEMP_CONFIG_DIR.name) / "rpc.json"
import server_http_wsgi


def _wsgi_environ(method, path, query=""):
    """构造 WSGI environ。

    真实网关（gunicorn/uwsgi）已对 PATH_INFO 做 percent-decode，再按
    PEP 3333 以 latin-1 承载原始字节；shim 只做 latin-1→utf-8 还原、不再
    unquote，因此这里必须直接给解码后的路径（中文按 utf-8→latin-1 模拟）。

    ``query`` 是**原始** QUERY_STRING（未解码），与真实网关一致；shim 从
    它解析一次别名参数，避免对已 unquote 的 self.path 二次解码。
    """
    carried = path.encode("utf-8").decode("latin-1")
    return {
        "REQUEST_METHOD": method,
        "PATH_INFO": carried,
        "QUERY_STRING": query,
        "REMOTE_ADDR": "127.0.0.1", "REMOTE_PORT": "1",
        "SERVER_NAME": "x", "SERVER_PORT": "80",
    }


def _call_wsgi_path(path, method="GET", query=""):
    """驱动一次 WSGI 请求，返回 (status_line, headers_dict, body_bytes)。"""
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    body = server_http_wsgi.application(
        _wsgi_environ(method, path, query), start_response)
    out = b"".join(body)
    return captured["status"], dict(captured["headers"]), out


def _call_wsgi(code):
    """按线上 URL 形态 /<key><code> 驱动一次 WSGI 请求。"""
    return _call_wsgi_path("/" + server_http_wsgi.RPC_KEY + code)


class WsgiApplicationTests(unittest.TestCase):
    def test_plain_expression_returns_200_with_value(self):
        status, _headers, out = _call_wsgi("r=1")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"1")

    def test_streaming_write_sets_octet_stream_without_content_length(self):
        status, headers, out = _call_wsgi(
            "response.set_header('X-Test','1');"
            "response.write(b'ab');response.write(b'cd')"
        )
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(headers.get("X-Test"), "1")
        self.assertEqual(headers.get("Content-Type"), "application/octet-stream")
        self.assertNotIn("Content-Length", headers, "流式响应长度未知，不能预置 Content-Length")
        self.assertEqual(out, b"abcd")

    def test_undefined_name_returns_500_with_traceback(self):
        status, _headers, out = _call_wsgi("nope_undefined_name")
        self.assertTrue(status.startswith("500"), status)
        self.assertIn(b"NameError", out)

    def test_chinese_path_is_roundtripped_without_mojibake(self):
        # PEP 3333：网关解码 %XX 后以 latin-1 承载字节，shim 必须还原成
        # UTF-8；否则中文代码会变乱码（edge-tts 逐字母朗读事故的根因）。
        status, _headers, out = _call_wsgi("r='你好'")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, "你好".encode("utf-8"))

    def test_path_without_key_prefix_is_forbidden(self):
        status, _headers, out = _call_wsgi_path("/r=1")
        self.assertTrue(status.startswith("403"), status)
        self.assertIn(b"Forbidden", out)

    def test_registered_alias_works_without_key_prefix(self):
        # alias_url='raw'：线上 /add 免 key 前缀，等价 /-r=1+1
        _call_wsgi("#alias add\nr=1+1")
        status, _headers, out = _call_wsgi_path("/add")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"2")

    def test_dotted_alias_is_persisted_and_routable(self):
        # 带点名字（ime.apk）同样能定义、持久化并免 key 访问
        _call_wsgi("#alias ime.apk\nr='apk'")
        status, _headers, out = _call_wsgi_path("/ime.apk")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"apk")
        self.assertIn("ime.apk", server_http.RPC_CONFIG_PATH.read_text(
            encoding="utf-8"))

    def test_alias_url_supports_path_argument(self):
        # /<别名>=<参数>：路径原文写进别名 body 的 _alias_arg
        _call_wsgi("#alias where\nr = _alias_arg")
        status, _headers, out = _call_wsgi_path(
            "/where=/home/suishou/multi_mqtt/")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"/home/suishou/multi_mqtt/")

    def test_query_params_injected_into_params_alias(self):
        # alias.md 8.1：WSGI 垫片从原始 QUERY_STRING 解析一次并交给执行器
        _call_wsgi("#alias addi(x, y=3)\nint(x) + int(y)")
        status, _headers, out = _call_wsgi_path("/-addi", query="x=4")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"7")

    def test_unknown_query_param_returns_500(self):
        _call_wsgi("#alias addi(x, y=3)\nint(x) + int(y)")
        status, _headers, out = _call_wsgi_path(
            "/-addi", query="x=4&z=9")
        self.assertTrue(status.startswith("500"), status)
        self.assertIn("未知参数".encode("utf-8"), out)

    def test_percent_encoded_query_is_not_decoded_twice(self):
        # 原始 QUERY_STRING 里的 %25 只解一次：值应为字面 '%25' 而非 '%'
        _call_wsgi("#alias echo(x)\nr = x")
        status, _headers, out = _call_wsgi_path(
            "/-echo", query="x=%2541")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"%41")

    def test_serve_file_alias_streams_via_wsgi(self):
        # #serve-file 别名由 server_http 直接流式输出，WSGI 下同样带
        # Content-Length 与文件字节（不经过执行器，不需要 response.write）
        handle, path = tempfile.mkstemp(prefix="mm_wsgi_", suffix=".bin")
        with os.fdopen(handle, "wb") as file:
            file.write(b"wsgi-bytes")
        self.addCleanup(os.unlink, path)
        _call_wsgi("#alias blob\n#serve-file " + json.dumps({
            "path": path,
            "filename": "blob.bin",
            "content_type": "application/octet-stream",
            "rate_limit": 0,
        }) + "\nr=1")
        status, headers, out = _call_wsgi_path("/blob")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(headers.get("Content-Length"), "10")
        self.assertEqual(headers.get("Content-Type"), "application/octet-stream")
        self.assertEqual(out, b"wsgi-bytes")

    def test_root_path_runs_root_alias(self):
        # 首页 / 执行 root 别名（定义后持久化到 rpc.json，重启/SIGHUP 后仍生效）
        _call_wsgi("#alias root\nr='<h1>home</h1>'")
        status, _headers, out = _call_wsgi_path("/")
        self.assertTrue(status.startswith("200"), status)
        self.assertEqual(out, b"<h1>home</h1>")
        self.assertTrue(server_http.RPC_CONFIG_PATH.exists())

    def test_non_get_post_method_rejected(self):
        captured = {}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = headers

        environ = _wsgi_environ("PUT", "/r=1")
        body = b"".join(server_http_wsgi.application(environ, start_response))
        self.assertTrue(captured["status"].startswith("405"), captured["status"])
        self.assertEqual(body, b"Method Not Allowed")


if __name__ == "__main__":
    unittest.main()
