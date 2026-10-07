import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import server_http_wsgi


def _wsgi_environ(method, path):
    """构造 WSGI environ。

    真实网关（gunicorn/uwsgi）已对 PATH_INFO 做 percent-decode，再按
    PEP 3333 以 latin-1 承载原始字节；shim 只做 latin-1→utf-8 还原、不再
    unquote，因此这里必须直接给解码后的路径（中文按 utf-8→latin-1 模拟）。
    """
    carried = path.encode("utf-8").decode("latin-1")
    return {
        "REQUEST_METHOD": method,
        "PATH_INFO": carried,
        "QUERY_STRING": "",
        "REMOTE_ADDR": "127.0.0.1", "REMOTE_PORT": "1",
        "SERVER_NAME": "x", "SERVER_PORT": "80",
    }


def _call_wsgi(code):
    """驱动一次 WSGI 请求，返回 (status_line, headers_dict, body_bytes)。"""
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    body = server_http_wsgi.application(
        _wsgi_environ("GET", "/" + code), start_response)
    out = b"".join(body)
    return captured["status"], dict(captured["headers"]), out


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
