import subprocess
import socket
import sys
import unittest
import urllib.parse
import urllib.request
from pathlib import Path

import server_http


class ServerHttpTests(unittest.TestCase):
    def test_module_exports_rpc_and_executor_interfaces(self):
        # 本仓库的 HTTP RPC 只有一个实现模块 server_http.py；旧用例还要求与
        # 外部项目的 rpc/rpc.py 包导出同名符号，该包从不属于本仓库，故移除
        # 对等断言，只校验本模块自身的公共接口。
        for name in (
            "start_rpc_server",
            "RPCRequestHandler",
            "ThreadedHTTPServer",
            "WebSocket",
            "get_bmp_bytes",
            "pretty_format",
            "qpsu",
            "stime",
        ):
            self.assertTrue(hasattr(server_http, name), name)
        self.assertTrue(hasattr(server_http, "rpc_executor"))
        self.assertEqual(server_http.rpc_executor.PythonExecutor().execute("1")["r"], 1)

    def test_http_response_wrapper_matches_rpc_contract(self):
        server = server_http.start_rpc_server(port=0, ip="127.0.0.1")
        try:
            code = "response.set_status(201); response.set_header('X-Test', 'ok'); response.set_data('body')"
            url = f"http://127.0.0.1:{server.server_port}/{urllib.parse.quote(code)}"
            with urllib.request.urlopen(url) as response:
                self.assertEqual(response.status, 201)
                self.assertEqual(response.headers["X-Test"], "ok")
                self.assertEqual(response.read().decode(), "body")
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)

    def test_websocket_upgrade_is_supported(self):
        server = server_http.start_rpc_server(
            port=0,
            ip="127.0.0.1",
            websocket_handler=lambda _handler, websocket, request: None,
        )
        try:
            with socket.create_connection(("127.0.0.1", server.server_port)) as client:
                client.sendall(
                    b"GET /ws HTTP/1.1\r\n"
                    b"Host: localhost\r\n"
                    b"Upgrade: websocket\r\n"
                    b"Connection: Upgrade\r\n"
                    b"Sec-WebSocket-Version: 13\r\n"
                    b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
                )
                response = client.recv(1024).decode("ascii")
            self.assertIn("HTTP/1.1 101 Switching Protocols", response)
            self.assertIn("Sec-WebSocket-Accept:", response)
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)

    def test_module_can_be_imported_from_its_directory(self):
        result = subprocess.run(
            [sys.executable, "-c", "import server_http; print(server_http.start_rpc_server.__name__)"],
            cwd=Path(server_http.__file__).parent,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "start_rpc_server")

    def test_http_execution_and_root_redirect(self):
        server = server_http.start_rpc_server(
            port=0,
            ip="127.0.0.1",
            globals={},
            locals={},
            redirect_root="/r=3",
        )
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/r%3D3"
            ) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(response.read().decode(), "3")

            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, request, response, code, msg, headers, new_url):
                    return None

            opener = urllib.request.build_opener(NoRedirect())
            with self.assertRaises(urllib.error.HTTPError) as context:
                opener.open(f"http://127.0.0.1:{server.server_port}/")
            self.assertEqual(context.exception.code, 302)
            self.assertEqual(context.exception.headers["Location"], "/r=3")
        finally:
            server.shutdown()
            server.server_close()
            # thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
