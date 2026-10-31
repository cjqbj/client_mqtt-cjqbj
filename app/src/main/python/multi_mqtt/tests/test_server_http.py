import base64
import json
import os
import subprocess
import socket
import sys
import tempfile
import threading
import time
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
                response = client.recv(1024).split(b"\r\n\r\n", 1)[0].decode("ascii")
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


class KeyPrefixAndAliasUrlTests(unittest.TestCase):
    """key 作为真正的 URL 前缀，以及 alias_url 开关（不改 executor）。"""

    def _start(self, **kwargs):
        # 本组只验证 URL 语义；持久化关闭，避免往仓库 rpc.json 写测试别名
        kwargs.setdefault('persist_aliases', False)
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={}, **kwargs
        )
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    @staticmethod
    def _get(port, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10) as response:
            return response.read().decode()

    def _assert_403(self, port, path):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=10)
        self.assertEqual(context.exception.code, 403)

    def test_key_is_true_url_prefix_and_legacy_form_still_works(self):
        port = self._start(key="-")
        # key='-' → /-r=1+1；旧地址 /-/r=1+1 也保留
        self.assertEqual(self._get(port, "/-1%2B1"), "2")
        self.assertEqual(self._get(port, "/-/1%2B1"), "2")
        self._assert_403(port, "/1%2B1")

    def test_key_strips_surrounding_slashes(self):
        port = self._start(key="/abc/")
        self.assertEqual(self._get(port, "/abc/1%2B1"), "2")
        self.assertEqual(self._get(port, "/abc1%2B1"), "2")
        self._assert_403(port, "/1%2B1")

    def test_alias_url_keyed_requires_key_prefix(self):
        port = self._start(key="-")
        # key 处理后的代码是 add，执行器自己会展开别名
        self.assertEqual(self._get(port, "/-%23alias%20add%0Ar%3D1%2B1"), "2")
        self.assertEqual(self._get(port, "/-add"), "2")
        self._assert_403(port, "/add")

    def test_alias_url_raw_allows_bare_alias_name(self):
        port = self._start(key="-", alias_url="raw")
        self.assertEqual(self._get(port, "/-%23alias%20add%0Ar%3D1%2B1"), "2")
        # /add 免 key 前缀，等价 /-r=1+1；未注册路径仍 403
        self.assertEqual(self._get(port, "/add"), "2")
        self.assertEqual(self._get(port, "/-add"), "2")
        self._assert_403(port, "/not_registered_alias")

    def test_alias_url_raw_supports_path_argument(self):
        port = self._start(key="-", alias_url="raw")
        self.assertEqual(self._get(port, "/-" + urllib.parse.quote(
            "#alias where\nr = _alias_arg", safe="")), "")
        # /<别名>=<参数>：路径原文（含 / 与 =）写进别名 body 的 _alias_arg
        self.assertEqual(self._get(port, "/where%3D%2Fhome%2Fu%2Fproj"),
                         "/home/u/proj")
        # 带 key 前缀的写法同样命中（执行器自己也做前缀展开）
        self.assertEqual(self._get(port, "/-where%3D%2Fhome%2Fu%2Fproj"),
                         "/home/u/proj")

    def test_alias_url_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            self._start(alias_url="definitely-not-valid")


class AliasQueryParamTests(unittest.TestCase):
    """HTTP query → 执行器调用方数据（alias.md 8.1 / 8.3 / 13）。

    server_http 只做两件事：把 query 解析成 dict（重复 key last-wins），
    再原样交给执行器；键校验 / 默认值 / 转换全部在执行器内完成。
    ``=arg`` 与 prefix 的参数走 `_alias_arg`，不吃 query（正交机制）。
    """

    def _start(self, **kwargs):
        kwargs.setdefault('key', '-')
        kwargs.setdefault('alias_url', 'raw')
        kwargs.setdefault('persist_aliases', False)
        server = server_http.start_rpc_server(
            port=0, ip='127.0.0.1', globals={}, locals={}, **kwargs)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    @staticmethod
    def _define(port, code):
        # 通过 HTTP 注册，与线上路径完全一致（含定义即执行）
        url = (f"http://127.0.0.1:{port}/-"
               + urllib.parse.quote(code, safe=''))
        with urllib.request.urlopen(url, timeout=10) as response:
            assert response.status == 200

    @staticmethod
    def _get(port, path):
        with urllib.request.urlopen(
                f"http://127.0.0.1:{port}{path}", timeout=10) as response:
            return response.read().decode()

    def _assert_error(self, port, path, *needles):
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}{path}", timeout=10)
        self.assertEqual(context.exception.code, 500)
        body = context.exception.read().decode('utf-8', 'replace')
        for needle in needles:
            self.assertIn(needle, body)
        return body

    def test_params_query_injection_defaults_and_routes(self):
        port = self._start()
        self._define(port, "#alias addi(x, y=3)\nint(x) + int(y)")
        # 缺省 y=3；dash 路由与 raw 路由同一条执行路径
        self.assertEqual(self._get(port, "/-addi?x=4"), "7")
        self.assertEqual(self._get(port, "/-addi?x=4&y=3"), "7")
        self.assertEqual(self._get(port, "/addi?x=4"), "7")

    def test_missing_and_unknown_query_params_fail_loud(self):
        port = self._start()
        self._define(port, "#alias addi(x, y=3)\nint(x) + int(y)")
        # C1：缺必填 x，错误里带声明原文
        self._assert_error(port, "/-addi?y=3", "缺少必填参数", "addi(x, y=3)")
        # C2：未知参数 z，错误里带可绑定集
        self._assert_error(port, "/-addi?x=4&z=9", "未知参数", "可绑定")

    def test_repeated_query_key_last_wins(self):
        # C4：重复 key 按 HTTP 标准 last-wins，不报错
        port = self._start()
        self._define(port, "#alias addi(x, y=3)\nint(x) + int(y)")
        self.assertEqual(self._get(port, "/-addi?x=1&x=4"), "7")

    def test_query_values_stay_strings_by_default(self):
        port = self._start()
        self._define(port, "#alias kind(x)\nr = type(x).__name__")
        self.assertEqual(self._get(port, "/-kind?x=4"), "str")

    def test_json_mode_decodes_values_and_keeps_bad_ones(self):
        # alias_url='json' 只是值解码开关，路由仍是 raw（alias.md 8.3）
        port = self._start(alias_url='json')
        self._define(port, "#alias kind(x)\nr = type(x).__name__")
        self.assertEqual(self._get(port, "/-kind?x=4"), "int")
        self.assertEqual(self._get(port, "/-kind?x=%5B1%2C2%5D"), "list")
        # 单个值 JSON 坏了只保留原字符串，不牵连请求
        self.assertEqual(self._get(port, "/-kind?x=abc"), "str")

    def test_plain_alias_query_uses_free_variables(self):
        # 13.4：无签名别名的可绑定集 = body 顶层自由变量集
        port = self._start()
        self._define(port, "#alias sum3\nint(x) + int(y) + int(z)")
        self.assertEqual(self._get(port, "/-sum3?x=1&y=2&z=3"), "6")
        # 显式 query 参与 C1：缺 z 时不会退化成 NameError
        self._assert_error(port, "/-sum3?x=1&y=2", "缺少必填参数", "sum3")

    def test_prefix_and_arg_forms_ignore_query(self):
        port = self._start()
        self._define(port, "#alias git+\nr = _alias_arg")
        # prefix：query 不参与，剩余串仍进 _alias_arg
        self.assertEqual(self._get(port, "/-git%20status%20-s"), "status -s")
        self.assertEqual(self._get(port, "/git%20status?x=1"), "status")
        # =arg：参数来自路径，query 被忽略
        self._define(port, "#alias where\nr = _alias_arg")
        self.assertEqual(
            self._get(port, "/-where%3D%2Fhome%2Fu?x=1"), "/home/u")


class AliasUrlAndQueryHelperTests(unittest.TestCase):
    """alias_url 归一化与 query 解析的纯函数行为（含坏数据降级）。"""

    def test_normalize_alias_url_supported_values(self):
        for value, expected in (
            ("dash", ("dash", False)),
            ("keyed", ("dash", False)),
            ("raw", ("raw", False)),
            ("json", ("raw", True)),
            ("raw-json", ("raw", True)),
            ("dash-json", ("dash", True)),
            ("DASH", ("dash", False)),
            (None, ("dash", False)),
        ):
            self.assertEqual(server_http.normalize_alias_url(value), expected,
                             value)

    def test_normalize_alias_url_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            server_http.normalize_alias_url("definitely-not-valid")

    def test_parse_query_params_last_wins_and_tolerance(self):
        self.assertEqual(
            server_http.parse_query_params("x=1&x=2&y=&flag"),
            {"x": "2", "y": "", "flag": ""})
        for bad in (None, "", 3):
            self.assertEqual(server_http.parse_query_params(bad), {}, bad)

    def test_decode_alias_query_json_mode_only_decodes_when_enabled(self):
        self.assertEqual(
            server_http.decode_alias_query({"x": "4"}), {"x": "4"})
        self.assertEqual(
            server_http.decode_alias_query({"x": "4", "y": "abc"}, as_json=True),
            {"x": 4, "y": "abc"})
        self.assertEqual(server_http.decode_alias_query(None, as_json=True), {})


class ServeFileSpecParseTests(unittest.TestCase):
    """#serve-file 规格解析：坏数据只返回 None（由调用方退回普通执行）。"""

    def test_parse_spec_and_defaults(self):
        spec = server_http.parse_serve_file(
            '#serve-file ' + json.dumps({'path': '/tmp/a.apk'}) + '\nr=1')
        self.assertEqual(spec['path'], '/tmp/a.apk')
        self.assertEqual(spec['filename'], 'a.apk')
        self.assertEqual(spec['content_type'], 'application/octet-stream')
        self.assertEqual(spec['rate_limit'], 0)
        self.assertEqual(spec['chunk_size'], server_http.SERVE_FILE_CHUNK)

    def test_parse_clamps_negative_values(self):
        spec = server_http.parse_serve_file(
            '#serve-file ' + json.dumps(
                {'path': '/tmp/a', 'rate_limit': -5, 'chunk_size': 1}))
        self.assertEqual(spec['rate_limit'], 0)
        self.assertEqual(spec['chunk_size'], 1024)

    def test_parse_rejects_bad_payloads(self):
        for code in (
            'r=1',
            '#serve-file',
            '#serve-file {}',
            '#serve-file not-json',
            '#serve-file []',
            '#serve-file {"path": 3}',
            '#serve-file {"path": "/tmp/a", "rate_limit": "x"}',
            None,
        ):
            self.assertIsNone(server_http.parse_serve_file(code), code)


class ServeFileAliasTests(unittest.TestCase):
    """文件直出别名：请求线程里直接流式输出，不占执行器锁。"""

    def _start(self):
        server = server_http.start_rpc_server(
            port=0, ip='127.0.0.1', globals={}, locals={},
            key='-', alias_url='raw', persist_aliases=False,
        )
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def _mount(self, server, name, spec, tail='r = "executed"'):
        executor = server.rpc_config['executor']
        executor.aliases[name] = {
            'code': '#serve-file ' + json.dumps(spec) + '\n' + tail,
            'created': 0, 'updated': 0, 'hits': 0, 'last_used': None,
        }

    @staticmethod
    def _temp_file(size):
        handle, path = tempfile.mkstemp(prefix='mm_serve_', suffix='.bin')
        with os.fdopen(handle, 'wb') as file:
            file.write(b'x' * size)
        return path

    def test_streams_file_bytes_and_headers(self):
        path = self._temp_file(2048)
        self.addCleanup(os.unlink, path)
        server = self._start()
        self._mount(server, 'blob', {
            'path': path,
            'filename': 'ime.apk',
            'content_type': 'application/vnd.android.package-archive',
            'rate_limit': 0,
        })
        url = f'http://127.0.0.1:{server.server_port}/blob'
        with urllib.request.urlopen(url, timeout=10) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers['Content-Length'], '2048')
            self.assertEqual(
                response.headers['Content-Type'],
                'application/vnd.android.package-archive')
            self.assertIn('attachment; filename="ime.apk"',
                          response.headers['Content-Disposition'])
            self.assertEqual(response.read(), b'x' * 2048)

    def test_missing_file_is_404(self):
        server = self._start()
        self._mount(server, 'gone', {'path': '/no/such/file.apk'})
        url = f'http://127.0.0.1:{server.server_port}/gone'
        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(url, timeout=10)
        self.assertEqual(context.exception.code, 404)
        self.assertIn('not found', context.exception.read().decode())

    def test_bad_spec_falls_back_to_normal_alias(self):
        server = self._start()
        executor = server.rpc_config['executor']
        executor.aliases['broken'] = {
            'code': '#serve-file {not-json\nr = 42',
            'created': 0, 'updated': 0, 'hits': 0, 'last_used': None,
        }
        url = f'http://127.0.0.1:{server.server_port}/broken'
        with urllib.request.urlopen(url, timeout=10) as response:
            self.assertEqual(response.read().decode(), '42')

    def test_slow_download_does_not_block_rpc(self):
        # 限速 32KB/s、64KB 文件 ≈ 2 秒；下载进行中 RPC 必须仍然秒回，
        # 否则说明文件直出退回了“占执行器锁”的老路径。
        path = self._temp_file(64 * 1024)
        self.addCleanup(os.unlink, path)
        server = self._start()
        self._mount(server, 'slow', {
            'path': path, 'rate_limit': 32 * 1024, 'chunk_size': 8192,
        })
        first_chunk = threading.Event()
        seen = {}

        def downloader():
            # 按真实客户端行为把整包读完：服务端在限速写，文件保持打开；
            # 读完（≈2s）后服务端收尾关文件，测试清理才能删临时文件。
            url = f'http://127.0.0.1:{server.server_port}/slow'
            try:
                with urllib.request.urlopen(url, timeout=15) as response:
                    seen['status'] = response.status
                    done = 0
                    while True:
                        block = response.read(8192)
                        if not block:
                            break
                        done += len(block)
                        if done >= 8192 and not first_chunk.is_set():
                            seen['bytes'] = done
                            first_chunk.set()
                    seen['total'] = done
            except Exception as error:   # pragma: no cover - 仅排查用
                seen['error'] = error
                first_chunk.set()

        thread = threading.Thread(target=downloader, daemon=True)
        thread.start()
        self.assertTrue(first_chunk.wait(10), seen)
        self.assertEqual(seen.get('status'), 200, seen)
        started = time.monotonic()
        with urllib.request.urlopen(
                f'http://127.0.0.1:{server.server_port}/-1%2B1',
                timeout=10) as response:
            self.assertEqual(response.read().decode(), '2')
        self.assertLess(time.monotonic() - started, 1.5)
        thread.join(15)
        self.assertEqual(seen.get('total'), 64 * 1024, seen)


class CliArgsTests(unittest.TestCase):
    """命令行参数拼写（--basic_auth / -basic_auth / -basic-auth 同义）。"""

    def test_basic_auth_flag_spellings(self):
        for flag in ("--basic_auth", "-basic_auth", "-basic-auth"):
            args = server_http.build_arg_parser().parse_args([flag, "admin:pw"])
            self.assertEqual(args.basic_auth, "admin:pw", flag)

    def test_defaults_keep_server_unauthenticated(self):
        args = server_http.build_arg_parser().parse_args([])
        self.assertEqual(args.basic_auth, "")
        self.assertEqual(args.key, "")
        self.assertEqual(args.alias_url, "dash")
        self.assertEqual(args.root_alias, "root")
        self.assertTrue(args.persist_aliases)
        self.assertEqual(args.port, 1144)

    def test_cli_basic_auth_value_gates_requests(self):
        # start_rpc_server 把 basic_auth 写进 RPCRequestHandler 类属性，同进程
        # 后跑的 WSGI 用例会继承它（shim 读类属性），必须还原
        old_basic_auth = server_http.RPCRequestHandler.basic_auth
        self.addCleanup(setattr, server_http.RPCRequestHandler,
                        "basic_auth", old_basic_auth)
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={},
            key="-", basic_auth="admin:pw",
        )
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        port = server.server_port
        url = f"http://127.0.0.1:{port}/-1%2B1"

        with self.assertRaises(urllib.error.HTTPError) as context:
            urllib.request.urlopen(url, timeout=10)
        self.assertEqual(context.exception.code, 401)

        request = urllib.request.Request(url)
        request.add_header(
            "Authorization",
            "Basic " + base64.b64encode(b"admin:pw").decode("ascii"),
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.read().decode(), "2")


class AliasPersistenceTests(unittest.TestCase):
    """别名持久化：rpc.json 的 http_aliases 段 + 根路径 / 显示 HTML。"""

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.config_path = Path(temp.name) / "rpc.json"
        # 预置 server_mqtt 的其它键，验证写入只合并自己的段、不覆盖别人
        self.config_path.write_text(
            json.dumps({"request_topic": "test/request"}), encoding="utf-8")
        old_path = server_http.RPC_CONFIG_PATH
        server_http.RPC_CONFIG_PATH = self.config_path
        self.addCleanup(setattr, server_http, "RPC_CONFIG_PATH", old_path)

    def _start(self, **kwargs):
        kwargs.setdefault("key", "-")
        kwargs.setdefault("alias_url", "raw")
        server = server_http.start_rpc_server(
            port=0, ip="127.0.0.1", globals={}, locals={}, **kwargs)
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server.server_port

    def _get(self, port, path):
        return urllib.request.urlopen(
            f"http://127.0.0.1:{port}{path}", timeout=10)

    def _document(self):
        return json.loads(self.config_path.read_text(encoding="utf-8"))

    def _saved(self):
        return self._document().get("http_aliases", {})

    @staticmethod
    def _quote(code):
        return urllib.parse.quote(code, safe="")

    def test_alias_definition_is_persisted_and_restored(self):
        port = self._start()
        with self._get(port, "/-" + self._quote("#alias addx\nr=1+1")) as response:
            self.assertEqual(response.read().decode(), "2")
        self.assertEqual(self._saved(), {"addx": "r=1+1"})
        # 其它模块的键原样保留
        self.assertEqual(self._document()["request_topic"], "test/request")

        # 新服务器 = 新执行器：从 rpc.json 恢复后 /addx 免 key 前缀可用
        port2 = self._start()
        with self._get(port2, "/addx") as response:
            self.assertEqual(response.read().decode(), "2")

    def test_dotted_alias_is_persisted_and_restored(self):
        port = self._start()
        with self._get(port, "/-" + self._quote("#alias ime.apk\nr='apk'")) as response:
            self.assertEqual(response.read().decode(), "apk")
        self.assertEqual(self._saved(), {"ime.apk": "r='apk'"})

        # 新服务器 = 新执行器：带点名字同样从 rpc.json 恢复，/ime.apk 免 key
        port2 = self._start()
        with self._get(port2, "/ime.apk") as response:
            self.assertEqual(response.read().decode(), "apk")

        with self._get(port2, "/-" + self._quote("#unalias ime.apk")):
            pass
        self.assertNotIn("ime.apk", self._saved())

    def test_params_alias_persists_signature_and_restores_mode(self):
        port = self._start()
        with self._get(port, "/-" + self._quote(
                "#alias addi(x, y=3)\nint(x) + int(y)")):
            pass
        # params 别名落盘为 object（code + 原始签名）；mode/params/defaults
        # 属于派生字段，启动时由执行器重新推导，不写进文件。
        self.assertEqual(
            self._saved()["addi"],
            {"code": "int(x) + int(y)", "signature": "x, y=3"})

        # 新服务器 = 新执行器：签名恢复后仍是 params，query 走默认值
        port2 = self._start()
        record = server_http.RPCRequestHandler.executor.aliases["addi"]
        self.assertEqual(record["mode"], "params")
        self.assertEqual(record["params"], ["x", "y"])
        self.assertEqual(record["defaults"], {"y": 3})
        with self._get(port2, "/-addi?x=4") as response:
            self.assertEqual(response.read().decode(), "7")

    def test_unalias_removes_persisted_entry(self):
        port = self._start()
        with self._get(port, "/-" + self._quote("#alias gonex\nr=7")):
            pass
        self.assertIn("gonex", self._saved())
        with self._get(port, "/-" + self._quote("#unalias gonex")) as response:
            self.assertIn("deleted", response.read().decode())
        self.assertNotIn("gonex", self._saved())

    def test_ephemeral_flags_skip_persistence(self):
        port = self._start()
        # 三种“不持久化”写法都只注册到内存
        for code in ("#alias eph1 !\nr=1", "#alias eph2 np\nr=2",
                     "#alias eph3!\nr=3"):
            with self._get(port, "/-" + self._quote(code)):
                pass
        for name, expected in (("eph1", "1"), ("eph2", "2"), ("eph3", "3")):
            with self._get(port, "/-" + name) as response:
                self.assertEqual(response.read().decode(), expected, name)
        self.assertEqual(self._saved(), {})

    def test_quoted_alias_name_persisted_and_routed(self):
        # #alias "a b" 用 Python 字符串字面量表达带空格的别名名；
        # rpc.json 里 key 也是解码后的 "a b"，重启后 /a%20b 仍能路由。
        port = self._start()
        with self._get(port, "/-" + self._quote('#alias "a b"\nr=9')) as response:
            self.assertEqual(response.read().decode(), "9")
        self.assertEqual(self._saved(), {"a b": "r=9"})

        port2 = self._start()
        with self._get(port2, "/a%20b") as response:
            self.assertEqual(response.read().decode(), "9")

    def test_quoted_alias_name_arg_form_over_url(self):
        port = self._start()
        with self._get(port, "/-" + self._quote('#alias "a b"\nr = _alias_arg')):
            pass
        # =arg 形态里名字也必须加引号（裸写 a b=/x 仍是错误语法）
        with self._get(port, "/" + self._quote('"a b"=/x')) as response:
            self.assertEqual(response.read().decode(), "/x")

    def test_quoted_name_ephemeral_marker_keeps_name(self):
        # 标记在引号字面量之外：只剥标记，名字内部空格/内容原样保留
        port = self._start()
        with self._get(port, "/-" + self._quote('#alias "a b" !\nr=9')) as response:
            self.assertEqual(response.read().decode(), "9")
        self.assertEqual(self._saved(), {})
        with self._get(port, "/a%20b") as response:
            self.assertEqual(response.read().decode(), "9")

    def test_bang_inside_quoted_name_is_not_flag(self):
        # #alias "apk!" 里的 ! 属于名字，不触发“只内存”语义
        port = self._start()
        with self._get(port, "/-" + self._quote('#alias "apk!"\nr=3')) as response:
            self.assertEqual(response.read().decode(), "3")
        self.assertEqual(self._saved(), {"apk!": "r=3"})

    def test_strip_ephemeral_flag_is_quote_aware(self):
        executor = server_http.rpc_executor.PythonExecutor()
        cases = [
            ('#alias "a b" !\nr=1', '#alias "a b"\nr=1', True),
            ('#alias "a b" np\nr=1', '#alias "a b"\nr=1', True),
            ('#alias "a b"!\nr=1', '#alias "a b"\nr=1', True),
            ('#alias "a b"\nr=1', '#alias "a b"\nr=1', False),
            ('#alias "apk!"\nr=1', '#alias "apk!"\nr=1', False),
            ('#alias "temp"\nr=1', '#alias "temp"\nr=1', False),
            ('#alias apk!\nr=1', '#alias apk\nr=1', True),
        ]
        for code, expected, ephemeral in cases:
            with self.subTest(code=code):
                self.assertEqual(
                    server_http.strip_ephemeral_alias_flag(code, executor),
                    (expected, ephemeral))

    def test_disabled_persistence_does_not_touch_file(self):
        port = self._start(persist_aliases=False)
        with self._get(port, "/-" + self._quote("#alias memx\nr=5")) as response:
            self.assertEqual(response.read().decode(), "5")
        self.assertEqual(self._saved(), {})
        with self._get(port, "/-memx") as response:
            self.assertEqual(response.read().decode(), "5")

    def test_root_path_runs_root_alias_as_html(self):
        port = self._start()
        body = ("response.set_header('Content-Type','text/html; charset=utf-8');"
                "response.set_data('<h1>apk</h1>')")
        with self._get(port, "/-" + self._quote("#alias root\n" + body)):
            pass
        self.assertIn("root", self._saved())
        with self._get(port, "/") as response:
            self.assertEqual(response.headers["Content-Type"],
                             "text/html; charset=utf-8")
            self.assertEqual(response.read().decode(), "<h1>apk</h1>")

    def test_root_path_stays_403_in_keyed_mode(self):
        # keyed 模式别名必须写在 key 之后：/ 不因 root 别名存在而放行
        port = self._start(alias_url="keyed")
        with self._get(port, "/-" + self._quote("#alias root\nr='x'")):
            pass
        with self.assertRaises(urllib.error.HTTPError) as context:
            self._get(port, "/")
        self.assertEqual(context.exception.code, 403)


if __name__ == "__main__":
    unittest.main()
