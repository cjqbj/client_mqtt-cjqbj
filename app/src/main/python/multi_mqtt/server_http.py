#!/usr/bin/env python3
import sys, io, urllib.parse, threading, traceback, struct, base64, hashlib, socket, logging, asyncio, textwrap, inspect, ast
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

import rpc_executor

_stdout_lock = threading.Lock()


def get_bmp_bytes(rgb=None, size=(16, 16)):
    if not rgb:
        rgb = (255, 0, 0)
    if isinstance(size, int):
        size = [size, size]
    width, height = size
    r, g, b = (max(0, min(255, c)) for c in rgb)
    bgr_color = bytes([b, g, r])
    bytes_per_pixel = 3
    bytes_per_row = (width * bytes_per_pixel + 3) // 4 * 4
    pixel_data_size = bytes_per_row * height
    file_size = 14 + 40 + pixel_data_size
    bmp_header = struct.pack('<2sIII', b'BM', file_size, 0, 54)
    bmp_info = struct.pack('<IIIHHIIIIII', 40, width, height, 1, 24, 0, pixel_data_size, 3780, 3780, 0, 0)
    pixels = b''
    for _ in range(height):
        row = bgr_color * width
        padding = b'\x00' * (bytes_per_row - len(row))
        pixels += row + padding
    return bmp_header + bmp_info + pixels


def pretty_format(obj, width=120):
    if isinstance(obj, str):
        return obj
    try:
        from IPython.lib.pretty import pretty
        return pretty(obj, max_width=width)
    except ImportError:
        try:
            from pprint import pformat
            return pformat(obj, width=width)
        except Exception:
            return repr(obj)


def stime():
    import time
    ft = time.time()
    return time.strftime('%Y-%m-%d__%H.%M.%S', time.localtime(ft)) + '__.' + f"{ft:.3f}".split('.')[1]


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class WebSocket:
    """Small text WebSocket connection used by the RPC server."""

    def __init__(self, handler):
        self.handler = handler
        self.socket = handler.connection
        self.send_lock = threading.Lock()
        self.closed = False

    def send(self, message):
        if isinstance(message, str):
            message = message.encode('utf-8')
        if len(message) >= 126:
            if len(message) < 65536:
                header = struct.pack('!BBH', 0x81, 126, len(message))
            else:
                header = struct.pack('!BBQ', 0x81, 127, len(message))
        else:
            header = bytes((0x81, len(message)))
        with self.send_lock:
            self.socket.sendall(header + message)

    def receive(self):
        """Return the next text message, or None when the peer disconnects."""
        fragments = []
        while True:
            header = self._read_exact(2)
            if not header:
                return None
            first, second = header
            final = bool(first & 0x80)
            opcode = first & 0x0f
            masked = bool(second & 0x80)
            length = second & 0x7f
            if length == 126:
                length = struct.unpack('!H', self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack('!Q', self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else b''
            payload = bytearray(self._read_exact(length))
            if masked:
                for index in range(length):
                    payload[index] ^= mask[index % 4]
            if opcode == 0x8:
                self.close()
                return None
            if opcode == 0x9:
                self._send_control(0xA, payload)
                continue
            if opcode in (0x1, 0x0):
                fragments.append(bytes(payload))
                if final:
                    return b''.join(fragments).decode('utf-8')
                continue
            if opcode == 0xA:
                continue
            raise ValueError(f'unsupported WebSocket opcode: {opcode}')

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                with self.send_lock:
                    self.socket.sendall(b'\x88\x00')
            except OSError:
                pass

    def _send_control(self, opcode, payload=b''):
        with self.send_lock:
            self.socket.sendall(bytes((0x80 | opcode, len(payload))) + payload)

    def _read_exact(self, size):
        data = b''
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk:
                return b''
            data += chunk
        return data


class RPCRequestHandler(BaseHTTPRequestHandler):
    key = ''
    # 复用一个持久化的执行器实例；请求级上下文通过 execute(globals=...) 注入。
    executor = None
    favicon_bytes = None
    websocket_handler = None
    websocket_handlers = {}
    websocket_path = '/ws'
    redirect_root = None
    main_loop = None

    def log_message(self, format, *args):
        print(f"[RPC] {stime()[12:]}  {self.client_address[0]}:{self.client_address[1]} {format % args}")

    def do_GET(self):
        websocket_path = self.path.split('?', 1)[0]
        websocket_handler = self.websocket_handlers.get(websocket_path, self.websocket_handler)
        if (websocket_handler and (websocket_path == self.websocket_path or websocket_path in self.websocket_handlers)
                and self.headers.get('Upgrade', '').lower() == 'websocket'):
            self.handle_websocket(websocket_handler)
            return
        if self.path == '/' and self.redirect_root:
            self.send_response(302)
            self.send_header('Location', self.redirect_root)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        if self.path == '/favicon.ico' and self.favicon_bytes:
            self.send_response(200)
            self.send_header('Content-Type', 'image/x-icon')
            self.send_header('Cache-Control', 'max-age=86400')
            self.send_header('Content-Length', str(len(self.favicon_bytes)))
            self.end_headers()
            self.wfile.write(self.favicon_bytes)
            return
        self.handle_rpc()

    def handle_websocket(self, websocket_handler):
        self.protocol_version = 'HTTP/1.1'
        key = self.headers.get('Sec-WebSocket-Key')
        if not key or self.headers.get('Sec-WebSocket-Version') != '13':
            self.send_error(400, 'WebSocket version 13 and key are required')
            return
        accept = base64.b64encode(hashlib.sha1(
            (key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode('ascii')
        ).digest()).decode('ascii')
        self.send_response(101, 'Switching Protocols')
        self.send_header('Upgrade', 'websocket')
        self.send_header('Connection', 'Upgrade')
        self.send_header('Sec-WebSocket-Accept', accept)
        self.end_headers()
        websocket = WebSocket(self)
        try:
            websocket_handler(websocket, self)
        except (ConnectionError, BrokenPipeError, socket.error):
            pass
        except Exception:
            traceback.print_exc()
        finally:
            websocket.close()

    def do_POST(self):
        self.handle_rpc()

    def _execute_coroutine(self, coro, exec_globals):
        target_loop = self.main_loop
        if not target_loop or not target_loop.is_running():
            for value in exec_globals.values():
                if hasattr(value, '_loop') and isinstance(value._loop, asyncio.AbstractEventLoop) and value._loop.is_running():
                    target_loop = value._loop
                    break
                if isinstance(value, asyncio.AbstractEventLoop) and value.is_running():
                    target_loop = value
                    break
        if target_loop and target_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, target_loop)
            return future.result()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def handle_rpc(self):
        try:
            raw_path = self.path
            if self.key:
                prefix = self.key + '/'
                if not raw_path.startswith(prefix):
                    self.send_error(403, "Forbidden")
                    return
                code_str = raw_path[len(prefix):]
            else:
                if raw_path.startswith('/'):
                    raw_path = raw_path[1:]
                code_str = raw_path
            if not code_str:
                self.send_error(400, "No code")
                return
            # 独立 HTTP 服务器下 self.path 仍是原始 percent-encoded 请求行，
            # 需要 unquote；WSGI 下 PATH_INFO 已被网关解码（shim 会设置
            # path_already_decoded），不能再解（否则中文变 Latin-1 乱码）。
            if getattr(self, "path_already_decoded", False):
                code = code_str
            else:
                code = urllib.parse.unquote(code_str)

            class ResponseWrapper:
                """RPC 代码里的 response/p 对象。

                set_data：一次性返回（执行结束后再写 body，原行为）。
                write：流式返回——首次调用立即提交状态头，之后每个分块
                直接写到 wfile，客户端可以边收边播放，大文本无需等待。
                """

                def __init__(self, handler):
                    self.handler = handler
                    self.status = 200
                    self.headers = {}
                    self.data = None
                    self._streamed = False

                def set_data(self, data):
                    self.data = data

                def set_status(self, code):
                    self.status = code

                def set_header(self, key, value):
                    self.headers[key] = value

                def write(self, data):
                    if not self._streamed:
                        self.handler.send_response(self.status)
                        for key, value in self.headers.items():
                            self.handler.send_header(key, value)
                        if 'Content-Type' not in self.headers:
                            self.handler.send_header(
                                'Content-Type', 'application/octet-stream')
                        # 告知浏览器本资源支持 Range：播放中可直接拖动跳转。
                        self.handler.send_header('Accept-Ranges', 'bytes')
                        # 流式响应长度未知，不发送 Content-Length
                        self.handler.end_headers()
                        self._streamed = True
                    if data:
                        self.handler.wfile.write(data)

            response = ResponseWrapper(self)

            # 请求级上下文变量：执行期间临时注入，执行完自动还原，
            # 不会污染执行器的持久命名空间。
            context = {
                'request': self,
                'q': self,
                'response': response,
                'p': response,
            }

            executor = self.executor
            try:
                with _stdout_lock:
                    old_stdout = sys.stdout
                    sys.stdout = io.StringIO()
                    try:
                        execution = executor.execute(code, globals=context)
                        output = execution['stdout']
                        if not execution['ok']:
                            raise RuntimeError(execution['error'])
                    finally:
                        sys.stdout = old_stdout

                # 流式响应：状态头与分块都已直接写出，跳过收尾逻辑。
                if getattr(response, '_streamed', False):
                    return

                namespace = executor.globals_dict
                if response.data is not None:
                    result_obj = response.data
                elif 'r' in namespace:
                    result_obj = namespace['r']
                elif output:
                    result_obj = output
                else:
                    result_obj = f"no 'r' variable, locals keys: {list(namespace.keys())}"

                result_str = pretty_format(result_obj)
                self.send_response(response.status)
                for key, value in response.headers.items():
                    self.send_header(key, value)
            except Exception:
                if getattr(response, '_streamed', False):
                    # 头已发出，无法改状态码；记录后结束分块流。
                    logging.getLogger("error").exception(
                        "RPC stream failed mid-flight client=%s path=%s",
                        self.client_address, self.path,
                    )
                    return
                result_str = traceback.format_exc()
                logging.getLogger("error").exception(
                    "RPC execution failed client=%s path=%s code=%s",
                    self.client_address, self.path, code[:500],
                )
                self.send_response(500)

            if 'Content-Type' not in response.headers:
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.end_headers()
            if response.data is not None:
                body = response.data
                if isinstance(body, str):
                    body = body.encode('utf-8')
                try:
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    logging.getLogger("app").info("RPC client disconnected before response path=%s", self.path)
            else:
                try:
                    self.wfile.write(result_str.encode('utf-8'))
                except (BrokenPipeError, ConnectionResetError):
                    logging.getLogger("app").info("RPC client disconnected before response path=%s", self.path)
        except Exception as error:
            if not isinstance(error, (BrokenPipeError, ConnectionResetError)):
                self.send_error(500, str(error))


def start_rpc_server(port=1133, key='', ip='0.0.0.0', globals=None, locals=None, daemon=True,
                     favicon_rgb=None, favicon_size=16, websocket_handler=None,
                     websocket_path='/ws', redirect_root=None, websocket_handlers=None,
                     main_loop=None, listen=True):
    if not key:
        key = ''
    RPCRequestHandler.key = key
    RPCRequestHandler.websocket_handler = websocket_handler
    RPCRequestHandler.websocket_path = websocket_path
    RPCRequestHandler.websocket_handlers = websocket_handlers or {}
    RPCRequestHandler.redirect_root = redirect_root
    RPCRequestHandler.main_loop = main_loop

    # 构造持久命名空间：合并调用方传入的 globals / locals。
    # 该命名空间只在服务器启动时构造一次，之后所有请求共享。
    persistent_ns = {}
    if globals:
        persistent_ns.update(globals)
    if locals:
        persistent_ns.update(locals)
    persistent_ns['__name__'] = '__rpc_exec__'

    # 只创建一个 PythonExecutor，请求级上下文通过 execute(globals=...) 注入。
    RPCRequestHandler.executor = rpc_executor.PythonExecutor(
        globals=persistent_ns,
        main_loop=main_loop,
    )

    if favicon_rgb is None:
        favicon_rgb = (port // 100, port % 100, 0)
    RPCRequestHandler.favicon_bytes = get_bmp_bytes(rgb=favicon_rgb, size=favicon_size)

    # listen=False：只初始化 RPCRequestHandler 的类级状态，不做 socket 绑定。
    # 供 WSGI / ASGI / 测试等外部容器复用全部 RPC 逻辑。
    if not listen:
        return None

    server = ThreadedHTTPServer((ip, port), RPCRequestHandler)
    thread = threading.Thread(target=server.serve_forever, name='RPC_Server', daemon=daemon)
    thread.start()
    server.thread = thread
    print(f"[RPC] {stime()} server at http://{ip}:{port}/{key}")
    return server


def qpsu(url="http://192.168.1.100/D%3A/test/qpsu.zip", write_to=''):
    import urllib.request, zipfile, io, sys, importlib.abc, importlib.machinery, importlib

    # 下载 zip 数据
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = resp.read()

    # 可选：保存到本地
    if write_to:
        with open(write_to, 'wb') as f:
            f.write(data)

    z = zipfile.ZipFile(io.BytesIO(data))
    names = set(z.namelist())

    class ZipImporter(importlib.abc.PathEntryFinder):
        def __init__(self, zf):
            self.zf = zf
            self.names = set(zf.namelist())

        def find_spec(self, fullname, path=None, target=None):
            pkg_path = fullname.replace('.', '/') + '/__init__.py'
            if pkg_path in self.names:
                return importlib.machinery.ModuleSpec(fullname, self, is_package=True)

            mod_path = fullname.replace('.', '/') + '.py'
            if mod_path in self.names:
                return importlib.machinery.ModuleSpec(fullname, self)

            return None

        def create_module(self, spec):
            return None

        def exec_module(self, module):
            fullname = module.__name__

            pkg_path = fullname.replace('.', '/') + '/__init__.py'
            if pkg_path in self.names:
                source = self.zf.read(pkg_path).decode('utf-8')
                module.__file__ = f"<zip://{pkg_path}>"
                module.__path__ = [f"<zip://{pkg_path}>"]
                module.__loader__ = self
                module.__package__ = fullname
                code = compile(source, module.__file__, 'exec')
                exec(code, module.__dict__)
                return

            mod_path = fullname.replace('.', '/') + '.py'
            source = self.zf.read(mod_path).decode('utf-8')
            module.__file__ = f"<zip://{mod_path}>"
            module.__loader__ = self
            module.__package__ = fullname.rpartition('.')[0]
            code = compile(source, module.__file__, 'exec')
            exec(code, module.__dict__)

    # 清理旧的 qgb 模块缓存，确保从新 zip 重新导入
    for name in list(sys.modules):
        if name == 'qgb' or name.startswith('qgb.'):
            del sys.modules[name]

    importer = ZipImporter(z)
    sys.meta_path.insert(0, importer)
    importlib.invalidate_caches()

    from qgb import py, U, T, N, F
    return py, U, T, N, F
    
if __name__ == '__main__':
    start_rpc_server(port=1144, key='', globals=globals(), locals=locals())
    input("Press Ctrl+C or anykey to stop\n")