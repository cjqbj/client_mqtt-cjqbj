#!/usr/bin/env python3
import sys, io, re, urllib.parse, threading, traceback, struct, base64, hashlib, socket, logging, asyncio, textwrap, inspect, ast, hmac, weakref
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

import rpc_executor
from rpc_executor import PythonExecutor, format_result ,stime


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


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    # 每服务器独立的 RPC 配置快照，由 start_rpc_server 在绑定后写入；
    # handler 经 _conf(self, name) 读取。None 表示走类属性回退。
    rpc_config = None
    # Windows 的 SO_REUSEADDR 与 Linux 语义不同：Linux 只允许复用 TIME_WAIT
    # 状态的端口，Windows 却允许两个都设了该选项的 socket 同时 bind 同一
    # 监听端口（已实测复现：第二次 bind 静默成功，netstat 出现两个
    # LISTENING）。旧实例没退干净时新实例照常"启动成功"，但新连接可能被
    # 投递给旧 listener 的内核 backlog——客户端表现为 TCP 连上、请求发出
    # 后永远无响应。Windows 改用 SO_EXCLUSIVEADDRUSE 占住端口：端口被占
    # 时启动立即报 WinError 10013/10048，快速失败而不是静默双绑挂死。
    if sys.platform == 'win32':
        allow_reuse_address = False  # 与 SO_EXCLUSIVEADDRUSE 互斥，必须关闭
    else:
        allow_reuse_address = True   # POSIX 保持原语义：仅复用 TIME_WAIT

    def server_bind(self):
        if sys.platform == 'win32':
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


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


def _split_code_strings(code):
    """按字符串字面量边界切段，返回 [(片段, 是否字符串), ...]。

    裸路径救援只允许改写字符串外的部分，因此先把 'xxx' / "xxx" / 三引号
    （含转义）整段切出来跳过，避免把字符串内容里的路径再包一层引号。
    """
    segments = []
    buf = []
    i, n = 0, len(code)
    while i < n:
        ch = code[i]
        if ch in '\'"':
            triple = code[i:i + 3]
            if triple in ('"""', "'''"):
                end = code.find(triple, i + 3)
                end = n if end == -1 else end + 3
            else:
                j = i + 1
                while j < n and code[j] != ch:
                    j += 2 if code[j] == '\\' else 1
                end = min(j + 1, n)
            segments.append((''.join(buf), False))
            buf = []
            segments.append((code[i:end], True))
            i = end
        else:
            buf.append(ch)
            i += 1
    segments.append((''.join(buf), False))
    return segments


# 表达式起始位置：前一个非空白字符是这些（或整段开头）时，token 才可能被救援。
# mid 表达式的 a/b 是合法除法，轮不到救援；能走到救援的前提是整段已 SyntaxError。
_EXPR_START_CTX = '=(,[{;'
# POSIX 绝对路径 /a/b.txt 与盘符路径 C:/a/b.txt、C:\\a\\b.txt（段内无空白）
_PATH_TOKEN_RE = re.compile(r'(?:[A-Za-z]:)?(?:[/\\][\w.\-]+)+[/\\]?')


def _rescue_path_syntax(code):
    """给裸路径 token 补引号，如 ``file=/tmp/txt`` → ``file='/tmp/txt'``。

    URL 即代码场景里手输路径常不带引号，而 /tmp/txt 在 Python 里是语法错误。
    仅当整段代码已 SyntaxError 时由 handle_rpc 调用本函数重试：跳过字符串
    字面量内部；只处理位于表达式起始位置（见 _EXPR_START_CTX）的路径 token，
    避免误伤除法与 http:// 协议头；含反斜杠的 Windows 路径转义后按普通串包。
    无法安全改写时原样返回，由调用方保留原始报错。
    """
    out = []
    changed = False
    for segment, is_string in _split_code_strings(code):
        if is_string or not segment:
            out.append(segment)
            continue
        pos = 0
        for m in _PATH_TOKEN_RE.finditer(segment):
            token = m.group(0)
            context = ''.join(out) + segment[:m.start()]
            prev = next((c for c in reversed(context) if not c.isspace()), '')
            if prev and prev not in _EXPR_START_CTX:
                continue
            literal = token.replace('\\', '\\\\')
            out.append(segment[pos:m.start()])
            out.append("'" + literal + "'")
            pos = m.end()
            changed = True
        out.append(segment[pos:])
    return ''.join(out) if changed else code


# 同进程内活着的 RPC 服务器（弱引用）：set_basic_auth 等运行期开关需要
# 同步推进每台服务器自己的 rpc_config，只改类属性碰不到它们。
# setdefault 而非直接赋值：importlib.reload 复用同一模块 __dict__ 重新
# 执行顶层代码，直接赋值会把名字重绑到全新空集，reload 前启动的服务器
# 全部失联（新代码 set_basic_auth 广播不到它们）；setdefault 在 reload
# 后取回旧 WeakSet，配合挂在服务器实例上的 rpc_config，跨 reload 的
# 动态开关依然完整生效。
_rpc_servers = globals().setdefault('_rpc_servers', weakref.WeakSet())


def _conf(handler, name):
    """按优先级取 RPC 配置：所属 server 的 rpc_config → handler 属性 → 类属性。

    配置快照挂在 ThreadedHTTPServer 实例上后，同进程第二次 start_rpc_server
    不再抢走第一个服务器的 key/executor/favicon 等状态。WSGI 垫片没有
    server 对象（listen=False 路径只写类属性），自然落到后面的回退级。
    """
    config = getattr(getattr(handler, 'server', None), 'rpc_config', None)
    if isinstance(config, dict) and name in config:
        return config[name]
    if hasattr(handler, name):
        return getattr(handler, name)
    return getattr(RPCRequestHandler, name)


class RPCRequestHandler(BaseHTTPRequestHandler):
    key = ''
    # HTTP Basic 认证凭据 'user:password'；空串不启用认证
    basic_auth = ''
    # 复用一个持久化的执行器实例；请求级上下文通过 execute(globals=...) 注入。
    executor = None
    favicon_bytes = None
    websocket_handler = None
    websocket_handlers = {}
    websocket_path = '/ws'
    redirect_root = None
    main_loop = None
    # 可选非阻塞日志出口 callable(str)；pty_client_mqtt 把它接到异步写
    # 线程上，避免控制台堵塞时 BaseHTTPRequestHandler 的 print 拖死请求线程
    log_sink = None

    @classmethod
    def set_basic_auth(cls, user='', pw=''):
        """传入 user/pw 即开启 HTTP Basic 认证；都为空则关闭。

        供 RPC 动态调用（如 /RPCRequestHandler.set_basic_auth('admin','123')），
        也可在代码里直接调用。除类属性外还要推进每台活服务器自己的
        rpc_config——native 请求优先读服务器快照，只改类属性碰不到它们。
        """
        value = f"{user}:{pw}" if (user or pw) else ''
        cls.basic_auth = value
        for server in list(_rpc_servers):
            config = getattr(server, 'rpc_config', None)
            if isinstance(config, dict):
                config['basic_auth'] = value
        return value or 'basic auth disabled'

    def log_message(self, format, *args):
        try:
            text = format % args
        except Exception:
            text = str(format)
        # /mcp 的一次工具调用在 HTTP 层会伴随 initialize/notifications/tool
        # call 多条 POST，2xx 访问行既无工具名也无参数，只会把日志冲散；
        # 真正的调用轨迹由 pty 侧 [MCP] <id> ┌/└ 首尾配对承担。这里只在
        # 4xx/5xx 保留访问行，便于排查协议层失败。
        if '/mcp' in text:
            try:
                code = int(text.rsplit('"', 1)[-1].split()[0])
            except Exception:
                code = 0
            if 200 <= code < 400:
                return
        line = (f"[RPC] {stime()[12:]}  {self.client_address[0]}:"
                f"{self.client_address[1]} {text}")
        sink = _conf(self, 'log_sink')
        if sink is not None:
            try:
                sink(line)
                return
            except Exception:
                pass  # sink 挂了退回 print，不能让日志本身打挂请求
        print(line)

    def check_basic_auth(self):
        """basic_auth 非空时校验 Authorization 头；未通过则回 401 挑战。

        浏览器收到 401 + WWW-Authenticate 会弹登录框，且同源的 WebSocket
        升级请求会自动带上凭据，因此校验统一放在 do_GET/do_POST 入口，
        RPC、重定向、favicon、WebSocket 全部被同一层拦截。
        """
        basic_auth = _conf(self, 'basic_auth')
        if not basic_auth:
            return True
        expected = b'Basic ' + base64.b64encode(basic_auth.encode('utf-8'))
        # 头字段由 http.server 按 latin-1 解码，编码回去做恒定时间比较，
        # 避免逐字节时序侧信道
        authorization = self.headers.get('Authorization', '').encode('latin-1', 'replace')
        if hmac.compare_digest(authorization, expected):
            return True
        body = b'401 Unauthorized: basic auth required'
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="RPC"')
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        return False

    def do_GET(self):
        if not self.check_basic_auth():
            return
        websocket_path = self.path.split('?', 1)[0]
        websocket_handlers = _conf(self, 'websocket_handlers')
        websocket_handler = websocket_handlers.get(websocket_path, _conf(self, 'websocket_handler'))
        if (websocket_handler and (websocket_path == _conf(self, 'websocket_path') or websocket_path in websocket_handlers)
                and self.headers.get('Upgrade', '').lower() == 'websocket'):
            self.handle_websocket(websocket_handler)
            return
        redirect_root = _conf(self, 'redirect_root')
        if self.path == '/' and redirect_root:
            self.send_response(302)
            self.send_header('Location', redirect_root)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        favicon_bytes = _conf(self, 'favicon_bytes')
        if self.path == '/favicon.ico' and favicon_bytes:
            self.send_response(200)
            self.send_header('Content-Type', 'image/x-icon')
            self.send_header('Cache-Control', 'max-age=86400')
            self.send_header('Content-Length', str(len(favicon_bytes)))
            self.end_headers()
            self.wfile.write(favicon_bytes)
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
            # 升级后连接已脱离 HTTP keep-alive 循环：主动关底层 socket，
            # 否则要等下一轮解析失败才回收（浏览器日志台频繁开关时堆积 fd）。
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.connection.close()
            except OSError:
                pass

    def do_POST(self):
        if not self.check_basic_auth():
            return
        self.handle_rpc()

    def _execute_coroutine(self, coro, exec_globals):
        target_loop = _conf(self, 'main_loop')
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
        # 认证门必须放在 handle_rpc 内部而不只是 do_GET/do_POST：WSGI 垫片、
        # Range 捕获等路径直接调用本方法，不经过 do_*，放在外面会被绕过。
        if not self.check_basic_auth():
            return
        try:
            raw_path = self.path
            key = _conf(self, 'key')
            if key:
                prefix = key + '/'
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

            # 裸路径救援：URL 里 file=/tmp/txt 不带引号是 SyntaxError，
            # 补引号后能通过语法检查才采用，否则保留原始报错。
            try:
                ast.parse(code)
            except SyntaxError:
                rescued = _rescue_path_syntax(code)
                if rescued is not code and rescued != code:
                    try:
                        ast.parse(rescued)
                        code = rescued
                    except SyntaxError:
                        pass

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

            executor = _conf(self, 'executor')
            try:
                # stdout 捕获由 PythonExecutor 内部的 _redirect_stdout 完成
                # （execution['stdout'] 即输出），外层不再包 _stdout_lock +
                # 二次重定向——那会把整个执行串成全局临界区，一条慢请求
                # （长任务/死循环）就冻结全部 HTTP RPC（含日志台/诊断口）。
                # 执行的串行化与超时守卫由执行器自身的锁与 timeout 保证。
                execution = executor.execute(code, globals=context)
                output = execution['stdout']
                if not execution['ok']:
                    raise RuntimeError(execution['error'])

                # 流式响应：状态头与分块都已直接写出，跳过收尾逻辑。
                if getattr(response, '_streamed', False):
                    return

                namespace = executor.globals_dict
                if response.data is not None:
                    result_obj = response.data
                elif execution.get('r') is not None:
                    # 尾表达式求值结果：与 MQTT 通道对齐，让 1+1 这类代码
                    # 直接返回 2，不再要求显式赋值 r 或 print
                    result_obj = execution['r']
                elif 'r' in namespace:
                    result_obj = namespace['r']
                elif output:
                    result_obj = output
                else:
                    result_obj = f"no 'r' variable, locals keys: {list(namespace.keys())}"

                result_str = format_result(result_obj)
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
                     main_loop=None, listen=True, log_sink=None, basic_auth=''):
    if not key:
        key = ''
    # basic_auth 接受 'user:password' 或 (user, password)，统一成前者；空则不启用认证
    if isinstance(basic_auth, (tuple, list)):
        basic_auth = ':'.join(str(item) for item in basic_auth)

    # 持久命名空间：默认零拷贝，直接持有调用方传入的 globals 字典引用，
    # 不做任何浅拷贝。这样服务器启动之后调用方才赋值的名字（例如 MQTT
    # 服务端实例 gms）在后续 HTTP RPC 中实时可见，与 MQTT 通道行为一致。
    # 模块级调用时 globals() 与 locals() 本就是同一个 dict；只有二者都
    # 未提供时才让执行器自建空字典。若确实需要与调用方字典隔离的快照，
    # 直接改用 PythonExecutor(..., copy_globals=True)。
    if isinstance(globals, dict):
        persistent_ns = globals
    elif isinstance(locals, dict):
        persistent_ns = locals
    else:
        persistent_ns = None

    if favicon_rgb is None:
        favicon_rgb = (port // 100, port % 100, 0)

    # 配置快照：每台服务器一份独立 dict，含各自的 executor（REPL 命名
    # 空间与 alias 注册随之隔离）。旧实现全部写 RPCRequestHandler 类属性，
    # 同进程第二次 start_rpc_server 会把第一个服务器的 key / executor /
    # favicon / basic_auth 等整体抢走，且旧命名空间与别名静默蒸发。
    config = {
        'key': key,
        'basic_auth': basic_auth or '',
        # 每服务器一个 PythonExecutor；不在这里强写 __name__：执行器内部
        # 用 setdefault，模块字典自带 __name__ 不会被覆盖；自建空字典时
        # 由执行器补成 __rpc_exec__。
        'executor': PythonExecutor(globals=persistent_ns, main_loop=main_loop),
        'favicon_bytes': get_bmp_bytes(rgb=favicon_rgb, size=favicon_size),
        'websocket_handler': websocket_handler,
        'websocket_path': websocket_path,
        'redirect_root': redirect_root,
        'main_loop': main_loop,
        # dict 里存原函数即可；只有挂到类属性才需要 staticmethod 包一层
        # （描述符协议会把普通函数变成 bound method，sink(line) 必抛
        # TypeError 被静默吞掉，每笔请求退回 print 直写终端）。
        'log_sink': log_sink,
    }
    for name, value in config.items():
        if name == 'log_sink' and callable(value):
            value = staticmethod(value)
        setattr(RPCRequestHandler, name, value)

    # websocket_handlers 是运行期共享注册表（realtime_editor 等直接在类
    # dict 上动态增删）：只合并、不替换对象。旧实现每次整体换新 dict，
    # 把已注册的处理器冲掉，realtime_editor.py:341 只能靠"必须在
    # start_rpc_server 之后注册"的顺序约定绕坑。
    if websocket_handlers:
        RPCRequestHandler.websocket_handlers.update(websocket_handlers)

    # listen=False：只初始化 RPCRequestHandler 的类级状态，不做 socket 绑定。
    # 供 WSGI / ASGI / 测试等外部容器复用全部 RPC 逻辑（无 server 对象，
    # 请求期经 _conf 回退读类属性）。
    if not listen:
        return None

    server = ThreadedHTTPServer((ip, port), RPCRequestHandler)
    server.rpc_config = config
    _rpc_servers.add(server)
    thread = threading.Thread(target=server.serve_forever, name='RPC_Server', daemon=daemon)
    thread.start()
    server.thread = thread
    boot_line = f"[RPC] {stime()} server at http://{ip}:{port}/{key}\n"
    if log_sink is not None:
        try:
            log_sink(boot_line)
        except Exception:
            print(boot_line)
    else:
        print(boot_line)
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