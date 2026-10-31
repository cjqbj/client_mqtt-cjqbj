#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""server_http_asgi.py — 把 server_http.py 的 RPC 处理器包装成原生 ASGI 应用（HTTP + WebSocket）。

与 server_http_wsgi.py 的差别：
    * 不再依赖 WSGI / a2wsgi：同一个 ASGI 应用既处理普通 HTTP RPC，也处理
      WebSocket 升级，部署链路里只有一条 RPC 通道；
    * WebSocket 不需要再往宿主应用（FastAPI / gradio.Server）注册额外路由，
      直接按 server_http.RPCRequestHandler.websocket_handlers 的路径表分发，
      realtime_editor 等运行期注册的处理器即刻可用（无需重启、无需 install）；
    * 宿主（如 app.py）可用 set_executor() 把执行器换成本模块 globals，HTTP RPC
      与 MQTT 通道共用同一份持久命名空间。

RPC 语义（key 门 / 别名 URL / 根别名 / basic auth / Range 切片 / 流式 write）
全部仍由 server_http.RPCRequestHandler.handle_rpc 负责，本文件只做传输层适配：
    请求 → _ASGIHandlerShim（冒充 BaseHTTPRequestHandler）
         → handle_rpc 在守护线程里执行
         → wfile 的每块字节经 asyncio 队列逐个 send 给客户端（真流式）。

部署（HuggingFace Space / 任意 uvicorn 应用）：
    import server_http_asgi
    server.mount('/rpc', server_http_asgi.application)        # 可选，兼容旧前缀
    server.add_middleware(MyRPCMiddleware,
                          rpc_app=server_http_asgi.application)

URL 形态与环境变量与 server_http_wsgi 一致（RPC_KEY / RPC_ALIAS_URL /
RPC_ROOT_ALIAS / RPC_PERSIST_ALIASES）：
    https://<host>/-<code>             例：/-r=1+1
    https://<host>/-/<code>            兼容旧写法
    https://<host>/<别名>              已注册别名免前缀（alias_url='raw'）
    https://<host>/                    执行 root 别名（RPC_ROOT_ALIAS，默认 root）
    wss://<host>/ws/realtime_editor/<md5前12位>   编辑器等 ws 处理器

单独跑（uvicorn server_http_asgi:application）时自带 lifespan 应答，可直接调试。
"""

import asyncio
import io
import logging
import os
import queue
import re
import traceback
import urllib.parse

import server_http
from server_http import RPCRequestHandler

logger = logging.getLogger(__name__)

# 本 ASGI 应用的部署配置（默认值、覆盖方式与 server_http_wsgi 保持一致）。
RPC_KEY = os.environ.get('RPC_KEY', '-')
ALIAS_URL = os.environ.get('RPC_ALIAS_URL', 'raw')
# alias_url 归一化成 (路由模式 'dash'/'raw', 是否 JSON 解码 query 值)；
# 环境变量写错只降级为默认 dash 并 WARNING，不让 ASGI 应用 import 失败。
try:
    _ALIAS_URL_MODE, _ALIAS_JSON = server_http.normalize_alias_url(ALIAS_URL)
except ValueError as _alias_url_error:
    logger.warning('RPC_ALIAS_URL=%r 无效，降级为 dash: %s',
                   ALIAS_URL, _alias_url_error)
    _ALIAS_URL_MODE, _ALIAS_JSON = server_http.ALIAS_URL_DASH, False
RPC_ROOT_ALIAS = os.environ.get('RPC_ROOT_ALIAS', 'root')
RPC_PERSIST_ALIASES = os.environ.get(
    'RPC_PERSIST_ALIASES', '1').strip().lower() not in ('0', 'false', 'no', 'off', '')

# 宿主应用自己的执行器（app.py 调 set_executor 绑定）；None 时回退到类属性。
# setdefault 防 importlib.reload 把宿主绑定的执行器重置成 None。
_APP_EXECUTOR = globals().setdefault('_APP_EXECUTOR', None)

_RANGE_SPEC_RE = re.compile(r'^bytes=(\d*)-(\d*)$')


def set_executor(executor):
    """把 RPC 执行器换成宿主应用自己的命名空间（例如 app.py 的 globals）。

    与 server_http_wsgi 不同：那边在 import 时就固化执行器，宿主之后再赋值
    RPCRequestHandler.executor 不生效；这里给一个显式入口，HTTP RPC 才能看到
    app.py 里的 server / tts / gms 等名字。

    同时把模块初始化时从 rpc.json 恢复的别名表带过去：换命名空间后如果丢掉
    aliases，线上已注册的免前缀别名（/<name>）会全部 404/403。
    """
    global _APP_EXECUTOR
    source = RPCRequestHandler.executor
    if source is not None and source is not executor:
        aliases = getattr(source, 'aliases', None)
        if isinstance(aliases, dict) and not getattr(executor, 'aliases', None):
            executor.aliases = {name: dict(info) for name, info in aliases.items()}
    _APP_EXECUTOR = executor
    # 兼容直接读类属性的代码；shim 优先用 _APP_EXECUTOR，二者指向同一对象。
    RPCRequestHandler.executor = executor
    return executor


# ---------------------------------------------------------------------------
# BaseHTTPRequestHandler 的最小模拟（ASGI 版）
# ---------------------------------------------------------------------------

class _ASGIWFile:
    """冒充 handler.wfile。

    end_headers 之前的写入先缓冲；之后的写入交给 state['write']，由
    ASGI 消费者协程立刻 send 出去——大文本/TTS 边生成边传，不需要攒齐。
    """

    __slots__ = ('_state',)

    def __init__(self, state):
        self._state = state

    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        if self._state['committed']:
            if data:
                self._state['write'](data)
        else:
            self._state['pre'].write(data)

    def flush(self):
        pass

    def getvalue(self):
        return self._state['pre'].getvalue()


class _CIDict(dict):
    """大小写不敏感的请求头字典（语义对齐 email.message.Message.get）。

    ASGI 的 scope['headers'] 键是小写 bytes，check_basic_auth / RPC 代码用
    'Authorization'、'Range' 之类的写法查询，普通 dict 会漏。
    """

    def get(self, key, default=None):
        for k in self:
            if k.lower() == str(key).lower():
                return dict.__getitem__(self, k)
        return default

    def __getitem__(self, key):
        for k in self:
            if k.lower() == str(key).lower():
                return dict.__getitem__(self, k)
        raise KeyError(key)

    def __contains__(self, key):
        return any(k.lower() == str(key).lower() for k in self)


class _ASGIHandlerShim:
    """让 RPCRequestHandler.handle_rpc(self) 在 ASGI 下原样运行的薄垫片。

    只提供 handle_rpc 实际依赖的属性 / 方法；ASGI scope 与 WSGI environ 的
    差异（query_string 是原始 bytes、path 已解码、请求体从 receive 拉）都在
    这里吸收掉。
    """

    def __init__(self, scope, body, state):
        self._state = state
        self.scope = scope
        self.command = (scope.get('method') or 'GET').upper()

        client = scope.get('client') or ('', 0)
        self.client_address = (client[0], client[1] if len(client) > 1 else 0)

        # ASGI 的 path 已是 percent-decode 后的 UTF-8 文本（与 WSGI PATH_INFO
        # 还原后的形态一致）；query_string 仍是原始字节，解成文本后按 UTF-8
        # unquote，拼成与 BaseHTTPRequestHandler.path 相同的形态。
        path = scope.get('path') or '/'
        raw_query = scope.get('query_string') or b''
        if isinstance(raw_query, (bytes, bytearray)):
            raw_query = bytes(raw_query).decode('latin-1')
        self.query_string = str(raw_query)
        self.path = path + (
            '?' + urllib.parse.unquote(self.query_string, encoding='utf-8')
            if self.query_string else '')
        # 别名参数（alias.md 8.1）：从原始 query 解析一次，handle_rpc 优先
        # 读这里，避免对已 unquote 的 self.path 二次解码。
        self.query_params = server_http.parse_query_params(self.query_string)
        # 告知 handle_rpc：路径已是解码后的文本，不要再 unquote。
        self.path_already_decoded = True

        # scope['headers'] 是 (bytes, bytes) 列表；同名多值按 HTTP 可合并头
        # 语义用逗号拼接（Cookie 等重复头不会丢掉后一条）。
        headers = {}
        for name, value in scope.get('headers') or []:
            key = bytes(name).decode('latin-1')
            text = bytes(value).decode('latin-1')
            if key in headers:
                headers[key] += ', ' + text
            else:
                headers[key] = text
        self.headers = _CIDict(headers)

        # 请求体一次性读入（RPC 代码约定：request.rfile / environ['wsgi.input']）。
        # 两种入口都给，兼容 client_http 生成代码里的 hasattr 分支。
        self.body = body or b''
        self.rfile = io.BytesIO(self.body)
        environ = {
            'REQUEST_METHOD': self.command,
            'PATH_INFO': path,
            'QUERY_STRING': self.query_string,
            'REMOTE_ADDR': str(self.client_address[0]),
            'REMOTE_PORT': str(self.client_address[1]),
            'wsgi.input': io.BytesIO(self.body),
        }
        for key, text in headers.items():
            environ['HTTP_' + key.upper().replace('-', '_')] = text
        if environ.get('HTTP_CONTENT_TYPE'):
            environ['CONTENT_TYPE'] = environ['HTTP_CONTENT_TYPE']
        if environ.get('HTTP_CONTENT_LENGTH'):
            environ['CONTENT_LENGTH'] = environ['HTTP_CONTENT_LENGTH']
        self.environ = environ

        # 部署配置固定在模块常量上：同进程再起别的 start_rpc_server（测试或
        # 本地调试）改类属性不能把线上 /- 地址整体 403。
        self.key = RPC_KEY
        self.alias_url = _ALIAS_URL_MODE
        self.alias_json = _ALIAS_JSON
        self.root_alias = RPC_ROOT_ALIAS
        self.persist_aliases = RPC_PERSIST_ALIASES
        self.executor = _APP_EXECUTOR or RPCRequestHandler.executor
        self.basic_auth = RPCRequestHandler.basic_auth

        self._status = 200
        self._headers = []          # list[(name, value)]
        self.wfile = _ASGIWFile(state)

    # ---- BaseHTTPRequestHandler 兼容方法 ---------------------------------

    # handle_rpc 入口的认证门会调 self.check_basic_auth()：直接复用
    # RPCRequestHandler 的实现（只依赖 basic_auth/headers/send_*/wfile）。
    check_basic_auth = RPCRequestHandler.check_basic_auth

    def send_response(self, code, message=None):
        self._status = int(code)

    def send_header(self, keyword, value):
        self._headers.append((keyword, str(value)))

    def end_headers(self):
        """提交响应：把状态行/头交给 state['start']，并冲刷提交前缓冲的字节。

        流式响应长度未知，不补 Content-Length，由 ASGI 服务器用分块传输。
        """
        state = self._state
        if state['committed']:
            return
        headers = list(self._headers)
        names = {key.lower() for key, _ in headers}
        if 'content-type' not in names:
            headers.append(('Content-Type', 'text/plain; charset=utf-8'))
        state['start'](self._status, headers)
        state['committed'] = True
        buffered = state['pre'].getvalue()
        if buffered:
            state['write'](buffered)

    def send_error(self, code, message=None, explain=None):
        # handle_rpc 在 403 / 400 / 500 兜底时会调用它：必须立刻提交，
        # 否则 application 的 finally 会把未提交的响应兜底成 200。
        self._status = int(code)
        self.end_headers()
        self.wfile.write(f"{code} {message or ''}".encode('utf-8'))

    def log_message(self, fmt, *args):
        # 避免占用共享 stdout / 触发 Windows 控制台阻塞
        pass


# ---------------------------------------------------------------------------
# ASGI 传输层
# ---------------------------------------------------------------------------

def _post(loop, target, item):
    """从执行线程把队列项送回事件循环；循环已关闭时静默丢弃。"""
    try:
        loop.call_soon_threadsafe(target.put_nowait, item)
    except RuntimeError:        # 事件循环已关闭（进程退出 / 请求被取消）
        pass


def _encode_headers(headers):
    return [(str(key).encode('latin-1', 'replace'),
             str(value).encode('latin-1', 'replace')) for key, value in headers]


async def _send_response(send, status, headers, body=b''):
    """一次性响应（405 / favicon / 302 / Range 切片等）。"""
    if isinstance(body, str):
        body = body.encode('utf-8')
    await send({'type': 'http.response.start', 'status': int(status),
                'headers': _encode_headers(headers)})
    await send({'type': 'http.response.body', 'body': body})


async def _consume_body_events(target, send, state):
    """队列 → ASGI send：'start' 项发状态行，bytes 项发分块，None 收尾。"""
    try:
        while True:
            item = await target.get()
            if item is None:
                await send({'type': 'http.response.body',
                            'body': b'', 'more_body': False})
                return
            if isinstance(item, tuple):
                await send({'type': 'http.response.start',
                            'status': item[1],
                            'headers': _encode_headers(item[2])})
            else:
                await send({'type': 'http.response.body',
                            'body': item, 'more_body': True})
    except Exception:           # 客户端断开 / 服务器取消
        state['failed'] = True
        logger.debug('ASGI response send aborted', exc_info=True)


async def _read_request_body(receive):
    """把 http.request 分块读成完整 bytes（GET 得到空消息，开销可忽略）。"""
    chunks = []
    while True:
        message = await receive()
        if message.get('type') != 'http.request':
            break
        chunks.append(message.get('body') or b'')
        if not message.get('more_body'):
            break
    return b''.join(chunks)


def _header_value(scope, name):
    wanted = name.lower().encode('latin-1')
    for key, value in scope.get('headers') or []:
        if bytes(key).lower() == wanted:
            return bytes(value).decode('latin-1')
    return ''


def _capture_full(scope, body):
    """完整执行一次 RPC 并收集全部响应字节（供 Range 请求切片）。

    阻塞到执行结束，返回 (status:int, headers:list, body:bytes)。
    """
    cap = {'committed': False, 'pre': io.BytesIO(),
           'status': 200, 'headers': [], 'parts': []}

    def start(status, headers):
        cap['status'] = int(status)
        cap['headers'] = list(headers)

    cap['start'] = start
    cap['write'] = cap['parts'].append

    shim = _ASGIHandlerShim(scope, body, cap)
    try:
        if RPCRequestHandler.check_basic_auth(shim):
            RPCRequestHandler.handle_rpc(shim)
    except Exception:
        tb = traceback.format_exc()
        if not cap['committed']:
            cap['status'] = 500
            cap['headers'] = [('Content-Type', 'text/plain; charset=utf-8')]
            cap['committed'] = True
            cap['parts'].append(tb.encode('utf-8'))
    finally:
        if not cap['committed']:
            cap['status'] = 200
            cap['headers'] = [('Content-Type', 'text/plain; charset=utf-8')]
            cap['committed'] = True
    return cap['status'], cap['headers'], b''.join(cap['parts'])


async def _serve_byte_range(scope, body, send, group1, group2):
    status, headers, out = await asyncio.get_running_loop().run_in_executor(
        None, _capture_full, scope, body)
    if not 200 <= status < 300:
        await _send_response(send, status, headers, out)
        return

    total = len(out)
    if group1 and group2:
        start, end = int(group1), min(int(group2), total - 1)
    elif group1:
        start, end = int(group1), total - 1
    elif group2:
        start, end = max(0, total - int(group2)), total - 1
    else:
        start, end = 0, total - 1

    if total == 0 or start >= total or start > end:
        msg = b"Requested Range Not Satisfiable"
        await _send_response(send, 416, [
            ('Content-Range', f'bytes */{total}'),
            ('Content-Type', 'text/plain; charset=utf-8'),
            ('Content-Length', str(len(msg))),
        ], msg)
        return

    ctype = 'application/octet-stream'
    for key, value in headers:
        if key.lower() == 'content-type':
            ctype = value
    await _send_response(send, 206, [
        ('Content-Type', ctype),
        ('Content-Range', f'bytes {start}-{end}/{total}'),
        ('Content-Length', str(len(out[start:end + 1]))),
        ('Accept-Ranges', 'bytes'),
    ], out[start:end + 1])


async def _serve_http(scope, receive, send):
    method = (scope.get('method') or 'GET').upper()

    # 只放行 GET/POST —— 与 server_http 的 do_GET / do_POST 一致
    if method not in ('GET', 'POST'):
        body_out = b'Method Not Allowed'
        await _send_response(send, 405, [
            ('Content-Type', 'text/plain; charset=utf-8'),
            ('Content-Length', str(len(body_out))),
            ('Allow', 'GET, POST'),
        ], body_out)
        return

    body = await _read_request_body(receive)

    # 播放中拖动进度条 → 浏览器发 Range 请求：完整捕获输出后返回 206。
    # 例外：'bytes=0-' 是媒体加载器的初始（打开式）请求，仍走流式路径返回
    # 200，保证点击后立刻边合成边播放。
    range_header = _header_value(scope, 'range')
    if range_header:
        matched = _RANGE_SPEC_RE.match(range_header.strip())
        if matched and not (matched.group(1) == '0' and matched.group(2) == ''):
            await _serve_byte_range(scope, body, send,
                                    matched.group(1), matched.group(2))
            return

    loop = asyncio.get_running_loop()
    target = asyncio.Queue()
    state = {
        'committed': False,
        'failed': False,        # 客户端断开后不再往队列里堆数据
        'pre': io.BytesIO(),
        'start': lambda code, headers: _post(loop, target, ('start', code, headers)),
        'write': lambda data: None if state['failed'] else _post(loop, target, data),
    }
    consumer = asyncio.create_task(_consume_body_events(target, send, state))
    shim = _ASGIHandlerShim(scope, body, state)

    def run():
        # 每请求一个线程：handle_rpc 是同步阻塞的（执行器有锁/超时守卫），
        # 不能占住事件循环；写出分块经 state['write'] 立刻回到事件循环。
        try:
            if not RPCRequestHandler.check_basic_auth(shim):
                return
            path = shim.path.split('?', 1)[0]
            # favicon 分支（对应 do_GET 中的同名分支）
            if path == '/favicon.ico' and RPCRequestHandler.favicon_bytes:
                favicon = RPCRequestHandler.favicon_bytes
                shim.send_response(200)
                shim.send_header('Content-Type', 'image/x-icon')
                shim.send_header('Cache-Control', 'max-age=86400')
                shim.send_header('Content-Length', str(len(favicon)))
                shim.end_headers()
                shim.wfile.write(favicon)
                return
            # 根路径重定向（对应 do_GET 中的同名分支）
            if path == '/' and RPCRequestHandler.redirect_root:
                shim.send_response(302)
                shim.send_header('Location', RPCRequestHandler.redirect_root)
                shim.send_header('Content-Length', '0')
                shim.end_headers()
                return
            RPCRequestHandler.handle_rpc(shim)
        except Exception:
            tb = traceback.format_exc()
            if not state['committed']:
                shim.send_response(500)
                shim.send_header('Content-Type', 'text/plain; charset=utf-8')
                shim.end_headers()
                try:
                    shim.wfile.write(tb.encode('utf-8'))
                except Exception:
                    pass
            else:
                logging.getLogger("error").exception(
                    "ASGI RPC stream failed mid-flight")
        finally:
            # 兜底：正常情况下 handle_rpc 必然已经提交。
            if not state['committed']:
                shim.send_response(200)
                shim.send_header('Content-Type', 'text/plain; charset=utf-8')
                shim.end_headers()
            _post(loop, target, None)

    try:
        await loop.run_in_executor(None, run)
        await consumer
    finally:
        consumer.cancel()


# ---------------------------------------------------------------------------
# WebSocket：ASGI 收发 ↔ server_http.WebSocket 同步接口
# ---------------------------------------------------------------------------

class AsgiWebSocket:
    """duck-type server_http.WebSocket：同步阻塞 receive、线程安全 send。

    与原生 WebSocket 的差异仅在传输层（ASGI 而非裸 socket）；Editor 等
    处理器只依赖 send/receive/close/socket 四个名字，均可直接使用。
    """

    def __init__(self, send, loop):
        self._send = send
        self._loop = loop
        self._recv = queue.SimpleQueue()
        self.closed = False
        # Editor 日志会打印 connection.socket，保留一个可读引用来标识连接
        self.socket = loop

    def send(self, message):
        if isinstance(message, str):
            payload = {'type': 'websocket.send', 'text': message}
        else:
            payload = {'type': 'websocket.send', 'bytes': bytes(message)}
        future = asyncio.run_coroutine_threadsafe(self._send(payload), self._loop)
        try:
            future.result(timeout=30)
        except (ConnectionError, OSError, asyncio.TimeoutError) as error:
            raise OSError(f'ASGI websocket send failed: {error}') from error

    def receive(self):
        """下一条文本消息；对端断开时返回 None（对齐原生语义）。"""
        return self._recv.get()

    def close(self):
        if not self.closed:
            self.closed = True
            self._recv.put(None)
            try:
                asyncio.run_coroutine_threadsafe(
                    self._send({'type': 'websocket.close', 'code': 1000}),
                    self._loop).result(timeout=5)
            except Exception:
                pass


async def _pump_receive(receive, connection):
    """ASGI receive 循环 → 同步队列；断开投递 None 结束处理器线程。"""
    while True:
        message = await receive()
        kind = message.get('type')
        if kind == 'websocket.disconnect':
            connection._recv.put(None)
            return
        if kind == 'websocket.receive':
            text = message.get('text')
            if text is None:
                text = (message.get('bytes') or b'').decode('utf-8', 'replace')
            connection._recv.put(text)


def _find_websocket_handler(path):
    """按原生 do_GET 的同一规则选处理器：路径表优先，再回退单处理器。"""
    handlers = RPCRequestHandler.websocket_handlers
    handler = handlers.get(path)
    if handler is not None:
        return handler
    if path == RPCRequestHandler.websocket_path:
        return RPCRequestHandler.websocket_handler
    return None


async def _serve_websocket(scope, receive, send):
    path = scope.get('path') or '/'
    handler = _find_websocket_handler(path)
    if handler is None:
        # 未注册的 ws 路径：拒绝升级（浏览器侧表现为握手 403/1008）
        await send({'type': 'websocket.close', 'code': 1008})
        return
    await send({'type': 'websocket.accept'})
    loop = asyncio.get_running_loop()
    connection = AsgiWebSocket(send, loop)
    pump = asyncio.create_task(_pump_receive(receive, connection))
    try:
        # 同步处理器（内部是收发死循环）放默认执行器线程跑，
        # 结束条件与原生路径一致：receive 返回 None 或 send 抛 OSError
        await loop.run_in_executor(None, handler, connection, None)
    except Exception:
        logger.exception('ASGI websocket handler terminated with error')
    finally:
        pump.cancel()
        connection.closed = True
        connection._recv.put(None)
        try:
            await send({'type': 'websocket.close', 'code': 1000})
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

async def application(scope, receive, send):
    """ASGI 入口：http / websocket / lifespan 三路分流。"""
    kind = scope.get('type')
    if kind == 'http':
        await _serve_http(scope, receive, send)
        return
    if kind == 'websocket':
        await _serve_websocket(scope, receive, send)
        return
    if kind == 'lifespan':
        # 独立 uvicorn 运行时才需要：宿主应用（FastAPI/Gradio）自己管 lifespan。
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return


# ---------------------------------------------------------------------------
# 启动：只初始化 RPCRequestHandler，不做 socket 绑定
# ---------------------------------------------------------------------------

server_http.start_rpc_server(
    port=int(os.environ.get('RPC_PORT', '6080')),
    ip='0.0.0.0',
    key=RPC_KEY,
    alias_url=_ALIAS_URL_MODE,
    root_alias=RPC_ROOT_ALIAS,
    persist_aliases=RPC_PERSIST_ALIASES,
    globals=globals(),
    locals=locals(),
    listen=False,           # 关键：只初始化状态，HTTP/WS 都交给宿主 ASGI 服务器
)
