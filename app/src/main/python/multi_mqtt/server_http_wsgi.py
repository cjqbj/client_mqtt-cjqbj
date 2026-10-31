#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
server_http_wsgi.py — 把 server_http.py 的 RPC 处理器包装成 WSGI 应用。

设计原则：不重写任何 RPC 逻辑。
    * 启动时调用 server_http.start_rpc_server(..., listen=False)，
      只做 RPCRequestHandler 的类级初始化（key / executor / favicon / ...），
      不做 socket 绑定。
    * 请求时用一个 duck-typed shim 冒充 BaseHTTPRequestHandler，把请求整个
      丢给 RPCRequestHandler.handle_rpc(self)。
      RPC 语义（response/p、r 变量、favicon、错误兜底、持久命名空间）
      完全由 server_http.py 负责。

部署：
    gunicorn -w 1 -b 0.0.0.0:6080 server_http_wsgi:application
    uwsgi --http :6080 --module server_http_wsgi:application --enable-threads
    python3 server_http_wsgi.py        # 本地调试（多线程 wsgiref）

URL 形态（RPC_KEY / RPC_ALIAS_URL 环境变量可覆盖，默认 '-' / 'raw'）：
    https://<host>/-<code>             例：/-r=1+1
    https://<host>/-/<code>            兼容旧写法
    https://<host>/<别名>              已注册别名免前缀（alias_url='raw'）
    https://<host>/                    执行 root 别名（RPC_ROOT_ALIAS，默认
                                       'root'；定义 #alias root 后首页显示它
                                       产出的 HTML）

别名持久化：默认开启，注册 / 更新 / 删除 #alias 都写仓库根目录的
rpc.json（http_aliases 段，与 server_mqtt 的 request_topic 等同文件共存），
进程重启 / SIGHUP 后自动恢复；RPC_PERSIST_ALIASES=0 可关闭。

注意：
    * RPC 的持久命名空间是进程内单例，请使用单 worker（-w 1）。
    * WSGI 不支持 WebSocket 升级；需要 /ws 请继续用 server_http.py。
"""

import io
import os
import re
import sys
import time
import queue
import logging
import threading
import traceback
import socket
import socketserver
import urllib.parse
import http.client as _http_client
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler

import server_http
from server_http import RPCRequestHandler


# 本 WSGI 应用的部署配置（pythonanywhere 线上默认 key='-'，URL 形如
# /-<code>；alias_url='raw' 让已注册别名可直接作 URL，如 /add）。
# 用环境变量覆盖便于同一份代码在本地调试或换前缀，置空 RPC_KEY 即回到无前缀。
RPC_KEY = os.environ.get('RPC_KEY', '-')
ALIAS_URL = os.environ.get('RPC_ALIAS_URL', 'raw')
# alias_url 归一化成 (路由模式 'dash'/'raw', 是否 JSON 解码 query 值)。
# 环境变量属于显式配置，写错时降级为默认 dash 并 WARNING，不让整个 WSGI
# 应用 import 失败（AGENTS.md：附加功能出问题只降级，不阻塞主流程）。
try:
    _ALIAS_URL_MODE, _ALIAS_JSON = server_http.normalize_alias_url(ALIAS_URL)
except ValueError as _alias_url_error:
    logging.getLogger('app').warning(
        'RPC_ALIAS_URL=%r 无效，降级为 dash: %s', ALIAS_URL, _alias_url_error)
    _ALIAS_URL_MODE, _ALIAS_JSON = server_http.ALIAS_URL_DASH, False
# 首页 / 执行的别名名（默认 root）与别名持久化开关（默认开启，写 rpc.json）。
RPC_ROOT_ALIAS = os.environ.get('RPC_ROOT_ALIAS', 'root')
RPC_PERSIST_ALIASES = os.environ.get(
    'RPC_PERSIST_ALIASES', '1').strip().lower() not in ('0', 'false', 'no', 'off', '')
# 本应用自己的执行器，start_rpc_server 初始化后绑定（见文件末尾）
_APP_EXECUTOR = None


def _wsgi_unescape(value: str) -> str:
    """把 WSGI 的 latin-1 承载文本还原为原始字节并按 UTF-8 解码。

    PEP 3333 规定 PATH_INFO 等环境变量以 ISO-8859-1 编码承载原始
    请求字节；直接当文本使用会让中文变成 'ä½ å¥½' 乱码（edge-tts
    会把乱码逐字母朗读出来，即“说的不是输入文字”事故的根因）。
    """
    raw = value.encode("latin-1", "surrogateescape")
    return raw.decode("utf-8", "surrogateescape")


# ---------------------------------------------------------------------------
# BaseHTTPRequestHandler 的最小模拟
# ---------------------------------------------------------------------------

class _WSGIWFile:
    """冒充 handler.wfile。

    提交（end_headers）之前的写入先缓冲；提交之后的写入直接推进分块
    队列，WSGI 可迭代对象立刻产出，ASGI 网关随即把字节发给客户端，
    实现真正的流式传输（大文本 TTS 边生成边播放）。
    """

    __slots__ = ('_state',)

    def __init__(self, state):
        self._state = state

    def write(self, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        if self._state['committed']:
            if data:
                self._state['chunks'].put(data)
        else:
            self._state['pre'].write(data)

    def flush(self):
        pass

    def getvalue(self):
        return self._state['pre'].getvalue()


class _CIDict(dict):
    """大小写不敏感的请求头字典。

    保留原始键名供迭代 / 内省；查找忽略大小写，对齐原生
    BaseHTTPRequestHandler.headers（email.message.Message）的语义。
    environ 的 HTTP_* 还原出的键是 'AUTHORIZATION' 这类大写形式，
    check_basic_auth / RPC 代码用 'Authorization' 查询，普通 dict 会漏。
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


class _WSGIHandlerShim:
    """让 RPCRequestHandler.handle_rpc(self) 在 WSGI 下原样运行的薄垫片。

    handle_rpc 只依赖下列属性 / 方法，全部按 BaseHTTPRequestHandler 的语义提供；
    RPC 逻辑一行不改。
    """

    def __init__(self, environ, state):
        self._state = state
        self.environ = environ
        self.command = environ.get('REQUEST_METHOD', 'GET')

        self.client_address = (
            environ.get('REMOTE_ADDR', ''),
            int(environ.get('REMOTE_PORT') or 0),
        )

        # 与 BaseHTTPRequestHandler.path 对齐：PATH + '?' + QUERY。
        # WSGI 按 latin-1 承载字节（PEP 3333），先还原成正确的 UTF-8 文本；
        # QUERY_STRING 是未解码的原始 query，按 UTF-8 unquote 后再拼上。
        qs = environ.get('QUERY_STRING', '') or ''
        path = _wsgi_unescape(environ.get('PATH_INFO', '') or '')
        self.path = path + (
            '?' + urllib.parse.unquote(qs, encoding='utf-8') if qs else '')
        # 别名参数（alias.md 8.1）：从**原始** QUERY_STRING 解析一次，
        # handle_rpc 优先读这里，避免对已 unquote 的 self.path 二次解码。
        self.query_params = server_http.parse_query_params(qs)
        # 告知 handle_rpc：路径已是解码后的文本，不要再 unquote。
        self.path_already_decoded = True

        # 把 environ 里的请求头还原成类似 handler.headers 的 dict
        headers = {}
        for name, value in environ.items():
            if name.startswith('HTTP_'):
                headers[name[5:].replace('_', '-')] = value
        if environ.get('CONTENT_TYPE'):
            headers['Content-Type'] = environ['CONTENT_TYPE']
        if environ.get('CONTENT_LENGTH'):
            headers['Content-Length'] = environ['CONTENT_LENGTH']
        self.headers = _CIDict(headers)

        # 每请求新建 shim。basic_auth 仍读类级状态，运行期 set_basic_auth
        # 下个请求即生效；key / alias_url 固定用本应用部署配置（RPC_KEY /
        # ALIAS_URL），否则同进程再起一台无 key 的 start_rpc_server（测试或
        # 本地调试）会把类属性改回 ''，线上 /- 地址会整体 403。
        self.key = RPC_KEY
        self.alias_url = _ALIAS_URL_MODE
        self.alias_json = _ALIAS_JSON
        # 首页别名 / 持久化开关同样固定在部署配置上：同进程再起别的服务器
        # （测试或本地调试）改类属性也不能改变线上首页与落盘行为。
        self.root_alias = RPC_ROOT_ALIAS
        self.persist_aliases = RPC_PERSIST_ALIASES
        # executor 固定绑本应用启动时的那一个（pythonanywhere 是单 app 进程）：
        # 同进程再起别的服务器会替换类属性，WSGI 请求不能跟着跑到别人的
        # 命名空间和别名表里。
        self.executor = _APP_EXECUTOR or RPCRequestHandler.executor
        self.basic_auth = RPCRequestHandler.basic_auth

        # 响应状态
        self._status = 200
        self._headers = []          # list[(name, value)]
        self.wfile = _WSGIWFile(state)

    # ---- BaseHTTPRequestHandler 兼容方法 ---------------------------------

    # handle_rpc 入口的认证门会调 self.check_basic_auth()：直接复用
    # RPCRequestHandler 的实现（只依赖 basic_auth/headers/send_*/wfile，
    # shim 均已提供），不复制逻辑。
    check_basic_auth = RPCRequestHandler.check_basic_auth

    def send_response(self, code, message=None):
        self._status = int(code)

    def send_header(self, keyword, value):
        self._headers.append((keyword, str(value)))

    def end_headers(self):
        """提交响应：调用 start_response，并冲刷提交前缓冲的字节。"""
        state = self._state
        if state['committed']:
            return
        headers = list(self._headers)
        names = {k.lower() for k, _ in headers}
        if 'content-type' not in names:
            headers.append(('Content-Type', 'text/plain; charset=utf-8'))
        # 流式响应长度未知，不设置 Content-Length，由网关使用分块传输。

        reason = _http_client.responses.get(self._status, 'Unknown')
        state['start_response'](f"{self._status} {reason}", headers)
        state['committed'] = True

        buffered = state['pre'].getvalue()
        if buffered:
            state['chunks'].put(buffered)

    def send_error(self, code, message=None, explain=None):
        # handle_rpc 在 403 / 400 / 500 兜底时会调用它。原生 send_error 会
        # 立刻发状态行；这里也必须先 end_headers 提交，否则 application 的
        # finally 会把未提交的响应兜底成 200，403 变成"200 + 空 body"。
        self._status = int(code)
        self.end_headers()
        self.wfile.write(f"{code} {message or ''}".encode('utf-8'))

    def log_message(self, fmt, *args):
        # 避免占用共享 stdout / 触发 Windows 控制台阻塞
        pass


# ---------------------------------------------------------------------------
# WSGI application 构造
# ---------------------------------------------------------------------------

_RANGE_SPEC_RE = re.compile(r'^bytes=(\d*)-(\d*)$')


def _build_wsgi_application():
    """基于已初始化的 RPCRequestHandler 构造流式 WSGI application。

    每个 RPC 请求在独立守护线程里跑原装的 handle_rpc；application
    立即返回一个队列驱动的可迭代对象——执行线程写出的每个分块都会
    立刻被 ASGI 网关转发给客户端，不等执行结束。
    """

    def _capture_full(environ):
        """完整执行一次 RPC 并收集全部响应字节（供 Range 请求切片）。

        使用独立的 capture 状态和空转的 start_response；阻塞到执行结束，
        返回 (status_line, headers, full_body)。
        """
        cap = {
            'committed': False,
            'chunks': queue.Queue(),
            'pre': io.BytesIO(),
            'status': '200 OK',
            'headers': [],
        }

        def cap_start_response(status, headers):
            cap['status'] = status
            cap['headers'] = list(headers)

        cap['start_response'] = cap_start_response

        def run():
            shim = _WSGIHandlerShim(environ, cap)
            try:
                RPCRequestHandler.handle_rpc(shim)
            except Exception:
                tb = traceback.format_exc()
                if not cap['committed']:
                    cap_start_response('500 Internal Server Error', [
                        ('Content-Type', 'text/plain; charset=utf-8')])
                    cap['committed'] = True
                    cap['chunks'].put(tb.encode('utf-8'))
            finally:
                if not cap['committed']:
                    cap_start_response('200 OK', [
                        ('Content-Type', 'text/plain; charset=utf-8')])
                    cap['committed'] = True
                cap['chunks'].put(None)  # 结束哨兵

        threading.Thread(target=run, name='RPC-WSGI-Range', daemon=True).start()

        parts = []
        while True:
            item = cap['chunks'].get()
            if item is None:
                break
            parts.append(item)
        return cap['status'], cap['headers'], b''.join(parts)

    def _serve_byte_range(environ, start_response, a, b):
        status, headers, body = _capture_full(environ)
        if not status.startswith('200'):
            start_response(status, headers)
            return [body]

        total = len(body)
        if a and b:
            start, end = int(a), min(int(b), total - 1)
        elif a:
            start, end = int(a), total - 1
        elif b:
            start, end = max(0, total - int(b)), total - 1
        else:
            start, end = 0, total - 1

        if total == 0 or start >= total or start > end:
            msg = b"Requested Range Not Satisfiable"
            start_response('416 Range Not Satisfiable', [
                ('Content-Range', f'bytes */{total}'),
                ('Content-Type', 'text/plain; charset=utf-8'),
                ('Content-Length', str(len(msg))),
            ])
            return [msg]

        ctype = 'application/octet-stream'
        for key, value in headers:
            if key.lower() == 'content-type':
                ctype = value
        out = body[start:end + 1]
        start_response('206 Partial Content', [
            ('Content-Type', ctype),
            ('Content-Range', f'bytes {start}-{end}/{total}'),
            ('Content-Length', str(len(out))),
            ('Accept-Ranges', 'bytes'),
        ])
        return [out]

    def application(environ, start_response):
        method = environ.get('REQUEST_METHOD', 'GET').upper()
        path = environ.get('PATH_INFO', '') or ''

        # 只放行 GET/POST —— 与 server_http 的 do_GET / do_POST 一致
        if method not in ('GET', 'POST'):
            body = b'Method Not Allowed'
            start_response('405 Method Not Allowed', [
                ('Content-Type', 'text/plain; charset=utf-8'),
                ('Content-Length', str(len(body))),
                ('Allow', 'GET, POST'),
            ])
            return [body]

        # 认证门在所有分支之前：favicon / 重定向 / Range / RPC 都先过
        # basic_auth，与原生 do_GET 首查对齐。401 由 shim 的 WSGI 语义
        # 写进提交缓冲（end_headers 已提交，body 是提交后的分块）。
        state = {
            'committed': False,
            'start_response': start_response,
            'chunks': queue.Queue(),
            'pre': io.BytesIO(),
        }
        shim = _WSGIHandlerShim(environ, state)
        if not RPCRequestHandler.check_basic_auth(shim):
            # 同步路径无并发写者，非阻塞排空已写入的分块即可
            parts = []
            while not state['chunks'].empty():
                parts.append(state['chunks'].get_nowait())
            return parts

        # favicon 分支（对应 do_GET 中的同名分支）
        if path == '/favicon.ico' and RPCRequestHandler.favicon_bytes:
            body = RPCRequestHandler.favicon_bytes
            start_response('200 OK', [
                ('Content-Type', 'image/x-icon'),
                ('Cache-Control', 'max-age=86400'),
                ('Content-Length', str(len(body))),
            ])
            return [body]

        # 根路径重定向（对应 do_GET 中的同名分支）
        if path == '/' and RPCRequestHandler.redirect_root:
            start_response('302 Found', [
                ('Location', RPCRequestHandler.redirect_root),
                ('Content-Length', '0'),
            ])
            return [b'']

        # 播放中拖动进度条 → 浏览器发 Range 请求：完整捕获输出后返回 206。
        # 例外：'bytes=0-' 是媒体加载器的初始（打开式）请求，仍走下面的
        # 流式路径返回 200，保证点击后立刻边合成边播放。
        range_header = environ.get('HTTP_RANGE')
        if range_header:
            match = _RANGE_SPEC_RE.match(range_header.strip())
            if match and not (match.group(1) == '0' and match.group(2) == ''):
                return _serve_byte_range(
                    environ, start_response, match.group(1), match.group(2))

        # 其余路径全部交给原装的 handle_rpc（在线程中执行）
        def run():
            try:
                RPCRequestHandler.handle_rpc(shim)
            except Exception:
                tb = traceback.format_exc()
                if not state['committed']:
                    state['start_response']('500 Internal Server Error', [
                        ('Content-Type', 'text/plain; charset=utf-8'),
                    ])
                    state['committed'] = True
                    state['chunks'].put(tb.encode('utf-8'))
                else:
                    logging.getLogger("error").exception(
                        "WSGI RPC stream failed mid-flight")
            finally:
                # 兜底：正常情况下 handle_rpc 必然已经提交。
                if not state['committed']:
                    state['start_response']('200 OK', [
                        ('Content-Type', 'text/plain; charset=utf-8'),
                    ])
                    state['committed'] = True
                state['chunks'].put(None)  # 结束哨兵

        threading.Thread(target=run, name='RPC-WSGI-Exec', daemon=True).start()

        def body_iter():
            chunks = state['chunks']
            while True:
                item = chunks.get()
                if item is None:
                    return
                yield item

        return body_iter()

    return application


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
    listen=False,           # 关键：只初始化状态
)

# 绑定本应用的执行器（持久命名空间/别名表），后续同进程再起别的服务器
# 不会影响 WSGI 请求。
_APP_EXECUTOR = RPCRequestHandler.executor

application = _build_wsgi_application()


# ---------------------------------------------------------------------------
# 本地调试：多线程 WSGI 服务器 + 快速 Ctrl+C
# ---------------------------------------------------------------------------

class _ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True
    # 同 server_http.ThreadedHTTPServer：Windows 上 SO_REUSEADDR 会静默
    # 双绑正在监听的端口（Linux 只复用 TIME_WAIT），改用 SO_EXCLUSIVEADDRUSE
    # 让端口被占时启动立即报错，快速失败而不是静默挂死。
    if sys.platform == 'win32':
        allow_reuse_address = False
    else:
        allow_reuse_address = True

    def server_bind(self):
        if sys.platform == 'win32':
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


class _QuietHandler(WSGIRequestHandler):
    # wsgiref 默认 HTTP/1.0；保持默认，避免 keep-alive 拖住线程
    def log_message(self, fmt, *args):
        sys.stdout.write("[RPC-WSGI] %s %s %s\n" % (
            time.strftime('%H:%M:%S'), self.address_string(), fmt % args))
        sys.stdout.flush()

    def log_error(self, fmt, *args):
        self.log_message(fmt, *args)


if __name__ == '__main__':
    host = os.environ.get('RPC_HOST', '0.0.0.0')
    port = int(os.environ.get('RPC_PORT', '6080'))

    print(f"[RPC-WSGI] serving on http://{host}:{port}/", flush=True)
    httpd = _ThreadingWSGIServer((host, port), _QuietHandler)
    httpd.set_app(application)
    try:
        httpd.serve_forever(poll_interval=0.2)   # ← 200ms，Ctrl+C 立刻响应
    except KeyboardInterrupt:
        print("\n[RPC-WSGI] bye", flush=True)
    finally:
        httpd.server_close()