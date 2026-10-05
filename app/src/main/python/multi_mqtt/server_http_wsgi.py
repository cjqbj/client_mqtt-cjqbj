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
import socketserver
import urllib.parse
import http.client as _http_client
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler

import server_http
from server_http import RPCRequestHandler


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
        self.headers = headers

        # 从 RPCRequestHandler 同步类级状态
        self.key = RPCRequestHandler.key
        self.executor = RPCRequestHandler.executor

        # 响应状态
        self._status = 200
        self._headers = []          # list[(name, value)]
        self.wfile = _WSGIWFile(state)

    # ---- BaseHTTPRequestHandler 兼容方法 ---------------------------------

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
        # handle_rpc 在 403 / 400 / 500 兜底时会调用它
        self._status = int(code)
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
        state = {
            'committed': False,
            'start_response': start_response,
            'chunks': queue.Queue(),
            'pre': io.BytesIO(),
        }

        def run():
            shim = _WSGIHandlerShim(environ, state)
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
    key='',
    globals=globals(),
    locals=locals(),
    listen=False,           # 关键：只初始化状态
)

application = _build_wsgi_application()


# ---------------------------------------------------------------------------
# 本地调试：多线程 WSGI 服务器 + 快速 Ctrl+C
# ---------------------------------------------------------------------------

class _ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    daemon_threads = True
    allow_reuse_address = True


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