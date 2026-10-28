"""ASGI WebSocket 桥：让 WSGI 通道（server_http_wsgi + a2wsgi）部署形态也拥有 /ws 能力。

背景：HuggingFace Space 的 app.py 用 FastAPI 挂 a2wsgi 的 WSGIMiddleware 跑
RPC；WSGI 协议本身不支持连接劫持，WebSocket 升级请求进了容器也到不了
server_http 的原生 ws 处理。uvicorn 把 websocket scope 交给 FastAPI 路由，
没注册对应路由时 Starlette 直接 close（浏览器侧表现为握手 403、页面
"连接断开，重连中"）。

方案：在 FastAPI/Starlette 应用上注册 /ws/{rest:path} 的 websocket 路由，
把 ASGI 的异步收发桥接成 server_http.WebSocket 同款的同步 send/receive
接口，然后调用 server_http.RPCRequestHandler.websocket_handlers 里运行期
注册的同步处理器（如 realtime_editor.Editor.websocket）。处理器跑在默认
执行器的线程上，桥接零改动复用既有业务代码。

用法（app.py 启动处，或运行期经 URL 即代码 RPC 热加载）：
    import server_http_asgi_ws
    server_http_asgi_ws.install(server)
"""
import asyncio
import logging
import queue

import server_http

logger = logging.getLogger(__name__)

ROUTE = '/ws/{rest:path}'


class AsgiWebSocket:
    """duck-type server_http.WebSocket：同步阻塞 receive、线程安全 send。

    与原生 WebSocket 的差异仅在传输层（ASGI 而非裸 socket）；Editor 等
    处理器只依赖 send/receive/close/socket 四个名字，均可直接使用。
    """

    def __init__(self, websocket, loop):
        self._websocket = websocket
        self._loop = loop
        self._recv = queue.SimpleQueue()
        self.closed = False
        # Editor 日志会打印 connection.socket，保留一个可读引用
        self.socket = websocket

    def send(self, message):
        if isinstance(message, str):
            coro = self._websocket.send_text(message)
        else:
            coro = self._websocket.send_bytes(bytes(message))
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
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
                    self._websocket.close(), self._loop).result(timeout=5)
            except Exception:
                pass


async def _pump_receive(websocket, connection):
    """ASGI receive 循环 → 同步队列；断开投递 None 结束处理器线程。"""
    while True:
        message = await websocket.receive()
        kind = message.get('type')
        if kind == 'websocket.disconnect':
            connection._recv.put(None)
            return
        if kind == 'websocket.receive':
            text = message.get('text')
            if text is None:
                text = (message.get('bytes') or b'').decode('utf-8', 'replace')
            connection._recv.put(text)


async def websocket_endpoint(websocket, rest):
    # 用完整请求路径查表：路由前缀被 FastAPI 剥进 rest，'/' + rest 拼回的键
    # 缺少 /ws 前缀必查不到；scope['path'] 与原生 do_GET 的分流键完全一致。
    path = websocket.scope.get('path') or ('/' + (rest or ''))
    handler = server_http.RPCRequestHandler.websocket_handlers.get(path)
    await websocket.accept()
    if handler is None:
        await websocket.close(code=1008)
        return
    loop = asyncio.get_running_loop()
    connection = AsgiWebSocket(websocket, loop)
    pump = asyncio.create_task(_pump_receive(websocket, connection))
    try:
        # 同步处理器（内部是收发死循环）放默认执行器线程跑，
        # 结束条件与原生路径一致：receive 返回 None 或 send 抛 OSError
        await loop.run_in_executor(None, handler, connection, None)
    except (Exception, asyncio.CancelledError):
        logger.exception('ASGI websocket handler terminated with error')
    finally:
        pump.cancel()
        connection.closed = True
        connection._recv.put(None)
        try:
            await websocket.close()
        except Exception:
            pass


def install(app, route=ROUTE):
    """在 FastAPI / gradio.Server / Starlette 应用上注册 /ws 桥接路由，幂等。"""
    for existing in getattr(app, 'routes', []) or []:
        if getattr(existing, 'path', None) == route and \
                getattr(existing, 'name', '') == websocket_endpoint.__name__:
            return app
    if hasattr(app, 'websocket'):            # FastAPI 系：支持路径参数签名
        from fastapi import WebSocket

        async def _endpoint(websocket: WebSocket, rest: str = ''):
            await websocket_endpoint(websocket, rest)
    else:                                    # 纯 Starlette：路径参数走 path_params
        async def _endpoint(websocket):
            await websocket_endpoint(websocket, websocket.path_params.get('rest', ''))
    _endpoint.__name__ = websocket_endpoint.__name__
    if hasattr(app, 'websocket'):
        app.websocket(route)(_endpoint)
    else:
        app.add_websocket_route(route, _endpoint)
    logger.info('ASGI websocket bridge installed at %s', route)
    return app
