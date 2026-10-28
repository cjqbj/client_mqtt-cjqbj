"""浏览器实时双向编辑服务器文件；可直接 import，由已在运行的 server_http RPC 服务打开。

无需命令行启动：只要服务器（server_mqtt.py / server_http.py）已在跑，浏览器访问
URL 即代码，等价于执行 ``import realtime_editor;realtime_editor.html(response,file='/tmp/txt')``：

    http://192.168.1.12:1177/import%20realtime_editor;realtime_editor.html(response,file=/tmp/txt)

路径可不加引号：server_http.handle_rpc 对整段语法错误的代码有裸路径救援，
会把表达式起始位置的 /tmp/txt、C:/x/y 自动补引号后重试。

文件不存在时自动创建（含父目录）。同一文件多次打开复用同一 Editor，多个标签页
互相同步；不同文件各得独立 WebSocket 路由 /ws/realtime_editor/<md5前12位>，
处理函数在首次打开时动态注册进 server_http.RPCRequestHandler.websocket_handlers
（类属性字典，请求期实时可见），因此模块被 URL import 的同时即可收 WebSocket
连接，不依赖启动参数 websocket_handler，也无需重启服务。

RPC 入口（p / response 由 server_http.handle_rpc 注入）：
    realtime_editor.html(p)                  编辑默认文件 default_file
    realtime_editor.html(p, '/tmp/txt')      位置参数亦可
    realtime_editor.status(p)                列出已打开文件、ws 路由、客户端数、revision

命令行独立跑（兼容旧用法）：
    python realtime_editor.py 文本文件 [--port 1133] [--host 0.0.0.0]
"""
import argparse
import hashlib
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import server_http

logger = logging.getLogger(__name__)

# html(p) 未传 file 时编辑的文件；main() 会把它替换成命令行参数
default_file = 'realtime_editor.txt'

# str(绝对路径) -> Editor；同一文件的多次打开复用同一实例
_editors = globals().setdefault('_editors', {})  # setdefault 防 importlib.reload：reload 复用同一模块 __dict__ 重执行顶层，普通赋值把名字重绑到空 dict——已开编辑器（带活 WS 推送连接）变孤儿、同一文件再开时新建第二个 Editor 双实例并发写
_editors_lock = globals().setdefault('_editors_lock', threading.RLock())  # 锁必须与表一起 setdefault：reload 重造新锁对象后，已攥旧锁的线程与拿新锁的线程互不排斥、同时进临界区

PAGE = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Realtime Editor</title>
<style>
:root { color-scheme: dark; --bg: #121619; --panel: #1b2226; --line: #344047; --text: #e8eeee; --muted: #91a1a5; --accent: #58c4b2; }
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; background: var(--bg); color: var(--text); font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
body { display: flex; flex-direction: column; overflow: hidden; }
header { height: 44px; flex: 0 0 44px; display: flex; align-items: center; padding: 0 16px; border-bottom: 1px solid var(--line); background: var(--panel); }
.title { font-size: 14px; font-weight: 700; letter-spacing: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
#editor { flex: 1 1 auto; width: 100%; resize: none; border: 0; outline: 0; padding: 18px 20px; color: var(--text); background: #101416; font: 14px/1.6 ui-monospace, SFMono-Regular, Consolas, monospace; tab-size: 4; }
footer { height: 30px; flex: 0 0 30px; display: flex; align-items: center; gap: 18px; padding: 0 12px; border-top: 1px solid var(--line); color: var(--muted); background: var(--panel); font-size: 12px; }
#state { color: var(--accent); } .error { color: #ee8f82 !important; }
</style>
</head>
<body>

<textarea id="editor" spellcheck="false" autofocus></textarea>
<footer><span id="state">连接中...</span><span id="latency">延迟 --</span><span id="server-time">服务器时间 --</span> <div class="title" id="filename"></div> </footer>
<script>
const fileName = __FILE_NAME__;
// 本页对应的 WebSocket 路由，按文件分发：/ws/realtime_editor/<md5前12位>
const wsPath = __WS_PATH__;
const editor = document.getElementById('editor');
const state = document.getElementById('state');
const latency = document.getElementById('latency');
const serverTime = document.getElementById('server-time');
document.getElementById('filename').textContent = fileName;
let socket;
let revision = 0;
let reconnectTimer;
let localChange = false;

function showServerTime(value) {
    if (value) serverTime.textContent = '服务器时间 ' + new Date(value).toLocaleString();
}
function connect() {
    clearTimeout(reconnectTimer);
    socket = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + wsPath);
    socket.onopen = () => { state.textContent = '已连接'; state.className = ''; };
    socket.onclose = () => {
        state.textContent = '连接断开，重连中'; state.className = 'error';
        reconnectTimer = setTimeout(connect, 1000);
    };
    socket.onerror = () => { state.textContent = '连接错误'; state.className = 'error'; };
    socket.onmessage = (event) => {
        const message = JSON.parse(event.data);
        showServerTime(message.server_time);
        if (message.type === 'snapshot') {
            revision = message.revision;
            if (document.activeElement !== editor || !localChange) editor.value = message.content;
            localChange = false;
        } else if (message.type === 'saved') {
            revision = message.revision;
            localChange = false;
            state.textContent = '同步完成'; state.className = '';
            if (message.sent_at) latency.textContent = '延迟 ' + Math.max(0, performance.now() - message.sent_at).toFixed(0) + ' ms';
        } else if (message.type === 'pong') {
            if (message.sent_at) latency.textContent = '延迟 ' + Math.max(0, performance.now() - message.sent_at).toFixed(0) + ' ms';
        } else if (message.type === 'changed') {
            revision = message.revision;
            // 记录当前光标位置，防止同步时闪动或光标跳到末尾
            const start = editor.selectionStart;
            const end = editor.selectionEnd;
            editor.value = message.content;
            editor.setSelectionRange(start, end);
            localChange = false;
            state.textContent = '已同步其他客户端修改';
        } else if (message.type === 'error') {
            state.textContent = message.message; state.className = 'error';
        }
    };
}
editor.addEventListener('input', () => {
    localChange = true;
    if (socket && socket.readyState === WebSocket.OPEN) {
        const sentAt = performance.now();
        socket.send(JSON.stringify({ type: 'edit', content: editor.value, revision, sent_at: sentAt }));
        state.textContent = '保存中...';
    }
});
setInterval(() => {
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ type: 'ping', sent_at: performance.now() }));
    }
}, 1000);
connect();
</script>
</body>
</html>'''


class Editor:
    def __init__(self, path):
        self.path = Path(path).expanduser().resolve()
        # 不存在则自动创建：URL 传入任意新路径即可开编，等价 nano 新文件
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text('', encoding='utf-8')
        # 按文件内容分发独立 ws 路由；md5 保证 URL 安全且跨进程稳定
        self.ws_path = '/ws/realtime_editor/' + hashlib.md5(str(self.path).encode('utf-8')).hexdigest()[:12]
        self.lock = threading.RLock()
        self.clients = set()
        self.revision = 0
        self.content = self._read()
        self.signature = self._signature()
        self.pending_signature = None
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Editor initialized for file: {self.path}")

    def _read(self):
        return self.path.read_text(encoding='utf-8')

    def _signature(self):
        try:
            stat = self.path.stat()
            return stat.st_mtime_ns, stat.st_size
        except FileNotFoundError:
            return None

    def _message(self, message_type, **values):
        return json.dumps({
            'type': message_type,
            'revision': self.revision,
            'server_time': datetime.now(timezone.utc).isoformat(),
            **values,
        }, ensure_ascii=False)

    # 增加 exclude 参数，用于在广播时跳过某个特定的客户端
    def broadcast(self, message, exclude=None):
        with self.lock:
            clients = tuple(self.clients)
        self.logger.debug(f"Broadcasting message type '{json.loads(message).get('type')}' to {len(clients)} client(s)")
        failed = []
        for client in clients:
            if client == exclude:
                continue
            try:
                client.send(message)
            except OSError:
                failed.append(client)
        if failed:
            with self.lock:
                self.clients.difference_update(failed)
            self.logger.warning(f"Removed {len(failed)} failed client(s) during broadcast")

    def save(self, content):
        temporary = self.path.with_name('.' + self.path.name + '.realtime-editor.tmp')
        temporary.write_text(content, encoding='utf-8',) # newline='' 低版本py不支持
        os.replace(temporary, self.path)
        self.content = content
        self.signature = self._signature()
        self.revision += 1
        self.logger.info(f"File saved, revision incremented to {self.revision}  {repr(content)}")

    def websocket(self, connection, _request):
        with self.lock:
            self.clients.add(connection)
            connection.send(self._message('snapshot', content=self.content))
        self.logger.info(f"WebSocket connection established, client count: {len(self.clients)}")
        try:
            while True:
                raw = connection.receive()
                if raw is None:
                    break
                self.logger.debug(f"Received raw message: {raw[:200]}{'...' if len(raw) > 200 else ''}")
                message = json.loads(raw)
                msg_type = message.get('type')
                if msg_type == 'ping':
                    self.logger.debug("Processing ping, sending pong")
                    connection.send(self._message('pong', sent_at=message.get('sent_at')))
                    continue
                if msg_type != 'edit' or not isinstance(message.get('content'), str):
                    self.logger.warning("Invalid message received (not edit or content not string), sending error")
                    connection.send(self._message('error', message='无效的编辑消息'))
                    continue

                self.logger.info(f"Received message  {connection.socket} type: {msg_type} len: {len(message.get('content',''))}")

                with self.lock:
                    self.save(message['content'])
                    # 1. 仅给发送方回复 saved（不带 content，因为发送方文本已经是最新的）
                    reply = self._message('saved', sent_at=message.get('sent_at'))
                    connection.send(reply)

                    # 2. 给其他所有客户端发送 changed（带有 content，强制更新他们的 textarea）
                    update_msg = self._message('changed', content=self.content)

                # 排除发送者本身，将内容广播给其他协同者
                self.broadcast(update_msg, exclude=connection)

        except (ConnectionError, OSError, ValueError, json.JSONDecodeError) as e:
            self.logger.warning(f"WebSocket connection terminated due to exception: {e}")
        finally:
            with self.lock:
                self.clients.discard(connection)
            self.logger.info(f"WebSocket connection closed, client count: {len(self.clients)}")

    def watch(self):
        self.logger.info("File watcher thread started")
        while True:
            time.sleep(0.25)
            current_signature = self._signature()
            with self.lock:
                if current_signature != self.signature and current_signature != self.pending_signature:
                    self.pending_signature = current_signature
                    continue
                self.pending_signature = None
                if current_signature == self.signature:
                    continue
                self.logger.info(f"External file change detected, previous signature: {self.signature}, new: {current_signature}")
                try:
                    self.content = self._read()
                except (FileNotFoundError, UnicodeDecodeError) as e:
                    self.logger.warning(f"Failed to read file after change: {e}")
                    self.signature = current_signature
                    continue
                self.signature = current_signature
                self.revision += 1
                message = self._message('changed', content=self.content)
                self.logger.info(f"Broadcasting 'changed' message, revision now {self.revision}")
            self.broadcast(message)


def get_editor(path=None):
    """取（必要时创建）某文件的编辑器。

    首次创建时把 editor.websocket 动态注册进 server_http.RPCRequestHandler.
    websocket_handlers——类属性字典在每个请求期实时读取，因此对已在运行的
    RPC 服务器（如 server_mqtt.py 启动的 1177）即刻生效，无需重启或命令行；
    同时启动外部修改监视线程。
    """
    resolved = Path(path or default_file).expanduser().resolve()
    with _editors_lock:
        editor = _editors.get(str(resolved))
        if editor is None:
            editor = Editor(resolved)
            _editors[str(resolved)] = editor
            server_http.RPCRequestHandler.websocket_handlers[editor.ws_path] = editor.websocket
            threading.Thread(target=editor.watch,
                             name=f'RealtimeEditorWatch-{editor.path.name}', daemon=True).start()
        return editor


def _render(editor):
    return (PAGE
            .replace('__FILE_NAME__', json.dumps(str(editor.path), ensure_ascii=False))
            .replace('__WS_PATH__', json.dumps(editor.ws_path)))


# ------------------------------------------------------------------ #
# HTTP RPC 入口：p / response 由 server_http.handle_rpc 注入
# ------------------------------------------------------------------ #
def html(p, file=None):
    """编辑页；file 缺省编辑 default_file，同一文件复用同一编辑器与 ws 路由。"""
    editor = get_editor(file)
    p.set_header('Content-Type', 'text/html; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    p.set_data(_render(editor))


preview_html = html


def status(p):
    """列出当前进程内所有编辑器：文件、ws 路由、在线客户端数、revision。"""
    return {str(e.path): {'ws_path': e.ws_path, 'clients': len(e.clients), 'revision': e.revision}
            for e in list(_editors.values())}


def main():
    parser = argparse.ArgumentParser(description='浏览器实时双向编辑服务器文件')
    parser.add_argument('file', help='要编辑的 UTF-8 文本文件')
    parser.add_argument('--port', type=int, default=1133)
    parser.add_argument('--host', default='0.0.0.0')
    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    global default_file
    default_file = args.file
    server = server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
        redirect_root='/preview_html(p)',
    )
    # websocket_handlers 现为共享注册表、只合并不替换：注册与 start 的
    # 先后顺序已无约束（preview_html 也会惰性补注册）
    editor = get_editor(args.file)
    print(f'Open http://127.0.0.1:{args.port}/preview_html(p)')
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.shutdown()

#import sys;'qgb.U' in sys.modules or sys.path.append('C:/QGB/miniforge3/Lib/site-packages/pythonwin/');from qgb import *
if __name__ == '__main__':
    main()
