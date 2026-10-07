import argparse
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

#import rpc
import server_http

logger = logging.getLogger(__name__)

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
    socket = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
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


def preview_html(response_obj):
    response_obj.set_header('Content-Type', 'text/html; charset=utf-8')
    response_obj.set_data(PAGE.replace('__FILE_NAME__', json.dumps(str(editor.path.name), ensure_ascii=False)))


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

    global editor
    editor = Editor(args.file)
    threading.Thread(target=editor.watch, name='RealtimeEditorWatcher', daemon=True).start()
    server= server_http.start_rpc_server(
        port=args.port,
        ip=args.host,
        globals=globals(),
        locals=locals(),
        websocket_handler=editor.websocket,
        websocket_path='/ws',
        redirect_root='/preview_html(p)',
    )
    print(f'Open http://127.0.0.1:{args.port}/preview_html(p)')
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        server.shutdown()

#import sys;'qgb.U' in sys.modules or sys.path.append('C:/QGB/miniforge3/Lib/site-packages/pythonwin/');from qgb import *
if __name__ == '__main__':
    main()