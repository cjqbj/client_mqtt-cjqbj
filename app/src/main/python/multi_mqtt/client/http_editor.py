"""纯 HTTP 协议的浏览器文件编辑器：界面同 realtime_editor.py，无 WebSocket。

与 realtime_editor 的区别：不要求实时同步——去掉 ws 路由注册、多标签广播、
文件监视线程；页面里每个动作（加载、保存、分块上传、探活）都是一次普通
HTTP GET，path 即 Python 表达式，由 server_http.handle_rpc 在持久命名空间里
执行（rdp.py 同款通信方式）。本模块零框架依赖、不 import server_http，
被已在运行的 RPC 服务（server_mqtt / server_http）URL import 即生效：

    http://192.168.1.12:1177/from%20client%20import%20http_editor;http_editor.html(p)

路径可不加引号（server_http.handle_rpc 对整段语法错误的代码有裸路径救援，
会自动给 C:/x/y、/tmp/txt 这类 token 补引号后重试）：

    http://192.168.1.12:1177/from%20client%20import%20http_editor;http_editor.html(p, C:/tmp/note.txt)

保存语义：输入停顿 0.8 秒自动保存（Ctrl+S 立即保存），临时文件 + os.replace
原子落盘；文件不存在时自动创建（含父目录）。多标签/多人同时编辑为最后写入
者胜，不做冲突检测。大文件超出单条 URL 承载（http.server 请求行上限
65536 字节）时页面自动切换分块上传：up_begin / up_push / up_end。

RPC 入口（p / response 由 server_http.handle_rpc 注入）：
    http_editor.html(p, file=None)           编辑页；file 缺省编 default_file
    http_editor.read(p, key=None)            读文件原文；key 为 token 或路径
    http_editor.save(p, key=None, content)   小文件整体保存
    http_editor.up_begin(p, key=None)        分块上传开始，返回 uid
    http_editor.up_push(p, uid, chunk)       追加一块，返回累计字符数
    http_editor.up_end(p, uid)               合并落盘，返回 revision
    http_editor.ping(p)                      探活 + 服务器时间
    http_editor.status(p)                    列出已打开文件与 revision
"""
import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# html(p) 未传 file 时编辑的文件（相对服务器进程 cwd），与 realtime_editor 一致
default_file = 'http_editor.txt'

_editors = globals().setdefault('_editors', {})  # str(绝对路径) -> Editor。setdefault 防 importlib.reload：reload 复用同一模块 __dict__ 重执行顶层，普通赋值把名字重绑到空 dict——已开编辑器变孤儿、同一文件被再 new 一个 Editor 双实例并发写；setdefault 在 reload 后原样取回旧表
_by_token = globals().setdefault('_by_token', {})  # token -> Editor；token = 绝对路径 md5 前 12 位，URL 安全。必须与 _editors 同为 setdefault：reload 丢表 = 已开页面的 token URL 全部 404
_lock = globals().setdefault('_lock', threading.RLock())  # 锁必须随表一起 setdefault：reload 重造新锁对象后，已攥旧锁的线程与拿新锁的线程互不排斥、同时进临界区
_uploads = globals().setdefault('_uploads', {})  # uid -> {'editor': Editor, 'parts': [str], 'started': float}。reload 丢表 = 进行中的分块上传会话悬挂失联
UPLOAD_TTL = 600    # 分块会话最长存活秒数，超时由下一次 up_begin 清扫


class Editor:
    """单个文件的读写与 revision 计数；无 ws 客户端集合、无外部修改监视线程。"""

    def __init__(self, path):
        self.path = Path(path).expanduser().resolve()
        # 不存在则自动创建：URL 传入任意新路径即可开编，等价 nano 新文件
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text('', encoding='utf-8')
        # 按绝对路径分发 token：页面后续 RPC 只带 12 位 hex，URL 短且免引号转义
        self.token = hashlib.md5(str(self.path).encode('utf-8')).hexdigest()[:12]
        self.lock = threading.RLock()
        self.revision = 0

    def read(self):
        return self.path.read_text(encoding='utf-8')

    def write(self, content):
        # 临时文件 + os.replace 原子落盘，读方永远不会看到半截文件
        temporary = self.path.with_name('.' + self.path.name + '.http-editor.tmp')
        temporary.write_text(content, encoding='utf-8')
        os.replace(temporary, self.path)
        with self.lock:
            self.revision += 1
            return self.revision


def get_editor(path=None):
    """取（必要时创建）某文件的编辑器并登记 token 映射；同文件复用同实例。"""
    resolved = Path(path or default_file).expanduser().resolve()
    with _lock:
        editor = _editors.get(str(resolved))
        if editor is None:
            editor = Editor(resolved)
            _editors[str(resolved)] = editor
            _by_token[editor.token] = editor
        return editor


def _resolve(key=None):
    """key 先按 token 匹配（页面用）；匹配不上再当文件路径，方便 curl 手工调用。"""
    if key is None:
        return get_editor(None)
    with _lock:
        editor = _by_token.get(str(key))
    return editor if editor is not None else get_editor(key)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json(p, payload):
    p.set_header('Content-Type', 'text/plain; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    p.set_header('X-Server-Time', _now())
    p.set_data(json.dumps(payload, ensure_ascii=False))


# ------------------------------------------------------------------ #
# HTTP RPC 入口：p / response 由 server_http.handle_rpc 注入
# ------------------------------------------------------------------ #
def html(p, file=None):
    """编辑页；file 缺省编辑 default_file。同一文件 token 稳定，可重复打开。"""
    editor = get_editor(file)
    p.set_header('Content-Type', 'text/html; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    p.set_data(PAGE
               .replace('__FILE_NAME__', json.dumps(str(editor.path), ensure_ascii=False))
               .replace('__TOKEN__', json.dumps(editor.token)))


preview_html = html


def read(p, key=None):
    """读文件原文；text/plain 原样返回，不经 format_result 包 repr。"""
    editor = _resolve(key)
    p.set_header('Content-Type', 'text/plain; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    p.set_header('X-Server-Time', _now())
    p.set_header('X-Revision', str(editor.revision))
    p.set_data(editor.read())


def save(p, key=None, content=''):
    """整体保存：小文件一条 URL 搞定；超出单条 URL 承载请走 up_* 分块。"""
    editor = _resolve(key)
    content = str(content)
    revision = editor.write(content)
    return _json(p, {'ok': True, 'revision': revision, 'size': len(content)})


def _sweep_uploads(now):
    for uid in [k for k, v in _uploads.items() if now - v['started'] > UPLOAD_TTL]:
        del _uploads[uid]


def up_begin(p, key=None):
    """分块上传开始：作废同文件旧会话与全局超时会话，返回新 uid。"""
    editor = _resolve(key)
    with _lock:
        _sweep_uploads(time.time())
        for uid in [k for k, v in _uploads.items() if v['editor'] is editor]:
            del _uploads[uid]
        uid = uuid.uuid4().hex[:12]
        _uploads[uid] = {'editor': editor, 'parts': [], 'started': time.time()}
    return _json(p, {'ok': True, 'uid': uid})


def up_push(p, uid, chunk):
    with _lock:
        upload = _uploads.get(str(uid))
        if upload is None:
            raise RuntimeError('upload session not found: %r' % (uid,))
        upload['parts'].append(str(chunk))
        received = sum(len(x) for x in upload['parts'])
    return _json(p, {'ok': True, 'received': received})


def up_end(p, uid):
    with _lock:
        upload = _uploads.pop(str(uid), None)
    if upload is None:
        raise RuntimeError('upload session not found: %r' % (uid,))
    content = ''.join(upload['parts'])
    revision = upload['editor'].write(content)
    return _json(p, {'ok': True, 'revision': revision, 'size': len(content)})


def ping(p):
    return _json(p, {'ok': True, 'server_time': _now()})


def status(p):
    """列出进程内全部编辑器：token、绝对路径、revision，及未完成的分块会话数。"""
    with _lock:
        result = {token: {'path': str(editor.path), 'revision': editor.revision}
                  for token, editor in _by_token.items()}
        result['uploads_pending'] = len(_uploads)
        return result


PAGE = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HTTP Editor</title>
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
<footer><span id="state">加载中...</span><span id="latency">延迟 --</span><span id="server-time">服务器时间 --</span> <div class="title" id="filename"></div> </footer>
<script>
const BASE = location.pathname.split(';')[0] + ';';   // /from%20client%20import%20http_editor;
const fileName = __FILE_NAME__;
const token = __TOKEN__;                              // 路径 md5 前 12 位，页面 RPC 凭它定位文件
const editor = document.getElementById('editor');
const state = document.getElementById('state');
const latency = document.getElementById('latency');
const serverTime = document.getElementById('server-time');
document.getElementById('filename').textContent = fileName;
let revision = 0;
let dirty = false;
let saving = false;
let saveTimer = null;

async function rpc(expr) {
    const sentAt = performance.now();
    const resp = await fetch(BASE + encodeURIComponent(expr), { cache: 'no-store' });
    latency.textContent = '延迟 ' + Math.max(0, Math.round(performance.now() - sentAt)) + ' ms';
    const server = resp.headers.get('X-Server-Time');
    if (server) serverTime.textContent = '服务器时间 ' + new Date(server).toLocaleString();
    const text = await resp.text();
    if (!resp.ok) throw new Error(text.trim().split('\n').pop() || ('HTTP ' + resp.status));
    return text;
}

function scheduleSave(delay) { clearTimeout(saveTimer); saveTimer = setTimeout(save, delay); }

// 大内容按编码后长度切块：CJK 每字符 9 字节、标点至多 3 字节，预算 15000
// 留足余量，单条 URL 仍远小于 http.server 请求行 65536 字节上限
function splitChunks(text, budget) {
    const chunks = [];
    let size = 4000, i = 0;
    while (i < text.length) {
        let piece = text.slice(i, i + size);
        while (piece.length > 1 && encodeURIComponent(piece).length > budget) {
            piece = piece.slice(0, Math.ceil(piece.length / 2));
        }
        size = piece.length;
        chunks.push(piece);
        i += piece.length;
        if (encodeURIComponent(piece).length * 2 < budget) size = Math.min(16000, size * 2);
    }
    return chunks;
}

async function save() {
    if (saving) return;
    const content = editor.value;
    saving = true;
    state.textContent = '保存中...'; state.className = '';
    try {
        let text;
        const expr = "http_editor.save(p,'" + token + "'," + JSON.stringify(content) + ")";
        if (encodeURIComponent(expr).length <= 15000) {
            text = await rpc(expr);
        } else {
            const uid = JSON.parse(await rpc("http_editor.up_begin(p,'" + token + "')")).uid;
            const chunks = splitChunks(content, 15000);
            for (let i = 0; i < chunks.length; i++) {
                state.textContent = '上传中 ' + (i + 1) + '/' + chunks.length;
                await rpc("http_editor.up_push(p,'" + uid + "'," + JSON.stringify(chunks[i]) + ")");
            }
            text = await rpc("http_editor.up_end(p,'" + uid + "')");
        }
        const info = JSON.parse(text);
        revision = info.revision;
        if (editor.value === content) { dirty = false; state.textContent = '已保存 revision ' + revision; }
        else { state.textContent = '已保存，新改动待存'; scheduleSave(200); }
    } catch (err) {
        state.textContent = '保存失败: ' + err.message; state.className = 'error';
    } finally {
        saving = false;
    }
}

editor.addEventListener('input', () => {
    dirty = true;
    state.textContent = '未保存';
    state.className = '';
    scheduleSave(800);
});
document.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && (e.key === 's' || e.key === 'S')) {
        e.preventDefault();
        clearTimeout(saveTimer);
        save();
    }
});
window.addEventListener('beforeunload', e => {
    if (dirty) { e.preventDefault(); e.returnValue = ''; }
});

async function load() {
    state.textContent = '加载中...';
    try {
        editor.value = await rpc("http_editor.read(p,'" + token + "')");
        dirty = false;
        state.textContent = '已加载';
    } catch (err) {
        state.textContent = '加载失败: ' + err.message; state.className = 'error';
    }
}

// 低频探活：刷新延迟/服务器时间，断网时把状态栏切到错误色
setInterval(() => {
    rpc("http_editor.ping(p)").catch(err => {
        state.textContent = '连接异常: ' + err.message; state.className = 'error';
    });
}, 30000);
load();
</script>
</body>
</html>'''
