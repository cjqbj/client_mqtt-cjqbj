#!/usr/bin/env python3
import sys, io, re, urllib.parse, threading, traceback, struct, base64, hashlib, socket, logging, asyncio, textwrap, inspect, ast, hmac, weakref, json, os, tempfile, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

import rpc_executor
from rpc_executor import PythonExecutor, format_result ,stime, utc_ms


# --------------------------------------------------------------------------- #
# 别名持久化：默认 rpc.json 与本文件同目录（测试 / 部署可改 RPC_CONFIG_PATH）
# --------------------------------------------------------------------------- #
# 与 server_mqtt 共用同一份 rpc.json：server_mqtt 写 request_topic 等键，
# 这里只读写自己的 http_aliases 段，读-改-写时其它键原样保留，互不覆盖。
RPC_CONFIG_PATH = Path(__file__).with_name("rpc.json")
ALIAS_CONFIG_KEY = 'http_aliases'
# 打开站点首页 / 时执行的别名名：定义 #alias root 后 / 直接显示它产出的
# HTML（例如 response.set_data(open('/home/<user>/apk.html').read())）。
ROOT_ALIAS = 'root'
# reload 会重造锁对象：正在写 rpc.json 的线程与拿新锁的线程会同时进临界区，
# setdefault 保住同一把锁跨 reload（与 server_mqtt 的写法一致）。
_ALIAS_CONFIG_LOCK = globals().setdefault(
    '_ALIAS_CONFIG_LOCK', threading.RLock())
# #alias 指令行尾出现这些标记 = 只注册到内存、不写 rpc.json：
#     #alias apk !      #alias apk np      #alias apk!
# 标记在本模块被剥掉后再交给执行器，rpc_executor.py 一行不改。
_ALIAS_EPHEMERAL_FLAGS = {'!', 'np', 'no-persist', 'nopersist', 'temp', 'ephemeral'}


def _alias_config_path(path=None):
    return Path(path) if path is not None else RPC_CONFIG_PATH


def _read_config_document(path=None):
    """读取 rpc.json 文档，返回 dict。

    文件不存在返回 {}；JSON 损坏 / 根节点不是 object 时返回 None —— 调用方
    据此拒绝写入，绝不用空文档覆盖用户已有的配置文件。
    """
    config_path = _alias_config_path(path)
    with _ALIAS_CONFIG_LOCK:
        try:
            with config_path.open('r', encoding='utf-8') as config_file:
                document = json.load(config_file)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as error:
            logging.getLogger('app').warning(
                '别名持久化读取失败，跳过 %s: %s', config_path, error)
            return None
        if not isinstance(document, dict):
            logging.getLogger('app').warning(
                '别名持久化跳过：%s 根节点不是 JSON object', config_path)
            return None
        return document


def _persisted_alias_records(path=None):
    """返回 rpc.json 里 http_aliases 段的 name -> ``{'code','signature'}``。

    兼容两种值形态：字符串（plain / prefix 别名）与 object（params 别名
    带签名）。其它形态跳过，不让手工编辑过的文件打断启动。params 别名
    只存名字 + 签名，模式 / 可绑定集 / 默认值启动时由执行器重新推导，
    避免文件里的派生字段过期。
    """
    document = _read_config_document(path)
    if not document:
        return {}
    stored = document.get(ALIAS_CONFIG_KEY)
    if not isinstance(stored, dict):
        return {}
    records = {}
    for name, value in stored.items():
        if isinstance(value, str):
            records[name] = {'code': value, 'signature': None}
        elif isinstance(value, dict):
            code = value.get('code')
            if not isinstance(code, str):
                continue
            signature = value.get('signature')
            records[name] = {
                'code': code,
                'signature': signature if isinstance(signature, str) else None,
            }
    return records


def _persisted_alias_codes(path=None):
    """兼容旧接口：只取 name -> code（params 签名请用 _persisted_alias_records）。"""
    return {name: payload['code']
            for name, payload in _persisted_alias_records(path).items()}


def _update_persisted_aliases(updates=None, deletes=(), path=None):
    """按名字增删 rpc.json 的 http_aliases 段，返回 True 表示文件已改写。

    读-改-写在模块级 RLock 内完成，并用同目录临时文件 + os.replace 原子
    替换：并发请求（HTTP worker / WSGI 分发线程）不会读到半截 JSON。
    """
    updates = updates or {}
    with _ALIAS_CONFIG_LOCK:
        document = _read_config_document(path)
        if document is None:
            return False
        stored = document.get(ALIAS_CONFIG_KEY)
        aliases = dict(stored) if isinstance(stored, dict) else {}
        changed = False
        for name, code in updates.items():
            if aliases.get(name) != code:
                aliases[name] = code
                changed = True
        for name in deletes:
            if aliases.pop(name, None) is not None:
                changed = True
        if not changed:
            return False
        if aliases:
            document[ALIAS_CONFIG_KEY] = aliases
        else:
            document.pop(ALIAS_CONFIG_KEY, None)
        config_path = _alias_config_path(path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        handle, temp_name = tempfile.mkstemp(
            prefix=config_path.name + '.', suffix='.tmp',
            dir=str(config_path.parent))
        try:
            with os.fdopen(handle, 'w', encoding='utf-8') as temp_file:
                json.dump(document, temp_file, ensure_ascii=False, indent=2)
                temp_file.write('\n')
            os.replace(temp_name, config_path)
        except BaseException:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
        return True


def load_persisted_aliases(executor, path=None):
    """启动时把 rpc.json 的 http_aliases 装回执行器别名表（内存优先）。

    返回本次恢复的 name -> code。命名空间里已存在的同名别名不覆盖；非法
    名字 / 空代码 / 坏签名跳过，防止手工编辑过的文件把执行器搞脏。名字
    规则与 #alias 一致（rpc_executor.is_valid_alias_name），带点的
    ime.apk、params 的 add(x, y=3)、prefix 的 git+ 都能长期恢复；模式 /
    可绑定集 / 默认值由执行器按签名重新推导，不信任文件里的派生字段。
    """
    if executor is None or not getattr(executor, 'alias', False):
        return {}
    now = utc_ms()
    restored = {}
    for name, payload in _persisted_alias_records(path).items():
        code = payload['code']
        if (not code.strip() or not isinstance(name, str)
                or not rpc_executor.is_valid_alias_name(name)):
            continue
        if name in executor.aliases:
            continue
        try:
            restored_name, record = rpc_executor.build_alias_record(
                name, code, signature=payload['signature'], now=now,
                decoded_name=True)
        except (ValueError, SyntaxError) as error:
            logging.getLogger('app').warning(
                '别名持久化跳过 %r：%s', name, error)
            continue
        executor.aliases[restored_name] = record
        restored[restored_name] = code
    if restored:
        logging.getLogger('app').info(
            '别名持久化: 从 %s 恢复 %d 个 (%s)',
            _alias_config_path(path), len(restored), ', '.join(sorted(restored)))
    return restored


def sync_alias_persistence(executor, execution, ephemeral=False, path=None):
    """把一次执行里注册 / 更新 / 删除的别名同步进 rpc.json。

    ``ephemeral=True``（本次定义带“不持久化”标记）时不写入，并把同名旧
    记录一并清掉，避免文件里留一份过期副本。其它指令（show / list）不动
    文件，避免每次查询都产生写放大与 hits 计数写入。

    plain / prefix 别名仍以字符串落盘（与旧格式逐字节兼容）；params 别名
    落盘为 ``{"code": ..., "signature": ...}``，否则重启后签名丢失、别名
    退化成 plain。
    """
    info = execution.get('alias') if isinstance(execution, dict) else None
    if not isinstance(info, dict):
        return None
    action = info.get('action')
    if action in ('saved', 'updated'):
        name, code = info.get('name'), info.get('code')
        if not isinstance(name, str) or not isinstance(code, str):
            return None
        if ephemeral:
            return _update_persisted_aliases(deletes=[name], path=path)
        signature = info.get('signature')
        payload = (code if signature is None
                   else {'code': code, 'signature': signature})
        return _update_persisted_aliases(updates={name: payload}, path=path)
    if action == 'unalias':
        deleted = [name for name in (info.get('deleted') or [])
                   if isinstance(name, str)]
        if deleted:
            return _update_persisted_aliases(deletes=deleted, path=path)
    return None


def _alias_flag_tokens(text):
    """把 ``#alias`` 首行参数按空白切成 token，字符串字面量整体保留原文。

    ``"a b" c`` → ``['"a b"', 'c']``；字面量内部的空格不会被折叠，字面量
    后面的 ``+`` / ``!`` 等后缀仍算在同一个 token 里。引号未闭合 / 转义
    非法时退回普通空白切分，交给执行器给出正式报错。
    """
    tokens = []
    index, size = 0, len(text)
    while index < size:
        if text[index].isspace():
            index += 1
            continue
        start = index
        if text[index] in '"\'':
            try:
                _, index = rpc_executor.scan_string_literal(text, index)
            except ValueError:
                index = start
                while index < size and not text[index].isspace():
                    index += 1
                tokens.append(text[start:index])
                continue
        while index < size and not text[index].isspace():
            index += 1
        tokens.append(text[start:index])
    return tokens


def _is_pure_string_literal(token):
    """token 是否恰好是一个完整的字符串字面量（引号里写的不当标记看）。"""
    if not token or token[0] not in '"\'':
        return False
    try:
        _, end = rpc_executor.scan_string_literal(token, 0)
    except ValueError:
        return False
    return end == len(token)


def strip_ephemeral_alias_flag(code, executor):
    """识别 #alias 指令行里的“不持久化”标记，返回 (code, ephemeral)。

    支持 ``#alias apk !`` / ``#alias apk np`` / ``#alias apk!`` 三种写法；
    标记只在首行生效，命中后剥掉再交给执行器（执行器不允许指令行出现
    第二个名字，因此标记必须在本层处理）。分词是**引号感知**的：
    ``#alias "a b" !`` 只剥掉末尾标记、字面量原样保留；``#alias "apk!"``
    里的 ``!`` 属于名字，不触发临时语义。
    """
    if executor is None or not getattr(executor, 'alias', False):
        return code, False
    if not isinstance(code, str) or not code.strip():
        return code, False
    head, separator, body = code.lstrip().partition('\n')
    token = head.strip()
    if not token.startswith('#'):
        return code, False
    words = token[1:].split(None, 1)
    if not words or words[0] not in getattr(executor, 'alias_keywords', ()):
        return code, False
    args = _alias_flag_tokens(words[1] if len(words) > 1 else '')
    if not args:
        return code, False
    ephemeral = False
    # 1) 独立标记 token（! / np / ...）；引号字面量里的同名文本是名字
    if not _is_pure_string_literal(args[-1]) \
            and args[-1].lower() in _ALIAS_EPHEMERAL_FLAGS:
        args = args[:-1]
        ephemeral = True
    # 2) 紧贴在名字末尾的 !（#alias apk! / #alias "a b"!）
    if args and not ephemeral and args[-1].endswith('!'):
        stripped = args[-1].rstrip('!')
        if stripped:
            args[-1] = stripped
            ephemeral = True
    if not ephemeral or not args:
        return code, False
    rebuilt = '#' + ' '.join([words[0]] + args)
    return rebuilt + (separator + body if separator else ''), True


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


def _status_of(text):
    """从 http.server 的访问行里取状态码；取不到返回 0（按“不可判定”当普通日志）。"""
    try:
        return int(text.rsplit('"', 1)[-1].split()[0])
    except Exception:
        return 0


# --------------------------------------------------------------------------- #
# 访问行静音过滤器：下游模块（例如 rdp 控制页的「服务端日志」开关）用
#
#     server_http.add_log_filter('rdp-slog', predicate)
#
# 注入「这条请求要不要静音」的判定，而不是在 log_message 里堆 if 分支。
# 约定：
#   * predicate(handler) 返回 True = 该次请求的访问行不写；假值 = 照常写；
#   * 只对 2xx/3xx 询问过滤器——4xx/5xx 一律保留，便于排查协议层/业务失败；
#   * 过滤器自身出错只降级成「照常写」，并对该过滤器打一次 WARNING，
#     绝不让日志钩子把请求打挂、也不静默丢日志；
#   * 同名注册覆盖旧函数（模块 reload 重复注册不会累积）；
#   * 过滤器只应做纯内存判断（读头/path），不许 I/O、不许阻塞。
# 用 setdefault 取回旧 dict：server_http 被 reload 后已注册的过滤器还在，
# 配合 rdp 模块 reload 时的同名覆盖，双向 reload 都不会丢规则或重复。
# --------------------------------------------------------------------------- #
_LOG_FILTERS = globals().setdefault('_LOG_FILTERS', {})
_WARNED_FILTERS = globals().setdefault('_WARNED_FILTERS', set())


def add_log_filter(name, predicate):
    """注册/替换一个访问行静音过滤器（name 相同即覆盖）。

    下游模块在 import 时调用即可；传入不可调用对象属于显式调用错误，
    当场抛 TypeError（文件/运行时错误才走降级，见 _log_silenced）。
    """
    if not callable(predicate):
        raise TypeError(f'日志过滤器 {name!r} 必须是可调用对象')
    _LOG_FILTERS[name] = predicate
    _WARNED_FILTERS.discard(name)
    return predicate


def remove_log_filter(name):
    """移除已注册的过滤器，返回是否真的移除了一个。"""
    removed = _LOG_FILTERS.pop(name, None) is not None
    _WARNED_FILTERS.discard(name)
    return removed


def _log_silenced(handler):
    """任一过滤器返回 True 即静音本次访问行；过滤器异常按“照常写”降级。"""
    for name, predicate in list(_LOG_FILTERS.items()):
        try:
            if predicate(handler):
                return True
        except Exception as error:
            if name not in _WARNED_FILTERS:
                _WARNED_FILTERS.add(name)
                logging.getLogger('app').warning(
                    '日志过滤器 %r 异常，已降级为照常输出: %r', name, error)
    return False


# 本模块不内置任何静音规则（包括 /mcp）：规则属于拥有该端点的下游模块，
# 由它们 import 时自行 add_log_filter(...) 注入（rdp 页、pty 的 MCP 服务都是
# 这样）。server_http reload 后注册表经 setdefault 保活，重新加载下游模块时
# 用同名注册覆盖即可，两侧都不会丢规则或重复判定。


ALIAS_URL_DASH = 'dash'
ALIAS_URL_RAW = 'raw'


def normalize_alias_url(value):
    """把 alias_url 配置归一化成 ``(路由模式, 是否 JSON 解码 query 值)``。

    路由形态只有 alias.md 8.2 定义的两种：

    * ``'dash'``（默认，兼容旧名 ``'keyed'``）—— 别名永远带 ``-`` 前缀；
    * ``'raw'`` —— 额外允许 ``/<别名>`` 免前缀（``/-<别名>`` 仍然可用）。

    ``'json'`` **不是第三种路由形态**（alias.md 8.3）：它只表示对 query
    值尝试 ``json.loads``，失败保留原始字符串。raw 是 dash 的超集，因此
    ``'json'`` 归一到 raw + 解码；需要 dash 路由 + JSON 解码时写
    ``'dash-json'``。未知值属于显式配置错误，当场 ValueError。

    ``ASGI / WSGI`` 垫片与 ``start_rpc_server`` 都经这里归一化，保证
    ``_conf(handler, 'alias_url')`` 在运行期只有两种稳定取值。
    """
    text = str(value or ALIAS_URL_DASH).strip().lower()
    if text in ('json', 'raw-json', 'json-raw'):
        return ALIAS_URL_RAW, True
    if text in ('dash-json', 'json-dash', 'keyed-json', 'json-keyed'):
        return ALIAS_URL_DASH, True
    if text in ('dash', 'keyed', 'key', 'key-prefix', 'after-key'):
        return ALIAS_URL_DASH, False
    if text in ('raw', 'url', 'original', 'before-key'):
        return ALIAS_URL_RAW, False
    raise ValueError(
        f"alias_url 只支持 'dash' / 'raw' / 'json'，收到 {text!r}")


def _alias_url_mode(handler):
    """当前 handler 的路由模式；运行期配置坏掉只降级为默认 dash。"""
    try:
        return normalize_alias_url(_conf(handler, 'alias_url'))[0]
    except ValueError:
        return ALIAS_URL_DASH


def parse_query_params(query_string):
    """把 URL query 解析成 dict（重复 key 按 HTTP 标准 last-wins）。

    容错：坏编码 / 非字符串输入返回 {}，绝不因为 query 解析让请求失败。
    WSGI / ASGI 垫片用原始 ``QUERY_STRING``（未解码）调用本函数，原生
    handler 用 ``self.path`` 里 ``?`` 之后的部分；只解析一次，避免二次
    解码把 ``%25`` 变成 ``%``。
    """
    if not query_string or not isinstance(query_string, str):
        return {}
    try:
        pairs = urllib.parse.parse_qsl(query_string, keep_blank_values=True)
    except Exception:
        return {}
    params = {}
    for key, value in pairs:
        params[key] = value
    return params


def decode_alias_query(params, as_json=False):
    """query dict → 注入执行器的调用方数据（alias.md 8.3）。

    默认原样传字符串（``?x=4`` → ``'4'``，body 自己 ``int(x)``）；
    ``as_json=True``（``alias_url='json'``）时逐值尝试 ``json.loads``，
    失败保留原始字符串——单个值坏掉不牵连同组其它值。
    """
    if not isinstance(params, dict):
        return {}
    if not as_json:
        return dict(params)
    decoded = {}
    for key, value in params.items():
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except (ValueError, TypeError):
                pass
        decoded[key] = value
    return decoded


def _alias_exact_record(executor, code):
    """``code`` 整段精确命中的别名记录；``=arg`` / prefix 形态返回 None。

    HTTP 只对 exact 调用注入 query 参数（alias.md 8.1）：``=arg`` 的参数
    属于 ``_alias_arg``，prefix 的剩余串同理，都是正交机制，不能被 query
    覆盖；prefix 记录本身也没有可绑定集。
    """
    if executor is None or not getattr(executor, 'alias', False):
        return None
    aliases = getattr(executor, 'aliases', None)
    if not isinstance(aliases, dict) or not isinstance(code, str):
        return None
    info = aliases.get(code.strip())
    if not isinstance(info, dict):
        return None
    mode = info.get('mode')
    if mode == 'prefix' or code.strip().endswith(rpc_executor.ALIAS_PREFIX_SUFFIX):
        return None
    return info


def _is_alias_url(handler, raw_path):
    """alias_url='raw' 时，判断路径（去掉前导 /）是否命中已注册别名。

    三种形态都放行（均只匹配**已注册**别名，非别名路径仍 403）：

    * ``/<别名>`` —— 整段 exact（含 ``git+`` 这种前缀记录名）；
    * ``/<别名>=<参数>`` —— ``=arg`` 形态，参数交给执行器的 _expand_alias，
      别名 body 里用 ``_alias_arg`` 读（如 ``/git_pull=/home/u/proj``）；
    * ``/<前缀><剩余>`` —— prefix 别名命中（如 ``/git status``、``/ime /a.apk``），
      剩余串进 ``_alias_arg``。

    dash 模式不放开免前缀形态，但 ``/-<别名>`` 仍由 key 路由处理。执行器
    alias 总开关关闭时不放行。query 部分不是路径，已由 handle_rpc 在调用
    本函数前切开。
    """
    if _alias_url_mode(handler) != ALIAS_URL_RAW:
        return False
    executor = _conf(handler, 'executor')
    if executor is None or not getattr(executor, 'alias', False):
        return False
    aliases = getattr(executor, 'aliases', None)
    if not isinstance(aliases, dict) or not raw_path.startswith('/'):
        return False
    candidate = raw_path[1:]
    if not getattr(handler, 'path_already_decoded', False):
        candidate = urllib.parse.unquote(candidate)
    if candidate in aliases:
        return True
    call = rpc_executor.split_alias_call(candidate)
    if call is not None and call[0] in aliases:
        return True
    return rpc_executor.match_prefix_alias(aliases, candidate) is not None


def _is_alias_call(executor, code):
    """``code`` 是否会被执行器当作别名调用展开。

    命中三种形态：整段 exact 的别名名、``<别名>=<参数>``、以及前缀别名
    命中（``git status``）。别名调用本身可能不是合法 Python（
    ``where=/home/u/proj`` 是非法赋值、``git status`` 是两个未定义名字），
    必须跳过下面的裸路径救援，否则参数 / 剩余串会被补引号再交给 body。
    """
    if executor is None or not getattr(executor, 'alias', False):
        return False
    aliases = getattr(executor, 'aliases', None)
    if not isinstance(aliases, dict) or not isinstance(code, str):
        return False
    text = code.strip()
    if text in aliases:
        return True
    call = rpc_executor.split_alias_call(text)
    if call is not None and call[0] in aliases:
        return True
    return rpc_executor.match_prefix_alias(aliases, text) is not None


SERVE_FILE_MARKER = '#serve-file'
SERVE_FILE_CHUNK = 64 * 1024
# 同一个坏规格只告警一次：坏别名每次请求都解析，逐次 warning 会刷爆日志
_WARNED_SERVE_SPEC = set()


def parse_serve_file(code):
    """解析别名 body 首行的 ``#serve-file {json}`` 文件直出规格。

    规格字段（都容错）：path（必填）、filename、content_type、
    rate_limit（字节/秒，0 = 全速）、chunk_size。不是该形态、JSON 坏了、
    字段类型写错，一律返回 None 并只打一次 WARNING——调用方退回普通别名
    执行，下载端点坏掉不能牵连 RPC 主流程。
    """
    if not isinstance(code, str):
        return None
    head = code.lstrip().partition('\n')[0].strip()
    if not head.startswith(SERVE_FILE_MARKER):
        return None
    payload = head[len(SERVE_FILE_MARKER):].strip()
    try:
        spec = json.loads(payload)
        if not isinstance(spec, dict):
            raise ValueError('规格不是 JSON object')
        path = spec.get('path')
        if not isinstance(path, str) or not path.strip():
            raise ValueError('path 缺失')
        rate_limit = int(spec.get('rate_limit') or 0)
        chunk_size = int(spec.get('chunk_size') or SERVE_FILE_CHUNK)
    except (ValueError, TypeError) as error:
        if payload not in _WARNED_SERVE_SPEC:
            _WARNED_SERVE_SPEC.add(payload)
            logging.getLogger('app').warning(
                '别名文件直出规格无效，退回普通执行: %s (%s)',
                payload[:120], error)
        return None
    filename = spec.get('filename')
    if not isinstance(filename, str) or not filename.strip():
        filename = os.path.basename(str(path).rstrip('/\\')) or 'download.bin'
    content_type = spec.get('content_type')
    if not isinstance(content_type, str) or not content_type.strip():
        content_type = 'application/octet-stream'
    return {
        'path': str(path),
        'filename': filename,
        'content_type': content_type,
        'rate_limit': max(0, rate_limit),
        'chunk_size': max(1024, chunk_size),
    }


def _serve_file_spec(executor, code):
    """``code`` 精确命中带文件直出规格的别名时返回 spec，否则 None。

    只认整段精确的别名名：``<别名>=<参数>`` 不退化成任意路径读取。
    """
    if executor is None or not getattr(executor, 'alias', False):
        return None
    aliases = getattr(executor, 'aliases', None)
    if not isinstance(aliases, dict) or not isinstance(code, str):
        return None
    info = aliases.get(code.strip())
    if not isinstance(info, dict):
        return None
    return parse_serve_file(info.get('code'))


def _stream_alias_file(handler, spec):
    """在请求线程里直接流式输出文件（不经过执行器，不占执行锁）。

    为什么不能走别名 body：body 在执行器的锁内运行，一个 65MB / 200KB/s
    的下载会把锁占 5 分钟，期间所有 RPC 排队超时；客户端中途取消还会让
    服务端继续慢慢写完整个文件。这里直接读文件写 socket，客户端一断开
    （BrokenPipe/ConnectionReset）立即收手。
    """
    path = os.path.abspath(os.path.expanduser(spec['path']))
    try:
        size = os.path.getsize(path) if os.path.isfile(path) else None
    except OSError:
        size = None
    if size is None:
        body = ('not found: %s' % path).encode('utf-8')
        handler.send_response(404)
        handler.send_header('Content-Type', 'text/plain; charset=utf-8')
        handler.send_header('Content-Length', str(len(body)))
        handler.end_headers()
        try:
            handler.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        return

    handler.send_response(200)
    handler.send_header('Content-Type', spec['content_type'])
    handler.send_header('Content-Length', str(size))
    handler.send_header(
        'Content-Disposition',
        'attachment; filename="%s"' % spec['filename'].replace('"', '_'))
    handler.send_header('Accept-Ranges', 'bytes')
    handler.end_headers()

    rate = spec['rate_limit']
    chunk = spec['chunk_size']
    sent = 0
    started = time.monotonic()
    try:
        with open(path, 'rb') as source:
            while True:
                block = source.read(chunk)
                if not block:
                    break
                if rate > 0:
                    gap = started + (sent + len(block)) / float(rate)
                    gap -= time.monotonic()
                    if gap > 0:
                        time.sleep(gap)
                handler.wfile.write(block)
                sent += len(block)
    except (BrokenPipeError, ConnectionResetError):
        logging.getLogger('app').info(
            '文件直出客户端提前断开 %s (%d/%d bytes)', path, sent, size)


def _root_alias_code(handler):
    """根路径 '/' 要执行的代码：root_alias 指向的已注册别名名。

    未注册（或别名总开关关闭）时返回 None，由调用方维持原来的 400/403
    行为。别名名必须真实存在，因此不会把任意代码放进执行器。
    """
    executor = _conf(handler, 'executor')
    if executor is None or not getattr(executor, 'alias', False):
        return None
    aliases = getattr(executor, 'aliases', None)
    name = str(_conf(handler, 'root_alias') or ROOT_ALIAS)
    if not isinstance(aliases, dict) or name not in aliases:
        return None
    return name


class RPCRequestHandler(BaseHTTPRequestHandler):
    key = ''
    # 别名 URL 的路由形态：'dash' 只认 key 处理后的代码，'raw' 额外允许
    # 已注册别名名直接作 URL（/<别名>，不带 key 前缀）。见 _is_alias_url。
    alias_url = ALIAS_URL_DASH
    # alias_url='json' 时对 query 值尝试 json.loads（alias.md 8.3）
    alias_json = False
    # 根路径 '/' 执行的别名名（见 _root_alias_code），默认 'root'
    root_alias = ROOT_ALIAS
    # 别名定义是否写 rpc.json（默认持久化；带“不持久化”标记的除外）
    persist_aliases = True
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
        # 2xx/3xx 访问行可被过滤器静音（见模块上部 _LOG_FILTERS）：规则全部
        # 由下游模块用 add_log_filter() 注入（如 rdp 页「服务端日志」关闭后
        # 每个请求都带 X-RDP-Log: 0、pty 的 MCP 端点）。4xx/5xx 一律保留，
        # 便于排查协议层/业务失败；过滤器异常只降级为照常输出，不打断请求。
        if 200 <= _status_of(text) < 400 and _log_silenced(self):
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
            # path 与 query 必须切开：路由只认路径部分，query 是别名调用
            # 的参数（alias.md 8.1）。原生 handler 的 self.path 是原始
            # percent-encoded 请求行；WSGI / ASGI 垫片已解码路径并把原始
            # query 解析好放在 self.query_params 上，避免二次解码。
            raw_path, _, query_text = self.path.partition('?')
            query_params = getattr(self, 'query_params', None)
            if not isinstance(query_params, dict):
                query_params = parse_query_params(query_text)
            key = _conf(self, 'key')
            if key:
                # key 是真正的 URL 前缀：代码紧跟在根斜杠之后，例如
                # key='-' 时 /-r=1+1 执行 r=1+1。旧实现拼 key + '/'，而
                # self.path 必以 '/' 开头，只有 key 自带前导斜杠才可能命中，
                # 实际上把带 key 的请求全部 403 了；这里同时保留
                # /<key>/<code>，已有地址不必改写。
                prefixed = '/' + key
                legacy_prefix = prefixed + '/'
                if raw_path.startswith(legacy_prefix):
                    code_str = raw_path[len(legacy_prefix):]
                elif raw_path.startswith(prefixed):
                    code_str = raw_path[len(prefixed):]
                elif raw_path == '/' and _alias_url_mode(self) == ALIAS_URL_RAW and _root_alias_code(self):
                    # 首页 / 没有 key 前缀：alias_url='raw' 时执行 root_alias
                    # 指向的已注册别名（例如输出 apk.html），保持与 /<别名> 同层。
                    code_str = _root_alias_code(self)
                elif _is_alias_url(self, raw_path):
                    # alias_url='raw'：已注册的别名名可直接作 URL（/add），
                    # 不必带 key 前缀。未注册路径仍 403，只对别名这一层放开。
                    code_str = raw_path[1:]
                else:
                    self.send_error(403, "Forbidden")
                    return
            else:
                if raw_path.startswith('/'):
                    raw_path = raw_path[1:]
                code_str = raw_path
                if not code_str:
                    # 无 key 时 / 直接落到根别名（未注册仍走下面的 400）
                    code_str = _root_alias_code(self) or ''
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

            executor = _conf(self, 'executor')
            # 别名指令行尾的“不持久化”标记由本层剥掉（执行器不允许指令行
            # 出现第二个名字），剩下的 code 再走语法检查 / 执行。
            code, alias_ephemeral = strip_ephemeral_alias_flag(code, executor)

            # 文件直出别名（body 首行 #serve-file {...}）：在请求线程里直接
            # 流式输出，不经过执行器。否则整段下载独占执行锁，几分钟内其它
            # RPC 全部排队超时；规格坏掉时 parse 返回 None，退回普通执行。
            if getattr(self, 'command', 'GET').upper() == 'GET':
                serve_spec = _serve_file_spec(executor, code)
                if serve_spec is not None:
                    _stream_alias_file(self, serve_spec)
                    return

            # 裸路径救援：URL 里 file=/tmp/txt 不带引号是 SyntaxError，
            # 补引号后能通过语法检查才采用，否则保留原始报错。
            # 别名调用（<别名>=<参数>）不是 Python，救援会把参数改成带引号
            # 的字符串，所以整段跳过，交给执行器原样展开。
            if not _is_alias_call(executor, code):
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

            # 别名调用的调用方数据（alias.md 8.1）：只有整段 exact 命中的
            # 别名吃 query，`=arg` / prefix 的参数由 _alias_arg 负责，是
            # 正交机制。query 为空时不传，保持 plain 别名的历史注入语义。
            alias_params = None
            if query_params and _alias_exact_record(executor, code) is not None:
                alias_params = decode_alias_query(
                    query_params, bool(_conf(self, 'alias_json')))

            try:
                # stdout 捕获由 PythonExecutor 内部的 _redirect_stdout 完成
                # （execution['stdout'] 即输出），外层不再包 _stdout_lock +
                # 二次重定向——那会把整个执行串成全局临界区，一条慢请求
                # （长任务/死循环）就冻结全部 HTTP RPC（含日志台/诊断口）。
                # 执行的串行化与超时守卫由执行器自身的锁与 timeout 保证。
                execution = executor.execute(
                    code, globals=context, alias_params=alias_params)
                # 别名定义 / 更新 / 删除写回 rpc.json（默认持久化）。执行
                # 失败（例如 body 运行报错）时别名可能已注册，因此放在
                # execute 之后立即同步；持久化本身出错不能打挂请求。
                if _conf(self, 'persist_aliases'):
                    try:
                        sync_alias_persistence(
                            executor, execution, alias_ephemeral)
                    except Exception:
                        logging.getLogger('error').exception(
                            '别名持久化写入失败 path=%s', self.path)
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
                     main_loop=None, listen=True, log_sink=None, basic_auth='',
                     alias_url=ALIAS_URL_DASH, root_alias=ROOT_ALIAS, persist_aliases=True):
    if not key:
        key = ''
    else:
        # key 是路径里的字面前缀，统一去掉首尾斜杠：/<key><code> 与
        # /<key>/<code> 由 handle_rpc 两种都认，key 写成 '/-' 或 '-/' 同义。
        key = str(key).strip('/')
    # 别名 URL 匹配模式。'dash'（默认，兼容旧名 'keyed'）只认 key 处理后的
    # 代码；'raw' 额外允许已注册别名名直接作 URL（如 key='-' 时 /add 等价
    # /-r=1+1）。'json' 不是路由形态，只表示 query 值尝试 json.loads。
    alias_url, alias_json = normalize_alias_url(alias_url)
    # 根路径 / 执行的别名名（未注册时 / 仍是原来的 400/403）；名字按原样
    # 保留，注册到执行器后再解析。
    root_alias = str(root_alias or ROOT_ALIAS).strip()
    # 别名持久化默认开启：注册 / 更新 / 删除都同步进 rpc.json 的
    # http_aliases 段，进程重启（含 pythonanywhere SIGHUP）后自动恢复。
    persist_aliases = bool(persist_aliases)
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
    # 每服务器一个 PythonExecutor；不在这里强写 __name__：执行器内部
    # 用 setdefault，模块字典自带 __name__ 不会被覆盖；自建空字典时
    # 由执行器补成 __rpc_exec__。
    executor = PythonExecutor(globals=persistent_ns, main_loop=main_loop)
    # 启动即恢复上次持久化的别名：进程重启 / SIGHUP 后 #alias 定义的
    # URL 仍可直接访问。持久化关闭时既不读也不写 rpc.json。
    if persist_aliases:
        load_persisted_aliases(executor)

    config = {
        'key': key,
        'alias_url': alias_url,
        'alias_json': alias_json,
        'root_alias': root_alias,
        'persist_aliases': persist_aliases,
        'basic_auth': basic_auth or '',
        'executor': executor,
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
    
def build_arg_parser():
    """构造命令行参数解析器（__main__ 与测试共用，便于锁定参数拼写）。"""
    import argparse

    parser = argparse.ArgumentParser(
        description='启动 URL 即代码的 HTTP RPC 服务器',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # 同时接受 --key 与仓库里常见的 -key / -k 写法；key 值本身可以就是
    # '-'（argparse 把单独的 '-' 当值而非选项），此时 URL 为 /-<code>。
    parser.add_argument('--port', '-port', '-p', type=int, default=1144)
    parser.add_argument('--ip', '--host', '-host', default='0.0.0.0')
    parser.add_argument('--key', '-key', '-k', default='',
                        help="URL 前缀，如 - 时 URL 为 /-<code>")
    parser.add_argument('--alias-url', '-alias-url', dest='alias_url',
                        choices=('dash', 'raw', 'json', 'keyed',
                                 'dash-json', 'raw-json'),
                        default=ALIAS_URL_DASH,
                        help="dash 时别名走 /-<别名>；raw 额外允许 /<别名>；"
                             "json（或 raw-json）再对 query 值尝试 json.loads")
    parser.add_argument('--root-alias', '-root-alias', dest='root_alias',
                        default=ROOT_ALIAS,
                        help="根路径 / 执行的已注册别名名（raw 模式生效）")
    parser.add_argument('--no-persist-aliases', '-no-persist-aliases',
                        dest='persist_aliases', action='store_false',
                        default=True,
                        help="关闭别名持久化（默认写 rpc.json 的 http_aliases）")
    # 三种写法同义：--basic_auth / -basic_auth / -basic-auth；值是
    # 'user:password'，空串（默认）表示不启用认证。--basic_auth 里若写成
    # 空格分隔的两个参数会被 argparse 拒绝，必须带冒号或用引号。
    parser.add_argument('--basic_auth', '-basic_auth', '-basic-auth',
                        dest='basic_auth', default='',
                        help="HTTP Basic 认证，格式 user:password；"
                             "空则不启用认证")
    return parser


if __name__ == '__main__':
    args = build_arg_parser().parse_args()
    start_rpc_server(port=args.port, ip=args.ip, key=args.key,
                     alias_url=args.alias_url, root_alias=args.root_alias,
                     persist_aliases=args.persist_aliases,
                     basic_auth=args.basic_auth,
                     globals=globals(), locals=locals())
    input("Press Ctrl+C or anykey to stop\n")