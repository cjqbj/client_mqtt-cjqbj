#!/usr/bin/env python3
import argparse,time,threading,os, sys, logging, codeop, importlib,builtins as _builtins

# 本模块已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/client_mqtt.py」直接运行与「from client import ...」包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from multi_mqtt import MultiMQTTManager, get_req_id, utc_ms, get_standard_pem_bytes,get_duplicated_kargs

logger = logging.getLogger(__file__)
from server_mqtt import REQUEST_TOPIC, DEFAULT_REPLY_TOPIC as REPLY_TOPIC
DEFAULT_TIMEOUT = 30

# ================= 统一别名表（小写变量名） =================
# 语义别名，不含 "--" / "%" 前缀。三处共用同一份：
#   1) rpc(**ka)   ->  get_duplicated_kargs(ka, *alias_xxx)
#   2) CLI 参数    ->  _cli_opts(*alias_xxx)  自动生成 --x / -x / x_x / x-x
#   3) %magic 命令 ->  cmd in alias_xxx
alias_code         =('code', 'c')
alias_request_topic=('request_topic', 'topic', 't','q')
alias_reply_topic  =('reply_topic', 'reply')
alias_private_key  =('private_key', 'private', 'key', 'k')
alias_allow_no_pub =('allow_no_server_pubkey_response','allow_no_pub','allow', 'all', 'a')
alias_history      =('history', 'history_file', 'his', 'hist')
alias_status       =('status','state','s')
alias_help         =('help','h','?')
alias_exit         =('exit', 'quit')
alias_timeout      =('timeout','time','wait','second','sec')

def _cli_opts(*names):
    """从别名生成 argparse 选项：自动加 --/- 前缀，_ 与 - 都支持；单字符只出 -x。

    模块级唯一实现，client_mqtt 自己的 CLI 以及 cmd_client_mqtt /
    pty_client_mqtt 的连接参数都通过它 + 上面的 alias_xxx 别名表生成，
    保证三处（rpc(**ka) / CLI / %magic）参数名永远一致。
    """
    out, seen = [], set()
    for n in names:
        for f in (n, n.replace('_', '-')):
            for c in (('-' + f,) if len(f) == 1 else ('--' + f, '-' + f)):
                if c not in seen:
                    seen.add(c); out.append(c)
    return out

_default_client = None
_default_client_lock = threading.Lock()

class MQTTClientNode:
    def __init__(self, client_private_key_bytes=None, allow_no_server_pubkey_response: bool = False):
        self.client_private_key_bytes = client_private_key_bytes
        self.allow_no_server_pubkey_response = allow_no_server_pubkey_response
        self.mqtt_net = MultiMQTTManager(log_messages=False, client_private_key_bytes=client_private_key_bytes)
        self.mqtt_net.set_on_message(self._on_message)
        self.pending_requests = {}
        self.lock = threading.Lock()
        self._subscribed_topics = set()

    def start(self, connect_timeout=10.0):
        self.mqtt_net.start()
        # 不再写死 sleep(2)：等第一个 broker 一连上就继续（实测最快约
        # 0.1s）；全部连不上时最多等到超时，随后台重连继续往下走，
        # 由后续请求自身的超时决定成败。
        online = self.mqtt_net.wait_connected(
            min_count=1, timeout=connect_timeout)
        if online <= 0:
            logger.warning(
                "启动后 %.0fs 内没有任何 broker 连接成功，继续尝试"
                "（paho 会在后台自动重连）", connect_timeout)
        self._subscribe_once(REPLY_TOPIC)

    def _subscribe_once(self, topic):
        with self.lock:
            if topic in self._subscribed_topics:
                return
            self._subscribed_topics.add(topic)
        try:
            self.mqtt_net.subscribe(topic)
        except Exception:
            with self.lock:
                self._subscribed_topics.discard(topic)
            raise

    def _on_message(self, topic, data, rx_broker):
        received_req_id = data.get("req_id")
        if not received_req_id:
            return
        is_unverified_echo = isinstance(received_req_id, str) and "|" in received_req_id
        base_req_id = received_req_id.split("|", 1)[0] if is_unverified_echo else received_req_id
        with self.lock:
            req_ctx = self.pending_requests.get(base_req_id)
        if req_ctx is None:
            return
        has_client_pri = bool(req_ctx.get("client_private_key_bytes"))
        allow_no_pub = req_ctx.get("allow_no_server_pubkey_response", False)
        if has_client_pri and is_unverified_echo and not allow_no_pub:
            logger.warning(
                f"⛔ [安全拦截] req_id={received_req_id} | 服务端未验签 "
                f"(原样返回了带签名的 req_id)，且 allow_no_server_pubkey_response=False，丢弃该响应。",
            )
            return
        resp = dict(data)
        if is_unverified_echo:
            resp["req_id"] = base_req_id
        cost_ms = (time.perf_counter() - req_ctx['start_time']) * 1000
        resp["latency_ms"] = round(cost_ms, 2)
        resp["client_from"] = rx_broker
        if req_ctx.get("response") is not None:
            # 首包已回收：同 req_id 的后续回包来自其他持相同 key 的服务端
            # 进程（一个请求会被每台在线设备各执行一次）。调用方开了
            # gather_window 时把这些迟到回包收集起来做归属仲裁，不覆盖首包。
            with self.lock:
                req_ctx.setdefault("extras", []).append(resp)
            return
        req_ctx['response'] = resp
        req_ctx['event'].set()

    def request(self, payload: str, request_topic: str = REQUEST_TOPIC, timeout: float = DEFAULT_TIMEOUT,
                client_private_key_bytes=None, allow_no_server_pubkey_response: bool = None,
                reply_topic: str = REPLY_TOPIC, gather_window: float = 0.0):
        none={}
        if client_private_key_bytes is None:
            client_private_key_bytes = self.client_private_key_bytes or getattr(self.mqtt_net, "client_private_key_bytes", None)
        if allow_no_server_pubkey_response is None:
            allow_no_server_pubkey_response = self.allow_no_server_pubkey_response
        ms = utc_ms()
        req_id = get_req_id(ms)
        start_time = time.perf_counter()
        req_data = {"req_id": req_id, "reply_topic": reply_topic, "code": payload, "timestamp": ms}
        event = threading.Event()
        req_ctx = {"event": event, "start_time": start_time, "response": None,
                   "extras": [],
                   "client_private_key_bytes": client_private_key_bytes,
                   "allow_no_server_pubkey_response": allow_no_server_pubkey_response}
        with self.lock:
            self.pending_requests[req_id] = req_ctx
        if reply_topic != REPLY_TOPIC:
            try:
                self._subscribe_once(reply_topic)
            except Exception as exc:
                logger.error(f"❌ [订阅失败] reply_topic={reply_topic} error={exc}")
                print(f"[ERROR] 订阅 reply_topic 失败: {exc}")
                with self.lock:
                    self.pending_requests.pop(req_id, None)
                return none
        try:
            self.mqtt_net.publish_broadcast(request_topic, req_data, client_private_key_bytes=client_private_key_bytes)
        except Exception as exc:
            with self.lock:
                self.pending_requests.pop(req_id, None)
            logger.error(f"❌ [请求发送失败] {request_topic} req_id={req_id} error={exc}")
            print(f"[ERROR] 请求发送失败: {exc}")
            return none
        is_success = False
        try:
            start_t = time.perf_counter()
            while time.perf_counter() - start_t < timeout:
                if event.wait(timeout=0.2):
                    is_success = True
                    break
            if is_success and gather_window > 0:
                # 首包到手后再留一个小窗：其他持相同 key 的服务端进程对同
                # 一请求的迟到回包在此到齐（PTY 多应答者归属仲裁用）。
                time.sleep(min(gather_window, max(0.0, timeout - (time.perf_counter() - start_t))))
        except KeyboardInterrupt:
            logger.warning(f"⚠️ [请求中断] {request_topic} req_id={req_id}")
            print("[INFO] 用户中断等待，已停止本次请求。")
            raise
        finally:
            with self.lock:
                self.pending_requests.pop(req_id, None)
        if is_success:
            if gather_window > 0:
                first = dict(req_ctx['response'])
                first["_extra_responses"] = list(req_ctx.get("extras", []))
                return first
            return req_ctx['response']
        logger.error(f"❌ [请求超时] {request_topic} req_id={req_id}")
        return none

    def stop(self):
        try:
            self.mqtt_net.stop()
        except KeyboardInterrupt:
            print("[INFO] 用户中断，MQTT 连接已停止。")
            logger.warning("⚠️ [用户中断] 已停止 MQTT 连接")
        finally:
            with self.lock:
                self._subscribed_topics.clear()

def rpc(code: str, request_topic: str = REQUEST_TOPIC, timeout: float = DEFAULT_TIMEOUT,
        client_private_key_bytes=None, allow_no_server_pubkey_response: bool = False,
        reply_topic: str = REPLY_TOPIC, **ka):
    """Execute code through a lazily started shared MQTT client."""
    global _default_client
    code=get_duplicated_kargs(ka,*alias_code,default=code)
    request_topic=get_duplicated_kargs(ka,*alias_request_topic,default=request_topic)
    client_private_key_bytes=get_duplicated_kargs(ka,*alias_private_key,default=client_private_key_bytes)
    allow_no_server_pubkey_response=get_duplicated_kargs(ka,*alias_allow_no_pub,default=allow_no_server_pubkey_response)
    timeout=get_duplicated_kargs(ka,*alias_timeout,default=timeout)
    with _default_client_lock:
        if _default_client is None:
            _default_client = MQTTClientNode(
                client_private_key_bytes=client_private_key_bytes,
                allow_no_server_pubkey_response=allow_no_server_pubkey_response,
            )
            _default_client.start()
        client = _default_client
    return client.request(
        code, request_topic=request_topic, timeout=timeout,
        client_private_key_bytes=client_private_key_bytes,
        allow_no_server_pubkey_response=allow_no_server_pubkey_response,
        reply_topic=reply_topic,
    )

def stop():
    """Stop the shared module-level MQTT client, if it was started."""
    global _default_client
    with _default_client_lock:
        client = _default_client
        _default_client = None
    if client is not None:
        client.stop()

# ---------------- Shell ----------------

_PT_STYLE_TO_ANSI = {
    "ansiblack":"\033[30m","ansired":"\033[31m","ansigreen":"\033[32m","ansiyellow":"\033[33m",
    "ansiblue":"\033[34m","ansimagenta":"\033[35m","ansicyan":"\033[36m","ansiwhite":"\033[37m",
    "ansibrightblack":"\033[90m","ansigray":"\033[90m","ansigrey":"\033[90m",
    "ansibrightred":"\033[91m","ansibrightgreen":"\033[92m","ansibrightyellow":"\033[93m",
    "ansibrightblue":"\033[94m","ansibrightmagenta":"\033[95m","ansibrightcyan":"\033[96m",
    "ansibrightwhite":"\033[97m","bold":"\033[1m","dim":"\033[2m","italic":"\033[3m","underline":"\033[4m",
}

class C:
    RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"; ITALIC="\033[3m"; UNDERLINE="\033[4m"
    RED="\033[31m"; GREEN="\033[32m"; YELLOW="\033[33m"; BLUE="\033[34m"; MAGENTA="\033[35m"
    CYAN="\033[36m"; WHITE="\033[37m"; GRAY="\033[90m"
    BRIGHT_RED="\033[91m"; BRIGHT_GREEN="\033[92m"; BRIGHT_YELLOW="\033[93m"
    BRIGHT_BLUE="\033[94m"; BRIGHT_MAGENTA="\033[95m"; BRIGHT_CYAN="\033[96m"

def _get_clipboard_text() -> str:
    try:
        import pyperclip
        return pyperclip.paste() or ""
    except Exception:
        pass
    if sys.platform=="win32":
        try:
            import win32clipboard
            win32clipboard.OpenClipboard()
            try:
                return win32clipboard.GetClipboardData(win32clipboard.CF_UNICODETEXT) or ""
            finally:
                win32clipboard.CloseClipboard()
        except Exception:
            pass
    try:
        import tkinter
        r = tkinter.Tk(); r.withdraw()
        try:
            return r.clipboard_get() or ""
        finally:
            r.destroy()
    except Exception:
        return ""

def _normalize_history_path(path):
    if not path:
        return None
    path = os.path.expanduser(str(path))
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
    except OSError:
        pass
    return path

def _build_prompt(history_path=None):
    try:
        prompt_toolkit = importlib.import_module("prompt_toolkit")
        pt_key_binding = importlib.import_module("prompt_toolkit.key_binding")
        pt_lexers = importlib.import_module("prompt_toolkit.lexers")
        pt_styles = importlib.import_module("prompt_toolkit.styles")
        pt_keys = importlib.import_module("prompt_toolkit.keys")
        pt_history = importlib.import_module("prompt_toolkit.history")
        pt_filters = importlib.import_module("prompt_toolkit.filters")
        pygments_lexers = importlib.import_module("pygments.lexers")
    except ImportError:
        _state = {"path": _normalize_history_path(history_path)}
        def _get(): return _state["path"]
        def _set(p):
            _state["path"] = _normalize_history_path(p)
            return _state["path"]
        return _fallback_code_input, False, {"get": _get, "set": _set}

    key_bindings = pt_key_binding.KeyBindings()
    is_searching = pt_filters.is_searching # 搜索状态过滤器，用于让 Enter 在搜索时放行默认绑定

    @key_bindings.add("enter", filter=~is_searching) # 非搜索状态下才走自定义逻辑；搜索时交还默认绑定处理
    def accept_on_empty_line(event):
        buffer = event.current_buffer
        current_line = buffer.document.current_line
        if current_line.lstrip().startswith('%'): # magic 命令：直接提交，不换行，光标在中间也提交
            cleaned = buffer.text.rstrip("\n")
            if cleaned != buffer.text:
                buffer.text = cleaned
            buffer.validate_and_handle()
            return
        if buffer.document.current_line_before_cursor.strip():
            buffer.insert_text("\n")
        else:
            cleaned = buffer.text.rstrip("\n")
            if cleaned != buffer.text:
                buffer.text = cleaned
            buffer.validate_and_handle()

    @key_bindings.add(pt_keys.Keys.BracketedPaste)
    def _on_bracketed_paste(event):
        data = event.data.replace("\r\n", "\n").replace("\r", "\n")
        event.current_buffer.insert_text(data)

    @key_bindings.add("escape", "[", "2", ";", "2", "~")
    def _on_shift_insert_paste(event):
        text = _get_clipboard_text()
        if text:
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            event.current_buffer.insert_text(text)

    session_state = {"session": None, "path": None}

    def _rebuild(path):
        norm = _normalize_history_path(path)
        hist = pt_history.FileHistory(norm) if norm else pt_history.InMemoryHistory()
        session = prompt_toolkit.PromptSession(
            lexer=pt_lexers.PygmentsLexer(pygments_lexers.PythonLexer),
            style=pt_styles.Style.from_dict({"prompt": "ansicyan"}),
            multiline=True, key_bindings=key_bindings, history=hist,
        )
        session_state["session"] = session
        session_state["path"] = norm
        return norm

    _rebuild(history_path)
    def prompt_with_pt(): return session_state["session"].prompt(">>> ")
    def _get(): return session_state["path"]
    def _set(p): return _rebuild(p)
    return prompt_with_pt, True, {"get": _get, "set": _set}

def _make_print(has_pt: bool):
    if has_pt:
        from prompt_toolkit import print_formatted_text as _pt_print
        from prompt_toolkit.formatted_text import FormattedText as _pt_ft, ANSI as _pt_ansi
    else:
        _pt_print = _pt_ft = _pt_ansi = None

    def _print(*args, sep=' ', end='\n', file=None, flush=False, color=''):
        text = sep.join(str(a) for a in args)
        if file is None:
            file = sys.stdout
        if not color:
            if has_pt: _pt_print(text, end=end, file=file, flush=flush)
            else: _builtins.print(text, end=end, file=file, flush=flush)
            return
        if color.startswith("\033["):
            colored = f"{color}{text}\033[0m"
            if has_pt: _pt_print(_pt_ansi(colored), end=end, file=file, flush=flush)
            else: _builtins.print(colored, end=end, file=file, flush=flush)
            return
        if has_pt:
            _pt_print(_pt_ft([(color, text)]), end=end, file=file, flush=flush)
        else:
            ansi = _PT_STYLE_TO_ANSI.get(color, "")
            colored = f"{ansi}{text}\033[0m" if ansi else text
            _builtins.print(colored, end=end, file=file, flush=flush)
    return _print

_MAGIC_HELP = """\
可用魔术命令：
  %topic [name]      查看/设置 request_topic；%topic -reset 恢复默认
  %reply [name]      查看/设置 reply_topic；%reply -reset 恢复默认
  %key   [path|pem]  加载客户端私钥；%key -clear 清除；不带参查看状态
  %allow [on|off]    查看/设置 allow_no_server_pubkey_response
  %his [path]        查看/切换磁盘历史文件；%his -clear 关闭磁盘历史
  %history [path]    %his 的别名，功能相同
  %status            打印当前会话状态
  %help, %?          显示本帮助
  %exit, %quit       退出

Python 代码直接输入即可；空行提交。
"""

_RESET_WORDS = {"-reset", "reset"}
_KEY_CLEAR_WORDS=_HIST_CLEAR_WORDS = {"-clear", "clear", "none", "-", "-reset", "reset", "off"}
# _KEY_CLEAR_WORDS = {"-clear", "clear", "none", "-", "-reset", "reset"}

def _handle_magic(line, state, print_fn):
    body = line[1:].strip()
    if not body:
        print_fn("空魔术命令，输入 %help 查看帮助", color=C.YELLOW)
        return True
    parts = body.split(None, 1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd in alias_request_topic:
        if not arg:
            print_fn(f"request_topic = {state['request_topic']}", color=C.CYAN)
        elif arg in _RESET_WORDS:
            state["request_topic"] = state["default_request_topic"]
            print_fn(f"request_topic <- {state['request_topic']} (默认)", color=C.GREEN)
        else:
            state["request_topic"] = arg
            print_fn(f"request_topic <- {arg}", color=C.GREEN)

    elif cmd in alias_reply_topic:
        if not arg:
            print_fn(f"reply_topic = {state['reply_topic']}", color=C.CYAN)
        elif arg in _RESET_WORDS:
            state["reply_topic"] = REPLY_TOPIC
            print_fn(f"reply_topic <- {state['reply_topic']} (默认)", color=C.GREEN)
        else:
            state["reply_topic"] = arg
            print_fn(f"reply_topic <- {arg}", color=C.GREEN)

    elif cmd in alias_private_key:
        if not arg:
            kb = state.get("key")
            print_fn(f"查询到 已设置客户端私钥（{len(kb)} bytes）" if kb else "未设置客户端私钥", color=C.CYAN)
        elif arg in _KEY_CLEAR_WORDS:
            state["key"] = None
            print_fn("已清除客户端私钥", color=C.YELLOW)
        else:
            try:
                kb = get_standard_pem_bytes(arg)
            except Exception as exc:
                print_fn(f"加载私钥失败: {exc}", color=C.RED)
            else:
                state["key"] = kb
                print_fn(f"已加载客户端私钥（{len(kb)} bytes）", color=C.GREEN)

    elif cmd in alias_allow_no_pub:
        if not arg:
            print_fn(f"allow_no_server_pubkey_response = {state['allow_no_pub']}", color=C.CYAN)
        elif arg.lower() in {"on", "true", "1", "yes"}:
            state["allow_no_pub"] = True
            print_fn("allow_no_server_pubkey_response <- True", color=C.GREEN)
        elif arg.lower() in {"off", "false", "0", "no"}:
            state["allow_no_pub"] = False
            print_fn("allow_no_server_pubkey_response <- False", color=C.GREEN)
        else:
            print_fn(f"未知取值: {arg}（应为 on/off）", color=C.RED)

    elif cmd in alias_timeout:
        try:
            state['timeout']=float(arg)
        except Exception as e:
            print_fn(f'timeout 格式错误 {e}')
    elif cmd in alias_history:
        hist_ctl = state.get("hist_ctl")
        if not arg:
            cur = state.get("history_path")
            print_fn(f"history file = {cur if cur else '<disabled>'}", color=C.CYAN)
        elif arg in _HIST_CLEAR_WORDS:
            if hist_ctl:
                hist_ctl["set"](None)
            state["history_path"] = None
            print_fn("history file <- <disabled>", color=C.YELLOW)
        else:
            try:
                new_path = hist_ctl["set"](arg) if hist_ctl else None
            except Exception as exc:
                print_fn(f"设置历史文件失败: {exc}", color=C.RED)
            else:
                state["history_path"] = new_path
                print_fn(f"history file <- {new_path}", color=C.GREEN)

    elif cmd in alias_status:
        kb = state.get("key")
        hist_path = state.get("history_path")
        print_fn(
            f"request_topic = {state['request_topic']}\n"
            f"reply_topic   = {state['reply_topic']}\n"
            f"timeout       = {state['timeout']}\n"
            f"key           = {'<set, %d bytes>' % len(kb) if kb else '<none>'}\n"
            f"allow_no_pub  = {state['allow_no_pub']}\n"
            f"history file  = {hist_path if hist_path else '<disabled>'}",
            color=C.CYAN,
        )
        if _default_client:print_fn(_default_client.mqtt_net.stats.get_report(is_windows_cmd=(sys.platform=="win32")))

    elif cmd in alias_help:
        print_fn(_MAGIC_HELP, color=C.CYAN)

    elif cmd in alias_exit:
        os._exit(0)

    else:
        print_fn(f"未知魔术命令: %{cmd}（%help 查看帮助）", color=C.RED)
    return True

def run_shell(client, timeout: float = DEFAULT_TIMEOUT, request_topic: str = REQUEST_TOPIC,
              reply_topic: str = REPLY_TOPIC, client_private_key_bytes=None,
              allow_no_server_pubkey_response: bool = False, history_path=None):
    prompt, has_pt, hist_ctl = _build_prompt(history_path=history_path)
    print = _make_print(has_pt)  # noqa: A001
    state = {
        "request_topic": request_topic,
        "default_request_topic": REQUEST_TOPIC,
        "reply_topic": reply_topic,
        'timeout':timeout,
        "key": client_private_key_bytes,
        "allow_no_pub": allow_no_server_pubkey_response,
        "history_path": hist_ctl["get"](),
        "hist_ctl": hist_ctl,
    }
    print("输入 Python 代码，prompt_toolkit 模式支持多行；输入 exit() 或 Ctrl-D 退出。", color=C.CYAN)
    print("输入 %help 查看魔术命令（%topic 切换 topic，%key 设置私钥，%his 设置历史文件）。", color=C.BLUE)
    if state["history_path"]:
        print(f"[INFO] 历史文件: {state['history_path']}", color=C.GRAY)
    while True:
        try:
            code = prompt()
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前等待，回到命令提示符。", color=C.YELLOW)
            continue
        except EOFError:
            print("\nCtrl+D 退出", color=C.YELLOW)
            os._exit(0)
        stripped = code.strip()
        if stripped in {"exit()", "quit()"}:
            os._exit(0)
        if not stripped:
            continue
        if stripped.startswith("%"):
            _handle_magic(stripped, state, print)
            continue
        try:
            response = client.request(
                code, request_topic=state["request_topic"], timeout=state['timeout'],
                client_private_key_bytes=state["key"],
                allow_no_server_pubkey_response=state["allow_no_pub"],
                reply_topic=state["reply_topic"],
            )
        except KeyboardInterrupt:
            print("\n[INFO] 已中断当前请求，回到命令提示符。", color=C.YELLOW)
            continue
        except Exception as exc:
            print(f"[ERROR] 执行请求失败: {exc}", color=C.RED)
            continue
        if response is None:
            continue
        stdout = response.pop("stdout", '')
        r = response.pop("r", '')
        print("# " + str(response), color=C.GRAY)
        if stdout:
            print(stdout, end="", color=C.CYAN)
        if response.get("ok"):
            if r not in (None, ""):
                print(r,)
        else:
            print(response.get("error", "remote execution failed"), end="", color=C.RED)

def _fallback_code_input():
    lines = []
    prompt = ">>> "
    while True:
        try:
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            raise
        if not line.strip():
            continue
        lines.append(line)
        source = "\n".join(lines)
        try:
            compiled = codeop.compile_command(source, "<shell>", "exec")
        except (SyntaxError, IndentationError):
            _builtins.print("\033[33m[WARN] 非法 Python 语句，已忽略，请继续输入。\033[0m")
            lines = []
            prompt = ">>> "
            continue
        if compiled is not None:
            return source
        prompt = "... "

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT RPC client")
    parser.add_argument(
        *_cli_opts(*alias_private_key),
        dest="client_private_key", default=None,
        help="客户端私钥文件路径或 PEM 内容；启用后请求会带签名。",
    )
    parser.add_argument(
        *_cli_opts(*alias_timeout), type=float, default=DEFAULT_TIMEOUT,
        help="命令等待远端响应的超时时间，单位秒。",
    )
    parser.add_argument(
        *_cli_opts(*alias_allow_no_pub),
        action="store_true",
        help="允许在私钥模式下接收无公钥服务器的返回。默认会拦截。",
    )
    parser.add_argument(
        *_cli_opts(*alias_request_topic),
        type=str, default=REQUEST_TOPIC, dest="request_topic",
        help=f"REPL 起始 request_topic（默认 {REQUEST_TOPIC}）；运行中可用 %%topic 切换。",
    )
    parser.add_argument(
        *_cli_opts(*alias_reply_topic),
        type=str, default=REPLY_TOPIC, dest="reply_topic",
        help=f"REPL 起始 reply_topic（默认 {REPLY_TOPIC}）；运行中可用 %%reply 切换。",
    )
    parser.add_argument(
        *_cli_opts(*alias_history),
        dest="history",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "client_history.db"),
        help="磁盘历史记录文件路径（支持 ~ 展开）；不设置则不持久化。运行中可用 %%his 切换。",
    )
    parser.add_argument('--port', '-port', '-p', type=int, default=1166)
    parser.add_argument('--host','-host', default='0.0.0.0')
    args = parser.parse_args()

    key_bytes = None
    if args.client_private_key:
        try:
            key_bytes = get_standard_pem_bytes(args.client_private_key)
        except Exception as exc:
            print(f"[ERROR] 解析客户端私钥失败: {exc}")
            sys.exit(1)

    try:
        _default_client = MQTTClientNode(
            client_private_key_bytes=key_bytes,
            allow_no_server_pubkey_response=args.allow_no_server_pubkey_response,
        )
        _default_client.mqtt_net.is_windows_cmd=(sys.platform=="win32")
        _default_client.start()

        import server_http
        ghs = server_http.start_rpc_server(port=args.port, ip=args.host, globals=globals(), locals=locals())
        try:
            if key_bytes:
                print(f"[INFO] 已开启客户端私钥签名模式")
            print(f"[INFO] 允许未验签服务端响应: {args.allow_no_server_pubkey_response}")
            print(f"[INFO] 请求超时: {args.timeout}s  request_topic:{args.request_topic} , reply_topic:{args.reply_topic}")
            run_shell(
                _default_client, timeout=args.timeout,
                request_topic=args.request_topic, reply_topic=args.reply_topic,
                client_private_key_bytes=key_bytes,
                allow_no_server_pubkey_response=args.allow_no_server_pubkey_response,
                history_path=args.history,
            )
        finally:
            _default_client.stop()
    except KeyboardInterrupt:
        print("\n[INFO] 用户中断，程序已体面退出。")
        sys.exit(0)
    except Exception as exc:
        print(f"[ERROR] 启动 MQTT client 失败: {exc}")
        sys.exit(1)