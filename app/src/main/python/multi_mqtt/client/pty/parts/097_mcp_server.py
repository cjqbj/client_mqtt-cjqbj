# ==================== MCP 服务：把常驻 PTY 挂成标准 MCP 工具服务器 ====================
#
# 背景：本进程已通过 ai_bridge + 本地 HTTP RPC 口把常驻 PTY 暴露给外部进程，
# 但 AI 客户端侧还缺一个标准工具协议入口。MCP（Model Context Protocol）是
# 各 AI 客户端的通用工具协议；装好 ``pip install "mcp[cli]"`` 后，main 默认
# 把 MCP 端点**挂在 --port 已起的 RPC 服务器上**（路径 /mcp，与 RPC / 浏览器
# 日志台同口复用，不新开端口），AI 客户端即可把远端 shell 当普通 MCP 工具
# 集持续使用：
#
#     claude mcp add --transport http pty http://192.168.1.3:1188/mcp
#
# 同口复用的做法（参考 HuggingFace app.py 的保留路径分流中间件）：给
# server_http.RPCRequestHandler 包一层前置分发——命中 /mcp（sse 退回模式下
# 是 /sse、/messages）的请求桥接给 MCP 的 ASGI 应用，其余路径原样走既有
# RPC / WebSocket / 日志台逻辑。桥接要点：
# - mcp 1.x/2.x 双兼容：2.x 把 FastMCP 改名 MCPServer，ASGI 应用从
#   streamable_http_app()/sse_app() 取，SDK 不支持时逐级退回；
# - ASGI 应用跑在本模块专用事件循环线程上；lifespan（session manager
#   等启动上下文）手动驱动，等价 uvicorn 的启动阶段；
# - MCP 工具只是 ai_bridge 的薄封装：与 HTTP RPC 口共用同一把 busy 锁与
#   下行分流队列，MCP / RPC / 本地窗口三路调用看到的会话状态完全一致；
# - mcp 包懒导入：没装包只记一条本地日志，PTY 本体功能零影响；
# - 服务不碰主线程：桥接在 server_http 的请求线程 + 专用循环线程里完成。
#
# --mcp-port>0 时退回独立端口模式（自起 uvicorn，供 --port 0 关闭 RPC 口
# 但仍要 MCP 的场景）；--no-mcp 完全关闭。

import asyncio
import functools
import inspect
import logging
import time

try:  # mcp 2.x：FastMCP 改名 MCPServer，Context 在同名包
    from mcp.server.mcpserver import Context as _MCP_CTX
except ImportError:  # mcp 1.x
    try:
        from mcp.server.fastmcp import Context as _MCP_CTX
    except ImportError:  # 未安装 mcp：MCP 服务不会启动，工具也不会注册
        _MCP_CTX = None

_MCP_LOG_ARG_MAX = 400  # 开始行单个参数原始值最多保留的字符数
_MCP_DONE_ARG_MAX = 60  # 完成行单个参数值最多保留的字符数
_MCP_DONE_INDENT = "   "  # 完成行整行左缩进，视觉上从属于同 id 的开始行
_MCP_CALL_PARAMS_MAX = 200  # 在途调用参数缓存上限，防止异常中断的调用无限堆积
_MCP_CALL_PARAMS = globals().setdefault('_MCP_CALL_PARAMS', {})  # call_id -> 原始参数；跨 reload 保活，旧 wrapper 的完成行仍能取到
_MCP_PROGRESS_TICK = 0.5  # 有 progressToken 时的进度上报间隔（秒）
_MCP_PROGRESS_TAIL = 200  # 进度 message 里取实时缓冲尾部多少字节找最后一行
_MCP_WAIT_START_GRACE = 0.2  # wait>=0 时给执行线程的启动宽限（秒），让 busy/未连接这类秒回错误先暴露
_MCP_PARTIAL_OUT = 8000  # wait 到期提前返回时，out 取实时缓冲尾部多少字节



def _mcp_short_arg(value) -> str:
    text = repr(value)
    if len(text) > _MCP_LOG_ARG_MAX:
        return "%s…（共 %d 字符）" % (text[:_MCP_LOG_ARG_MAX], len(text))
    return text


def _mcp_done_arg(value) -> str:
    """完成行的参数值：只留前 60 字符，保证整行紧凑。"""
    text = repr(value)
    if len(text) <= _MCP_DONE_ARG_MAX:
        return text
    clipped = text[:_MCP_DONE_ARG_MAX]
    quote = text[0] if text[:1] in ("'", '"') else ""
    if quote and text.endswith(quote) and not clipped.endswith(quote):
        clipped = clipped[:-1] + "…" + quote  # 补回引号，避免截断值看起来没收尾
    else:
        clipped += "…"
    return clipped


def _mcp_log_begin(name, params) -> str:
    """工具调用一开始就落一条完整参数的日志，返回本次调用 id。

    id 由 os.urandom(3).hex() 生成，6 位十六进制，只用于把完成行和开始行
    关联起来；两行各自都带工具名与参数，可独立阅读：
        [MCP] 926fff 开始 fs_grep pattern='sherpa' path='/root/...' head_limit=30
           [MCP] 926fff 完成 fs_grep pattern='sherpa' path='/root/...' ok 0.21s
    记录的是客户端传进来的原始参数，不等远端返回；远端卡死/超时也能从
    日志台看出这次调用要做什么。
    """
    call_id = os.urandom(3).hex()
    try:
        _MCP_CALL_PARAMS[call_id] = dict(params or {})
        if len(_MCP_CALL_PARAMS) > _MCP_CALL_PARAMS_MAX:
            # dict 保序：批量丢弃最早登记的在途调用，避免缓存无限增长
            for stale in list(_MCP_CALL_PARAMS)[:_MCP_CALL_PARAMS_MAX // 2]:
                _MCP_CALL_PARAMS.pop(stale, None)
        parts = " ".join("%s=%s" % (key, _mcp_short_arg(value))
                         for key, value in (params or {}).items())
        _emit_local("[MCP] %s 开始 %s%s\n"
                    % (call_id, name, (" " + parts) if parts else ""))
    except Exception:
        pass
    return call_id


def _mcp_log_end(call_id, name, elapsed, result=None, exc=None) -> None:
    """与开始行同 id 的完成行：整行缩进，带参数短摘要、状态与耗时。"""
    params = _MCP_CALL_PARAMS.pop(call_id, None)
    try:
        if exc is not None:
            status = "FAIL %s: %s" % (type(exc).__name__, str(exc)[:200])
        elif isinstance(result, dict) and result.get("ok") is False:
            status = "FAIL %s" % str(result.get("error", ""))[:200]
        else:
            status = "ok"
        args = " ".join("%s=%s" % (key, _mcp_done_arg(value))
                        for key, value in (params or {}).items())
        _emit_local("%s[MCP] %s 完成 %s%s %s %.2fs\n"
                    % (_MCP_DONE_INDENT, call_id, name,
                       (" " + args) if args else "", status, elapsed))
    except Exception:
        pass


def _mcp_tool(fn):
    """给工具函数包一层「收到即记 + 同 id 收尾」的结构化日志。

    functools.wraps 保留 __wrapped__/__annotations__，FastMCP 仍能按原签名
    生成 schema；日志发生在调用线程、进入 ai_bridge 之前，所以不受远端耗时影响。
    同时支持同步与 async 工具：async 版本是新加的 pty_run 需要的（它要在等远端
    命令期间 await 进度上报，不能把事件循环卡死）。
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        sig = None

    def _bind(args, kwargs):
        if sig is None:
            return dict(kwargs)
        try:
            return dict(sig.bind_partial(*args, **kwargs).arguments)
        except Exception:
            return dict(kwargs)

    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            call_id = _mcp_log_begin(fn.__name__, _bind(args, kwargs))
            started = time.monotonic()
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                _mcp_log_end(call_id, fn.__name__,
                             time.monotonic() - started, exc=exc)
                raise
            _mcp_log_end(call_id, fn.__name__,
                         time.monotonic() - started, result=result)
            return result

        return async_wrapper

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        call_id = _mcp_log_begin(fn.__name__, _bind(args, kwargs))
        started = time.monotonic()
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            _mcp_log_end(call_id, fn.__name__, time.monotonic() - started, exc=exc)
            raise
        _mcp_log_end(call_id, fn.__name__, time.monotonic() - started, result=result)
        return result

    return wrapper


def _mcp_progress_token(ctx):
    """本次调用请求里的 ``_meta.progressToken``：有 = 客户端愿收进度通知。

    MCP 客户端能力（initialize.capabilities）里没有 progress 项，进度是**按
    单次请求**协商的：只有请求带 progressToken，服务端才允许回
    ``notifications/progress`` 并由 SDK 绑定到该 token；不带 token 时 SDK 的
    ``report_progress`` 本身就是 no-op，这里直接判空可以连轮询都省掉。
    """
    if ctx is None:
        return None
    try:
        meta = ctx.request_context.meta
    except Exception:  # 没有请求上下文（本地直调）或 SDK 版本字段不同
        return None
    if meta is None:
        return None
    if isinstance(meta, dict):
        # mcp 2.x 收进来的是 RequestParamsMeta（TypedDict），SDK 用
        # RequestParams.model_validate(..., by_name=False) 归一化，线上键
        # progressToken 落成 progress_token；1.x/裸 dict 则保持原样。
        for key in ("progressToken", "progress_token"):
            if key in meta:
                return meta.get(key)
        return None
    return (getattr(meta, "progressToken", None)
            or getattr(meta, "progress_token", None))


def _mcp_last_line(text: str, limit: int = 80) -> str:
    """进度 message 里附带的最后一行输出：取最后一段非空文本并截断。"""
    lines = [ln.strip() for ln in (text or "").replace("\r", "\n").split("\n")]
    lines = [ln for ln in lines if ln]
    if not lines:
        return ""
    return lines[-1][:limit]


def _mcp_partial_result(st, wait) -> dict:
    """``wait`` 到期但命令未结束时的返回：任何字段都不允许被误读成"跑完了"。

    ``out`` 只是实时缓冲的最新一段，不是完整结果；``rc`` 必然是 None，
    ``completed/partial/running/wait_exceeded`` 四个标志互相印证，再加
    一条 ``next_action`` 指明唯一正确的后续动作（pty_tail 轮询到 END 标记）。
    """
    st = st or {}
    out = st.get("tail") or ""
    # 与完整结果的 _pack_result 对齐：从最后一条 BEGIN 标记行之后取正文，
    # 否则 tmux 原始回显里的 "echo __AI_BEGIN__; ... __AI_END_...:$__ai_rc"
    # 会留在 out 开头，容易被误读成"已有结束标记"。
    idx = out.rfind("__AI_BEGIN_")
    if idx >= 0:
        nl = out.find("\n", idx)
        if nl >= 0:
            out = out[nl + 1:]
    out = out.strip("\r\n")
    return {
        "ok": False,
        "completed": False,
        "partial": True,
        "running": True,
        "wait_exceeded": True,
        "wait_seconds": wait,
        "rc": None,
        "out": out,
        "elapsed": st.get("elapsed"),
        "total": st.get("total"),
        "channel": st.get("channel"),
        "error": ("wait %gs 到期但命令未结束：out 只是不完整片段（%d 字节），"
                  "rc 未知，命令仍在远端继续执行" % (wait, len(out))),
        "next_action": ("用 pty_tail(max_bytes=4000) 反复轮询查看最新输出，"
                        "直到 tail 中出现 __AI_END_<时间戳>__:<rc> 标记才表示命令结束；"
                        "在此之前再调 pty_run 会返回 busy。"),
    }


def _mcp_run_orphan(fut) -> None:
    """wait 提前返回后，执行线程还在跑：挂回调消费结果/异常，只落本地日志。

    不消费的话 executor future 的异常会变成 "exception was never retrieved"
    噪音；消费了还能在日志台看到这条命令最终跑成什么样。
    """
    def _done(f):
        try:
            res = f.result()
        except Exception as exc:
            _emit_local("[MCP] wait 提前返回的任务最终异常: %s: %s\n"
                        % (type(exc).__name__, str(exc)[:200]))
            return
        if isinstance(res, dict):
            _emit_local(
                "[MCP] wait 提前返回的任务最终完成 rc=%s timed_out=%s out=%d 字节\n"
                % (res.get("rc"), res.get("timed_out"),
                   len(res.get("out") or "")))
    try:
        fut.add_done_callback(_done)
    except Exception:
        pass


async def _mcp_await_run(fut, wait, token, ctx, tail_fn=None):
    """等执行线程的 future，按 ``wait`` 决定最多等多久。

    返回 ``(result, st)``：``result`` 非 None = 命令已结束（含 ai_bridge
    自己的 timeout 到达，此时 result 里 ``timed_out=True``）；``result`` 为
    None = ``wait`` 到期、命令仍在跑，``st`` 是 tail() 快照。

    ``wait < 0``：等到 future 完成（原阻塞行为）；``wait = 0``：立即返回；
    ``wait > 0``：最多等这么多秒。带 progressToken 时每 0.5s 上报一条进度。
    """
    wait = float(wait)
    tail = tail_fn or ai_bridge.tail
    loop = asyncio.get_running_loop()

    async def _report():
        st = tail(_MCP_PROGRESS_TAIL)
        last = _mcp_last_line(st.get("tail") or "")
        try:
            await ctx.report_progress(
                float(st.get("total") or 0), None,
                "已运行 %ss，已收 %d 字节%s"
                % (st.get("elapsed"), st.get("total") or 0,
                   (": " + last) if last else ""))
        except Exception:
            pass  # 客户端断开/不支持时不影响命令本身

    if wait < 0:
        if token is None:
            await asyncio.wait([fut])
            return fut.result(), None
        while True:
            await asyncio.wait([fut], timeout=_MCP_PROGRESS_TICK)
            if fut.done():
                return fut.result(), None
            await _report()

    # wait>=0 也留一小段启动宽限：不然 busy / 未连接这类秒回错误会被当成
    # "已启动并在跑"，白白返回一个空的不完整快照。
    deadline = loop.time() + max(wait, _MCP_WAIT_START_GRACE)
    while True:
        remain = deadline - loop.time()
        if remain <= 0:
            break
        step = min(_MCP_PROGRESS_TICK, remain) if token is not None else remain
        await asyncio.wait([fut], timeout=step)
        if fut.done():
            return fut.result(), None
        if token is not None:
            await _report()
    # 到点边界上刚好完成也算完成，避免把成功结果误报成不完整
    if fut.done():
        return fut.result(), None
    return None, tail(_MCP_PARTIAL_OUT)



def _mcp_quiet_sdk_logs() -> None:
    """压掉无状态 streamable-http 的 INFO 噪音。

    stateless 模式每请求新建一个临时 session，SDK 在拆 session 时打
    "Terminating session: None"，既没有 id 也没法跟工具调用对应，只会把
    真正的日志冲散。只降这一条 logger 的级别，WARNING 以上照常输出。
    """
    logging.getLogger("mcp.server.streamable_http").setLevel(logging.WARNING)


_MCP_SERVER = globals().setdefault('_MCP_SERVER', None)  # 启动/挂载成功后保存 MCP server 实例（诊断/测试用）；setdefault 防 reload 重绑丢引用
_MCP_ASGI_APP = globals().setdefault('_MCP_ASGI_APP', None)  # 挂 1188 口模式下的 ASGI 应用。最要命的一个：已打进 RPCRequestHandler 的前置分发 wrapper 运行期按模块全局名查本变量，reload 把名字重绑回 None 后 /mcp 请求静默回落成普通 RPC 执行——setdefault 让旧钩子跨 reload 继续拿到活应用
_MCP_HTTP_PATHS = globals().setdefault('_MCP_HTTP_PATHS', frozenset())  # 需在 RPC 口上拦截分流的路径集合；与 _MCP_ASGI_APP 同生共死，reload 丢集合同样让分流失效
_MCP_LOOP = globals().setdefault('_MCP_LOOP', None)  # ASGI 专用事件循环（后台线程）；reload 丢引用会让 _mcp_ensure_loop 再起第二条循环线程
_MCP_LOOP_LOCK = globals().setdefault('_MCP_LOOP_LOCK', threading.Lock())  # 循环线程启动互斥锁；reload 重造新锁 + _MCP_LOOP 名字回 None = 判空双起循环线程，锁必须与引用一起跨 reload 保活

_MCP_INSTRUCTIONS = (
    "远端服务器的常驻交互 PTY。pty_run 跑非交互命令并回收 rc/输出"
    "（wait>=0 会提前返回不完整结果，此时 completed=false，用 pty_tail 续看）；"
    "交互提示（密码/y/n）用 pty_send 喂键；pty_status 看会话与 broker "
    "状态；pty_log 看本地诊断日志；画面冻结时 pty_redraw 自愈。"
    "同一 shell 串行执行，重叠调用返回 busy。"
    "文件工具经 MQTT RPC 在远端 server 进程里执行，路径语义是远端机器的"
    "路径（不是 pty 客户端本机），默认允许任意路径：fs_read/fs_ls/"
    "fs_glob/fs_grep 只读；fs_write/fs_edit/fs_mkdir/fs_move/fs_delete 可写，"
    "仅 --fs-read-only 时整体禁用写操作。")


def _mcp_pick_transport(server, prefer: str) -> str:
    """选 HTTP transport：显式指定优先；自动时按 SDK 能力选。"""
    if prefer in ("streamable-http", "sse"):
        return prefer
    return "streamable-http" if hasattr(server, "streamable_http_app") else "sse"


def _new_mcp_server(host=None, port=None):
    """构造 MCP server 实例（mcp 1.x/2.x 双兼容），返回 (server, kind)。

    mcp 2.x 把 FastMCP 改名 MCPServer 且 host/port 移进 run() 参数；1.x
    的 host/port 在构造器 Settings 里。host/port 仅独立端口模式需要。
    不可用时返回 (None, None)。
    """
    try:
        try:  # mcp 2.x
            from mcp.server.mcpserver import MCPServer as _K
            kind = "2"
        except ImportError:  # mcp 1.x
            from mcp.server.fastmcp import FastMCP as _K
            kind = "1"
        try:
            if kind == "1" and port is not None:
                return _K("pty", instructions=_MCP_INSTRUCTIONS,
                          host=str(host), port=int(port)), kind
            return _K("pty", instructions=_MCP_INSTRUCTIONS), kind
        except TypeError:  # 构造器不收 instructions/host/port 的老版本
            server = _K("pty")
            if kind == "1" and port is not None:
                try:
                    server.settings.instructions = _MCP_INSTRUCTIONS
                    server.settings.host = str(host)
                    server.settings.port = int(port)
                except Exception:
                    pass
            return server, kind
    except Exception as exc:
        _emit_local('[pty][MCP] mcp 包不可用，跳过（pip install "mcp[cli]" 启用）: %s\n' % exc)
        return None, None


def _mcp_register_tools(server) -> None:
    """注册 MCP 工具：7 个 PTY 工具（直通 ai_bridge）+ 9 个文件工具（远端执行）。"""
    _mcp_quiet_sdk_logs()

    @server.tool()
    @_mcp_tool
    async def pty_run(cmd: str, timeout: float = 60.0,
                      acquire_timeout: float = 2.0, pane: str = "",
                      wait: float = -1.0, encoding: str = "",
                      ctx: _MCP_CTX = None) -> dict:
        """在常驻远程 shell 里跑一条命令，等结束标记并回收输出与退出码。

        返回 {"ok", "rc", "out", "timed_out", "pane", "channel", ...}；
        命令串行执行，上一条未结束时立刻返回 {"ok": False, "busy": True}
        （可加大 acquire_timeout 或稍后重试）。超时只停止回收，远端命令继续
        跑，可用 pty_send 干预或再 pty_run 收尾。

        ``wait``：本次调用最多阻塞多久（秒）。默认 -1 = 一直等到命令结束或
        ``timeout`` 上限（原行为）；0 = 启动后立即返回；正数 = 最多等这么久。
        提前返回时**结果一定不完整**，返回里明确标注
        ``completed=False / partial=True / running=True / wait_exceeded=True``
        且 ``rc=None``，``out`` 只是最新一段输出；此时按 ``next_action`` 用
        ``pty_tail`` 轮询到 ``__AI_END_<时间戳>__:<rc>`` 标记为止（这之前再调
        pty_run 会 busy）。不要把它当成命令失败或已完成。

        投递窗口：默认用会话启动时固化的 tmux 窗口（session 内 window_index
        最小的窗口，可用 pty_pane 或 --ai-pane 改）；pane 传 "%2"/"0:2"/窗口名
        可临时改本次目标。tmux 不可用时自动回落为直写本 PTY。

        ``encoding``：留空 = 自动（无控制字符原文直发；有裸换行且会话 shell
        是 bash/zsh 时用 ``$'...'`` 保持窗口可读；否则 base64）；``dollar``
        强制 ANSI-C 引用；``base64`` 强制严格编码。选择只看字符构成，与命令
        长短无关；多行命令在 Windows/未知 shell 上会自动走 base64，窗口里
        看到的是密文，这是预期行为。

        进度上报：客户端在本次请求里带 ``_meta.progressToken`` 时，命令执行
        期间每 0.5 秒回一条 notifications/progress，message 带已运行时长、
        已收字节数和最后一行输出；没带 token 就完全不开轮询（零额外开销）。
        注意进度只是**中途预览**，完整结果仍以本调用最终返回的 out/rc 为准。
        """
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(
            None,
            functools.partial(
                ai_bridge.run, str(cmd), timeout=float(timeout),
                acquire_timeout=float(acquire_timeout),
                pane=(str(pane) if pane else None),
                encoding=(str(encoding) if encoding else None)))
        token = _mcp_progress_token(ctx)
        result, st = await _mcp_await_run(fut, wait, token, ctx)
        if result is None:
            # 调用先返回，后台线程继续跑：挂回调消费结果/异常，避免
            # "exception was never retrieved" 并且日志台仍能看到最终 rc。
            _mcp_run_orphan(fut)
            return _mcp_partial_result(st, float(wait))
        return result

    @server.tool()
    @_mcp_tool
    def pty_pane(pane: str = "") -> dict:
        """查询或设置 AI 命令的默认 tmux 窗口。

        pane 留空 = 只查询当前值；"auto" = 清空固化值并按「index 最小的窗口」
        重新解析；其他值 = 固化为该 target（%2 最稳，也接受 0:2 / 窗口名）。
        返回 {"ok", "pane", "pane_source", "tmux_ok", "encoding"}。
        """
        if not str(pane or "").strip():
            return ai_bridge.get_pane()
        return ai_bridge.set_pane(pane)

    @server.tool()
    @_mcp_tool
    def pty_tail(max_bytes: int = 8000) -> dict:
        """查看**正在跑**的长命令已产出的输出（不占 busy 锁，可与 pty_run 并发）。

        pty_run 要等命令结束才返回；长任务期间用本工具反复调用即可看到最新
        进度（服务端推送通道边产生边累积）。返回 {"ok", "tail", "cmd",
        "channel", "elapsed", "total"}；channel 为 pty / tmux / tmux+push。
        """
        return ai_bridge.tail(max_bytes=int(max_bytes))

    @server.tool()
    @_mcp_tool
    def pty_send(data: str) -> dict:
        """向常驻 PTY 原样喂键，不回收输出：回答密码/y/n、发 Ctrl-C 等。

        例："y\\n" 确认提示、"\\x03" Ctrl-C、"vim file\\r" 打开编辑器。
        """
        return ai_bridge.send(data)

    @server.tool()
    @_mcp_tool
    def pty_status() -> dict:
        """查询常驻会话状态：是否 attached/已结束、是否忙、broker 在线数、
        终端写线程卡滞秒数（>2 建议 pty_redraw）等。"""
        return ai_bridge.status()

    @server.tool()
    @_mcp_tool
    def pty_log(n: int = 100) -> str:
        """读最近 n 行本地诊断日志（broker 连接/RPC 请求/告警），n 夹在 1~500。"""
        return get_log(max(1, min(int(n), 500)))

    @server.tool()
    @_mcp_tool
    def pty_redraw() -> dict:
        """终端写线程卡滞自愈：必要时切换本地写线程，并通知远端整屏重绘。"""
        steps = []
        if _OUT_WRITER is not None and _OUT_WRITER.stall_seconds() > 2.0:
            steps.append(reset_out_writer())
        pty = getattr(ai_bridge, "pty", None)
        if pty is None or getattr(pty, "end_reason", None):
            return {"ok": False, "error": "pty 未连接或已结束", "steps": steps}
        try:
            pty.configure(redraw=True)
        except Exception as exc:
            steps.append({"redraw": False, "error": str(exc)})
            return {"ok": False, "steps": steps}
        steps.append({"redraw": True})
        return {"ok": True, "steps": steps}

    # 文件工具（098_mcp_files）：经 transport.request 在远端执行，不经过 PTY shell
    _mcp_register_fs_tools(server)


def _mcp_loop_main(loop):
    """ASGI 专用事件循环线程体。"""
    global _MCP_LOOP
    asyncio.set_event_loop(loop)
    _MCP_LOOP = loop
    loop.run_forever()


def _mcp_ensure_loop():
    """懒启动专用事件循环线程，返回 loop（桥接与 lifespan 共用）。"""
    global _MCP_LOOP
    with _MCP_LOOP_LOCK:
        if _MCP_LOOP is None or _MCP_LOOP.is_closed():
            loop = asyncio.new_event_loop()
            threading.Thread(target=_mcp_loop_main, args=(loop,),
                             name="pty-mcp-loop", daemon=True).start()
            while _MCP_LOOP is None:
                time.sleep(0.01)
    return _MCP_LOOP


def _mcp_drive_lifespan(app, loop) -> None:
    """手动驱动 ASGI lifespan（等价 uvicorn 启动阶段）：起 session manager 等。

    只发 startup 不发 shutdown：常驻服务随进程退出（线程为 daemon）。
    """
    q = asyncio.Queue()

    async def receive():
        return await q.get()

    async def send(message):
        if message.get("type") == "lifespan.startup.failed":
            _emit_local("[pty][MCP] MCP lifespan 启动失败: %s\n"
                        % message.get("message", "?"))

    async def _drive():
        await q.put({"type": "lifespan.startup"})
        await app({"type": "lifespan", "asgi": {"version": "3.0",
                                                "spec_version": "2.0"},
                   "state": {}}, receive, send)

    asyncio.run_coroutine_threadsafe(_drive(), loop)


class _MCPHttpBridge:
    """1188 口 /mcp 请求 -> MCP ASGI 应用的同步桥。

    server_http 每请求一线程；ASGI 应用跑在专用事件循环线程上：请求体
    一次性读齐，receive 首次返回后即挂起；send 的响应字节由循环线程直接
    写回（此刻请求线程正阻塞等待，单写者安全）。无 content-length 的流式
    响应按 Connection: close 收尾；SSE 长连接由客户端断开，写失败即整条
    请求结束。
    """

    def __init__(self, handler):
        self.h = handler

    def run(self, app) -> None:
        h = self.h
        path, _, query = h.path.partition("?")
        try:
            length = int(h.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = h.rfile.read(length) if length > 0 else b""
        body_state = {"sent": False}
        state = {"started": False, "streaming": False}

        async def receive():
            if not body_state["sent"]:
                body_state["sent"] = True
                return {"type": "http.request", "body": body,
                        "more_body": False}
            await asyncio.Event().wait()  # 请求已完整读入：挂起等断开

        async def send(message):
            if message["type"] == "http.response.start":
                state["started"] = True
                h.log_request(message["status"])
                h.send_response_only(message["status"])
                has_len = False
                for k, v in message.get("headers", []):
                    name = bytes(k).decode("latin-1")
                    value = bytes(v).decode("latin-1")
                    low = name.lower()
                    if low in ("connection", "transfer-encoding"):
                        continue  # 由桥统一决定：流式时补 Connection: close
                    if low == "content-length":
                        has_len = True
                    h.send_header(name, value)
                state["streaming"] = not has_len
                if state["streaming"]:
                    h.send_header("Connection", "close")
                h.end_headers()
            elif message["type"] == "http.response.body":
                chunk = message.get("body", b"") or b""
                if chunk:
                    h.wfile.write(chunk)
                    h.wfile.flush()
                if state["streaming"] and not message.get("more_body"):
                    h.close_connection = True

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": h.request_version.rsplit("/", 1)[-1] or "1.1",
            "method": h.command,
            "scheme": "http",
            "path": path,
            "query_string": query.encode("latin-1"),
            "root_path": "",
            "server": (h.server.server_address[0], h.server.server_address[1]),
            "client": (h.client_address[0], h.client_address[1]),
            "headers": [(k.lower().encode("latin-1"), v.encode("latin-1"))
                        for k, v in h.headers.items()],
        }
        try:
            fut = asyncio.run_coroutine_threadsafe(app(scope, receive, send),
                                                   _mcp_ensure_loop())
            fut.result()
        except (ConnectionError, BrokenPipeError, OSError):
            h.close_connection = True  # 客户端断开：正常收尾
        except Exception as exc:
            _emit_local("[pty][MCP] /mcp 桥接异常: %r\n" % exc)
            h.close_connection = True
            if not state["started"]:
                try:
                    h.send_error(502, "MCP bridge error")
                except Exception:
                    pass


def _mcp_hook_handler(Handler) -> None:
    """给 RPCRequestHandler 打前置分发钩子（幂等）：保留路径分流给 ASGI 桥。"""
    if not hasattr(Handler, "do_DELETE"):
        # BaseHTTPRequestHandler 对未实现方法回 501；补上让 /mcp 的 DELETE
        # （MCP 会话结束信号）有机会被钩子接管，其余 DELETE 维持 501 原状。
        def _do_delete(self):
            self.send_error(501, "Unsupported method (%r)" % self.command)
        Handler.do_DELETE = _do_delete

    def _wrap(fn):
        def wrapper(self, *args, **kwargs):
            if (_MCP_ASGI_APP is not None
                    and self.path.split("?", 1)[0] in _MCP_HTTP_PATHS):
                _MCPHttpBridge(self).run(_MCP_ASGI_APP)
                return None
            return fn(self, *args, **kwargs)
        wrapper.__name__ = getattr(fn, "__name__", "handler")
        wrapper.__doc__ = getattr(fn, "__doc__", None)
        wrapper._mcp_wrapped = True  # 幂等键挂在被包装函数上而非模块级 _MCP_HOOKED 标志：标志在 reload 后重置 False 会把同一个类再包一层（请求链上叠双层路径检查），而函数对象自带标记跨 reload 永不失真
        return wrapper

    for name in ("do_GET", "do_POST", "do_DELETE"):
        base = getattr(Handler, name, None)
        # 已带 _mcp_wrapped 的（含 reload 前包过的旧类）直接跳过；server_http 被
        # reload 出新类时此处自然给新类补钩子，旧类旧钩子经 setdefault 保住的
        # _MCP_ASGI_APP 继续工作，两边都不脱靶
        if base is not None and not getattr(base, "_mcp_wrapped", False):
            setattr(Handler, name, _wrap(base))


def mount_mcp_on_rpc(prefer: str = "", host: str = "0.0.0.0") -> dict:
    """把 MCP 端点挂到 --port 已起的 HTTP RPC 服务器上（同口复用，不新开端口）。

    1188 一个口上 RPC / 浏览器日志台 / MCP 三种服务并存；返回启动结果
    dict；任何失败只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER, _MCP_ASGI_APP, _MCP_HTTP_PATHS  # 原 _MCP_HOOKED 模块标志已删除：幂等改由被包装函数上的 _mcp_wrapped 标记承担，标志在 reload 后重置 False 会失真
    if _MCP_ASGI_APP is not None:
        return {"ok": True, "already": True, "paths": sorted(_MCP_HTTP_PATHS)}
    try:
        import server_http
    except Exception as exc:
        return {"ok": False, "error": "server_http 不可用: %s" % exc}
    server, kind = _new_mcp_server()
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)

    app, paths = _mcp_build_asgi(server, prefer, host)
    if app is None or not paths:
        return {"ok": False, "error": "当前 mcp SDK 未提供可挂载的 HTTP 应用"}

    _mcp_hook_handler(server_http.RPCRequestHandler)  # 无条件调用：钩子按 fn._mcp_wrapped 自幂等，不再依赖会被 reload 重置的模块标志
    loop = _mcp_ensure_loop()
    _mcp_drive_lifespan(app, loop)
    _MCP_SERVER = server
    _MCP_HTTP_PATHS = frozenset(paths)
    _MCP_ASGI_APP = app
    _emit_local("[pty][MCP] MCP 工具服务已挂到 RPC 口：路径 %s（与 RPC/日志台同口复用）"
                "  工具：pty_run/pty_send/pty_status/pty_log/pty_redraw + "
                "fs_read/fs_write/fs_edit/fs_glob/fs_grep/fs_ls/fs_mkdir/fs_move/fs_delete\n"
                % "、".join(sorted(paths)))
    return {"ok": True, "paths": sorted(paths)}


def _mcp_build_asgi(server, prefer: str, host: str):
    """按 SDK 能力取 ASGI 应用与它占用的 HTTP 路径（优先 streamable-http）。"""
    if prefer != "sse" and hasattr(server, "streamable_http_app"):
        try:
            try:
                # 无状态模式：每请求新建 transport，不校验 Mcp-Session-Id。
                # 有状态会话只存在进程内存里，pty 重启即整体失效；客户端断线后
                # 往往只重连 SSE、不重新 initialize，会持续拿旧 session id 请求
                # 并被 404 "Session not found" 拒绝，表现为「list tools failed」。
                # 无状态下旧 id 直接忽略，重启不再断链。
                app = server.streamable_http_app(host=str(host), stateless_http=True)
            except TypeError:  # 旧版 SDK 不认 stateless_http：退回有状态模式
                try:
                    app = server.streamable_http_app(host=str(host))
                except TypeError:
                    app = server.streamable_http_app()
            base = str(getattr(getattr(server, "settings", None),
                               "streamable_http_path", None) or "/mcp").rstrip("/")
            return app, {base, base + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] streamable-http 应用创建失败，改试 sse: %r\n" % exc)
    if hasattr(server, "sse_app"):
        try:
            try:
                app = server.sse_app(host=str(host))
            except TypeError:
                app = server.sse_app()
            st = getattr(server, "settings", None)
            sse = str(getattr(st, "sse_path", None) or "/sse").rstrip("/")
            msg = str(getattr(st, "message_path", None) or "/messages").rstrip("/")
            return app, {sse, sse + "/", msg, msg + "/"}
        except Exception as exc:
            _emit_local("[pty][MCP] sse 应用创建失败: %r\n" % exc)
    return None, set()


def start_mcp_server(port, host="0.0.0.0", prefer: str = "") -> dict:
    """独立端口模式：在 daemon 线程里自起 MCP HTTP 服务（--mcp-port>0 时用）。

    挂 1188 口的常规路径见 mount_mcp_on_rpc。返回启动结果 dict；任何失败
    只进本地日志，不影响 PTY 会话。
    """
    global _MCP_SERVER
    port = int(port)
    if port <= 0:
        return {"ok": False, "error": "mcp_port<=0，独立 MCP 服务未开启"}
    if _MCP_SERVER is not None:
        return {"ok": True, "already": True, "port": port}
    server, kind = _new_mcp_server(host=host, port=port)
    if server is None:
        return {"ok": False, "error": "mcp 包不可用"}
    _mcp_register_tools(server)
    _MCP_SERVER = server

    transport = _mcp_pick_transport(server, (prefer or "").strip().lower())

    def _serve():
        try:
            if kind == "2":  # 2.x：host/port 是 run() 的 transport 参数
                server.run(transport=transport, host=str(host), port=port)
            else:
                server.run(transport=transport)
        except Exception as exc:
            _emit_local("[pty][MCP] MCP 服务异常退出: %s\n" % exc)

    threading.Thread(target=_serve, name="pty-mcp", daemon=True).start()
    path = "/mcp" if transport == "streamable-http" else "/sse"
    _emit_local("[pty][MCP] MCP 工具服务已开启（%s 独立端口）：http://%s:%d%s  "
                "工具：pty_run/pty_send/pty_status/pty_log/pty_redraw + 9 个 fs_* 文件工具\n"
                % (transport, host, port, path))
    return {"ok": True, "transport": transport, "port": port, "path": path}


