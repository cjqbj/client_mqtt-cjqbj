"""与传输层无关的 Python RPC 执行器。"""

import ast
import asyncio
import io
import keyword
import sys
import threading
import textwrap
import traceback


# ------------------------------------------------------------------ #
# code alias 指令关键字
# ------------------------------------------------------------------ #
# 关键字本身定义为 list：运行期可动态增删元素来自定义触发词。例如
# ``rpc_executor.ALIAS_KEYWORDS.append('a')`` 之后 ``#a abc`` 与
# ``#alias abc`` 等价；增删查改所有指令都从同一个 list 匹配。
# 两个 list 不应包含相同元素（重叠时按 alias 语义解析）。
ALIAS_KEYWORDS = ["alias",'a']
UNALIAS_KEYWORDS = ["unalias",'ua','d']
#发送只有 #alias 三个字（不带名字、不带代码）即可 list all alias

 


class PythonExecutor:
    """在多次请求之间保持状态的 Python 代码片段执行器。

    命名空间模型
    ------------
    ``self.globals_dict`` 和 ``self.locals_dict`` 指向**同一个**字典对象。
    这是 CPython 的 ``exec`` 语义所要求的：由 ``exec`` 创建出来的函数，
    在解析其全局名 / 自由名时，只会在传给 ``exec`` 的 *globals* 字典里
    查找——永远不会查 *locals* 字典。如果两者是不同对象，那么所有模块级
    的名字（import、赋值、def）在由被执行代码定义的函数内部都会变得不可
    见，例如::
        g = 3
        def fa(): return g    # 若 globals/locals 分离，这里会 NameError

    因此公开 API（``__init__`` 和 ``execute``）只暴露一个 ``globals``
    参数；``locals_dict`` 在内部作为别名存在，以便两个名字都可被内省 /
    兼容使用。

    命名空间拷贝策略（``copy_globals``）
    ----------------------------------
    默认 ``copy_globals=False``：执行器**直接持有**调用方传入的字典引用，
    不做任何拷贝。调用方在外部对该字典新增 / 修改的名字（例如启动后才
    赋值的服务端实例）在后续 RPC 中实时可见。MQTT 通道依赖这一语义。

    仅当显式传 ``copy_globals=True`` 时，才对传入字典做一次**浅拷贝**
    （``dict(globals)``），执行器此后与调用方字典彻底脱钩，等价于服务
    启动时拍一张快照。HTTP 通道若需要请求间与调用方 globals 隔离，可
    显式开启；注意浅拷贝只复制顶层键，可变对象仍是共享引用。

    ipy 中可以验证 globals() == locals()
    Out[11]: True

    如果要在函数内部 调用 PythonExecutor 执行时绕过共享命名空间，可以直接赋值 ``self.locals_dict``。
    """

    # 指令关键字：与模块级 list 共享同一对象，运行期改任一引用都立即生效
    alias_keywords = ALIAS_KEYWORDS
    unalias_keywords = UNALIAS_KEYWORDS

    def __init__(self, globals=None, main_loop=None, copy_globals=False, alias=True):
        # 默认零拷贝：直接持有调用方字典，外部对它的修改实时可见
        # （server_mqtt 通道依赖此行为，启动后才创建的 gms 等名字必须可见）。
        # 仅在 copy_globals=True 时浅拷贝一次，与调用方字典脱钩形成快照。
        if globals is not None:
            self.globals_dict = dict(globals) if copy_globals else globals
        else:
            self.globals_dict = {}

        self.globals_dict.setdefault("__name__", "__rpc_exec__")

        # 必须指向同一个对象（原因见类文档字符串）。
        self.locals_dict = self.globals_dict

        self.main_loop = main_loop
        self.lock = threading.RLock()

        # code alias 总开关（默认启用），execute 可按调用覆盖；
        # 注册表：name -> {"code","created","updated","hits","last_used"}
        self.alias = alias
        self.aliases = {}

        # [timeout 守卫] 上一次 execute 已超时但后台代码仍在运行时为 True：
        # 此期间新 execute 一律快速失败，避免把调用线程（MQTT 分发线程 /
        # HTTP worker）再次挂住；后台线程结束后自动回 False（见 _run_bounded）。
        self._stuck = False

    # ------------------------------------------------------------------ #
    # 公开 API
    # ------------------------------------------------------------------ #

    def execute(self, code,globals=None,format_result=False,alias=None,timeout=999):
        """执行 ``code``。

        ``globals`` 若提供，应是一个包含**请求级**上下文变量的映射。
        这些变量只在本次调用期间注入共享命名空间，执行完毕后会被还原
        （或删除），因此不会泄漏到持久的 REPL 状态中。

        注意：整个“保存旧值 → 注入 → 执行 → 还原”的过程都在同一把
        可重入锁内完成。否则并发请求会互相看到对方注入的上下文变量，
        并且还原顺序错乱，导致请求级名字永久泄漏到持久命名空间里。

        code alias
        ----------
        ``alias`` 传 ``None``（默认）时沿用构造参数 ``alias``（默认
        ``True``）。关闭时 ``#alias`` / ``#unalias`` 首行只被当作普通
        注释、整段精确匹配的别名替换也不发生，行为与未引入 alias 前
        完全一致，可用于在个别调用里临时绕过别名（例如确实想执行
        名字恰好等于某别名的变量，或调试别名本身）。

        启用时：首行是 ``#alias`` / ``#unalias``（或 ALIAS_KEYWORDS /
        UNALIAS_KEYWORDS 列表里的任意关键字）时，按增删查改指令处理，
        不走常规执行；define 指令注册成功后会把 body 立即执行一次，
        注册信息附在返回 dict 的 ``alias`` 键上。整段输入**精确等于**
        某个已注册别名时，替换为别名 body 后再执行（因此 ``abc+1``、
        ``print(abc)`` 等普通代码完全不受别名影响）。

        timeout
        -------
        最长等待秒数（默认 999，兼容长任务；``None`` 表示无限等待的
        旧同步行为）。执行始终发生在 daemon 工作线程上：

        - 正常结束：返回执行结果 dict，语义与之前完全一致；
        - 超时且代码已在运行：立即返回 ok=False 超时错误，执行器进入
          busy——后续 execute 快速失败并给出明确错误，后台线程结束后
          自动恢复。调用线程（MQTT 分发线程 / HTTP worker）不再被
          死循环或超长任务永久挂死；
        - 超时仍在排队等锁：本请求作废（后台取到锁后即退出），不会
          晚到执行造成重复副作用。
        """
        if not isinstance(code, str) or not code.strip():
            return {"r": "", "stdout": "", "ok": False, "error": "code is required"}

        # alias=None 沿用实例构造时的总开关
        if alias is None:
            alias = self.alias

        def _job():
            with self.lock:
                return self._execute_inner(code, globals, format_result, alias)

        return self._run_bounded(_job, timeout)

    def _run_bounded(self, job, timeout):
        """在 daemon 工作线程里执行 job，最多等 ``timeout`` 秒。

        ``timeout=None`` 时同步直跑（旧行为，供库内调用兼容）。job 自身
        持 self.lock，串行语义与旧版完全一致；锁由工作线程持有，调用
        线程超时先行返回不会破坏命名空间串行性——仍在运行的后台代码
        独占锁，新请求要么排队要么被 busy 拒绝，绝不与之交错执行。
        """
        if timeout is None:
            return job()
        if self._stuck:
            return {"r": None, "stdout": "", "ok": False,
                    "error": "executor busy: 上一次执行已超时且仍在后台运行，"
                             "其结束后自动恢复接收新请求"}

        result = {}
        done = threading.Event()
        started = threading.Event()
        abandoned = threading.Event()

        def _work():
            try:
                with self.lock:
                    # 先 started 再查 abandoned，调用方则先 abandoned 再查
                    # started：双检次序保证两者判定一致——调用方判"未开始
                    # →作废"时，worker 要么还没到这行（必见作废退出），要么
                    # 已 set started（调用方会看到并转入超时路径），不存在
                    # "调用方已宣布作废、job 仍晚到执行"的窗口。
                    started.set()
                    if abandoned.is_set():
                        return          # 等锁期间调用方已超时作废本请求
                    result["v"] = job()
            except BaseException:       # job 自带兜底返回 dict，这里再保险一层
                result["v"] = {"r": None, "stdout": "", "ok": False,
                               "error": traceback.format_exc()}
            finally:
                # 顺序必须是先 done 再清 busy：调用方只在 done 未置位时才
                # 会置 _stuck，此序保证后台的清除必然晚于那次置位。
                done.set()
                self._stuck = False

        threading.Thread(target=_work, name="rpc-exec", daemon=True).start()

        def _result_or_internal_error(v):
            # worker 在 started 后撞见作废标志退出时不产生结果：仍须返回
            # 结构完整的错误 dict，绝不让 execute 返回 None 破坏通道封装
            if isinstance(v, dict):
                return v
            return {"r": None, "stdout": "", "ok": False,
                    "error": "executor internal: no result produced "
                             "(request aborted before job started)"}

        if done.wait(timeout):
            return _result_or_internal_error(result.get("v"))
        # 先标作废再判断是否已开始（与 worker 的次序配对，见 _work 注释）
        abandoned.set()
        if started.is_set():
            # 代码已在运行且超时：标记 busy，本调用立即返回超时错误
            self._stuck = True
            if done.is_set():
                # 最后瞬间跑完：收回 busy 标记并正常取结果
                self._stuck = False
                return _result_or_internal_error(result.get("v"))
            return {"r": None, "stdout": "", "ok": False,
                    "error": f"execution timed out after {timeout}s；"
                             f"代码仍在后台运行，执行器将拒绝新请求直至其结束"}
        # 尚未开始：作废生效，worker 必然在 started 后查到作废并退出
        return {"r": None, "stdout": "", "ok": False,
                "error": f"executor busy: 等待上一个执行释放锁超过 {timeout}s，"
                         f"本请求已作废（不会被延迟执行）"}

    def _execute_inner(self, code, globals=None, format_result=False, alias=True):
        """execute 的同线程同步版（含 alias 指令解析与递归）。

        调用方必须已持有 self.lock（工作线程 / alias 定义即执行的递归）。
        _alias_define 的递归必须走这里：若再进公开 execute 会另起工作
        线程排队等自己已持有的锁，直到超时。
        """
        if alias:
            directive = self._parse_alias_directive(code)
            if directive is not None:
                return self._handle_alias_directive(directive, globals, format_result, alias)
        return self._execute_code_locked(code, globals, format_result, alias)

    def _execute_code_locked(self, code, globals=None, format_result=False, alias=True):
        """execute 的常规执行主体：alias 展开 / 请求级注入 / 执行 / 还原。

        调用方必须已持有 self.lock。返回结构见 execute 文档。
        """
        output = io.StringIO()
        if alias:
            code = self._expand_alias(code)
        saved = {}
        injected = []
        try:
            if isinstance(globals, dict):
                for k, v in globals.items():
                    if k in self.globals_dict:
                        saved[k] = self.globals_dict[k]
                    self.globals_dict[k] = v
                    injected.append(k)

            try:
                with _redirect_stdout(output):
                    result = self._execute(code)
                if format_result: #让 r始终是 str 对象
                    # 参数名 format_result 遮蔽了模块级同名函数，经 sys.modules 取原函数；
                    # 非 JSON 可序列化对象（如 sys 模块）在此统一转为字符串
                    result = sys.modules[__name__].format_result(result)
                return {
                    "r": result,
                    "stdout": output.getvalue(),
                    "ok": True,
                }
            except (Exception, SystemExit): # 修复点：增加捕获 SystemExit，防止直接 kill server
                return {
                    "r": None,
                    "stdout": output.getvalue(),
                    "ok": False,
                    "error": traceback.format_exc(),
                }
        finally:
            # 还原 / 删除请求级名字。仍在锁内，保证其他线程不会在
            # 我们还原到一半时观察到半成品命名空间。
            for k in injected:
                if k in saved:
                    self.globals_dict[k] = saved[k]
                else:
                    self.globals_dict.pop(k, None)

    # ------------------------------------------------------------------ #
    # code alias：#alias / #unalias 指令与别名替换
    # ------------------------------------------------------------------ #
    # 语法（关键字取自 ALIAS_KEYWORDS / UNALIAS_KEYWORDS，均可运行期增删）：
    #   #alias                    列出全部别名（name -> code）
    #   #alias abc                查询单个别名（完整记录含 hits 等元数据）
    #   #alias abc ⏎ <code>       增 / 改：注册并立即执行一次 body
    #   #unalias abc [def ...]    删：支持一次删多个；不存在则报错
    #   abc                       用：整段输入精确等于别名时替换执行。
    # 指令行必须是非空首行；其后的所有行整体作为 body（自动 dedent）。
    # ------------------------------------------------------------------ #

    def _parse_alias_directive(self, code):
        """识别首行 ``#<关键字> ...`` 指令，返回 dict 或 None。

        返回 ``{"kind": "alias"|"unalias", "arg": str, "body": str}``；
        首行不是指令时返回 ``None``，代码按普通路径执行。纯解析不改
        状态，无需持锁。
        """
        head, _, body = code.lstrip().partition("\n")
        token = head.strip()
        if not token.startswith("#"):
            return None
        words = token[1:].split(None, 1)
        if not words:
            return None
        keyword, arg = words[0], (words[1].strip() if len(words) > 1 else "")
        # alias 优先于 unalias 匹配：两个 list 有重叠元素时按 alias 语义解析
        if keyword in self.alias_keywords:
            return {"kind": "alias", "arg": arg, "body": body}
        if keyword in self.unalias_keywords:
            return {"kind": "unalias", "arg": arg, "body": body}
        return None

    def _handle_alias_directive(self, directive, globals=None, format_result=False, alias=True):
        """按指令 kind 分发增删查改。调用方（execute）已持有 self.lock。"""
        if directive["kind"] == "unalias":
            return self._alias_unalias(directive["arg"])

        body = textwrap.dedent(directive["body"]).strip()
        names = directive["arg"].split()
        if not names:
            if body:
                return self._alias_error(
                    "usage: #<关键字> <名字> 换行 <代码>；#<关键字> 查全部，"
                    "#<关键字> <名字> 查单个"
                )
            return self._alias_list_all()
        if len(names) > 1:
            return self._alias_error(f"指令行只允许一个别名名字，收到: {names}")
        if body:
            return self._alias_define(names[0], body, globals, format_result, alias)
        return self._alias_show(names[0])

    def _expand_alias(self, code):
        """整段输入精确等于某个已注册别名时替换为别名 body。

        只做顶层精确匹配：``abc+1``、``print(abc)`` 等不受影响，
        因此别名不会与命名空间里的同名变量产生隐蔽干扰——
        想访问同名变量时写出更长的表达式即可。
        """
        info = self.aliases.get(code.strip())
        if info is None:
            return code
        info["hits"] = info.get("hits", 0) + 1
        info["last_used"] = utc_ms()
        return info["code"]

    def _alias_define(self, name, body, globals=None, format_result=False, alias=True):
        """注册（重名覆盖，保留 created / hits 计数）并立即执行一次 body。

        请求级 ``globals`` 透传给 body 的执行，因此定义时可见的上下文
        变量在“定义即执行”这一次运行里同样可用、执行后同样被还原。
        body 运行报错不影响注册结果（语法错误才拒绝保存）。
        """
        if not name.isidentifier():
            return self._alias_error(f"别名名字必须是合法 Python 标识符: {name!r}")
        if keyword.iskeyword(name):
            return self._alias_error(f"别名名字不能是 Python 关键字: {name}")
        try:
            ast.parse(body)
        except SyntaxError:
            # 顶层 await 走 _execute 的异步回退路径，放行到运行期处理
            if "await" not in body:
                return self._alias_error("别名代码语法错误，未保存:\n" + traceback.format_exc())

        now = utc_ms()
        old = self.aliases.get(name)
        self.aliases[name] = {
            "code": body,
            "created": old["created"] if old else now,
            "updated": now,
            "hits": old["hits"] if old else 0,
            "last_used": old["last_used"] if old else None,
        }
        # 定义即执行：注册信息附在 alias 键上，执行结果保持普通返回结构，
        # 两条传输通道（HTTP / MQTT）对此都无需特判。alias 开关透传，
        # 逐调用启用 alias 时 body 内的别名替换行为与调用方一致。
        # 递归走 _execute_inner（同线程、已持锁）：再进公开 execute 会
        # 另起工作线程排队等自己持有的锁，直到超时。
        execution = self._execute_inner(body, globals=globals, format_result=format_result, alias=alias)
        execution["alias"] = {
            "action": "updated" if old else "saved",
            "name": name,
            "code": body,
        }
        return execution

    def _alias_show(self, name):
        info = self.aliases.get(name)
        if info is None:
            known = ", ".join(sorted(self.aliases)) or "(none)"
            return self._alias_error(f"alias '{name}' not found; known: {known}")
        return self._alias_result(
            {"name": name, **info}, alias={"action": "show", "name": name}
        )

    def _alias_list_all(self):
        listing = {name: info["code"] for name, info in sorted(self.aliases.items())}
        return self._alias_result(listing, alias={"action": "list", "count": len(listing)})

    def _alias_unalias(self, arg):
        names = arg.split()
        if not names:
            return self._alias_error(
                "usage: #<unalias关键字> <名字> [<名字> ...]；一次可删多个"
            )
        deleted, missing = [], []
        for name in names:
            (deleted if self.aliases.pop(name, None) is not None else missing).append(name)
        if not deleted:
            return self._alias_error(f"alias not found: {missing}")
        return self._alias_result(
            {"deleted": deleted, "missing": missing},
            alias={"action": "unalias", "deleted": deleted, "missing": missing},
        )

    @staticmethod
    def _alias_result(r, **extra):
        result = {"r": r, "stdout": "", "ok": True}
        result.update(extra)
        return result

    @staticmethod
    def _alias_error(message):
        return {"r": None, "stdout": "", "ok": False, "error": message}

    # ------------------------------------------------------------------ #
    # 内部执行逻辑
    # ------------------------------------------------------------------ #

    def _execute(self, code):
        # 顶层的 ``await`` 会让 ``ast.parse(..., mode="exec")`` 抛出
        # SyntaxError；这种情况下回退到异步路径。
        try:
            tree = ast.parse(code, filename="<rpc>", mode="exec")
        except SyntaxError:
            if "await" in code:
                return self._execute_awaitable(code)
            raise

        # 裸 ``await`` 同样会让 ``compile`` 抛出。
        try:
            compiled_tree = compile(tree, "<rpc>", "exec")
        except SyntaxError:
            if "await" in code:
                return self._execute_awaitable(code)
            raise

        # REPL 语义：若最后一条语句是裸表达式，则求值并返回其结果。
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            prefix = ast.Module(body=tree.body[:-1], type_ignores=[])
            if prefix.body:
                exec(
                    compile(prefix, "<rpc>", "exec"),
                    self.globals_dict,
                    self.locals_dict,
                )
            expression = ast.Expression(tree.body[-1].value)
            return eval(
                compile(expression, "<rpc>", "eval"),
                self.globals_dict,
                self.locals_dict,
            )

        exec(compiled_tree, self.globals_dict, self.locals_dict)
        # 若代码片段没有以表达式结尾，则回退到用户自己设置的 ``r``。
        return self.locals_dict.get("r", self.globals_dict.get("r"))

    def _execute_awaitable(self, code):
        indented_code = textwrap.indent(code, "    ")

        # 检测函数体是否为单个表达式，以便用 ``return (expr)``，
        # 从而对 await 也保留 REPL 行为。
        try:
            wrapped_tree = ast.parse(f"async def _rpc_async():\n{indented_code}")
            body = wrapped_tree.body[0].body
            is_single_expr = len(body) == 1 and isinstance(body[0], ast.Expr)
        except SyntaxError:
            is_single_expr = False

        if is_single_expr:
            async_code = f"async def __rpc_async__():\n    return ({code})"
        else:
            async_code = (
                f"async def __rpc_async__():\n{indented_code}\n"
                f"    return locals()"
            )

        exec(async_code, self.globals_dict, self.locals_dict)
        coroutine_function = self.locals_dict.pop("__rpc_async__")
        result = self._run_coroutine(coroutine_function())

        if isinstance(result, dict):
            # 多语句函数体：把异步函数内部的局部变量合并回共享命名空间，
            # 然后取出 ``r``。
            self.locals_dict.update(result)
            return self.locals_dict.get("r", self.globals_dict.get("r"))
        if result is not None:
            self.locals_dict["r"] = result
        return result

    # ------------------------------------------------------------------ #
    # 事件循环相关
    # ------------------------------------------------------------------ #

    def _run_coroutine(self, coroutine):
        # 若提供的主事件循环正在运行，就把协程调度到那里执行。
        # 警告：如果调用线程正是该循环所在的线程，这里会死锁。这种
        # 情况下，调用方应改为直接 await 该协程，或从其他线程调用本
        # 执行器。
        if self.main_loop is not None and self.main_loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coroutine, self.main_loop)
            return future.result()

        # 新建 loop 时同步设置为当前线程的事件循环，否则协程内部若调用
        # asyncio.get_event_loop()（不少第三方库会这么做）会拿到别的
        # loop，甚至抛出 "no running event loop"。
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coroutine)
        finally:
            try:
                loop.close()
            finally:
                # 还原为“当前线程没有事件循环”，避免把已关闭的 loop
                # 留在 threading.local 里被后续代码误用。
                asyncio.set_event_loop(None)


class _redirect_stdout:
    def __init__(self, stream):
        self.stream = stream
        self.previous = None

    def __enter__(self):
        self.previous = sys.stdout
        sys.stdout = self.stream
        return self.stream

    def __exit__(self, exc_type, exc_value, traceback_info):
        sys.stdout = self.previous


def format_result(obj, width=120):
    if isinstance(obj, str):
        return obj
    try:
        from IPython.lib.pretty import pretty
        return pretty(obj, max_width=width)
    except ImportError:
        try:
            from pprint import pformat
            return pformat(obj, width=width)
        except Exception:
            return repr(obj)

import time
def stime(ms=0, format='%Y-%m-%d__%H.%M.%S', ms_splitor='__.'):
    """可读毫秒级时间戳。ms 传整数毫秒；不传则取当前 UTC 毫秒。"""
    if not ms:
        ms = utc_ms()
    elif isinstance(ms, float) and ms < 1e11:
        # 看起来是 time.time() 的秒级浮点，转成整数毫秒
        ms = round(ms * 1000)
    else:
        ms = int(ms)
    sec, milli = divmod(ms, 1000)
    
    if ms_splitor:ms_splitor=ms_splitor + f"{milli:03d}"
    return time.strftime(format, time.localtime(sec)) + ms_splitor

def utc_ms():
    """Return the current UTC Unix timestamp in integer milliseconds."""
    return time.time_ns() // 1_000_000
            

def http_import(url, save_to=''): # 定义核心导入函数
    import urllib.request, zipfile, io, sys, importlib, importlib.abc, importlib.machinery, os # 导入必要标准库
    if save_to and os.path.isdir(save_to): save_to = os.path.join(save_to, os.path.basename(url.split('?')[0].rstrip('/')) or 'module.py') # 若save_to为已存在文件夹则自动拼接URL文件名
    class HttpImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader): # 继承标准查找与加载器接口实现自定义导入
        def __init__(self, url, save_to): self.url, self.save_to = url, save_to; self._fetch() # 保存参数并在初始化时拉取一次数据
        def _fetch(self): # 将网络请求与解析封装为方法，供初始化和reload触发
            self.data = None # 初始化数据容器
            if self.save_to and os.path.exists(self.save_to): # 遵循规则：如果有save_to且存在则直接读取
                with open(self.save_to, 'rb') as f: self.data = f.read() # 读取本地缓存
            else: # 否则发起网络请求
                with urllib.request.urlopen(self.url, timeout=30) as resp: self.data = resp.read() # 获取最新网络字节流
                if self.save_to: # 如果提供了保存路径则落盘
                    with open(self.save_to, 'wb') as f: f.write(self.data) # 写入本地
            self.is_zip, self.zf = False, None # 重置zip相关状态
            try: # 尝试按zip格式解析数据
                self.zf = zipfile.ZipFile(io.BytesIO(self.data)) # 将字节流载入内存zip结构
                self.is_zip, self.names = True, set(self.zf.namelist()) # 成功解析则标记状态并缓存文件列表
                roots = [n.split('/')[0] for n in self.names if not n.startswith('__') and ('/' in n or n.endswith('.py'))] # 提取包的根目录
                self.mod_name = roots[0].replace('.py', '') if roots else 'unknown' # 锁定主包名
            except Exception: # 解析zip失败则视为普通的单文件py代码
                self.names = set() # 单文件清空names集合
                self.mod_name = (os.path.basename(self.save_to) if self.save_to else self.url.split('?')[0].split('/')[-1]).replace('.py', '') # 从URL或文件路径直接提取模块名
        def find_spec(self, fullname, path=None, target=None): # 拦截Python的import机制
            if self.is_zip: # zip模式下的路径匹配逻辑
                if fullname.replace('.', '/') + '/__init__.py' in self.names: return importlib.machinery.ModuleSpec(fullname, self, is_package=True) # 匹配到包
                if fullname.replace('.', '/') + '.py' in self.names: return importlib.machinery.ModuleSpec(fullname, self) # 匹配到单文件模块
            elif fullname == self.mod_name: return importlib.machinery.ModuleSpec(fullname, self) # 单文件模式精确匹配全名
            return None # 规则不符交由其它加载器处理
        def create_module(self, spec): return None # 返回None以沿用Python默认的模块创建机制
        def exec_module(self, module): # 编译代码并注入到模块命名空间(reload时会重新调用)
            if getattr(module, '_http_loaded', False): self._fetch() # 【核心修复点】通过自定义标记判断，仅在被 importlib.reload() 显式触发时才重新拉取最新数据，避免初次加载发出两次请求
            fn = module.__name__ # 提取当前需要加载的完整模块名
            if self.is_zip: # 处理zip内的代码读取
                pkg_path = fn.replace('.', '/') + '/__init__.py' # 优先探测包初始化文件
                if pkg_path in self.names: source, module.__file__, module.__path__, module.__package__ = self.zf.read(pkg_path).decode('utf-8'), f"<zip://{pkg_path}>", [f"<zip://{fn.replace('.','/')}>"], fn # 注入包特有元数据
                else: source, module.__file__, module.__package__ = self.zf.read(fn.replace('.', '/') + '.py').decode('utf-8'), f"<zip://{fn.replace('.', '/')}.py>", fn.rpartition('.')[0] # 注入单文件特有元数据
            else: source, module.__file__, module.__package__ = self.data.decode('utf-8'), f"<http://{fn}.py>", fn.rpartition('.')[0] # 单文件模式直接解码最新内存数据
            module.__loader__ = self # 绑定当前加载器以完美支持 importlib.reload()
            exec(compile(source, module.__file__, 'exec'), module.__dict__) # 编译最新代码并覆盖执行到模块字典
            module._http_loaded = True # 标记该模块已完成初始加载，后续若再次进入此函数必然是 reload 行为
    importer = HttpImporter(url, save_to) # 实例化定制加载器对象
    sys.meta_path = [m for m in sys.meta_path if type(m).__name__ != 'HttpImporter'] # 清除历史自定义加载器防止重复堆叠引发异常
    sys.meta_path.insert(0, importer) # 将新加载器置于最高优先级以接管目标模块
    importlib.invalidate_caches() # 强制刷新Python导入缓存
    for name in list(sys.modules): # 遍历系统已加载模块
        if name == importer.mod_name or name.startswith(importer.mod_name + '.'): del sys.modules[name] # 剔除旧模块及子模块以确保首次能全新加载
    return importlib.import_module(importer.mod_name) # 返回最终动态导入的顶级模块实例