"""与传输层无关的 Python RPC 执行器。"""

import ast
import asyncio
import io
import sys
import threading
import textwrap
import traceback


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

    def __init__(self, globals=None, main_loop=None, copy_globals=False):
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

    # ------------------------------------------------------------------ #
    # 公开 API
    # ------------------------------------------------------------------ #

    def execute(self, code, globals=None):
        """执行 ``code``。

        ``globals`` 若提供，应是一个包含**请求级**上下文变量的映射。
        这些变量只在本次调用期间注入共享命名空间，执行完毕后会被还原
        （或删除），因此不会泄漏到持久的 REPL 状态中。

        注意：整个“保存旧值 → 注入 → 执行 → 还原”的过程都在同一把
        可重入锁内完成。否则并发请求会互相看到对方注入的上下文变量，
        并且还原顺序错乱，导致请求级名字永久泄漏到持久命名空间里。
        """
        if not isinstance(code, str) or not code.strip():
            return {"r": "", "stdout": "", "ok": False, "error": "code is required"}

        output = io.StringIO()
        # 整段注入 / 执行 / 还原都在锁内，避免并发下互相污染。
        with self.lock:
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


def format_result(value):
    if isinstance(value, str):
        return value
    try:
        import importlib
        pretty = importlib.import_module("IPython.lib.pretty").pretty
        return pretty(value, max_width=120)
    except ImportError:
        try:
            from pprint import pformat
            return pformat(value, width=120)
        except ImportError:
            return repr(value)

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