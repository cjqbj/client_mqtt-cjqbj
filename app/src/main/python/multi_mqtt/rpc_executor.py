"""与传输层无关的 Python RPC 执行器。"""

import ast
import asyncio
import builtins
import collections
import io
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


# 别名带参数调用：``<别名>=<参数>``（如 ``git_pull=/home/u/dir``）。参数按
# 字符串原样写进 ALIAS_ARG_NAME，别名 body 用 ``_alias_arg`` 读；空参数是
# 空串，body 自己兜底（``dir = _alias_arg or '/默认/路径'``）。
ALIAS_ARG_SEP = "="
ALIAS_ARG_NAME = "_alias_arg"
# 前缀别名：``#alias git+`` 注册后，输入 ``git status`` 命中，前缀之后剩余
# 的 ``status`` 写进 ALIAS_ARG_NAME。有效前缀（去掉末尾 +）最少这么长，
# 避免 ``a+`` 这类单字符前缀劫持所有以该字符开头的输入。
ALIAS_PREFIX_SUFFIX = "+"
ALIAS_PREFIX_MIN_LEN = 2
# 输入首字符是 ``\`` 时剥掉并**完全跳过**别名展开（三种模式都不匹配），
# 原样作为 Python 代码执行。
ALIAS_ESCAPE = "\\"
# 未知参数默认严格报错（C2）；置 True 改为忽略并在返回 dict 里写 warnings。
ALIAS_PARAMS_LENIENT = False
# 预留：完整正则别名。v1 规范只承诺前缀语义，默认永不开启。
ALIAS_REGEX = False

# body 顶层自由变量里的内置名不算「可绑定参数」：它们不属于调用方数据，
# 也不该被必填校验（print/open 等永远从内置命名空间解析）。
_BUILTIN_NAMES = frozenset(dir(builtins))


class AliasSpecError(ValueError):
    """别名注册期规格错误（R1–R6），调用方据此拒绝注册。"""


def scan_string_literal(text, start=0):
    """扫描 ``text[start:]`` 开头的 Python 字符串字面量，返回 ``(值, 结束下标)``。

    只支持单行、无前缀的 ``'...'`` / ``"..."``（``ast.literal_eval`` 语义，
    因此 ``\\n`` / ``\\t`` / ``\\"`` / ``\\\\`` / ``\\xNN`` 等转义都按 Python
    规则解码）。引号未闭合、转义非法或求值结果不是 ``str`` 时抛
    :class:`AliasSpecError`，调用方据此拒绝注册。
    """
    if not isinstance(text, str) or start < 0 or start >= len(text):
        raise AliasSpecError('alias error: 缺少字符串字面量')
    quote = text[start]
    if quote not in '"\'':
        raise AliasSpecError(
            'alias error: 名字的字符串字面量必须以引号开头: %r' % text[start:])
    index = start + 1
    while index < len(text):
        char = text[index]
        if char == '\\':
            index += 2
            continue
        if char == quote:
            literal = text[start:index + 1]
            try:
                value = ast.literal_eval(literal)
            except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
                raise AliasSpecError(
                    'alias error: 名字不是合法的字符串字面量: %r' % literal)
            if not isinstance(value, str):
                raise AliasSpecError(
                    'alias error: 名字的字符串字面量必须求值为 str: %r' % literal)
            return value, index + 1
        index += 1
    raise AliasSpecError(
        'alias error: 字符串名字的引号未闭合: %r' % text[start:])


def split_string_literal(text):
    """拆出开头的字符串字面量，返回 ``(解码值, 字面量之后的剩余原文)``。"""
    value, end = scan_string_literal(text, 0)
    return value, text[end:]


def split_alias_call(code):
    """把 ``<别名>=<参数>`` 拆成 ``(别名, 参数)``；不是该形态返回 None。

    只用**第一个** ``=`` 作分隔符，因此参数里可以继续出现 ``=`` 和 ``/``
    （路径、查询串都原样保留，不做 strip / 解码以外的处理）。

    名字含空白时不能裸写（``a b=1`` 仍返回 None），必须用字符串字面量：
    ``"a b"=/x``、``'a b'=/x``；字面量之后必须紧跟 ``=``。
    """
    if not isinstance(code, str):
        return None
    text = code.strip()
    if not text:
        return None
    if text[0] in '"\'':
        try:
            name, rest = split_string_literal(text)
        except AliasSpecError:
            return None
        if not rest.startswith(ALIAS_ARG_SEP):
            return None
        if not is_valid_alias_name(name):
            return None
        return name, rest[len(ALIAS_ARG_SEP):]
    name, sep, arg = text.partition(ALIAS_ARG_SEP)
    name = name.strip()
    if not sep or not name or any(ch.isspace() for ch in name):
        return None
    return name, arg


def split_alias_names(text):
    """按空白切分 ``#unalias`` 的名字列表，但把字符串字面量当作一个整体。

    ``#unalias "a b" c`` → ``['a b', 'c']``；字面量里的空格不会被折叠，
    引号未闭合时抛 :class:`AliasSpecError`。裸名字不含空白，因此与旧的
    ``str.split()`` 行为完全一致。
    """
    if not isinstance(text, str):
        return []
    names = []
    index, size = 0, len(text)
    while index < size:
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if char in '"\'':
            name, index = scan_string_literal(text, index)
            names.append(name)
            continue
        start = index
        while index < size and not text[index].isspace():
            index += 1
        names.append(text[start:index])
    return names


def quote_alias_name(name):
    """把解码后的名字渲染成**可粘贴回指令行**的形式。

    能裸写（无空白、不以引号开头、不会被误解析成 prefix / 签名形态）时
    原样返回；否则用 ``repr()`` 加引号与转义（``repr('a b')`` →
    ``'a b'``），保证 ``#alias <列表输出>`` 能再次注册出同一个名字。
    """
    if isinstance(name, str) and name \
            and not any(ch.isspace() for ch in name) \
            and name[0] not in '"\'':
        try:
            spec = parse_alias_signature(name)
        except AliasSpecError:
            spec = None
        if spec is not None and spec['name'] == name \
                and spec['mode'] == 'plain':
            return name
    return repr(name)


def is_valid_alias_name(name):
    """别名名字是否可注册并长期保存（#alias 与 rpc.json 共用同一套规则）。

    名字不再限制为 Python 标识符：``ime.apk``、``v1.2``、中文等都可以，
    因为别名替换只做整段精确匹配 + ``<名字>=<参数>`` 前缀匹配 + 前缀模式，
    不走 Python 标识符解析。

    裸名字（``#alias a b``）仍不允许空白（保留“一行一个名字”的既有语义）；
    需要空白/引号/换行等字符时，指令行里写成 Python 字符串字面量
    （``#alias "a b"``），解码后即可保存。这里校验的是**解码后的名字**：

    * 非空；
    * 首尾不能有空白（指令行与 URL 会 strip，否则永远匹配不上）；
    * 允许内部空白（空格、制表符、``\\n`` 等，来自显式字面量）；
    * 其它控制字符（NUL、DEL 等）仍拒绝，避免 URL / JSON / 终端歧义。
    """
    if not isinstance(name, str) or not name:
        return False
    if name != name.strip():
        return False
    return not any(
        (ord(ch) < 32 and not ch.isspace()) or ord(ch) == 127
        for ch in name)


# ------------------------------------------------------------------ #
# 别名注册规格解析（名字 / params 签名 / 前缀）与自由变量计算
# ------------------------------------------------------------------ #

def _split_top_level(text, separator=','):
    """按顶层分隔符切分，忽略字符串字面量与括号内部的分隔符。"""
    parts, buffer = [], []
    depth = 0
    quote = None
    index, size = 0, len(text)
    while index < size:
        char = text[index]
        if quote is not None:
            buffer.append(char)
            if char == '\\' and index + 1 < size:
                buffer.append(text[index + 1])
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in '"\'':
            quote = char
            buffer.append(char)
        elif char in '([{':
            depth += 1
            buffer.append(char)
        elif char in ')]}':
            depth -= 1
            buffer.append(char)
        elif char == separator and depth == 0:
            parts.append(''.join(buffer))
            buffer = []
        else:
            buffer.append(char)
        index += 1
    parts.append(''.join(buffer))
    return parts


def _partition_top_level(text, separator='='):
    """在顶层第一个分隔符处切开，返回 ``(左, 右, 是否找到)``。"""
    depth = 0
    quote = None
    index, size = 0, len(text)
    while index < size:
        char = text[index]
        if quote is not None:
            if char == '\\' and index + 1 < size:
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue
        if char in '"\'':
            quote = char
        elif char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif char == separator and depth == 0:
            return text[:index], text[index + 1:], True
        index += 1
    return text, '', False


def _unquote_param_name(text):
    """参数名允许写成字符串字面量（``"选项A"=1``）；非字符串原样返回。"""
    if len(text) >= 2 and text[0] in '"\'' and text[-1] == text[0]:
        try:
            value = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            raise AliasSpecError(
                'alias error: 参数名不是合法字符串字面量: %r' % text)
        if not isinstance(value, str) or not value:
            raise AliasSpecError(
                'alias error: 参数名必须是非空字符串: %r' % text)
        return value
    return text


def _parse_signature_params(signature):
    """把 ``x, y=3`` 解析成 ``(可绑定名列表, 默认值 dict)``。

    默认值必须 ``ast.literal_eval`` 可求值（R2）；``*args`` / ``**kwargs`` /
    keyword-only / positional-only / 类型注解一律拒绝（R1）；``_alias_arg``
    是保留名（R4）。
    """
    if not signature or not signature.strip():
        return [], {}
    params, defaults = [], {}
    for raw in _split_top_level(signature, ','):
        item = raw.strip()
        if not item:
            raise AliasSpecError(
                'alias error: 参数列表存在空参数: %r' % signature)
        if item == '*':
            raise AliasSpecError(
                'alias error: 不支持 keyword-only（* 分隔）: %r' % item)
        if item == '/':
            raise AliasSpecError(
                'alias error: 不支持 positional-only（/ 分隔）: %r' % item)
        if item.startswith('*'):
            raise AliasSpecError(
                'alias error: 不支持变长参数 *args/**kwargs: %r' % item)
        left, right, has_default = _partition_top_level(item, '=')
        raw_name = left.strip()
        if not raw_name:
            raise AliasSpecError('alias error: 参数缺少名字: %r' % item)
        if ':' in raw_name:
            raise AliasSpecError(
                'alias error: 不支持类型注解（HTTP 侧零 Python 知识）: %r' % item)
        name = _unquote_param_name(raw_name)
        # 参数字面量会成为 body 里的 Python 变量名，必须能直接写出：空白 /
        # 控制字符都不允许（名字规则放宽后要在这里单独拒绝）。
        if not is_valid_alias_name(name) or any(ch.isspace() for ch in name):
            raise AliasSpecError(
                'alias error: 非法参数名 %r（不能含空白/控制字符）' % name)
        if name == ALIAS_ARG_NAME:
            raise AliasSpecError(
                'alias error: %r 是带参数调用机制的保留变量，不能作参数名'
                % ALIAS_ARG_NAME)
        if name in params:
            raise AliasSpecError('alias error: 重复参数名 %r' % name)
        params.append(name)
        if has_default:
            try:
                defaults[name] = ast.literal_eval(right.strip())
            except (ValueError, SyntaxError, TypeError, MemoryError, RecursionError):
                raise AliasSpecError(
                    'alias error: 默认值必须是字面量（ast.literal_eval 可求值）: '
                    '%r=%r' % (name, right.strip()))
    return params, defaults


def _alias_spec_from_decoded(name, signature=None):
    """用**已解码**的名字构造注册规格（引号名字与持久化恢复共用）。

    ``signature`` 非 None 表示 params 模式；否则按名字是否以 ``+`` 结尾
    区分 prefix / plain。名字合法性按解码后的规则（:func:`is_valid_alias_name`）
    校验，因此允许内部空白，但首尾空白与控制字符仍拒绝。
    """
    if not isinstance(name, str) or not name:
        raise AliasSpecError('alias error: 别名名不能为空')
    if signature is not None:
        if name.endswith(ALIAS_PREFIX_SUFFIX):
            raise AliasSpecError(
                'alias error: v1 不支持前缀 + 签名组合: %r' % name)
        if not is_valid_alias_name(name):
            raise AliasSpecError(
                'alias error: 非法别名名 %r（不能为空、首尾空白或含控制字符）'
                % name)
        params, defaults = _parse_signature_params(signature)
        return {
            'name': name, 'mode': 'params',
            'signature': signature or None,
            'params': params, 'defaults': defaults,
        }
    if name.endswith(ALIAS_PREFIX_SUFFIX):
        prefix = name[:-len(ALIAS_PREFIX_SUFFIX)]
        if len(prefix) < ALIAS_PREFIX_MIN_LEN:
            raise AliasSpecError(
                'alias error: 前缀太短（%r 的有效前缀 %r，最少 %d 个字符），'
                '建议 git+、ime+ 这类有意义的前缀'
                % (name, prefix, ALIAS_PREFIX_MIN_LEN))
        if not is_valid_alias_name(prefix):
            raise AliasSpecError(
                'alias error: 非法前缀别名 %r（前缀不能为空或含控制字符）'
                % name)
        return {'name': name, 'mode': 'prefix', 'signature': None,
                'params': [], 'defaults': {}}
    if not is_valid_alias_name(name):
        raise AliasSpecError(
            'alias error: 非法别名名 %r（不能为空、首尾空白或含控制字符）'
            % name)
    return {'name': name, 'mode': 'plain', 'signature': None,
            'params': None, 'defaults': {}}


def parse_alias_signature(text):
    """解析 ``#alias`` 指令行的名字部分，返回注册规格 dict。

    三种形态：

    * ``abc`` → ``{'mode': 'plain'}``（params 稍后由 body 自由变量计算）；
    * ``add(x, y=3)`` → ``{'mode': 'params', 'signature': 'x, y=3', ...}``；
    * ``git+`` → ``{'mode': 'prefix', 'name': 'git+'}``（有效前缀 ``git``）。

    名字含空白 / 换行 / 引号等字符时，指令行里写成 **Python 字符串字面量**
    （``#alias "a b"`` / ``'a b'`` / ``"a\\tb"`` / ``"a\\nb"`` / ``"a\\"b"``），
    字面量之后可以继续跟 ``+`` 或 ``(参数列表)``。解码后的名字允许内部空白，
    但裸名字仍不允许（``#alias a b`` 保持“一行一个名字”的既有错误语义）。

    非法规格抛 :class:`AliasSpecError`，注册表不产生记录（fail fast）。
    """
    if not isinstance(text, str) or not text.strip():
        raise AliasSpecError('usage: #alias <名字> ...')
    name_part = text.strip()
    if name_part[0] in '"\'':
        name, rest = split_string_literal(name_part)
        rest = rest.strip()
        if not rest:
            return _alias_spec_from_decoded(name)
        if rest == ALIAS_PREFIX_SUFFIX:
            return _alias_spec_from_decoded(name + ALIAS_PREFIX_SUFFIX)
        if rest.startswith('(') and rest.endswith(')'):
            return _alias_spec_from_decoded(name, rest[1:-1].strip())
        raise AliasSpecError(
            'alias error: 引号名字后面只能跟 + 或 (参数列表): %r' % name_part)
    if name_part.endswith(')') and '(' in name_part:
        name, _, rest = name_part.partition('(')
        name = name.strip()
        signature = rest[:-1].strip()
        if not name or any(ch.isspace() for ch in name):
            raise AliasSpecError(
                'alias error: 签名名字不能为空或包含空白（空白请用引号字面量，'
                '如 #alias "a b"(x)）: %r' % name_part)
        return _alias_spec_from_decoded(name, signature)
    if any(ch.isspace() for ch in name_part):
        raise AliasSpecError(
            '指令行只允许一个别名名字（名字含空白请写成 Python 字符串字面量，'
            '如 #alias "a b"），收到: %r' % name_part)
    return _alias_spec_from_decoded(name_part)


class _FreeVarCollector(ast.NodeVisitor):
    """收集 body 顶层自由变量（顶层 Load 且未在顶层 Store）。

    函数 / 闭包内部引用的名字**不算**参数（规范 3.3）：那是闭包，运行期
    从持久命名空间解析。函数默认值与装饰器在定义时求值，按顶层读取计入。
    """

    def __init__(self):
        self.loads = set()
        self.stores = set()

    def collect(self, statements):
        for statement in statements:
            self.visit(statement)

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self.loads.add(node.id)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.stores.add(node.id)

    def _visit_defaults(self, args_node):
        defaults = list(getattr(args_node, 'defaults', []) or [])
        defaults += [item for item in (getattr(args_node, 'kw_defaults', []) or [])
                     if item is not None]
        for default in defaults:
            self.visit(default)

    def visit_FunctionDef(self, node):
        self.stores.add(node.name)
        for decorator in node.decorator_list:
            self.visit(decorator)
        self._visit_defaults(node.args)
        # body 不访问：闭包名运行期从持久命名空间解析

    visit_AsyncFunctionDef = visit_FunctionDef

    # import 绑定名字但不是 Name 节点：不记录成 Store 会把后续的
    # `import asyncio` / `from x import y` 误判成自由变量（进而误判成
    # 可绑定参数 / 必填参数）。
    def visit_Import(self, node):
        for alias in node.names:
            self.stores.add(alias.asname or alias.name.split('.')[0])

    def visit_ImportFrom(self, node):
        for alias in node.names:
            if alias.name == '*':
                continue
            self.stores.add(alias.asname or alias.name)

    def visit_Lambda(self, node):
        self._visit_defaults(node.args)

    def visit_ClassDef(self, node):
        self.stores.add(node.name)
        for decorator in node.decorator_list:
            self.visit(decorator)
        for base in node.bases:
            self.visit(base)
        for keyword in node.keywords:
            self.visit(keyword.value)


def alias_free_variables(code):
    """返回 body 的顶层自由变量列表（排序、已去掉内置名与保留变量）。

    顶层 ``await`` 的 body 按异步函数体解析，与执行器的 REPL 语义一致。
    body 语法错误抛 ``SyntaxError``（由注册流程转成 R7）。
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        if 'await' not in code:
            raise
        tree = ast.parse('async def __alias_await__():\n'
                         + textwrap.indent(code, '    '))
        collector = _FreeVarCollector()
        collector.collect(tree.body[0].body)
    else:
        collector = _FreeVarCollector()
        collector.collect(tree.body)
    return sorted(collector.loads - collector.stores
                  - _BUILTIN_NAMES - {ALIAS_ARG_NAME})


def match_prefix_alias(aliases, text):
    """在 ``aliases`` 里找**最长**有效前缀别名，返回 ``(record, 剩余串)``。

    剩余串永远是字符串（可能为空，见 C5），并已去掉前导空白。没有任何
    前缀命中返回 ``None``。只看名字以 ``+`` 结尾的记录，所以与 exact /
    ``=arg`` 形态互不干扰。
    """
    if not isinstance(aliases, dict) or not isinstance(text, str):
        return None
    text = text.lstrip()
    best = None
    for name, info in aliases.items():
        if not isinstance(name, str) or not name.endswith(ALIAS_PREFIX_SUFFIX):
            continue
        if not isinstance(info, dict):
            continue
        mode = info.get('mode')
        if mode is not None and mode != 'prefix':
            continue
        prefix = name[:-len(ALIAS_PREFIX_SUFFIX)]
        if len(prefix) < ALIAS_PREFIX_MIN_LEN or not text.startswith(prefix):
            continue
        if best is None or len(prefix) > len(best[0]):
            best = (prefix, info)
    if best is None:
        return None
    prefix, info = best
    return info, text[len(prefix):].lstrip()


def alias_display_name(name, info):
    """列表 / 展示用的名字：params 渲染成 ``add(x, y=3)``，prefix 渲染成 ``git+``。

    名字含空白 / 引号等不能裸写时用 ``repr()`` 加引号（``'a b'``、
    ``'a b'+``），保证 ``#alias`` 的列表输出可以原样粘贴回指令行。
    """
    if not isinstance(name, str):
        return name
    if isinstance(info, dict) and info.get('mode') == 'params' \
            and info.get('signature'):
        return '%s(%s)' % (quote_alias_name(name), info['signature'])
    if name.endswith(ALIAS_PREFIX_SUFFIX):
        return (quote_alias_name(name[:-len(ALIAS_PREFIX_SUFFIX)])
                + ALIAS_PREFIX_SUFFIX)
    return quote_alias_name(name)


def build_alias_record(name_or_spec, code, signature=None, old=None, now=None,
                       decoded_name=False):
    """构造注册记录，返回 ``(name, record)``；规格非法抛异常。

    ``name_or_spec`` 默认是 ``#alias`` 指令行的名字部分（``add(x, y=3)`` /
    ``git+`` / ``abc``）；持久化恢复时改传 ``decoded_name=True`` + 已解码
    的名字 + ``signature``，避免把含空白 / 引号的名字重新拼成指令文本后
    再解析（那样会丢信息甚至解析失败）。
    注册期校验（R1–R7）全部在返回记录之前完成。
    """
    if not isinstance(code, str) or not code.strip():
        raise AliasSpecError('alias error: 别名代码不能为空')
    if decoded_name:
        if signature is not None:
            spec = _alias_spec_from_decoded(name_or_spec, signature)
        else:
            spec = _alias_spec_from_decoded(name_or_spec)
    elif signature is not None:
        spec = parse_alias_signature('%s(%s)' % (name_or_spec, signature))
    else:
        spec = parse_alias_signature(name_or_spec)
    name = spec['name']
    # R7：body 语法错误（顶层 await 由 alias_free_variables 放行）
    free = alias_free_variables(code)
    params = spec['params']
    if spec['mode'] == 'params':
        declare = list(params or [])
        dead = [item for item in declare if item not in free]
        if dead:
            raise AliasSpecError(
                'alias error: 签名参数 %s 在 body 中未被读取（dead param）；'
                'body 自由变量: %r'
                % (', '.join(repr(item) for item in dead), free))
        params = declare
    elif spec['mode'] == 'plain':
        # plain：可绑定名集合 = body 顶层自由变量集（注册时缓存）
        params = free
    else:
        # prefix：无可绑定名，输入一律走 _alias_arg（alias.md 4.1）
        params = []
    now = utc_ms() if now is None else now
    old = old if isinstance(old, dict) else {}
    record = {
        'code': code,
        'mode': spec['mode'],
        'signature': spec['signature'],
        'params': params,
        'defaults': dict(spec['defaults']),
        'created': old.get('created', now),
        'updated': now,
        'hits': old.get('hits', 0),
        'last_used': old.get('last_used'),
    }
    return name, record


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
        # 别名带参数调用时 body 读 ALIAS_ARG_NAME，值由每次调用按
        # “exact → 空串 / =arg → 参数 / prefix → 剩余串”临时注入，并在
        # finally 中还原或删除（alias.md 10：参数值永不残留）。因此这里
        # 不在持久命名空间里预置该名字，避免遗留一个调用期才该存在的变量。

        # 必须指向同一个对象（原因见类文档字符串）。
        self.locals_dict = self.globals_dict

        self.main_loop = main_loop
        self.lock = threading.RLock()

        # code alias 总开关（默认启用），execute 可按调用覆盖；
        # 注册表：name -> {"code","mode","signature","params","defaults",
        #                  "created","updated","hits","last_used"}
        self.alias = alias
        self.aliases = {}
        # 未知参数策略（executor 级）：False 严格报错（C2）/ True 忽略并
        # 写进返回 dict 的 warnings。默认取模块级 ALIAS_PARAMS_LENIENT。
        self.params_lenient = ALIAS_PARAMS_LENIENT

        # [timeout 守卫] 上一次 execute 已超时但后台代码仍在运行时为 True：
        # 此期间新 execute 一律快速失败，避免把调用线程（MQTT 分发线程 /
        # HTTP worker）再次挂住；后台线程结束后自动回 False（见 _run_bounded）。
        self._stuck = False

    # ------------------------------------------------------------------ #
    # 公开 API
    # ------------------------------------------------------------------ #

    def execute(self, code,globals=None,format_result=False,alias=None,timeout=999,
                alias_params=None, alias_prefix=True):
        """执行 ``code``。

        ``globals`` 若提供，应是一个包含**请求级**上下文变量的映射。
        这些变量只在本次调用期间注入共享命名空间，执行完毕后会被还原
        （或删除），因此不会泄漏到持久的 REPL 状态中。

        ``alias_params`` 是**调用方数据**（HTTP query 解析结果）：只允许
        落在别名 record 的可绑定名集合里，并参与 C1/C2 校验；通道上下文
        仍走 ``globals``。为 ``None`` 时按本地调用语义把 ``globals`` 本身
        当作调用方数据（params 别名严格校验，plain 别名保持旧行为）。
        ``alias_prefix=False`` 只关闭前缀匹配（第 ④ 步），exact / ``=arg``
        不受影响。

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
        注册信息附在返回 dict 的 ``alias`` 键上。展开优先级（alias.md
        第 7 章）：``\\`` 转义 → 整段 exact → ``<名字>=<参数>`` → 最长
        前缀别名 → 普通 Python。因此 ``abc+1``、``print(abc)`` 等普通
        代码完全不受别名影响。

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
                return self._execute_inner(
                    code, globals, format_result, alias,
                    alias_prefix=alias_prefix, alias_params=alias_params)

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

    def _execute_inner(self, code, globals=None, format_result=False, alias=True,
                       alias_prefix=True, alias_params=None):
        """execute 的同线程同步版（含 alias 指令解析与递归）。

        调用方必须已持有 self.lock（工作线程 / alias 定义即执行的递归）。
        _alias_define 的递归必须走这里：若再进公开 execute 会另起工作
        线程排队等自己已持有的锁，直到超时。
        """
        if alias:
            directive = self._parse_alias_directive(code)
            if directive is not None:
                return self._handle_alias_directive(
                    directive, globals, format_result, alias,
                    alias_prefix=alias_prefix, alias_params=alias_params)
        return self._execute_code_locked(
            code, globals, format_result, alias,
            alias_prefix=alias_prefix, alias_params=alias_params)

    def _execute_code_locked(self, code, globals=None, format_result=False, alias=True,
                             alias_prefix=True, alias_params=None):
        """execute 的常规执行主体：alias 展开 / 请求级注入 / 执行 / 还原。

        调用方必须已持有 self.lock。返回结构见 execute 文档。别名的参数
        绑定（C1/C2）与注入全部在锁内、真正执行之前完成；默认值与调用方
        数据都走既有的“保存旧值 → 注入 → finally 还原”管线，所以参数值
        永不残留在持久命名空间里（规范 4.3 / 4.5）。
        """
        output = io.StringIO()
        merged_globals = dict(globals) if isinstance(globals, dict) else {}
        warnings = []
        if alias:
            # provided（HTTP query）与本地 globals 的语义差别交给 _expand_alias：
            # 前者是显式调用方数据（plain 别名也做 C1/C2），后者对 plain 别名
            # 保持历史行为（任意请求级上下文注入都不报未知参数）。
            data = alias_params if alias_params is not None else merged_globals
            expansion = self._expand_alias(
                code, available=merged_globals, data=data,
                explicit=alias_params is not None, alias_prefix=alias_prefix)
            if expansion.error is not None:
                return expansion.error
            code = expansion.code
            merged_globals.update(expansion.injected)
            warnings = expansion.warnings
        saved = {}
        injected = []
        try:
            for k, v in merged_globals.items():
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
                execution = {
                    "r": result,
                    "stdout": output.getvalue(),
                    "ok": True,
                }
                if warnings:
                    execution["warnings"] = warnings
                return execution
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
    # 语法与语义以仓库根目录 alias.md 为唯一规范；关键字取自
    # ALIAS_KEYWORDS / UNALIAS_KEYWORDS（list，可运行期增删）：
    #   #alias                    列出全部别名（按模式渲染名字）
    #   #alias abc                查询单个别名（完整记录含 mode/params/hits）
    #   #alias abc ⏎ <code>       增 / 改：注册并立即执行一次 body（plain）
    #   #alias add(x, y=3) ⏎ ...  增 / 改：params 别名（签名 + 默认值）
    #   #alias git+ ⏎ ...         增 / 改：prefix 别名（git 开头的输入都命中）
    #   #unalias abc [def ...]    删：支持一次删多个；不存在则报错
    #   abc                       用：整段 exact 命中，替换为 body 再执行
    #   abc=<参数>                用：参数原样写进 _alias_arg（不参与 params）
    #   git status                用：最长前缀别名命中，剩余串进 _alias_arg
    #   \abc                      用：剥掉 \ 后原样执行，完全跳过别名展开
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

    def _handle_alias_directive(self, directive, globals=None, format_result=False,
                                alias=True, alias_prefix=True, alias_params=None):
        """按指令 kind 分发增删查改。调用方（execute）已持有 self.lock。"""
        if directive["kind"] == "unalias":
            return self._alias_unalias(directive["arg"])

        body = textwrap.dedent(directive["body"]).strip()
        arg = directive["arg"].strip()
        if not arg:
            if body:
                return self._alias_error(
                    "usage: #<关键字> <名字> 换行 <代码>；#<关键字> 查全部，"
                    "#<关键字> <名字> 查单个"
                )
            return self._alias_list_all()
        if body:
            return self._alias_define(
                arg, body, globals, format_result, alias,
                alias_prefix=alias_prefix, alias_params=alias_params)
        # 查询：允许写成签名形态（#alias add(x, y=3)），按名字查，签名只做展示容错
        try:
            spec = parse_alias_signature(arg)
        except AliasSpecError as error:
            return self._alias_error(str(error))
        return self._alias_show(spec["name"])

    _AliasExpansion = collections.namedtuple(
        '_AliasExpansion', 'code injected warnings error')

    def _expand_alias(self, code, available=None, data=None, explicit=False,
                      alias_prefix=True):
        """按 alias.md 第 7 章顺序展开整段输入，返回 _AliasExpansion。

        ① 转义：首字符 ``\\`` → 剥掉后原样执行（后续步骤全跳过）；
        ② exact：整段等于别名名（params 别名在这里做参数绑定 4.3）；
        ③ ``=arg``：``<名>=<参数>``，参数原样进 ``_alias_arg``，不参与
            params 注入（正交机制）；
        ④ prefix（``alias_prefix=False`` 时跳过）：最长有效前缀优先；
        ⑤ 都不命中：原样返回，交给普通 Python 执行。

        ``available`` 是本次执行可见的请求级变量（通道上下文 + 本地
        globals），``data`` 是调用方数据，``explicit=True`` 表示 data 来自
        外部通道（HTTP query）：此时 plain 别名也做 C1/C2 校验。
        失败返回 ``error`` 字段非空的展开结果，由调用方当作 ok=False 返回。
        """
        text = code.strip()
        expansion = self._AliasExpansion(code, {}, [], None)
        # ① 转义：\git status → 字面执行（不展开任何模式）
        if text.startswith(ALIAS_ESCAPE):
            literal = code.lstrip()[len(ALIAS_ESCAPE):]
            return self._AliasExpansion(literal, {}, [], None)

        # ② exact：整段等于记录名（含 prefix 记录名本身，如 "git+"）
        info = self.aliases.get(text)
        if isinstance(info, dict):
            self._mark_alias_used(info)
            injected = {ALIAS_ARG_NAME: ''}
            warnings = []
            bindable = info.get('params')
            mode = info.get('mode') or ('prefix' if text.endswith(
                ALIAS_PREFIX_SUFFIX) else 'plain')
            # params 别名永远严格；plain 别名只在通道显式给数据时严格
            # （本地 globals 对 plain 保持历史注入行为，见规范 12 兼容性）
            if isinstance(bindable, list) and (mode == 'params' or explicit):
                bound, warnings, error = self._bind_alias_params(
                    info, text, data, available, mode)
                if error is not None:
                    return self._AliasExpansion(None, None, None, error)
                injected.update(bound)
            elif isinstance(bindable, list) and isinstance(data, dict):
                # 旧记录 / 手工挂载：不校验，按普通请求级变量注入
                injected.update(data)
            elif explicit and isinstance(data, dict):
                injected.update(data)
            return self._AliasExpansion(
                info.get('code', ''), injected, warnings, None)

        # ③ <别名>=<参数>：参数原样写进 _alias_arg，不做 params 注入
        call = split_alias_call(text)
        if call is not None:
            name, arg = call
            info = self.aliases.get(name)
            if isinstance(info, dict):
                self._mark_alias_used(info)
                return self._AliasExpansion(
                    info.get('code', ''), {ALIAS_ARG_NAME: arg}, [], None)

        # ④ prefix：最长前缀优先；剩余串（去前导空白）进 _alias_arg
        if alias_prefix:
            matched = match_prefix_alias(self.aliases, text)
            if matched is not None:
                info, remainder = matched
                self._mark_alias_used(info)
                return self._AliasExpansion(
                    info.get('code', ''), {ALIAS_ARG_NAME: remainder}, [], None)

        # ⑤ fallback：abc+1、print(abc) 等普通代码完全不受别名影响
        return expansion

    def _mark_alias_used(self, info):
        info["hits"] = info.get("hits", 0) + 1
        info["last_used"] = utc_ms()

    def _bind_alias_params(self, info, name, data, available, mode):
        """校验并合并一次 params 调用的实参（规范 4.3 ①②③）。

        返回 ``(merged, warnings, error)``；``error`` 非空表示调用失败
        （C1 缺必填 / C2 未知参数），调用方直接把它当作执行结果返回。
        """
        bindable = list(info.get('params') or [])
        defaults = dict(info.get('defaults') or {})
        provided = dict(data) if isinstance(data, dict) else {}
        warnings = []
        unknown = [key for key in provided if key not in bindable]
        if unknown:
            if self.params_lenient:
                warnings.append('未知参数已忽略: %r；别名 %r 可绑定: %r'
                                % (unknown, name, bindable))
                for key in unknown:
                    provided.pop(key, None)
            else:
                return {}, warnings, {
                    'r': None, 'stdout': '', 'ok': False,
                    'error': 'alias param error: 未知参数 %r；别名 %r 可绑定: %r'
                             % (unknown, name, bindable),
                }
        # C1：签名声明里没有默认值的名字必须由调用方提供；plain 别名的
        # 自由变量按“程序语义”允许从持久命名空间 / 通道上下文取到。
        missing = []
        for param in bindable:
            if param in defaults or param in provided:
                continue
            if mode != 'params' and (
                    param in (available or {}) or param in self.globals_dict):
                continue
            missing.append(param)
        if missing:
            return {}, warnings, {
                'r': None, 'stdout': '', 'ok': False,
                'error': 'alias param error: 缺少必填参数 %r（声明: %s）'
                         % (missing, self._alias_declaration(name, info)),
            }
        merged = dict(defaults)
        merged.update(provided)
        return merged, warnings, None

    @staticmethod
    def _alias_declaration(name, info):
        """错误信息里的声明原文：params 用签名，其余用“别名 + 可绑定集”。

        两种写法都带别名名，满足 alias.md 5.3（错误里必须能定位到别名）。
        """
        if info.get('signature'):
            return '%s(%s)' % (quote_alias_name(name), info['signature'])
        return '%r 可绑定 %r' % (name, list(info.get('params') or []))

    def _alias_define(self, name_or_spec, body, globals=None, format_result=False,
                      alias=True, alias_prefix=True, alias_params=None):
        """注册（重名覆盖，保留 created / hits 计数）并立即执行一次 body。

        注册期校验（R1–R7）全部在写注册表之前完成，失败时注册表不产生
        记录；body 运行报错不影响注册结果（语法错误才拒绝保存）。请求级
        ``globals`` 透传给 body 的执行，因此定义时可见的上下文变量在
        “定义即执行”这一次运行里同样可用、执行后同样被还原；params 别名
        的默认值也会注入这一次运行。
        """
        try:
            spec = parse_alias_signature(name_or_spec)
        except AliasSpecError as error:
            return self._alias_error(str(error))
        old = self.aliases.get(spec['name'])
        try:
            name, record = build_alias_record(name_or_spec, body, old=old)
        except AliasSpecError as error:
            return self._alias_error(str(error))
        except SyntaxError:
            # 顶层 await 已被 alias_free_variables 放行；其余语法错误拒绝保存
            return self._alias_error(
                "别名代码语法错误，未保存:\n" + traceback.format_exc())

        self.aliases[name] = record
        run_globals = dict(globals) if isinstance(globals, dict) else {}
        if record['mode'] == 'params':
            run_globals.update(record['defaults'])
        # 无参调用参数复位为 ''：定义即执行这一次也不该读到旧参数
        run_globals[ALIAS_ARG_NAME] = ''
        # 定义即执行这一次没有任何调用方数据：params 别名的必填参数、
        # plain / prefix 别名在命名空间里还拿不到的自由变量，硬跑只会
        # 得到 NameError；跳过这一次执行，但注册结果照样返回 ok=True，
        # 避免 HTTP /<别名> 定义后返回 500 让人以为没存上。
        if record['mode'] == 'params':
            missing = [item for item in (record['params'] or [])
                       if item not in record['defaults']]
            reason = '缺少必填参数 %r' % (missing,)
        else:
            # plain 的可绑定集已缓存；prefix 按规范无可绑定名（4.1），
            # 这里临时算自由变量只为判断“定义即执行”是否拿得到值。
            free = (record['params'] if record['mode'] == 'plain'
                    else alias_free_variables(body))
            missing = [item for item in (free or [])
                       if item not in run_globals
                       and item not in self.globals_dict]
            reason = '缺少自由变量 %r' % (missing,)
        if missing:
            execution = self._alias_result(
                "alias '%s' saved（%s，定义即执行已跳过）"
                % (alias_display_name(name, record), reason))
        else:
            # 定义即执行：注册信息附在 alias 键上，执行结果保持普通返回结构，
            # 两条传输通道（HTTP / MQTT）对此都无需特判。alias / 前缀开关透传，
            # 逐调用启用 alias 时 body 内的别名替换行为与调用方一致。
            # 递归走 _execute_inner（同线程、已持锁）：再进公开 execute 会
            # 另起工作线程排队等自己持有的锁，直到超时。
            execution = self._execute_inner(
                body, globals=run_globals, format_result=format_result, alias=alias,
                alias_prefix=alias_prefix, alias_params=None)
        execution["alias"] = {
            "action": "updated" if old else "saved",
            "name": name,
            "code": body,
            "mode": record['mode'],
            "signature": record['signature'],
            "params": list(record['params']),
            "defaults": dict(record['defaults']),
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
        # 按模式渲染名字：plain `hi` / params `add(x, y=3)` / prefix `git+`
        listing = {alias_display_name(name, info): info.get("code", "")
                   for name, info in sorted(self.aliases.items())}
        return self._alias_result(listing, alias={"action": "list", "count": len(listing)})

    def _alias_unalias(self, arg):
        # 引号感知分词："a b" 整体算一个名字，与 #alias "a b" 对称
        try:
            names = split_alias_names(arg)
        except AliasSpecError as error:
            return self._alias_error(str(error))
        if not names:
            return self._alias_error(
                "usage: #<unalias关键字> <名字> [<名字> ...]；一次可删多个，"
                "含空白/转义的名字用 Python 字符串字面量（如 #unalias \"a b\"）"
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