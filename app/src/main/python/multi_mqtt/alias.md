# alias.md — Code Alias 设计规范

> 适用对象：`PythonExecutor` 的 code alias 子系统（`#alias` / `#unalias` 指令、
> 别名展开、参数绑定、前缀匹配），以及所有传输通道（HTTP / MQTT / 本地直接调用）。
>
> 本文档是唯一行为规范。实现与文档冲突时，先改文档再改代码。

---

## 0. 一页速览

| 模式 | 注册写法 | 调用写法 | 参数来源 | 典型场景 |
|---|---|---|---|---|
| **plain**（既有） | `#alias abc` 换行 body | `abc` | 无（body 自由变量可从持久命名空间读） | 固定小脚本 |
| **params**（新增） | `#alias add(x, y=3)` 换行 body | `add` + 参数注入 | 签名声明 + 默认值 + 调用方数据 | 带参数的可复用片段 |
| **prefix**（新增） | `#alias git+` 换行 body | `git pull`（任意前缀命中输入） | 命中后的**剩余字符串**写入 `_alias_arg` | 命令前缀、快捷指令 |

三条核心规则：

1. **别名是程序，不是模板**——body 就是 Python 代码，按 REPL 语义执行，
   参数以“注入变量”的方式进入，代码文本永不被改写。
2. **代码永不改变，URL / 调用方只提供数据**——同一段 `x + y`，
   本地 `execute('add', globals={'x':1,'y':2})` 与 HTTP `/-add?x=1&y=2`
   走**完全相同的执行路径**，行为必然一致。
3. **注册时 fail fast，调用时 fail loud**——能在 `#alias` 注册时拒绝的错，
   绝不留到调用时；调用时的参数错误必须返回明确的 `ok=False`，永不静默忽略。

---

## 1. 设计原则（Design Principles）

### 1.1 别名是程序，不是模板

别名 body 是普通 Python 代码片段，按 executor 既有的 REPL 语义执行：
最后一条语句是裸表达式则返回其值，否则回退读取 `r`。
不存在字符串替换式的“占位符填充”。参数不是模板的 hole，而是
**运行期注入到共享命名空间的请求级变量**。

由此得到一个重要推论：**参数的值可以是任意 Python 对象**（本地调用时），
字符串只是 HTTP 传输的自然形态。转换逻辑（`int(x)`、`json.loads(x)` 等）
是 body 的责任，由别名作者在代码里显式写出——因为代码是程序，作者本来就会写。

### 1.2 代码永不改变，参数只是数据

任何调用方（HTTP query、`globals=` 注入）都只能提供**数据**（键值对），
不能改变别名代码本身。这带来两个性质：

- **幂等可审计**：别名 body 在注册时就是最终形态，审计 body 即审计行为；
- **路径统一**：参数注入走的是 executor 既有的
  “保存旧值 → 注入 → 执行 → `finally` 还原”管线，不为参数机制新增任何执行语义。

### 1.3 传输层无关

参数机制的全部语义都实现在 executor 内部。传输层（`server_http` /
`server_mqtt`）只负责把外部请求翻译成
`executor.execute(code, alias_params=params)`（HTTP query 是显式调用方数据）
或 `executor.execute(code, globals=params)`（本地 / MQTT 上下文注入）。
同一个别名，本地调用与 HTTP 调用**代码路径逐字节相同**，
不存在“HTTP 版”和“本地版”两套语义。这一点优先于任何语法便利性——
我们明确**拒绝函数化签名在执行语义上的特殊化**（见 1.4）。

### 1.4 HTTP 侧零 Python 知识

HTTP 通道解析参数时**不需要理解签名、默认值、类型注解**。
它只读取别名记录里注册时就缓存好的字段（`mode` / `params` / `defaults`），
O(1) 完成校验与注入。所有需要 AST 的工作（计算自由变量集、
校验签名一致性）都发生在**注册时**，由 executor 完成。

### 1.5 复用既有并发原语，不造第二套

参数注入复用 `execute` 已有的请求级注入与还原（在 `self.lock` 内完成）。
参数永远不会写进持久命名空间，线程安全由既有 RLock 兜底。
参数机制**不引入任何新的锁、队列或线程**。

### 1.6 错误设计总纲

- **注册时 fail fast**：签名非法、默认值非法、签名与 body 不一致、
  前缀太短……全部在 `#alias` 指令返回时拒绝，注册表不产生记录；
- **调用时 fail loud**：缺必填参数、传了未知参数，返回 `ok=False` 并给出
  缺失/未知名字清单和声明原文，**默认严格模式，静默忽略永不作为默认行为**。

---

## 2. 三种模式

### 2.1 plain（既有，语义不变）

```
#alias hi
print('hello')
```

- 整段输入精确等于 `hi` 时展开执行；无参数概念。
- 行为与引入 params/prefix 之前**完全一致**，零迁移成本。

### 2.2 params（新增）

```
#alias add(x, y=3)
x + y
```

> 与既有 `名=参` 形态的关系：`git_pull=/home/...` 走第 7 章第 ③ 步，
> 参数原样进 `_alias_arg`；它和 params 签名是**两个正交机制**，不冲突，
> 也不需要再引入“用 `=`/`-` 预先切分的单参数模式”（见 4.4）。

- 指令行的参数部分是**签名**：`名字(参数列表)`；
- 签名只做两件事：**声明可绑定名集合**、**为部分名字提供默认值**；
- body 直接读写这些名字，就像它们已经在命名空间里一样；
- 签名本身**不是可执行代码**，注册后从 body 中剥离，不参与执行。

### 2.3 prefix（新增）

```
#alias git+
import subprocess, shlex
subprocess.run('git ' + _alias_arg, shell=True, cwd='/repo')
```

- 名字以 `+` 结尾表示前缀别名（`git+` 的有效前缀串是 `git`）；
- 任何**以该前缀开头**的整段输入都命中，前缀之后的剩余字符串
  （去前导空白）写入 `_alias_arg`，body 自行解析（见第 6 章）。

### 2.4 注册记录（record）字段

`executor.aliases[name]` 在既有字段（`code` / `created` / `updated` /
`hits` / `last_used`）之上增加：

```python
{
    "code":     "x + y",               # 签名已剥离后的 body
    "mode":     "params",               # plain | params | prefix
    "signature": "x, y=3",              # params 模式：原始签名文本；其余模式 None
    "params":   ["x", "y"],             # 可绑定名集合（注册时算好，O(1) 可查）
    "defaults": {"y": 3},               # 有默认值的名字及其字面量值
    # ... created / updated / hits / last_used 不变
}
```

`params` 字段对 plain 模式也有定义：未给签名时，**可绑定名集合 = body 顶层
自由变量集**（计算规则见 4.1），同样注册时缓存。于是三条调用路径
（本地 `globals=` / HTTP query）都只需读 record，无需运行期 AST。

---

## 3. 语法细则

### 3.1 指令行（沿用既有规则）

- 首行 `#<关键字> ...`，关键字取自 `ALIAS_KEYWORDS` / `UNALIAS_KEYWORDS`；
- `#alias`（无参数）→ 列出全部；`#alias name` → 查询单个；
- `#alias name` 换行 body → 增/改（定义即执行一次）；
- 定义即执行这一次没有调用方数据：params 别名的**必填**参数、plain 别名
  在命名空间里还拿不到的自由变量，这一次执行跳过（返回消息说明缺哪些
  名字），注册本身仍然成功；
- `#unalias a b c` → 删除，支持多个；名字含空白时同样写成字符串字面量
  （`#unalias "a b" 'c d'`），见 3.1.1；
- body 自动 `textwrap.dedent`，指令行之后的所有行整体为 body。

### 3.1.1 名字：裸写与 Python 字符串字面量

**裸写规则不变**：一个指令行只能有一个名字，裸名字不能含任何空白，
所以 `#alias a b` 仍然是错误。名字不要求是 Python 标识符
（`ime.apk`、`v1.2`、中文都可以）。

需要空白、引号、制表符、换行等字符时，把名字写成 **Python 字符串字面量**
（与 Python 源码同一套转义，`ast.literal_eval` 语义；只支持单行、无前缀的
`'...'` / `"..."`）：

```
#alias "a b"          # 名字 == 'a b'（空格）
#alias 'a b'          # 单引号等价
#alias "a\tb"         # \t → 制表符
#alias "a\nb"         # \n → 真实换行
#alias "a\"b"         # 转义引号 → a"b
#alias "a\\b"         # 转义反斜杠 → a\b
```

字面量后面可以继续跟 `+`（前缀模式）或 `(参数列表)`（params 模式）：

```
#alias "git status"+              # 前缀别名，有效前缀 'git status'
#alias "add two"(x, y=3)          # params 别名，名字 'add two'
```

注意：`+` 后缀在**解码之后**判定。所以 `#alias "git+"` 注册的仍是
有效前缀 `git` 的 prefix 别名，而不是名字为 `git+` 的 plain 别名。

解码后的名字（下称“名字”）规则：

| 规则 | 说明 |
|---|---|
| 非空 | `#alias ""` 拒绝 |
| 首尾不能有空白 | 指令行与 URL 都会 `strip`，首尾空白永远匹配不上 |
| **允许内部空白** | 空格 / `\t` / `\n` 都可以（只能来自显式字面量） |
| 拒绝控制字符 | NUL、`\x01`、DEL 等仍拒绝，避免 URL / JSON / 终端歧义 |

对应的调用写法：

| 调用 | 写法 |
|---|---|
| exact | 直接输入解码后的名字：`a b` |
| `=arg` | 名字含空白时必须加引号：`"a b"=/x`（裸写 `a b=/x` 仍是错误） |
| prefix | 直接输入前缀开头的文本：`git status -s` |

`#unalias` 同样引号感知：`#unalias "a b" 'c d'` 一次删两个带空格的名字；
引号未闭合会报错，裸名字的分词行为与旧的 `split()` 完全一致。

列表 / 展示（`#alias`）对不能裸写的名字用 `repr()` 渲染（`'a b'`、
`'git status'+`、`'add two'(x, y=3)`），保证输出可以原样粘贴回指令行。

持久化（`rpc.json` 的 `http_aliases`）：key 就是解码后的名字（如 `"a b"`），
启动恢复时直接按解码名重建记录（`build_alias_record(..., decoded_name=True)`），
不再把名字拼回指令文本；URL 侧 `/a%20b` 在 `unquote` 后精确命中 `a b`。

### 3.2 params 签名语法

指令行参数部分形如 `add(x, y=3)`。判定规则：去掉首尾空白后，
若整体以 `)` 结尾且含 `(`，按 `名字(参数列表)` 解析（名字含空白时
必须写成字符串字面量，见 3.1.1）；参数列表按顶层逗号切分，逐项解析为
`名字[=默认值]`，参数名允许写成字符串字面量（`"选项A"`），因此中文等
非标识符名也能表达，但**参数名本身不能含空白**（它要作为 body 里的
Python 变量名直接写出）；默认值必须 `ast.literal_eval` 可求值。

允许的参数列表：

```
add(x, y)          # 全部必填
add(x, y=3)        # y 有默认值
add(x, "选项A"=1)  # 参数名可写成字面量（中文等非标识符名），但不能含空白
```

**显式拒绝**（注册时报错拒绝，消息指明原因）：

| 拒绝项 | 例子 | 理由 |
|---|---|---|
| `*args` / `**kwargs` | `add(*a)` | 变长参数与“可绑定名集合是有限集”冲突 |
| 类型注解 | `add(x: int)` | HTTP 侧零知识原则，注解只会骗 HTTP 用户 |
| keyword-only（`*` 分隔） | `add(*, x)` | 注入是命名空间级的，没有“关键字”概念 |
| 默认值非字面量 | `add(y=x+1)` | 默认值必须 `ast.literal_eval` 可求值，保证注册期确定、可序列化、可展示 |
| 默认值引用其它参数 | `add(x, y=x)` | 字面量规则自动排除；注入顺序无关性 |
| 保留名 | `add(_alias_arg)` | `_alias_arg` 是带参数调用机制的保留变量 |

### 3.3 签名与 body 的一致性（注册时校验）

签名里的每个名字**必须是 body 的自由变量**：

> **自由变量定义**：在 body 顶层以 `Load` 上下文被读取、且**未在 body 顶层
> 被赋值 / 定义**的名字。内层函数闭包引用的名字**不算**——那是闭包，
> 运行期从持久命名空间解析，不是参数。

```
#alias add(x, y=3)
x + y            # OK：x、y 都是自由变量

#alias bad(x, z)
x + y            # 拒绝：z 在 body 中未被读取（dead param）
```

- 签名名不是自由变量 → **拒绝注册**（错误 R3）；
- body 有自由变量但签名没声明 → **不是错误**：这些名字运行期从持久
  命名空间解析（查不到就是常规 `NameError`，带 traceback）。

这一条保证了“签名 ⊆ 自由变量集”，即签名是纯声明，不给 body 增加任何
需要理解的语义。

### 3.4 查询与列表的输出

- `#alias add`（或 `#alias add(x, y=3)`，按名字查，签名写法只做展示容错）
  返回完整记录：含 `mode` / `signature` / `params` / `defaults` / `hits`；
- `#alias` 列表按模式渲染，用户一眼看清别名“长什么样、会抢什么输入”：

```
plain:
  hi                        → print('hello')
params:
  add(x, y=3)               → x + y
prefix:
  git+                      → import subprocess; ...
```

含空白 / 引号等不能裸写的名字用 `repr()` 加引号渲染（输出可粘贴回指令行）：

```
  'a b'                     → r=9
  'git status'+             → r = _alias_arg
  'add two'(x, y=3)         → int(x) + int(y)
```

---

## 4. 参数绑定语义

### 4.1 可绑定名集合（bindable set）

```
签名模式      →  签名声明的名字（params 字段 = 签名名）
未给签名      →  body 顶层自由变量集（params 字段，注册时 ast 算好缓存）
prefix 模式   →  无可绑定名；输入走 _alias_arg（v1 不支持签名 + 前缀组合）
```

可绑定名集合是**参数机制的边界**：调用方注入的键只允许落在这个集合里。

### 4.2 默认值语义

- 默认值在**注册时**用 `ast.literal_eval` 求值一次，结果存入 `record["defaults"]`；
- 调用时，调用方未提供的名字用默认值补齐；无默认值且未提供 → 缺失错误（C1）；
- 默认值是注册时的快照：body 在本次执行里对参数名重新赋值，只影响本次
  请求级命名空间，**不改 record**（代码永不改变）。

### 4.3 绑定流程（一次 params 调用）

```
调用方数据 D（本地 globals= / HTTP query 解析结果）
        │
        ▼
① 键校验：D 的键 ⊆ record["params"]？
        └─ 否 → ok=False（未知参数清单 + 可绑定集；lenient 模式见 5.4）
        ▼
② 必填校验：(record["params"] − record["defaults"]) ⊆ D 的键？
        └─ 否 → ok=False（缺失清单 + 声明原文）
        ▼
③ 合并：M = defaults ∪ D            # D 覆盖默认值
        ▼
④ execute 常规管线：保存旧值 → 注入 M → 展开别名执行 body → finally 还原
        ▼
⑤ 返回 {"r": ..., "stdout": ..., "ok": True}
```

①②③ 全部发生在 executor 内部、`self.lock` 持有期间；
④ 与既有请求级注入是**同一段代码**，无新增执行路径。

### 4.4 调用方式汇总

| 方式 | 例子 | 说明 |
|---|---|---|
| 本地裸名 | `execute('add')` | 缺必填 → C1 错误 |
| 本地带参 | `execute('add', globals={'x':4,'y':3})` | 与 HTTP 同路径 |
| 既有 `=arg` 形态 | `ime=/sdcard/a.apk` | **优先级高于 params 注入**（见第 7 章）；命中 params 别名时同样先按 `name=arg` 拆，参数进 `_alias_arg`，**不**参与注入 |
| HTTP | `/-add?x=4&y=3` | server_http 翻译为 `execute('add', alias_params={'x':'4','y':'3'})` |
| HTTP raw 模式 | `/add?x=4&y=3` | `alias_url='raw'` 时去掉 `-` 前缀路由（见 8.2） |

要点：`=arg` 形态注入的是 `_alias_arg`，与 params 注入是**两个正交机制**。
`add(x=1)` 这种输入按 `=arg` 形态解析后，`x=1` 是 `_alias_arg` 的值字符串，
**不是**对参数 `x` 的注入。想注参，用 query 或 `globals=`。

### 4.5 值类型与转换

- 本地调用：`globals=` 里是什么对象就是什么对象，不转换；
- HTTP：query 值天然是字符串，**默认不转换**（`?x=4` → `'4'`），
  body 自行 `int(x)`；`alias_url='json'` 模式下 server_http 对值尝试
  `json.loads`，失败则保留原始字符串（见 8.3）；
- **executor 永不隐式类型转换**。转换是 body 的责任，因为代码是程序。
  转换失败抛出的 `ValueError` 等按常规运行错误返回 `ok=False` + traceback。

---

## 5. 错误设计（参数出错怎么设计）

### 5.1 注册期错误（fail fast，拒绝注册）

| 编号 | 错误 | 例子 | 返回 |
|---|---|---|---|
| R1 | 签名解析失败 / 含 `*args`、注解、keyword-only | `#alias add(*a)` | `ok=False`，指明不支持的语法项 |
| R2 | 默认值非字面量 | `#alias add(y=x+1)` | `ok=False`，给出默认值原文与要求 |
| R3 | 签名名不是 body 自由变量 | `#alias bad(x, z)` + body `x+y` | `ok=False`，指出 dead param `z` 及其在 body 中未被读取 |
| R4 | 保留名 | `#alias add(_alias_arg)` | `ok=False`，列出保留名 |
| R5 | prefix 名带签名 | `#alias git+(x)` | `ok=False`，v1 不支持组合 |
| R6 | 前缀有效长度不足 | `#alias a+` | `ok=False`，提示最小长度 `ALIAS_PREFIX_MIN_LEN`（默认 2），建议 `git+`、`ime+` 这类有意义前缀 |
| R7 | body 语法错误 | 沿用既有 | 既有行为（顶层 await 放行到运行期） |

### 5.2 调用期错误（fail loud）

| 编号 | 错误 | 例子 | 返回 |
|---|---|---|---|
| C1 | 缺必填参数 | `execute('add', globals={'y':3})` | `ok=False`，列出缺失名，附签名原文 |
| C2 | 未知参数 | `?x=1&z=2` | `ok=False`，列出未知名 + 可绑定名集合 |
| C3 | 值转换失败 | `?x=abc` 而 body `int(x)` | 常规运行错误：`ok=False` + traceback（不特判） |
| C4 | 重复 key | `?x=1&x=2` | HTTP 标准行为 last-wins，文档明示，不报错 |
| C5 | 前缀别名 remainder | `git`（恰好等于前缀，无剩余） | 合法调用：`_alias_arg=''`，body 自己兜底 |

### 5.3 错误信息格式约定

```
alias error: 签名参数 'z' 在 body 中未被读取（dead param）；body 自由变量: ['x', 'y']
alias param error: 缺少必填参数 ['x']（声明: add(x, y=3)）
alias param error: 未知参数 ['z']；可绑定: ['x', 'y']
```

- 错误信息必须包含：**别名名、违规名字清单、声明原文或可绑定集**——
  让调用方不改代码就能定位是自己拼错 key 还是别名声明变了；
- 所有参数错误都走统一的 `{"r": None, "stdout": "", "ok": False, "error": ...}`
  结构，两条传输通道无需特判。

### 5.4 strict / lenient

- 默认 **strict**：未知 key 报错（C2）。理由：URL 是数据，拼错 key 应当
  立刻暴露，而不是静默少传一个参数算出错误结果；
- 配置 `ALIAS_PARAMS_LENIENT = True`（executor 级）切换为 lenient：
  未知 key 被忽略，名字记入返回 dict 的可选字段 `"warnings": [...]`
  （通道透传，不破坏既有结构）。

---

## 6. 前缀匹配

### 6.1 注册规则

- 名字以 `+` 结尾即前缀别名，有效前缀 = 名字去掉末尾 `+`；
- 有效前缀长度 ≥ `ALIAS_PREFIX_MIN_LEN`（默认 **2**，可配）：
  `#alias a+` 拒绝（R6）。单字符前缀破坏面太大——它会劫持所有以该
  字符开头的表达式，默认禁掉，逼用户起 `git+`、`ime+` 这种有意义的短前缀；
- 其余命名规则沿用 `is_valid_alias_name`（解码后的名字：非空、首尾无空白、
  允许内部空白、拒绝控制字符，见 3.1.1）；有效前缀同样按解码名计算，
  所以 `#alias "git status"+` 的前缀是 `git status`。

### 6.2 匹配语义

- 整段输入 `lstrip()` 后以有效前缀开头即命中；
- 剩余部分 `lstrip()` 后写入 `_alias_arg`（永远是字符串，可能为空）；
- body 自行解析 `_alias_arg`（`shlex.split`、正则、前缀匹配都是 body 的自由），
  因为**代码是程序**；
- 多个前缀同时命中 → **最长前缀优先**（`gi+` 与 `git+` 并存时
  `git status` 命中 `git+`）。

### 6.3 转义

`ALIAS_ESCAPE = '\\'`：输入首字符是 `\` 时，剥掉 `\` 后**完全跳过别名展开**
（三种模式都不匹配），原样作为 Python 代码执行。比逐次传参关闭更轻：

```
git status      → 命中 git+ 别名，_alias_arg='status'
\git status     → 字面执行 Python 代码（大概率 NameError，但绝不展开）
```

### 6.4 逐次 / 全局关闭

| 开关 | 作用域 | 效果 |
|---|---|---|
| `execute(..., alias=False)` | 单次调用 | 关闭整个别名机制（含指令行、三种模式展开） |
| `execute(..., alias_prefix=False)` | 单次调用 | 仅关闭前缀匹配（第 ④ 步），exact / `=arg` 不受影响 |
| `PythonExecutor(alias=False)` | 实例级 | 全局关闭 |

调试、以及“确实要执行一个恰好叫 `git` 的变量”的场景，优先用 `\` 转义；
需要程序化绕过时用 `alias_prefix=False`。

### 6.5 为什么 v1 不做完整正则

设计时评估过 `#alias prefix<expr>` 的完整正则形态，v1 明确**不做**，理由：

1. **可预期性**：前缀匹配等价于一个受限的“行首正则子集”，优先级
   （最长优先）和命中范围（整段开头）可以精确推演；任意正则的
   命中范围无法一眼看出，违背最小惊讶；
2. **安全性**：任意正则无法廉价地拒绝 ReDoS（灾难性回溯），
   而这是一个接受网络输入的执行器；
3. **成本不对称**：前缀 + `_alias_arg` 已覆盖“命令前缀”场景的全部
   实际需求（git、adb、ime 安装等），正则带来的表达能力在这些场景里
   用不上。

未来若确有需要，以**显式扩展**方式开启：`ALIAS_REGEX = True` 时，
允许名字以 `re:` 开头按正则注册，编译后按注册顺序 first-match，
ReDoS 责任由开启者承担，默认永远关闭。**v1 规范只承诺前缀语义。**

---

## 7. 别名展开优先级总表

整段输入 `text`（已 `strip()`）按以下顺序匹配，命中即止：

| 步骤 | 形态 | 例子 | 说明 |
|---|---|---|---|
| ① 转义 | `\` 开头 | `\git status` | 剥 `\`，原样执行，后续全部跳过 |
| ② exact | 整段等于别名名 | `add`、`hi` | params 别名在此进入注入流程（4.3） |
| ③ `=arg` | `<名>=<参数>` | `ime=/sdcard/a.apk`、`git=pull`、`"a b"=/x` | 只用第一个 `=` 分隔；参数原样进 `_alias_arg`，**不**参与 params 注入；名字含空白时必须用字符串字面量 |
| ④ prefix | 整段以有效前缀开头 | `git pull` | 最长前缀优先；remainder 进 `_alias_arg` |
| ⑤ fallback | 都不命中 | `abc+1`、`print(abc)` | 原样作为 Python 执行，别名零影响 |

`alias=False` 跳过 ②③④；`alias_prefix=False` 只跳过 ④。

---

## 8. HTTP 集成（server_http 侧）

### 8.1 路由与翻译

```
GET /-add?x=4&y=3        → executor.execute('add', alias_params={'x': '4', 'y': '3'})
GET /-git status -s      → executor.execute('git status -s')   # prefix 展开
GET /-hi                 → executor.execute('hi')              # exact 展开
GET /-a%20b              → executor.execute('a b')             # 带空格的 exact
GET /-%22a%20b%22=%2Fx   → executor.execute('"a b"=/x')        # 带空格的 =arg
```

URL 里名字的空白写成 `%20`、引号写成 `%22`；server_http 在路由前统一
`unquote` 一次，因此 `/a%20b` 精确命中注册名 `a b`（3.1.1）。

server_http 职责只有三步：

1. 路由解析出**整段 code**（`-` 前缀路由是防止别名名与内建路由冲突的约定）；
2. 若是 params 模式别名（读 record 的 `mode`），把 query 解析为 dict；
   键值校验与默认值合并按 record 里的 `params` / `defaults` 在执行器内
   O(1) 完成——**零 Python 知识，不需要 AST**；
3. 调用 `executor.execute(code, alias_params=params)`，把返回 dict 原样回传。
   `alias_params` 是**显式调用方数据**，C1/C2 校验对 plain 别名同样生效
   （13.4）；`globals=` 仍只放 request / q / response / p 这类通道上下文，
   保持既有注入语义。

注册期算好、运行期只读 record——这是“HTTP 侧零 Python 知识”的落地方式。

### 8.2 `alias_url` 模式

| 模式 | 路由 | 说明 |
|---|---|---|
| `'dash'`（默认） | `/-<别名>` | 别名永远带 `-` 前缀，绝不与内建路由冲突 |
| `'raw'` | `/<别名>` | 更短的 URL；要求别名名避开内建路由段，自担冲突风险。`/-<别名>` 仍可用（raw 是 dash 的超集） |

`'json'` 不是第三种路由形态：它归一化成 raw + 值解码（见 8.3）。

### 8.3 值类型

- 默认：query 值原样作为字符串注入；
- `alias_url='json'`：对每个值尝试 `json.loads`，成功则注入解析结果
  （`?x=[1,2]` → `[1, 2]`），失败保留字符串。名称中的 `json` 只描述
  “尝试 JSON 解码”，不改变路由形态。

### 8.4 线程安全

注入、执行、还原全部在 `self.lock`（RLock）内完成，与任何其它请求
（包括不带参数的别名执行）天然串行。HTTP worker 之间、HTTP 与 MQTT
之间互不可见对方的请求级参数。参数机制不引入新的并发原语。

---

## 9. 本地调用与 HTTP 同路径论证

同一段别名 `add`（body `x + y`）：

```python
# 本地：参数是 Python 对象
executor.execute('add', globals={'x': 4, 'y': 3})        # r == 7

# HTTP：参数是字符串（或 json 模式下的解析结果）
# GET /-add?x=4&y=3
# server_http → executor.execute('add', alias_params={'x': '4', 'y': '3'})
```

两条调用从 `_expand_alias` 命中之后走的是**同一段代码**：
同一个 record、同一次键校验、同一个注入/还原管线、同一把锁。
唯一差别是值的类型（对象 vs 字符串），而这个差别显式地由调用方式决定、
由 body 的转换代码处理。**不存在任何“HTTP 特判”分支**——这是本设计
相对于“函数化签名”方案的核心优势：函数化签名必然产生“本地调用”和
“HTTP 调用”两套语义（签名绑定 vs query 绑定），而注入式设计只有一套。

三个难得对齐的点（设计论证存档）：

1. **HTTP 侧零 Python 知识**：AST 工作全部在注册期完成，运行期只读 record；
2. **复用既有请求级注入**：`finally` 还原 + RLock 已经存在，参数机制
   零新增并发原语、零命名空间泄漏；
3. **别名是程序不是模板**：同一段代码本地与 HTTP 行为必然一致，
   可审计、可推演、可测试。

---

## 10. 保留字与边界情况

| 项 | 规则 |
|---|---|
| `_alias_arg` | 保留变量；签名里禁止（R4）；无参 / exact 调用时显式复位为 `''`，body 永不留旧值 |
| `r` | 非保留但**强烈不建议**作参数名：非表达式结尾的 body 用 `r` 兜底返回值，参数 `r` 会被执行结果语义缠绕；起名避开结果变量 |
| 闭包自由名 | 内层函数引用的外层名**不是**参数，运行期从持久命名空间解析 |
| body 顶层赋值名 | 被赋值的名字不再是自由变量，不参与绑定集；它与注入同名变量时，本次执行内是同一对象，执行后随请求级命名空间一起还原 |
| 持久命名空间可见性 | 注入是请求级的：执行完毕即还原，参数值**永不残留**在共享命名空间 |
| 重名覆盖 | 重新 `#alias add(...)` 覆盖时，`created` / `hits` 沿用旧值，`signature` / `params` / `defaults` 整体替换 |
| prefix 别名被 exact 命中 | 注册了 `git` 又注册 `git+` 时，输入 `git` 按 exact（②）命中 `git`，`git status` 走 ④——两模式可安全共存 |

---

## 11. 配置项汇总

| 配置 | 默认 | 作用 |
|---|---|---|
| `ALIAS_KEYWORDS` | `["alias", "a"]` | 指令关键字（list，运行期可增删） |
| `UNALIAS_KEYWORDS` | `["unalias", "ua", "d"]` | 删除指令关键字 |
| `ALIAS_ARG_SEP` | `"="` | `名=参` 分隔符 |
| `ALIAS_ARG_NAME` | `"_alias_arg"` | 参数注入变量名（保留） |
| `ALIAS_PREFIX_MIN_LEN` | `2` | 前缀别名有效前缀最小长度 |
| `ALIAS_ESCAPE` | `"\\"` | 转义字符，剥后跳过全部别名展开 |
| `ALIAS_PARAMS_LENIENT` | `False` | 未知参数：False 报错（C2）/ True 忽略并记 `warnings` |
| `ALIAS_REGEX` | 关闭（未实现） | 预留：完整正则别名，默认永不开启 |
| `PythonExecutor(alias=...)` | `True` | 实例级总开关 |
| `execute(alias=...)` | `None`（沿用实例） | 单次调用总开关 |
| `execute(alias_prefix=...)` | `True` | 单次调用前缀开关 |

---

## 12. 兼容性

- 既有 plain 别名：语义、记录字段、命中行为**零变化**；
- 既有 `#alias` / `#unalias` 指令：查询、列表、删除、定义即执行，零变化；
- record 仅**新增** `mode` / `signature` / `params` / `defaults` 字段，
  旧代码读取不受影响；
- 旧调用 `execute(code, globals=...)` 对 plain 别名的注入行为不变；
- `#alias a b` 这类“裸名字含空白”的指令行**仍然是错误**（既有语义不变）；
  带空白 / 转义的名字只能写成 Python 字符串字面量（3.1.1）；
- 无迁移步骤，升级即生效。

---

## 13. 完整示例

### 13.1 params：加法

```
#alias add(x, y=3)
x + y
```

```python
executor.execute('add', globals={'x': 4})            # r == 7  （y 用默认值）
executor.execute('add', globals={'x': 4, 'y': 10})   # r == 14
executor.execute('add')                               # C1: 缺少必填参数 ['x']
executor.execute('add', globals={'x': 1, 'z': 2})     # C2: 未知参数 ['z']
```

```
GET /-add?x=4        → TypeError（'4' + 3：HTTP 值默认是字符串，见 4.5）
GET /-add?x=4&y=3    → r == '43'（字符串拼接，不是 7；要算术请见 13.2）
```

HTTP 侧需要算术时在 body 里显式转换，见 13.2。

### 13.2 params：有转换的 body

```
#alias addi(x, y=3)
int(x) + int(y)
```

```
GET /-addi?x=4&y=3   → r == 7（字符串经 body 显式转换）
```

### 13.3 prefix：git 前缀

```
#alias git+
import subprocess
subprocess.run('git ' + _alias_arg, shell=True, cwd='/repo')
```

```
GET /-git pull               → 后台执行 git pull
GET /-git status -s          → _alias_arg == 'status -s'
GET /-git                    → _alias_arg == ''，body 自行兜底（如打印用法）
GET /-\git status            → 转义：字面执行（不展开别名）
```

### 13.4 无签名别名也能注参（自由变量即参数集）

```
#alias sum3
x + y + z
```

```python
executor.execute('sum3', globals={'x':1, 'y':2, 'z':3})   # r == 6
```

```
GET /-sum3?x=1&y=2&z=3   → r == '123'（字符串拼接；要算术请写 int(x) + int(y) + int(z)）
# GET /-sum3?x=1&y=2      → C1: 缺少必填参数 ['z']
```

注册时 `record['params'] == ['x', 'y', 'z']`、`defaults == {}`——
**必填由“无默认值”推导**，签名只是可选的声明 sugar。

### 13.5 引号名字：带空格的别名

```
#alias "a b"
r = 9
```

```
a b              → r == 9（exact，名字就是解码后的 'a b'）
"a b"=/x         → 命中 =arg 形态，_alias_arg == '/x'
# GET /a%20b     → r == 9
# GET /%22a%20b%22=%2Fx → r == '/x'
```

```
#alias "git status"+
r = _alias_arg
```

```
git status -s    → _alias_arg == '-s'
git status       → _alias_arg == ''（C5）
```

```
#alias "add two"(x, y=3)
int(x) + int(y)
```

```
GET /-%22add%20two%22?x=4   → r == 7（query 注入 + 默认值 y=3）
```

持久化重启后同样成立：`rpc.json` 的 key 是解码后的 `a b` / `add two`，
`build_alias_record(..., decoded_name=True)` 直接按解码名重建记录。

### 13.6 注册错误一览

```
#alias a b                   → 裸名字含空白：仍按“一行一个名字”拒绝（3.1.1）
#alias "a b               → 引号未闭合：拒绝注册
#alias ""                    → 名字为空：拒绝注册
#alias a+                    → R6: 前缀太短（最小 2），建议 git+、ime+
#alias add(*a)               → R1: 不支持 *args
#alias add(y=x+1)            → R2: 默认值必须是字面量
#alias bad(x, z)  +  x+y     → R3: 'z' 是 dead param
#alias add(_alias_arg)       → R4: 保留名
#alias git+(x)               → R5: v1 不支持 前缀+签名 组合
#alias zh("选项 A"=1)        → 参数名含空白：拒绝注册（参数名要能作 Python 变量）
```

---

## 附：实现检查清单

- [x] 指令行支持签名形态（名字后带 `(...)`），解析出 name / signature / body
- [x] 注册时：`ast.literal_eval` 校验默认值；计算 body 自由变量集（顶层 Load − 顶层 Store）
- [x] R1–R6 校验全部在写注册表**之前**完成
- [x] record 写入 `mode` / `signature` / `params` / `defaults`
- [x] `_expand_alias`：② exact（params 别名转入注入流程）→ ③ `=arg` → ④ prefix（最长优先），① 转义最先
- [x] 键校验（C2）与必填校验（C1）在锁内、注入之前
- [x] exact / prefix 无参与调用时 `_alias_arg` 复位为 `''`
- [x] `#alias` 列表按模式渲染（plain / `name(x, y=3)` / `name+`）
- [x] `alias_prefix=False` 单次开关
- [x] lenient 模式经 `"warnings"` 字段透传未知 key
- [x] 名字支持 Python 字符串字面量（`#alias "a b"` / 转义 / `"git status"+` / `"add two"(x, y=3)`），裸名字含空白仍报错
- [x] `split_alias_call` / `#unalias` 分词 / 临时标记剥离都引号感知
- [x] 持久化恢复按解码名重建（`build_alias_record(..., decoded_name=True)`）
- [x] server_http：读 record（O(1)）决定是否注入 query，`alias_url` 支持 `'dash'` / `'raw'` / `'json'` 值解码
