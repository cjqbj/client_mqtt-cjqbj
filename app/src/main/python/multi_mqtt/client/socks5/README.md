# client/socks5 —— socks5_client_mqtt 的多文件包版本

冻结基线是 [`../old=socks5_client_mqtt.py`](../old=socks5_client_mqtt.py)
（2734 行，一个字节都不许改）；正式单文件 `../socks5_client_mqtt.py` 由本包
重组生成。本包即唯一编辑入口：函数/类/常量从基线**逐字节搬运**（每个模块
头部注释标注来源行号），改完模块用 `generate_single_file.py` 写回单文件，
`verify_parity.py` 机器校验两侧逐符号一致。

## 目录结构

```
socks5/
├── __init__.py          # 包入口：sys.path/环境变量引导 + 全量 re-export（命名空间与单文件版对齐）
├── __main__.py          # python -m client.socks5 入口（兼容直接按脚本运行）
├── config.py            # 统一别名表 + 调优常量（纯数据，无逻辑依赖）         ← 原文 113-159 行
├── util.py              # _info：stderr 直写日志                                ← 原文 162-167 行
├── server_template.py   # _SOCKS5_START_TEMPLATE 服务端模板（握手整段下发）
│                        #   + build_socks5_start_code 注入器                    ← 原文 170-1144 行
├── reassembly.py        # _ConnReassembly：per-cid 下行重组（缺口绝不跳号）     ← 原文 1149-1304 行
├── conn.py              # _ClientConn：会话内单连接状态                         ← 原文 1307-1347 行
├── session.py           # RemoteSocks5：远端出口会话（帧收发/竞速统计/NACK/
│                        #   pacing/专用 bulk 连接）                             ← 原文 1350-2321 行
├── local_server.py      # _recv_exact/_socks_reply/_Socks5Server：本地监听      ← 原文 2326-2579 行
├── cli.py               # build_parser / main                                  ← 原文 2584-2730 行
├── verify_parity.py     # 与冻结单文件版的逐符号校验（命名空间/源码/模板/CLI）
├── generate_single_file.py  # 包 → 单文件重组器（切片+冻结版面 → 逐字节还原原文件）
└── README.md
```

## 用法

```bash
# 包导入（等价 from client.socks5_client_mqtt import ...，公共 API 完全同名）
from client.socks5 import RemoteSocks5, main, build_parser

# 命令行（等价 python client/socks5_client_mqtt.py）
python -m client.socks5
python client/socks5/__main__.py        # 直接脚本运行也可

# 改完包内模块后：写回正式单文件（只生成，不比对）/ 显式对照基线 / 逐符号校验
python client/socks5/generate_single_file.py -f
python client/socks5/generate_single_file.py --check
python client/socks5/verify_parity.py
```

## 校验的四层

| 层 | 内容 |
| --- | --- |
| 命名空间 | 单文件版全部顶层定义（42 个：函数/类/常量/别名表）在 `client.socks5` 同名存在 |
| 源码 | 10 个函数/类的 def 块文本逐字节一致（`inspect.getsource`） |
| 模板 | `_SOCKS5_START_TEMPLATE` 全文与 `build_socks5_start_code(样例)` 产物一致 |
| CLI | `build_parser().format_help()` 完全一致 |

## 单文件重组（generate_single_file.py）

```bash
python client/socks5/generate_single_file.py -f         # 只生成：写 client/socks5_client_mqtt.py（不比对）
python client/socks5/generate_single_file.py --check    # 只比对：对照冻结基线 old=socks5_client_mqtt.py（字节+AST）
python client/socks5/generate_single_file.py -o 路径 -f # 只生成：写到指定文件
```

生成器把 8 个模块「来源标记」之后的逐字节切片，按内嵌的冻结版面（头部
docstring/imports/bootstrap、切片间的空行与分节注释、`__main__` 守卫）拼回
自包含单文件。**默认只生成、不与任何现有文件比对**；`--check` 才与冻结基线
比对——包未编辑时重组产物与基线 **sha256 完全一致**（实测 `ed3249f3…`，
132408 字节逐字节相同），编辑过则区分**纯版面差异**（AST 仍一致，退出码 2）
与**语义差异**（AST 不一致，退出码 3，配合 verify_parity.py 定位是哪个符号
变了）。

## 规则

1. 冻结基线 `old=socks5_client_mqtt.py` 一个字节都不许改；正式单文件
   `socks5_client_mqtt.py` 由生成器产出，不要手改。所有修改在本包模块内做，
   改完 `generate_single_file.py -f` 写回单文件，需要对照基线时 `--check`。
2. 各模块按依赖单向分层：`config → util / server_template → reassembly /
   conn → session → local_server → cli`，禁止反向 import。
3. 新代码直接写进对应模块，不要用 exec 拼接；需要单文件分发时用
   `generate_single_file.py` 重组（那是版面还原，不是运行机制）。

## 设计说明：为什么本包能拆成真模块，而 pty 不能

`client/pty` 拆分时采用「parts 字节切片 + `__init__` 共享命名空间 exec」
方案，根因是 pty 单文件里 `run_session` 会 `global _OUT_WRITER` 重绑定
写线程、多处函数跨段改写 `_ERR_WRITER` 等模块级可变全局——拆成真模块后
`from .x import y` 拿到的是**绑定快照**，重绑定只落在定义模块，其他模块
永远读到旧值，语义必然漂移。

`socks5_client_mqtt.py` 全文 **2734 行没有任何 `global` 语句**，模块级
名字全部是常量（int/float/str/tuple）与函数/类定义，运行期从不重绑定。
真模块拆分后各模块 `from .config import X` 拿到的绑定永远指向同一对象，
不存在拷贝漂移——因此可以用正常 Python 项目的写法（真 import、IDE 可
导航、grep 可达），不需要 exec 魔法与重组器，校验也从"重组产物 sha256
一致"升级为"逐符号源码一致"（`verify_parity.py`）。
