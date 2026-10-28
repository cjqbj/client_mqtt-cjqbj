# client/pty —— pty_client_mqtt 的多文件版本

单文件 [`../pty_client_mqtt.py`](../pty_client_mqtt.py) 由本目录**生成**：
`parts/` 分片是唯一编辑入口，改完用生成器重组写回。

## 目录结构

```
pty/
├── parts/                    # 单文件的分片（编号即拼接顺序，唯一编辑入口）
│   ├── 000_head.py           # 模块 docstring / 导入 / sys.path 与环境引导
│   ├── 010_alias.py          # 统一别名表（CLI 选项 + 魔术栏 + rpc 别名共用）
│   ├── 020_console.py        # 本地终端 raw 输入/输出、输入转码、剪贴板粘贴
│   ├── 030_termwrite.py      # 二进制流锁定、_TerminalWriter 异步写线程、终端复位、控制台守卫
│   ├── 031_console_sink.py   # 控制台输出通道 _ConsoleSink：UTF-8 跨帧 carry + 零进度保护 + 新句柄自愈
│   ├── 040_hotkey.py         # 热键按键名解析（ctrl-] / ctrl-alt-insert → VT 序列）
│   ├── 050_status.py         # broker 在线状态、stderr 异步写出
│   ├── 060_logring.py        # 本地日志环形缓冲 _LogRing / _RingTextIO / _RingLogHandler
│   ├── 070_loghub.py         # 浏览器实时日志台 LogHub（WebSocket 快照+增量）
│   ├── 075_logpage.py        # 日志台 HTML 页面、ring 日志接管安装、_info
│   ├── 080_writerdiag.py     # 写线程诊断与自愈、QuickEdit 选择模式自动解除
│   ├── 090_aibridge.py       # AI 桥：_strip_ansi、base64 严格编码、AIBridge、ai_bridge
│   ├── 095_aicallers.py      # AI 桥调用方便捷函数（local_rpc / ai_pty_run / …）
│   ├── 097_mcp_server.py     # MCP 服务：pty_run/pty_send/pty_status/pty_log/pty_redraw
│   ├── 098_mcp_files.py      # MCP 文件工具：fs_read/write/edit/glob/grep/ls/mkdir/move/delete
│   ├── 100_magic.py          # 本地命令栏（魔术命令 _run_magic / 参数规格表）
│   ├── 105_exit.py           # _hard_exit 硬退出、_join_pre_command
│   ├── 110_session.py        # run_session 会话主循环
│   └── 120_cli.py            # build_parser / main / __main__ 入口
├── __init__.py               # 按序把全部分片 exec 进同一命名空间（包模式入口）
├── __main__.py               # python -m client.pty 入口
├── generate_single_file.py   # 分片 → 单文件重组器（默认只生成；--check 才比对）
└── README.md
```

## 用法

```bash
# 多文件版：包导入（等价 from client.pty_client_mqtt import ...）
from client.pty import ai_pty_run, ai_pty_status, ai_bridge, start_mcp_server

# 多文件版：命令行（等价 python client/pty_client_mqtt.py）
python -m client.pty

# 改完分片后重组写回正式单文件（默认只生成、不比对；parts 是唯一编辑入口）
python client/pty/generate_single_file.py -f

# 只比对磁盘上的单文件与分片拼接结果（字节+AST，不写文件）
python client/pty/generate_single_file.py --check
```

## MCP 工具服务

`pip install "mcp[cli]"` 后，运行中的 pty 进程把常驻 PTY 持续暴露为标准 MCP
工具：**默认复用 `--port` 的 1188 口**（路径 `/mcp`，与 RPC/浏览器日志台同口
并存，不新开端口——给 `server_http.RPCRequestHandler` 打保留路径前置分流，
参考 HuggingFace app.py 的中间件写法；mcp 2.x 走 streamable-http，旧版退回
`/sse`+`/messages`；未装包只记一条日志，PTY 照常工作）：

```bash
claude mcp add --transport http pty http://192.168.1.3:1188/mcp
# 旧版 SDK：claude mcp add --transport sse pty http://192.168.1.3:1188/sse
```

工具：`pty_run`（跑命令回收 rc/输出）、`pty_send`（喂交互键）、`pty_status`
（会话/broker 状态）、`pty_log`（本地诊断日志）、`pty_redraw`（写卡滞自愈
+整屏重绘）。与 1188 HTTP RPC 口同源同锁，三路调用状态一致。`--no-mcp` 关闭；
`--mcp-port N`（N>0）改为另起独立端口（`--port 0` 关闭 RPC 口时仍要 MCP 用）。

文件工具（098 分片）**作用于远端机器**：客户端把实现层源码打包，经
`ai_bridge.transport.request`（与 `client_mqtt.rpc` 同一条 MQTT RPC 通道）
发到远端 `server_mqtt.py` 进程里 `exec`，因此 `/root/...` 这类路径解析的是
远端文件系统；不经过常驻 PTY shell，也不占 `ai_bridge` 的 busy 锁。

| 工具 | 参数 | 说明 |
| --- | --- | --- |
| `fs_read` | `file_path, offset=0, limit=2000` | 带行号读取 UTF-8 文本，二进制拒绝 |
| `fs_write` | `file_path, content` | 整文件覆盖写（父目录需已存在） |
| `fs_edit` | `file_path, old_string, new_string, replace_all=false` | 精确替换，多命中默认报错；CRLF 自动保持 |
| `fs_glob` | `pattern, path=".", max_results=200` | glob 查文件，按 mtime 倒序 |
| `fs_grep` | `pattern, path=".", glob, ignore_case, output_mode, context, head_limit, offset` | 正则搜索，自动跳过 .git/二进制 |
| `fs_ls` | `path=".", show_hidden=true` | 列目录条目 |
| `fs_mkdir` | `path, parents=true` | 创建目录 |
| `fs_move` | `src, dst, overwrite=false` | 移动/重命名 |
| `fs_delete` | `path, recursive=false` | 删除文件或目录（目录需 recursive=true） |

权限：读写工具默认都可作用于任意路径——`pty_run` 本就能执行任意代码，
给写操作加根限制没有实际意义。仅 `--fs-read-only`（默认关闭）会禁用写家族。
单次读写上限 5MB，grep 有 2 万文件 / 5 秒上限。

每次工具调用分配 6 位调用 id（`os.urandom(3).hex()`）：开始行带完整参数，
完成行整行缩进、带参数短摘要 + 状态 + 耗时，两行各自可独立阅读，并发交错
也不影响归属：
`[MCP] 3f9a2b 开始 fs_grep pattern='sherpa' path='/root/...'` 与
`   [MCP] 3f9a2b 完成 fs_grep pattern='sherpa' path='/root/...' ok 0.21s`
（业务失败为 FAIL + 原因，异常为 FAIL + 异常类型）。参数按 repr 转义成单行，
开始行单值超 400 字符截断、完成行单值只留前 60 字符，不等远端返回。
`/mcp` 的 2xx HTTP 访问行已过滤（每次调用伴随多条 POST、不含工具名与参数），
只保留 4xx/5xx；SDK 的 `Terminating session: None` INFO 已压到 WARNING。

> 注：文件操作作用于**远端 server_mqtt.py 所在机器**（与 `pty_run` 的目标同一台），
> 不是运行 pty 客户端的本机；`--fs-read-only` 只禁写，读仍可用。

## 规则

1. **只编辑 `parts/` 分片**；不要手改 `client/pty_client_mqtt.py`
   （它由生成器产出）。
2. 分片之间**不能调整顺序**、不能留未编号文件；新增分片用等宽 3 位数字
   前缀插入编号序列（按文件名排序即拼接顺序）。
3. 改完分片后 `python client/pty/generate_single_file.py -f` 重组写回
   （默认只生成、不比对）；`--check` 可随时校验分片与单文件是否同步
   （字节+AST，不写文件）。
4. **启动自检**：单文件每次启动自动做 `check_parts_sync()`——单文件字节
   本来就等于分片拼接，直接比对两侧 sha256，不一致（分片改了没重组/手改了
   单文件）会在日志里打 WARN 并给出重组命令；运行中也可随时查：
   `curl "http://127.0.0.1:1188/r=check_parts_sync()"`。部署机没有 parts
   目录时自动跳过。
5. 分片不是独立模块，不要单独 import；包模式一律从 `client.pty` 进。

## 设计说明：为什么分片不能是互相 import 的真模块

原文件里 `run_session` 会 `global _OUT_WRITER` 重新绑定写线程，
`_stderr_writer` / `reset_out_writer` / `main` 也会跨段改写
`_ERR_WRITER`、`_LOG_MIRROR_STDERR` 等模块级可变全局。若拆成真模块，
各模块经 `from .x import y` 拿到的是**绑定快照**，重绑定只落在定义模块，
其他段永远读到旧值——语义必然漂移（终端复位/硬退出路径会拿到过期的
`None`）。因此 `__init__.py` 采用**共享命名空间顺序 exec**：加载语义与
单文件完全一致；重组侧则因为分片是逐字节切片，拼接结果与原文件
**逐字节一致**（sha256 相同），功能不变有硬证据。

## 设计说明：控制台输出通道（031_console_sink）

2026-10-25 现场实证（1188 RPC + py-spy）确认了控制台冻结的完整链路：

1. 远端输出按字节分帧，切点可能落在 UTF-8 字符中间（汉字 3B / emoji 4B）；
2. `_io._WindowsConsoleIO.write` 对**末尾不完整的多字节序列**返回 0 字节且
   不抛异常（实测 `b"abc\xe4\xb8"` 只写 3B，剩下的 `b"\xe4\xb8"` 永远返回 0）；
3. CPython `BufferedWriter.flush()` 的 C 循环在 0 进度上无限重试：持锁 +
   约占满一个核（3 秒采样 73% 单核，线程停在 flush 行）；
4. 旧实现所有 `_TerminalWriter` 共用 `_bin_stdout()` 缓存的**同一个**
   `BufferedWriter`，`reset_out_writer()` 换线程换不掉那把锁，于是每 60 秒
   多留一个永久阻塞线程（实测 62 个在等同一把锁）。

因此 `_ConsoleSink` 做了四件事：直接 Win32 `WriteConsoleW`（非控制台退回
`os.write`），彻底绕开 `BufferedWriter`；写前把末尾不完整的 UTF-8 字节留进
carry，下一帧补齐，半个字符永不进控制台；原生写 0 进度只记账并保留数据，
绝不原地重试；每个 sink 自带锁和自己的 `CONOUT$` 句柄，reset 即换新实例，
旧实例卡死也拖不住新写线程。`writer_diag()` 会给出通道快照（`kind`、
`zero_progress`、`carried_bytes`、`holder` 持锁线程、`busy_seconds`），
一眼区分"锁死"与"零进度"。
