---
name: mqtt-remote-shell
description: 通过mqtt 像 SSH 一样操作远端设备，含一次性命令、就地 grep/编辑、文件传输与交互式 PTY 终端。当用户要求在 MQTT 设备或容器上跑命令、改文件、开远程终端（ssh/pty/remote shell/远程执行）时使用；纯本地任务不要用。
---

# MQTT 远程 Shell / PTY（像 SSH 一样用）

## 1. 心智模型

- 远端机器/容器只需运行通用 `server_mqtt.py`（POSIX 环境），**不需要开放任何入站端口、不需要装 sshd**。客户端经 ~13 个公共 MQTT broker 广播 Python 代码，服务端执行后把结果广播回协商 topic。
- 本仓库提供两种远程操作形态，**先选对形态再动手**：

| 形态 | 入口 | 模型 | 谁适合用 |
|---|---|---|---|
| 一次性命令 / 文件操作 | `client/cmd_client_mqtt.py`、`client.remote_cmd.RemoteShell` | 一问一答，结构化 JSON，有退出码 | **AI 自动化默认用这个** |
| 交互式终端 | `client/pty_client_mqtt.py`、`client.remote_cmd.RemotePty` | 常驻 shell、逐键透传、ANSI 全屏 | 人类用户；vim/top/密码提示等真交互 |

## 2. AI 选型规则（重要）

默认走一次性命令通道，不要为"跑几条命令"去驱动 PTY：

- 执行命令拿 stdout/退出码 → `client/cmd_client_mqtt.py run "..."`
- 看目录/读小文件/grep/改文件/传文件 → 对应子命令或 `RemoteShell` 方法
- 多步编排、需要解析结果 → Python API `MqttTransport` + `RemoteShell`
- **同一轮要连发很多条命令、或希望人类实时围观** → 开一个常驻 PTY 监控窗口，AI 走 `ai_pty_run` 复用，免每条命令重连（见第 6 节）
- **仅当**目标是全屏/有状态交互程序（`vim`、`top/htop`、`passwd`、嵌套 `ssh`、交互式 REPL）时才用 PTY

硬约束：`client/pty_client_mqtt.py` 启动时检查 `sys.stdin.isatty()`，非真终端直接退出码 2。AI 在管道/非交互执行环境里**无法**使用它；需要向 PTY 喂字节时用 `RemotePty` Python API 自行收 ANSI 输出（解析自己负责）。

## 3. 连接参数（两个入口共用）

| 参数 | 默认 | 说明 |
|---|---|---|
| `-t/--topic/--request-topic` | `sys/device/request` | 命令下发 topic |
| `--reply/--reply-topic` | `sys/device/response` | 应答 topic |
| `-k/--key` | 空（不签名） | 客户端私钥：整数表达式如 `123+456`、PEM 文本、私钥文件路径 |
| `-a/--allow` / `--no-allow` | allow 开 | 是否接受服务端未验签的回包（只管回包，不管请求是否被执行） |
| `--timeout` | 30s（PTY 握手同） | 单次问答超时秒数 |

注意：服务端配置了公钥验签时，`-k` 不匹配会被服务端直接拒绝执行（不是超时）；排查"命令没反应"先确认密钥与 topic。

## 4. 一次性命令（AI 首选）

CLI（不需要 TTY，退出码即远端进程 rc）：

```bash
# 基本：一条 shell 命令，和 ssh host "cmd" 等价
python client/cmd_client_mqtt.py run "uname -a" -k "123+456"
python client/cmd_client_mqtt.py -t sys/device/request -k "123+456" run "ps aux | head"

# 长命令给远端超时；注入环境变量
python client/cmd_client_mqtt.py run "apt-get update" --cmd-timeout 300 -e DEBIAN_FRONTEND=noninteractive

python client/cmd_client_mqtt.py info                 # 远端环境 JSON（user/os/python/cwd/home）
python client/cmd_client_mqtt.py ls /etc              # 目录列表 JSON
python client/cmd_client_mqtt.py stat /etc/hostname
python client/cmd_client_mqtt.py grep -r "ERROR" /var/log --include "*.log"
python client/cmd_client_mqtt.py cat /etc/hostname
python client/cmd_client_mqtt.py mkdir /tmp/x
python client/cmd_client_mqtt.py rm /tmp/x -r
python client/cmd_client_mqtt.py apt "htop curl"      # 远端 apt-get 非交互安装
python client/cmd_client_mqtt.py pip "requests"
```

文件传输（≤1MiB，超限默认拒绝）：

```bash
python client/cmd_client_mqtt.py get /etc/hostname ./hostname.txt   # 远端 → 本地
python client/cmd_client_mqtt.py put ./local.txt /root/local.txt    # 本地 → 远端
python client/cmd_client_mqtt.py write /root/x.txt --data "hello"
python client/cmd_client_mqtt.py edit /etc/app.conf --old "v1" --new "v2"   # 精确替换，默认备份 .bak
```

整目录拉取（远端纯 Python 标准库打包，**不依赖 tar/base64 等外部命令**）：

```bash
# 打包 → 校验(md5) → 安全解压到本地目录；--exclude 可重复，fnmatch 匹配任意层级目录/文件名组件
python client/cmd_client_mqtt.py pull-dir /root/proj ./proj_mirror \
    --exclude .git --exclude build --exclude __pycache__ --exclude '*.pyc'
python client/cmd_client_mqtt.py getdir /etc/nginx ./nginx_mirror   # 别名 getdir/pulldir
python client/cmd_client_mqtt.py pull-dir /root/proj --tgz proj.tgz # 只存压缩包不解压
python client/cmd_client_mqtt.py pull-dir /root/proj ./m --max-bytes 1048576  # 默认 700KiB，硬顶 1MiB
```

压缩包超限时**远端拒绝打包**并返回面向 AI 的诊断（中文）：已达字节数、原始总
字节、文件/目录数、**最大的 15 个文件清单**、本次 excludes、以及缩小范围的具
体建议。客户端抛 `DirArchiveTooLarge`（`.env` 为结构化字段）。REPL 里对应
`%getdir <远端目录> [本地目录] [--exclude P]... [--tgz PATH]`。

Python API（多步编排、要结构化结果时用，避免反复起进程）：

```python
from client.cmd_client_mqtt import MqttTransport
from client.remote_cmd import RemoteShell

tr = MqttTransport(request_topic="sys/device/request",
                   reply_topic="sys/device/response",
                   private_key="123+456", allow_no_pub=True)
sh = RemoteShell(transport=tr)          # 或 MqttRemoteShell(...)
try:
    print(sh.info())                    # dict
    r = sh.run("uname -a", timeout=30)  # CmdResult: .ok .rc .stdout(bytes) .stderr .duration
    matches = sh.grep("ERROR", paths=("src",), fixed=False)
    sh.edit_replace("/etc/x", "old", "new", backup=True)
    sh.download("/var/log/app.log", "./app.log")   # >1MiB 默认拒绝
    # 整目录：tar.gz 在远端用纯标准库打包，回包 md5+长度双校验后安全解压
    meta = sh.pull_dir("/root/proj", "./proj_mirror",
                       excludes=[".git", "build", "__pycache__", "*.pyc"])
    blob, meta = sh.pull_dir_bytes("/etc/nginx")   # 只要压缩包字节
    # 抓远端 tmux 窗格：
    txt = sh.tmux_capture_pane("main:2.1", max_lines=500, capture_args="-J -e")
finally:
    sh.close()
```

`pull_dir(remote, local, excludes=(), max_bytes=None) -> meta`：
`excludes` 为 fnmatch 模式（匹配任意层级路径组件或相对路径，目录命中即剪枝）；
`max_bytes` 默认 700KiB（base64 后仍 < 1000KiB 报文线），**硬顶 1MiB**，超限抛
`DirArchiveTooLarge`（异常文本含最大文件清单与排除建议，`.env.largest` 可程序化读取）；
符号链接/设备文件远端跳过。内部安全解压拒绝绝对路径/`..` 穿越/链接条目。

可用方法：`run/py/info/cd/grep/edit_replace/append_text/read/write/download/upload/
pull_dir/pull_dir_bytes/ls/stat/exists/mkdir/rm/mv/cp/chmod/hash/install_packages/
apt_install/pip_install/which/tmux_capture_pane/stream/monitor`。详见 [remote_cmd.py](file:///c:/test/github/multi_mqtt/client/remote_cmd.py) 中 `RemoteShell`。

## 5. 交互式 PTY（人类终端 / 全屏程序）

```bash
python client/pty_client_mqtt.py -t sys/device/request -k "123+456"
python client/pty_client_mqtt.py -i 0.2          # 高延迟网络：服务端最多攒批 0.2s
python client/pty_client_mqtt.py --shell /bin/bash --cwd /root
python client/pty_client_mqtt.py --size 24x100   # 脚本/窗口探测不准时强制尺寸
```

会话内行为：

- 连接后服务端只 fork **一次**常驻登录 shell，之后每个按键都写同一个 PTY（不会每条命令重启 bash），cd/export/作业控制全程保持。
- **Ctrl-]** 本地强制脱离（可用 `--detach-key` 改）；远端 `exit` / Ctrl-D 自然结束。
- 本地窗口缩放会自动同步 winsz（SIGWINCH）。
- 默认 5s 心跳；服务端被杀/断连后最多 15s（≥3 倍心跳）自动退出，不会无限干等。`--heartbeat 0` 关闭，`--dead-timeout 0` 关闭死亡检测。
- Windows 本地会自动开 VT 输入/输出与 UTF-8 代码页；方向键/功能键自动翻译成 VT 序列。

Python API（需要程序化驱动 PTY 时）：

```python
import queue
from client.cmd_client_mqtt import MqttTransport
from client.remote_cmd import RemotePty

tr = MqttTransport(private_key="123+456")
pty = RemotePty(tr, timeout=30)
outq = queue.Queue()
env = pty.open(46, 163, cwd="/root", flush_interval=0.0,
               heartbeat=5.0,
               on_data=outq.put,
               on_heartbeat=lambda ts: None)
print(env["shell"], env["pid"])
pty.send(b"ls -la\r")      # bytes，回车是 \r
pty.resize(24, 80)
pty.detach()               # 或对端 exit 后 pty.end_reason 置位
```

## 6. 常驻监控窗口（AI 高频调用首选，免重连）

每次 `python client/cmd_client_mqtt.py ...` 都是**新进程**：重新并发连接 ~13 个 broker、等首个 broker 上线（通常 <1s，慢时数秒）。需要连续跑很多条命令时，改成开一个常驻 PTY 窗口，AI 走本机 HTTP 复用它，零 MQTT 重连，且每条命令在人类的监控窗口里实时可见：

```bash
# 人类先在一台终端里开着（建议只绑本机）；窗口就是监控屏
python client/pty_client_mqtt.py -k "123+456" --host 127.0.0.1
# Windows 本机须用 C:\QGB\miniforge3\python.exe（3.14，含 paho）；
# PATH 默认的 Anaconda 3.7 无法导入本仓库代码（新式类型注解语法）
```

### AI 30 秒上手（其他 AI 先看这段）

1. **先探活**：`GET http://127.0.0.1:1188/r=ai_bridge.status()`，看 `attached` / `busy` / `end_reason` / `brokers_online`。
2. **跑命令**：Python 环境用 `ai_pty_run(...)`；只有 HTTP 时用 `/r=ai_bridge.run("...")`。
3. `busy=true` 就等上一条结束，**不要并发**；`end_reason` 非 null 说明监控窗口已退出，请人类重启（AI 自己无法启动，PTY 要求交互 TTY）。
4. 命令全程在人类窗口可见；`run()` 返回结构 `{ok, rc, out, timed_out, ...}`。

### 裸 HTTP：优先用最短的 `/r=表达式`

控制口把 **URL 路径直接当 Python 源码执行**，返回值按优先级取：
`p.set_data(...)` 显式响应体 → 持久命名空间里的变量 `r` → print 捕获的 stdout。
所以单表达式一条路径就够，不必 `import json` + 百分号编码整串代码：

```
GET http://127.0.0.1:1188/r=ai_bridge.status()
GET http://127.0.0.1:1188/r=ai_bridge.run("uname%20-a")
GET http://127.0.0.1:1188/r=ai_bridge.send("y\n")
# 实时日志首选浏览器，不必刷 RPC：
#   浏览器开 http://192.168.1.3:1188/  → 根路径自动 302 到全屏日志台
#   页面经 WebSocket /wslog 先推快照再推增量（snapshot/lines 帧）
GET http://192.168.1.3:1188/r=get_log()
GET http://192.168.1.3:1188/r=get_log(50)
GET http://192.168.1.3:1188/r=clear_log()
```

注意：

- 路径里的空格、引号等仍需 percent-encode；参数一复杂就改用 Python helper（`ai_pty_run/send`），不要在 URL 里堆多语句。
- 需要精确 JSON 字符串时才用编码后的 `p.set_data(json.dumps(obj, ensure_ascii=False))`。
- `r` 留在持久命名空间，会被下一次 `/r=...` 覆盖，不要依赖上一次的残留值。
- **终端与日志彻底分离**：PTY 窗口只渲染远端 shell；broker 连接/重连、MultiMQTT/paho 的 logging、每笔 `[RPC]` 请求行、`[WARN]`/banner，乃至任何库的 print / stdout / stderr 全部进进程内环形缓冲（约 256KB/2000 行，超量丢最旧），真实终端零写入。查看通道：① 浏览器全屏实时日志台 `http://<本机IP>:1188/`（无工具栏、黑底等宽、滚到底自动跟随、上翻暂停、ANSI 着色、慢消费丢旧行不反压业务线程，手机同局域网可开）；② `/r=get_log(n)` 取最近 n 行（默认 200，n<=0 全部）、`/r=clear_log()` 清空；③ 窗口命令栏（Ctrl+Alt+Insert）输 `log [n]`（自擦覆盖层）。实现约定：组合方在 import multi_mqtt 前置 `CMQ_NO_STDERR_LOG=1`（import 期只挂 NullHandler），起 RPC 口成功后 `_install_log_capture()` 锁定真实控制台二进制流并把 root logging + sys.stdout/stderr 换成环；`_RingTextIO` 刻意无 buffer，PTY 渲染侧自动回退 `sys.__stdout__`。仅会话退出时的致命错误仍直写真实 stderr（终端已复位）；`--port 0` 关 RPC 口时无查看通道，日志退回 stderr 镜像。

```python
# AI / 任意进程：只是一次 localhost HTTP，不碰 broker
from client.pty_client_mqtt import ai_pty_run, ai_pty_status, ai_pty_send

ai_pty_status()                         # {'attached': True, 'brokers_online': 13, 'busy': False, ...}
r = ai_pty_run("uname -a")             # {'ok': True, 'rc': 0, 'out': '...', 'timed_out': False}
r = ai_pty_run("apt-get install -y htop", timeout=300)
ai_pty_send("y\n")                     # 交互提示喂键（确认/密码）；Ctrl-C 发 "\x03"
```

约定与边界：

- 命令经同一常驻 shell 执行（cd/export 状态保持）；**串行化**，重叠调用立即返回 `{'ok': False, 'busy': True}`，不要并发。
- `run()` 用标记回收输出和退出码，超时只停止回收、不杀远端命令（输出继续在窗口刷，可再 `send` 干预）；返回的 `out` 已剥 ANSI。
- 全屏/真交互程序（vim/top/密码 TUI）不要用 `run()`，人工在窗口操作或只用 `send()` 喂键。
- 窗口进程退出（Ctrl-]/exit）后调用返回 `pty 已结束`；控制口默认 1188，`--port 0` 关闭；裸 HTTP 首选 `GET /r=<表达式>`（见上），多语句才 percent-encode 后走 `p.set_data(...)`。

## 7. 协议速览（排错用，完整版见 `references/protocol.md`）

- 握手：客户端先订阅 `pty/<sid>/out`，再走普通签名 RPC 把自包含 PTY 启动代码下发到 request topic；服务端在自己进程里 openpty + 起 shell，回 `{ok, sid, in_topic, out_topic, shell, pid, ...}`。**服务端无需任何 PTY 专用代码**。
- topic：`pty/pty-<毫秒时间戳>-<随机>/in`（按键上行）、`.../out`（输出下行），会话级唯一。
- 字节承载：任意字节按 latin-1 1:1 放进 JSON 字符串；PTY 单帧 ≤16KiB。
- 去重（多 broker 是必然而非异常，实测同一帧能收到约 15 份）：
  - 普通 RPC 帧靠网络层 `req_id` 的 30s TTLCache 去重；
  - PTY 下行按帧内 `seq` 去重，上行按帧内 `iseq` 去重（`send/resize/detach` 统一打号）。排查"按一个键蹦一串提示符/重复执行"先查这两条。
  - 下行还有 `hb`（心跳毫秒）与 `end`（`{reason, rc}`）两类控制帧。

## 8. 红线与常见问题

- **单报文 1MiB 是 broker 实测硬上限**（1000KiB 可过、1200KiB 丢，512KiB 已需数秒）。大文件/大输出一律远端就地处理：`grep/sed/curl/tar/apt/pip`，不要 cat 大文件回传。整目录回传用 `pull-dir`（纯 Python 打包、md5 校验）：压缩包硬顶 1MiB、默认阈值 700KiB（base64 膨胀 4/3 后仍可过线），超限远端拒绝并返回最大文件清单与 exclude 建议——按建议缩小范围重试，不要自己拼 `tar|base64|split` 手工分块。
- PTY 只能在 POSIX 服务端建立（依赖 openpty/termios）；Windows 只能当客户端。
- 启动有固有延迟：`node.start()` 固定等 2s + 各 broker 建连 0.1~13s；首次命令超时就把 `--timeout` 调大。
- PTY 里输出卡顿：公共 broker 延迟高时加 `-i 0.1~0.5` 攒批，牺牲一点实时性换帧数。
- 会话是孤儿 TTL 模型，默认 12h（上限 24h），客户端异常退出后远端 shell 到时才收尸；要立即杀 shell 就正常 `exit` 或确保发出 Ctrl-]/detach 帧。

## 9. 常用配方

```bash
# AI 排查远端问题的标准起手式（全程无 TTY、可解析）
python client/cmd_client_mqtt.py info
python client/cmd_client_mqtt.py run "uptime; df -h; free -m"
python client/cmd_client_mqtt.py run "journalctl -u myapp -n 200 --no-pager" --cmd-timeout 60

# 拉整个小源码目录回本地（纯 Python 打包，替代手工 tar|base64|split；先排除产物目录）
python client/cmd_client_mqtt.py pull-dir /root/proj ./proj \
    --exclude .git --exclude build --exclude out --exclude __pycache__ --exclude '*.pyc'

# 只想看持续刷新的 top（这个子命令本身是订阅推送模型，Ctrl-C 停）
python client/cmd_client_mqtt.py top

# 人类用户要登进去手动操作时，才给交互式终端
python client/pty_client_mqtt.py -k "123+456"
```
