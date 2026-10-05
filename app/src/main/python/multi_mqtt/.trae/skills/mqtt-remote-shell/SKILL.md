---
name: mqtt-remote-shell
description: 通过公共 MQTT broker 像 SSH 一样操作远端设备，含一次性命令、就地 grep/编辑、文件传输与交互式 PTY 终端。当用户要求在 MQTT 设备或容器上跑命令、改文件、开远程终端（ssh/pty/remote shell/远程执行）时使用；纯本地任务不要用。
---

# MQTT 远程 Shell / PTY（像 SSH 一样用）

## 1. 心智模型

- 远端机器/容器只需运行通用 `server_mqtt.py`（POSIX 环境），**不需要开放任何入站端口、不需要装 sshd**。客户端经 ~13 个公共 MQTT broker 广播 Python 代码，服务端执行后把结果广播回协商 topic。
- 本仓库提供两种远程操作形态，**先选对形态再动手**：

| 形态 | 入口 | 模型 | 谁适合用 |
|---|---|---|---|
| 一次性命令 / 文件操作 | `cmd_client_mqtt.py`、`remote_cmd.RemoteShell` | 一问一答，结构化 JSON，有退出码 | **AI 自动化默认用这个** |
| 交互式终端 | `pty_client_mqtt.py`、`remote_cmd.RemotePty` | 常驻 shell、逐键透传、ANSI 全屏 | 人类用户；vim/top/密码提示等真交互 |

## 2. AI 选型规则（重要）

默认走一次性命令通道，不要为"跑几条命令"去驱动 PTY：

- 执行命令拿 stdout/退出码 → `cmd_client_mqtt.py run "..."`
- 看目录/读小文件/grep/改文件/传文件 → 对应子命令或 `RemoteShell` 方法
- 多步编排、需要解析结果 → Python API `MqttTransport` + `RemoteShell`
- **仅当**目标是全屏/有状态交互程序（`vim`、`top/htop`、`passwd`、嵌套 `ssh`、交互式 REPL）时才用 PTY

硬约束：`pty_client_mqtt.py` 启动时检查 `sys.stdin.isatty()`，非真终端直接退出码 2。AI 在管道/非交互执行环境里**无法**使用它；需要向 PTY 喂字节时用 `RemotePty` Python API 自行收 ANSI 输出（解析自己负责）。

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
python cmd_client_mqtt.py run "uname -a" -k "123+456"
python cmd_client_mqtt.py -t sys/device/request -k "123+456" run "ps aux | head"

# 长命令给远端超时；注入环境变量
python cmd_client_mqtt.py run "apt-get update" --cmd-timeout 300 -e DEBIAN_FRONTEND=noninteractive

python cmd_client_mqtt.py info                 # 远端环境 JSON（user/os/python/cwd/home）
python cmd_client_mqtt.py ls /etc              # 目录列表 JSON
python cmd_client_mqtt.py stat /etc/hostname
python cmd_client_mqtt.py grep -r "ERROR" /var/log --include "*.log"
python cmd_client_mqtt.py cat /etc/hostname
python cmd_client_mqtt.py mkdir /tmp/x
python cmd_client_mqtt.py rm /tmp/x -r
python cmd_client_mqtt.py apt "htop curl"      # 远端 apt-get 非交互安装
python cmd_client_mqtt.py pip "requests"
```

文件传输（≤1MiB，超限默认拒绝）：

```bash
python cmd_client_mqtt.py get /etc/hostname ./hostname.txt   # 远端 → 本地
python cmd_client_mqtt.py put ./local.txt /root/local.txt    # 本地 → 远端
python cmd_client_mqtt.py write /root/x.txt --data "hello"
python cmd_client_mqtt.py edit /etc/app.conf --old "v1" --new "v2"   # 精确替换，默认备份 .bak
```

Python API（多步编排、要结构化结果时用，避免反复起进程）：

```python
from cmd_client_mqtt import MqttTransport
from remote_cmd import RemoteShell

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
finally:
    sh.close()
```

可用方法：`run/py/info/cd/grep/edit_replace/append_text/read/write/download/upload/
ls/stat/exists/mkdir/rm/mv/cp/chmod/hash/install_packages/apt_install/pip_install/
which/stream/monitor`。详见 [remote_cmd.py](file:///c:/test/github/multi_mqtt/remote_cmd.py) 中 `RemoteShell`。

## 5. 交互式 PTY（人类终端 / 全屏程序）

```bash
python pty_client_mqtt.py -t sys/device/request -k "123+456"
python pty_client_mqtt.py -i 0.2          # 高延迟网络：服务端最多攒批 0.2s
python pty_client_mqtt.py --shell /bin/bash --cwd /root
python pty_client_mqtt.py --size 24x100   # 脚本/窗口探测不准时强制尺寸
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
from cmd_client_mqtt import MqttTransport
from remote_cmd import RemotePty

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

## 6. 协议速览（排错用，完整版见 `references/protocol.md`）

- 握手：客户端先订阅 `pty/<sid>/out`，再走普通签名 RPC 把自包含 PTY 启动代码下发到 request topic；服务端在自己进程里 openpty + 起 shell，回 `{ok, sid, in_topic, out_topic, shell, pid, ...}`。**服务端无需任何 PTY 专用代码**。
- topic：`pty/pty-<毫秒时间戳>-<随机>/in`（按键上行）、`.../out`（输出下行），会话级唯一。
- 字节承载：任意字节按 latin-1 1:1 放进 JSON 字符串；PTY 单帧 ≤16KiB。
- 去重（多 broker 是必然而非异常，实测同一帧能收到约 15 份）：
  - 普通 RPC 帧靠网络层 `req_id` 的 30s TTLCache 去重；
  - PTY 下行按帧内 `seq` 去重，上行按帧内 `iseq` 去重（`send/resize/detach` 统一打号）。排查"按一个键蹦一串提示符/重复执行"先查这两条。
  - 下行还有 `hb`（心跳毫秒）与 `end`（`{reason, rc}`）两类控制帧。

## 7. 红线与常见问题

- **单报文 1MiB 是 broker 实测硬上限**（1000KiB 可过、1200KiB 丢，512KiB 已需数秒）。大文件/大输出一律远端就地处理：`grep/sed/curl/tar/apt/pip`，不要 cat 大文件回传。
- PTY 只能在 POSIX 服务端建立（依赖 openpty/termios）；Windows 只能当客户端。
- 启动有固有延迟：`node.start()` 固定等 2s + 各 broker 建连 0.1~13s；首次命令超时就把 `--timeout` 调大。
- PTY 里输出卡顿：公共 broker 延迟高时加 `-i 0.1~0.5` 攒批，牺牲一点实时性换帧数。
- 会话是孤儿 TTL 模型，默认 12h（上限 24h），客户端异常退出后远端 shell 到时才收尸；要立即杀 shell 就正常 `exit` 或确保发出 Ctrl-]/detach 帧。

## 8. 常用配方

```bash
# AI 排查远端问题的标准起手式（全程无 TTY、可解析）
python cmd_client_mqtt.py info
python cmd_client_mqtt.py run "uptime; df -h; free -m"
python cmd_client_mqtt.py run "journalctl -u myapp -n 200 --no-pager" --cmd-timeout 60

# 只想看持续刷新的 top（这个子命令本身是订阅推送模型，Ctrl-C 停）
python cmd_client_mqtt.py top

# 人类用户要登进去手动操作时，才给交互式终端
python pty_client_mqtt.py -k "123+456"
```
