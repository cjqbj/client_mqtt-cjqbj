# pty_client_mqtt.py 深入解析 —— PTY over 公共 MQTT 的完整手册

> 本文只围绕 **pty_client_mqtt.py** 一个文件展开，但为了讲清它做了什么，
> 必须连带说明它调用的两个邻居：`remote_cmd.RemotePty`（协议核心）和
> `cmd_client_mqtt.MqttTransport` / `multi_mqtt.MultiMQTTManager`（网络层）。
> 阅读对象：想理解、维护或二次开发这条链路的人（或 AI）。

---

## 1. 这个文件是什么

`pty_client_mqtt.py` 是一个**交互式远程终端客户端**，体验和 `ssh user@host` 几乎一样：

- 本地终端进入 raw 模式，每个按键实时发到远端；
- 远端常驻一个真实 shell（bash/sh，跑在 `forkpty` 出来的伪终端里）；
- 远端输出（含 ANSI 颜色、光标控制）原样回传本地渲染；
- 中间不走 SSH、不开端口、不需要公网 IP —— 全部流量经过 **公共 MQTT broker** 转发。

关键架构决策：**服务端零改动**。远端只需要跑着通用的 `server_mqtt.py`
（一个"执行客户端下发的 Python 代码并回包"的 RPC 服务）。PTY 所需的全部服务端逻辑
（openpty、fork shell、读写线程、心跳）是一段自包含 Python 模板
（`remote_cmd._PTY_START_TEMPLATE`），在握手时由本客户端作为一次普通 RPC 下发执行。

文件自身只有约 370 行，做四件事：

| 区块 | 位置 | 职责 |
|---|---|---|
| `_enable_output_vt` | L54-69 | Windows 上打开 ANSI VT 渲染 + UTF-8 代码页 |
| `_PosixRawConsole` / `_WinRawConsole` | L74-153 | 本地终端 raw 输入（POSIX termios / Windows VT 或 msvcrt 回退） |
| `_hard_exit` + `run_session` | L158-292 | 会话主循环、存活检测、所有退出路径统一 `os._exit` |
| `build_parser` / `main` | L297-369 | CLI 参数与启动 |

---

## 2. 完整网络流程

### 2.1 拓扑：一条逻辑连接，十几条物理连接

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────┐
│  本机 client │────────▶│  ~13 个公共 broker │────────▶│ 远端 server  │
│ (pty_client) │◀────────│ (broker.emqx.io…) │◀────────│(server_mqtt)│
└─────────────┘         └──────────────────┘         └─────────────┘
      每帧 publish 到所有在线 broker（广播），订阅也在所有 broker 上进行。
      同一帧会被服务端收到约 N+ 份（实测 11 个连接能到 ~15 份）。
```

这是本项目最核心的设计：**用数量换可用性**。任何一个 broker 挂了/被墙了，
只要还有一个活着，链路就通。代价是**每一帧都有多份重复副本**，
因此全链路到处是去重逻辑（见 §4）。

### 2.2 阶段一：建立 RPC 通道（`main` → `MqttTransport`）

`main()` 做的事（L337-365）：

1. 解析 `--size` 或读本地终端大小（`shutil.get_terminal_size`）；
2. **硬性检查 `sys.stdin.isatty()`**（L351）——PTY 必须有一个真实本地终端，
   管道/重定向下直接报错退出（返回码 2）。这也是 AI 自动化不应使用本文件的原因；
3. `_enable_output_vt()`：Windows 打开 `ENABLE_VIRTUAL_TERMINAL_PROCESSING`
   和 CP65001，让 conhost 能渲染远端的 ANSI 序列；
4. 构造 `MqttTransport(request_topic, reply_topic, private_key, allow_no_pub)`：
   - 内部建 `MQTTClientNode` → `MultiMQTTManager`，**并行连接 BROKER_LIST 里
     全部 ~13 个公共 broker**（QoS 0，keepalive 300s，断线自动重连）；
   - 订阅默认回包 topic `sys/device/response`。

此时只有一个"一问一答"通道：客户端把 `{"req_id":..., "code":..., "timestamp":...}`
发到 `sys/device/request`，服务端执行后把结果发到 `sys/device/response`。

### 2.3 阶段二：PTY 握手（`run_session` → `RemotePty.open`）

这是整个协议最精巧的一步（`remote_cmd.py` L1664-1745）：

```
client                                     server
  │                                          │
  │ 1. 生成 sid = "pty-<毫秒时间>-<4位随机hex>"   │
  │    in_topic  = "pty/<sid>/in"            │
  │    out_topic = "pty/<sid>/out"           │
  │                                          │
  │ 2. 本地先订阅 out_topic（防丢最早输出）       │
  │                                          │
  │ 3. ──RPC(request topic)────────────────▶ │
  │    code = _PTY_START_TEMPLATE            │
  │    payload = {sid, in_topic, out_topic,  │
  │      rows, cols, shell, term, cwd,       │
  │      login, flush_interval, ttl,         │
  │      frame_max, heartbeat}               │
  │    （此帧带 req_id + ECDSA 签名）          │
  │                                          │
  │                            4. 服务端进程内执行模板:
  │                               pty.openpty()
  │                               fork 常驻 shell（只此一次）
  │                               劫持消息回调，给 in_topic 装路由
  │                               起 3 个线程: _in_loop/_out_loop/_hb_loop
  │                                          │
  │ 5. ◀──RPC(reply topic)────────────────── │
  │    {ok, sid, pid, shell, term, rows,     │
  │     cols, cwd, in_topic, out_topic, …}   │
  │                                          │
  │ ═══════ 之后数据面走 in/out 两个专属 topic ═══════ │
```

模板在服务端做的关键动作（`_PTY_START_TEMPLATE`，remote_cmd.py L669-949）：

- **仅限 POSIX**（`os.name != "posix"` 直接报错）；
- `pty.openpty()` 拿到 master/slave fd，`TIOCSWINSZ` 设置初始窗口；
- `subprocess.Popen([argv0], executable=shell, preexec_fn=_become)`，
  `_become` 里 `setsid()` + `TIOCSCTTY` —— 和 SSH 完全同款的"新会话 + 控制终端"；
  `argv0` 默认带 `-` 前缀（login shell，可用 `--no-login` 关）；
- **回调路由注入**（L772-787）：把服务端网络层的 `message_callback` 换成自己的
  `_router`，凡是 topic 命中路由表（`in_topic`）的 dict 帧直接入队给 PTY，
  **不再下传给原 RPC 处理器**（否则每个按键帧都会被 `handle_message` 当成
  "missing code" 的坏请求打日志）。其余 topic 原样透传，互不影响；
- shell **只 fork 一次**，之后所有按键都写进同一个 master fd。

### 2.4 阶段三：数据面（按键 ↔ 屏幕）

**上行（本地按键 → 远端 shell）：**

```
本地键盘 → raw console.read() → input_loop 线程
  → pty.send(bytes) → _publish_input 打 iseq 序号
  → transport.publish(in_topic) → 广播到全部 broker (QoS 0)
  → 服务端每个 broker 各收一份 → _router → _inq 队列
  → _in_loop 线程: 按 iseq 去重(256 窗口) → os.write(master_fd, 按键字节)
  → shell 读到按键
```

上行三种帧（全是 JSON dict，二进制按键用 latin-1 1:1 映射成字符串）：

```json
{"pty": "<sid>", "iseq": 42, "k": "<latin-1 按键字节>"}
{"pty": "<sid>", "iseq": 43, "winsz": [24, 100]}
{"pty": "<sid>", "iseq": 44, "stop": true}
```

**下行（远端输出 → 本地屏幕）：**

```
shell 输出 → master fd → _out_loop 线程 (poll)
  → 攒批（flush_interval=0 立即发；>0 按秒合并）→ seq 序号
  → publish_broadcast(out_topic, {"pty":sid, "seq":n, "d":latin-1})
  → 客户端每个 broker 各收一份 → stream 分发链 → RemotePty.handler
  → 按 seq 去重(256 窗口 deque) → on_data(bytes) → outq
  → 主线程 sys.stdout.buffer.write 原样渲染
```

下行三种帧：

```json
{"pty": "<sid>", "seq": 7, "d": "<latin-1 输出>"}
{"pty": "<sid>", "hb": 1759598400123}
{"pty": "<sid>", "end": true, "reason": "exit|stopped|ttl", "rc": 0}
```

### 2.5 阶段四：存活检测（为什么要有心跳）

公共 broker 模型下有个致命盲区：**服务端进程被杀时，broker 还活着**，
`end` 帧永远发不出来，客户端会无限干等（按键全进黑洞）。

解决方案（pty_client_mqtt.py L195-204 + 模板 L920-932）：

- 服务端 `_hb_loop` 按 `--heartbeat`（默认 5s）周期发心跳帧；
- 客户端 `signal_state["last"]` 记录**最近一次收到任何远端帧**（输出或心跳）的时间；
- 主循环每 0.3s 检查一次：超过 `--dead-timeout`（默认 15s，且不小于 3 倍心跳）
  没收到任何帧 → 判定服务器已死，立即退出（返回码 3）；
- `--heartbeat 0` 时检测自动失效（无法区分"shell 静默"和"服务器死了"，不误杀）。

### 2.6 阶段五：退出路径（全部汇到 `_hard_exit`）

| 触发 | 路径 | 退出码 |
|---|---|---|
| 远端 `exit` / Ctrl-D | shell 退出 → `_out_loop` 收 HUP → 发 `end` 帧 → 主循环看到 `pty.end_reason` | 0 |
| 本地 Ctrl-]（`--detach-key` 可改） | `input_loop` 命中脱离键 → `stop_ev.set` → 主循环落到末尾 | 0 |
| 心跳超时 | 主循环检测 `dead_timeout` | 3 |
| Ctrl-C | `KeyboardInterrupt` | 130 |

`_hard_exit`（L158-186）的设计哲学是**绝不拖泥带水**：

1. `pty.detach()` —— fire-and-forget 发一帧 `stop`（能发出去就让远端收尸，发不出去也不等）；
2. `console.exit()` —— **必须先恢复本地终端 raw 模式**（因为下一步 `os._exit` 跳过所有 finally）；
3. 写退出信息到 stderr、flush stdout；
4. `os._exit(code)` —— 不做 `pty.close()`（会干等 end 帧 1.5s）、不 join 线程、
   不逐个 disconnect broker。旧路径这些清理在服务器已死时纯属浪费，界面"卡住"就是这么来的。

服务端侧的收尸：收到 `stop` 帧 → `_kill()` 先 `SIGHUP` 进程组、0.5s 后没死再
`SIGKILL`；`_out_loop` 退出时关闭 master fd、从路由表摘掉 `in_topic`、发最终 `end` 帧。
即使客户端直接消失，也有 **TTL 兜底**（`--ttl` 默认 12h，上限 24h），到期服务端自动杀会话。

---

## 3. 线程模型全景

**客户端进程（pty_client_mqtt）：**

| 线程 | 来源 | 职责 |
|---|---|---|
| main | `run_session` | 从 outq 取远端输出写 stdout；每 0.3s 检查 end_reason / 心跳超时 |
| pty-input | L262 | raw 读本地键盘 → `pty.send`；命中脱离键则置 stop_ev |
| pty-resize | L263 | 每 0.5s 轮询本地窗口大小，变了就 `pty.resize` |
| paho 网络线程 ×13 | paho-mqtt | 每个 broker 一条，收发 MQTT 帧 |
| MQTTMsgDispatch | MultiMQTTManager | 单线程串行分发所有 broker 的消息到回调链 |

**服务端进程（server_mqtt 内，由模板启动）：**

| 线程 | 职责 |
|---|---|
| pty-in | 从 `_inq` 取上行帧 → iseq 去重 → 写 master fd / 改窗口 / 处理 stop |
| pty-out | poll master fd → 攒批 → publish 下行帧；检测 HUP/TTL；发 end |
| pty-hb | 周期发心跳帧（`heartbeat>0` 时） |

注意：服务端这些线程跑在 **server_mqtt 的持久全局命名空间**里（RPC 的 REPL 语义），
所以会话能跨多次 RPC 存活；回调路由表 `_cmq_pty_router` 也挂在那里。

---

## 4. 多 broker 去重：三层防线

广播模型下同一帧会到多份，每一层都有自己的去重机制：

| 层 | 保护对象 | 机制 | 位置 |
|---|---|---|---|
| ① 网络层 | 带 `req_id` 的 RPC 帧（含 PTY 握手） | `TTLCache`（30s TTL，5 万条 FIFO 上限）按 req_id 首帧放行 | `multi_mqtt._on_message` |
| ② PTY 下行 | 输出/心跳帧 | 客户端 `RemotePty.handler` 里 `deque(maxlen=256)` 按 `seq` 去重 | remote_cmd.py L1697-1701 |
| ③ PTY 上行 | 按键/控制帧 | 服务端 `_in_loop` 里 `deque(maxlen=256)` 按 `iseq` 去重 | 模板 L794-807 |

**③ 是后补的关键修复**：PTY 帧没有 `req_id`，天然绕过 ①；最初上行没有序号，
按一次回车会被十几个 broker 副本各写一遍（命令跑一次 + 一串空提示符）。
修复方案是客户端 `_publish_input` 作为上行唯一出口统一打单调递增 `iseq`，
服务端只放行首帧；**无 `iseq` 的旧帧照旧放行**（向后兼容）。

256 窗口的含义：容忍 broker 间的乱序和延迟，只要副本在首帧之后 256 帧内到达
都能被认出。超过窗口的迟到副本会被当成新帧 —— 对按键流来说实际不可能发生。

---

## 5. 安全措施（以及它*没有*做什么）

### 5.1 有什么

**① 握手帧的 ECDSA 签名（NIST256p + SHA-256）**

- 客户端 `publish_broadcast` 对含 `code` 的帧强制签名：
  `sign_msg = "{req_id}|{code}|{timestamp}"`，签名 hex 拼进
  `req_id` 变成 `"<base>|<sig_hex>"`（multi_mqtt.py L1184-1201）；
- 服务端若配置了公钥（`--pub`），`_on_message` 对含 `code` 的帧**强制验签**，
  缺签名结构或签名无效直接丢弃（L1066-1097）；
- PTY 启动模板就是靠这个通道下发的 —— **没有私钥的人无法让服务端启动 PTY**；
- 私钥支持多种形式：整数 secexp、安全整数表达式（AST 白名单求值）、
  PEM、OpenSSH 私钥文件（multi_mqtt.py `get_standard_pem_bytes`）。

**② 客户端的"服务端没验签"检测（`allow_no_pub`）**

服务端若没配公钥，会把带 `|` 的签名 req_id 原样回显。客户端 `_on_message`
据此识别"服务端根本没验签"：默认 `allow_no_pub=True` 放行（兼容模式），
关掉则丢弃这类响应并打安全拦截日志（client_mqtt.py L59-75）。

**③ 资源与滥用防护**

- `req_id` 长度上限（`MAX_REQ_ID_LEN`）拒绝异常帧；
- `TTLCache` 有 5 万条 FIFO 上限，防恶意灌 req_id 顶爆内存；
- PTY TTL 钳制在 [60s, 24h]，孤儿会话必然被回收；
- 服务端分发队列有界（满了丢消息打日志，不阻塞 paho 网络线程）。

**④ 上行帧路由隔离**

PTY 专属 topic 的帧在 `_router` 层就被截走，永远不会进入 `handle_message`
的代码执行路径 —— 按键数据不会被误当成代码执行。

### 5.2 没有什么（必须清楚的威胁模型）

| 缺失 | 后果 | 缓解 |
|---|---|---|
| **无加密** | 所有按键和屏幕输出对任何订阅了该 topic 的人**明文可见**。`enable_crypto` 开关存在但默认关闭，且 PTY 数据帧不走它 | 不要在会话里输入密码/密钥；或用私有 broker |
| **公共 broker 无鉴权** | 任何人都能向 `pty/<sid>/in` 发帧 | sid 含毫秒时间+4 位随机 hex，可猜性低但**不是秘密凭证**；攻击者若在握手时旁观到 sid 即可注入按键 |
| **数据帧不签名** | 上行 `k`/`winsz`/`stop` 帧无签名，服务端只看 `pty==sid` | 同上。把 `request_topic`/`reply_topic` 改成非默认值可缩小暴露面 |
| **QoS 0** | 帧可能丢失（对按键流可接受，TCP 语义不保证） | 协议不自恢复，丢了就丢了；shell 层会表现为少一个字符 |

一句话：**这套系统的安全边界是"握手签名 + topic 隐蔽性"，不是加密通道。
把它当 telnet over 公开信道用，不要当 SSH 用。**

### 5.3 安全使用清单

1. 服务端务必配 `--pub`（验签），客户端配对应 `--key`；
2. 改默认 `request_topic`/`reply_topic`（`-t` / `--reply`），别用 `sys/device/*`；
3. 会话里不要输入任何机密（sudo 密码、私钥、token）；
4. 高敏环境换私有 broker（改 `BROKER_LIST`），公共 broker 只用于演示；
5. `--no-allow` 可强制客户端拒绝"服务端未验签"的响应。

---

## 6. 尺寸与性能红线

| 常量 | 值 | 含义 |
|---|---|---|
| broker 单报文上限 | **1 MiB**（实测 1000KiB 过、1200KiB 丢） | 所有帧的硬天花板 |
| `PTY_FRAME_MAX` | 16 KiB | 下行单帧原始字节上限（latin-1 转义后 < 96KiB，远低于红线） |
| 上行单次 read | 4096 B | 本地一次读键盘的字节数 |
| `flush_interval` | 0（默认实时） | >0 时服务端按秒攒批，高延迟网络省帧数 |
| 心跳 / 死判 | 5s / 15s（≥3×心跳） | 公共 broker 短暂重连不会误判 |

带宽直觉：每个下行帧会被客户端收到 ~13+ 份副本，屏幕狂刷时流量放大十几倍，
这是广播模型的固有成本；`--interval 0.2` 能显著降帧数。

---

## 7. CLI 参数速查

```
python pty_client_mqtt.py [选项]

连接（与 cmd_client_mqtt 一致）：
  -t/--request-topic   请求 topic（默认 sys/device/request）
  --reply-topic        回包 topic（默认 sys/device/response）
  -k/--key             私钥：整数表达式/PEM/文件路径；空串不签名
  --no-allow           拒绝"服务端未验签"的响应
  --timeout            握手等待秒数（默认 30）

PTY：
  --shell              远端 shell（默认登录 shell）
  --term               TERM（默认本地 $TERM 或 xterm-256color）
  --cwd                启动目录（默认远端 HOME）
  -i/--interval        服务端攒批秒数（默认 0=实时）
  --no-login           不用 login shell
  --ttl                孤儿会话存活上限（默认 12h，≤24h）
  --heartbeat          服务端心跳秒（默认 5，0=关）
  --dead-timeout       收不到任何远端帧多少秒判定死（默认 15，0=关）
  --size ROWSxCOLS     强制窗口（默认取本地终端）
  --detach-key         本地脱离键（默认 Ctrl-] = \x1d）
```

退出码：`0` 正常/脱离，`2` 参数或握手错误，`3` 心跳超时，`130` Ctrl-C。

---

## 8. 常见故障排查

| 现象 | 最可能原因 | 处理 |
|---|---|---|
| 启动报 "PTY 需要一个交互式本地终端" | stdin 被管道/重定向（含 AI agent、CI） | 换用 `cmd_client_mqtt.py` 的一次性命令 |
| 按一个键执行了十几次 | 旧版本无 iseq 去重 | 升级 remote_cmd.py 后**重连客户端**即生效（模板握手时下发，服务端不用动） |
| 连上后无任何输出也不退 | 心跳关了且 shell 静默 | 保持 `--heartbeat>0` |
| 服务器重启后客户端干等 | 旧版无心跳检测 | 新版 15s 内自动退出（码 3） |
| 握手报 "missing code/payload" 满屏 | 旧模板没装路由，按键帧进了 RPC 处理器 | 新版模板 `_router` 已解决 |
| Windows 下方向键/F 键失灵 | VT 输入模式失败 | 代码自动回退 msvcrt 翻译表，仍不行检查终端 |
| 输出乱码 | 本地终端非 UTF-8 | Windows 已由 `_enable_output_vt` 设 CP65001 |

---

## 9. 二次开发要点

- **服务端永远不用改**：改 PTY 行为只改 `_PTY_START_TEMPLATE`，客户端下次握手自动生效；
- 上行新增控制帧：客户端走 `_publish_input`（自动打 iseq），服务端在 `_in_loop`
  里加分支；下行新帧在 `_out_loop`/`handler` 对称添加；
- 换网络层（HTTP/TCP/WS）：实现 `Transport.request` + `publish` +
  `stream_subscribe`/`stream_unsubscribe` 四个方法，`RemotePty` 原样可用；
- `_PTY_START_TEMPLATE` 是**自包含**的（只 import 标准库、不依赖远端文件），
  模板内函数名带 `_` 前缀避免污染服务端命名空间，`_cmq_pty_start` 禁止改名。
