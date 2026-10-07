# PTY over MQTT 协议参考（排错 / 二次开发用）

主文档见上级 `SKILL.md`。本文件描述握手、帧格式、去重与线程模型，常量与代码均以
`client/remote_cmd.py`（`_PTY_START_TEMPLATE`、`RemotePty`）和 `multi_mqtt.py` 为准。

## 1. 拓扑

```
本地 client/pty_client_mqtt / RemotePty          远端 server_mqtt.py（通用，无 PTY 专用代码）
        |  ① 签名 RPC：整段 PTY 启动代码            |
        |  ---- sys/device/request (broadcast) -->  gms.handle_message -> exec 代码
        |  <-- sys/device/response --------------  返回 {ok, sid, topics, shell, pid}
        |                                          （代码在服务端进程内常驻：openpty+shell+3 线程）
        |  ② 订阅 out，按键写 in（会话专属 topic）    |
        |  == pty/<sid>/in  {"pty","iseq","k"} ==>  _in_loop -> write(master)
        |  <== pty/<sid>/out {"pty","seq","d"} ===  _out_loop <- read(master)
        |  <== pty/<sid>/out {"pty","hb"} ========  _hb_loop（存活证明）
        |  <== pty/<sid>/out {"pty","end",...} ===  shell 退出 / TTL / stop
```

所有消息由 `MultiMQTTManager.publish_broadcast` 广播到 broker 列表中的每一个连接
（约 13 个 broker，集群型 broker 还会双投）；两端各自订阅全部 broker，靠去重收敛。

## 2. 握手细节

1. 客户端生成 `sid = "pty-<utc_ms>-<urandom2.hex>"`，得 `in_topic=pty/<sid>/in`、
   `out_topic=pty/<sid>/out`（均可显式覆盖）。
2. 先 `stream_subscribe(out_topic, handler)`，再把 `build_pty_start_code(payload)`
   当普通带签名 code 经 `Transport.request` 发往 request topic——所以握手享受
   req_id 的一问一答去重与超时。
3. payload 字段：`sid, in_topic, out_topic, rows, cols, shell, term, cwd, login,
   flush_interval, ttl, frame_max, heartbeat`。
4. 服务端下发代码内 `_find_net()` 扫描服务端全局命名空间找到 `gms.mqtt_net`
   （复用现有连接，不新建），并注入一次性路由：
   `_net.message_callback` 被替换为 `_router`——命中 `_cmq_pty_router[topic]`
   的消息进 PTY 队列且**不再透传**给 RPC 回调（避免 "missing code" 噪声），
   其余消息原样调用保存的 `_cmq_pty_orig`。
5. 回包 env：`{ok, sid, heartbeat, in_topic, out_topic, shell, pid, term, rows,
   cols, cwd, flush_interval, ttl, login}`。失败为 `{ok:false, error:<traceback>}`。

shell 解析：显式 `--shell` > `$SHELL` > passwd 用户 shell > `/bin/bash` > `/bin/sh`；
login shell 的 argv0 带 `-` 前缀；子进程 `setsid()` + `TIOCSCTTY` 拿控制终端。

## 3. 帧格式

全部为 JSON dict，二进制字节按 latin-1 映射为 U+0000..U+00FF（1 字节 ↔ 1 字符）。

上行（client → `pty/<sid>/in`），均含单调 `iseq`：

| 用途 | 帧 |
|---|---|
| 按键 | `{"pty": sid, "iseq": n, "k": "<latin-1>"}` |
| 改窗口 | `{"pty": sid, "iseq": n, "winsz": [rows, cols]}` |
| 结束会话 | `{"pty": sid, "iseq": n, "stop": true}` |

下行（server → `pty/<sid>/out`）：

| 用途 | 帧 |
|---|---|
| 输出 | `{"pty": sid, "seq": n, "d": "<latin-1>"}` |
| 心跳 | `{"pty": sid, "hb": <服务器毫秒>}` |
| 结束 | `{"pty": sid, "end": true, "reason": str, "rc": int}` |

## 4. 去重（多 broker 环境的正确性核心）

- **RPC 帧**：网络层 `MultiMQTTManager.dedup_cache`（TTLCache，30s，5 万条）按
  `req_id` 首帧放行；签名帧的 req_id 形如 `<base>|<sig_hex>`，验签后剥签名。
- **PTY 下行**：`RemotePty` 内 `deque(maxlen=256)` 按 `seq` 去重；只认同号副本，
  不假设严格递增（跨 broker 会乱序）。
- **PTY 上行**：服务端 `_in_loop` 内 `deque(maxlen=256)` 按 `iseq` 去重；序号由
  `RemotePty._publish_input` 对 `send/resize/detach` 统一打号（线程安全）。
  无 `iseq` 的旧格式帧仍放行（兼容）。
- 心跳与 end 帧无序号：心跳只是刷新存活时间戳，重复无害；end 帧客户端置位
  `end_reason`/event，天然幂等。

症状对照：一次回车出现多条命令/一串空提示符 = 上行去重失效（旧客户端）；
一份输出打印多遍 = 下行 seq 去重失效。

## 5. 输出攒批与存活检测

`_out_loop` 对 master fd 用 `select.poll`：

- `flush_interval=0`（默认）：读到数据立即成帧（`seq` 自增）。
- `flush_interval>0`：距首字节攒满间隔、或缓冲达 `frame_max` 才发一帧。
- 轮询超时随 TTL 截止时间收敛；POLLHUP/ERR 时排空剩余输出后发 end 帧。

心跳：`_hb_loop` 每 `heartbeat` 秒发一帧 `hb`（默认 5s，0=关闭）。客户端以
"最后收到任意输出/心跳帧的时间"做死亡判定，`dead_timeout` 自动抬到至少
`3×heartbeat`（默认 15s），超时直接本地退出。心跳关闭时不做误杀（shell 静默
与服务器死亡无法区分）。

退出路径：

- shell 自然退出（exit/Ctrl-D）→ HUP → end 帧 `reason="exit"`；
- 收到 stop → `_kill()`：先 SIGHUP 整个进程组，0.5s 后 SIGKILL，`reason="stopped"`；
- TTL 到 → `reason="ttl"`。
- 客户端所有退出路径直接 `os._exit`（先恢复终端 raw 模式，stop 帧 fire-and-forget），
  不做 paho 慢清理。

## 6. 尺寸/时间常量

| 常量 | 值 | 含义 |
|---|---|---|
| `PTY_FRAME_MAX` | 16384 | 单帧原始字节上限（latin-1 转义后最坏 <96KiB） |
| `DEFAULT_PTY_TTL` / `MAX_PTY_TTL` | 12h / 24h | 孤儿会话寿命 |
| `WIRE_BUDGET` | 128KiB | 文件分块单报文目标在线尺寸 |
| `MAX_TRANSFER` | 1MiB | 文件经报文通道传输硬上限 |
| `MAX_DIR_ARCHIVE` / `DEFAULT_DIR_ARCHIVE_MAX` | 1MiB / 700KiB | `pull_dir` 压缩包硬顶（客户端+远端双侧强制）/默认阈值（base64×4/3 后仍 <1000KiB） |
| broker 实测 | 1000KiB 可过 / 1200KiB 丢 | 大文件必须远端就地处理 |

`pull_dir`（普通签名 RPC op，非 PTY）：payload `{op:"pull_dir", path, excludes[],
max_bytes, top_n}`；远端纯标准库 `os.walk`（exclude 按路径组件+相对路径 fnmatch，
命中目录即剪枝）→ `tarfile+gzip(mtime=0)` 边压边计数、超限即停 → 成功回
`{ok,b64,md5,arc_bytes,raw_bytes,files,dirs,excluded,skipped_links,skipped_special,
excludes}`，超限回 `{ok:false,reason:"too_large",largest:[{path,size}]...}`，
客户端映射为 `DirArchiveTooLarge`。符号链接/特殊文件不入包。

## 7. 二次开发要点

- 新增传输层（HTTP/TCP/WS）只需实现 `client.remote_cmd.Transport`：`request(code,
  timeout)` 为必需；PTY 另需 `publish`、`stream_subscribe`、`stream_unsubscribe`
  （参考 `client.cmd_client_mqtt.MqttTransport`）。
- 改动 PTY 服务端行为时改 `_PTY_START_TEMPLATE` 字符串本身——它是逐字下发
  执行的代码，改名/改缩进前确认模板内 `_o/_t/_th/...` 等短别名一致；
  `__PAYLOAD__` 占位符替换为 `json.dumps(json.dumps(payload))` 的双编码字面量。
- 模板改动无需升级部署服务端：下次客户端握手即生效（但旧客户端连新服务端、
  新客户端连旧服务端的混跑要保留无 iseq 兼容分支）。
