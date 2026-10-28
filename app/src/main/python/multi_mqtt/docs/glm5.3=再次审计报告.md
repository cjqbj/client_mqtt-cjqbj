# glm5.3=再次审计报告

- 审计日期：2026-10-24
- 审计基准：首轮《glm5.3=全库审计报告》之后的新状态 —— 修复基线 `bd04dfe`，本轮修复提交 `bd04dfe..HEAD` 共 **8 个**（e389797 / 8ed4f27 / 97507b0 / 5639e2c / 7ad472b / 3e5575e / 988e0c2 / b475d5a）。
- 范围声明：**本报告不重复首轮已列出的问题**（首轮 S1–S6、R1–R5、REPL 细项与架构建议均不再复述）。首轮问题中用户明确指示本轮修复的 7 项 + 修复过程中发现的 1 个自引入竞态，见第一部分；其余首轮问题（S1 dedup 先于验签、S3 默认信任锚/AES_KEY、S4 HTTP 面零鉴权 0.0.0.0、S5 仓库内第三方凭据、S6 数据面无认证等）**按指示保持未修**。
- 回归状态：`tests/` 主套件 **266 项** + `tests/socks5/` **51 项** 全绿（含新增 47 项测试）。

---

## 一、本轮 8 个修复提交清单与验证结论

| 提交 | 修复 | 新增测试 | 验证结论 |
|---|---|---|---|
| e389797 | S2 签名请求重放：`_on_message` 验签通过后校验 `\|now-timestamp\| ≤ timestamp_window`（默认 120s，构造参数可关） | test_replay_window.py 9 例（新鲜/过期/未来/宽窗/关闭/缺 ts/无验签不受影响/篡改签名/默认值） | ✅ 单位与口径已复核（客户端恒发 utc_ms 毫秒）；现有全部签名路径均带新鲜 timestamp，无兼容性破坏 |
| 8ed4f27 | R4 `_server_vk_invalid` 死代码链移除（multi_mqtt / server_mqtt `_is_verify_enabled` / FakeMQTTManager） | IsVerifyEnabledTests 5 例（三态判定/真实 manager 无该属性/坏公钥构造即抛=fail-closed 仍在） | ✅ |
| 97507b0 | global_topic_list 发布恒 qos=1（自定义 kwargs 也钉死，默认路径本就是 1，行为不变） | 1 例（默认 qos1+retain / 自定义 qos=0 被钉 1 / 发布链路捕获 qos==1） | ✅ 有意行为变更：自定义 kwargs 不再能降低报告 QoS（test_global_topic_list 契约同步更新） |
| 5639e2c | R1 `PythonExecutor.execute(timeout=999)`：执行移入 daemon 工作线程；超时立即返回错误；运行中→busy 快速失败+自动恢复；排队中→作废；alias 递归改走同线程 `_execute_inner`；server_http 删除 `_stdout_lock` 冗余临界区与重复 stdout 重定向 | test_executor_timeout.py 9 例（默认参数/快代码/超时+busy+恢复/排队作废不晚执行/并发串行/注入还原/SystemExit/None 同步/alias 不回归） | ✅（竞态缺陷一枚由 b475d5a 收紧，见下） |
| 7ad472b | R2 socks5 服务端模板 `_in_loop` 拆为 `_in_loop_run` + `_teardown` + 同名守卫 `_in_loop`（try/finally 保证收尾必达）；int(seq)/int(port)/_deliver/_retrans/_close_conn 全部单帧兜异常；新增 `_fr_errs` 计数经心跳 `fe` 字段暴露 | tests/socks5 2 例（畸形帧会话存活+deliver_error 收尾+fe 计数+stop 正常；非 dict/错 sid 忽略） | ✅ e2e 按函数名钻闭包的机制兼容（`_in_loop` 闭包经 `_in_loop_run` cell 可达，51 项含 e2e 全绿） |
| 3e5575e | REPL 四项：`watch df -h` 丢首词；`%grep --include` 缺参崩溃（解析抽为 `_parse_grep_opts`）；pty 魔术栏热调 heartbeat 按开局规则联动 dead_timeout；`RemotePty.leave()`（本地脱离不发 stop，远端 shell 继续跑），`_hard_exit(leave=True)` 仅用于 detach 路径 | test_cmd_client_mqtt.py 13 例 + test_pty_mqtt 3 例（leave 无 stop+退订+幂等、close 仍 stop、联动全路径） | ✅ 判死/会话结束/中断/窗口信号路径维持原 stop 行为 |
| 988e0c2 | R3 rpc_tcp 适配现行 executor：删 NameError 的 `locals_dict`；请求级 `client_sock` 经 `execute(globals=...)` 注入还原；port=0 回填；stop 守卫；`__main__` 对齐 server_http 模式 | test_rpc_tcp.py 9 例（构造不崩/往返/stdout/注入可见/不泄漏/持久命名空间/异常不断服/req_id 连续/非可序列化 repr 回退） | ✅ 旧 TODO「r=client_sock 卡死」由帧编码 `default=repr` 兜底解决 |
| b475d5a | 再次审计发现 5639e2c 的 TOCTOU：worker「先查 abandoned 再 started.set()」与调用方「先读 started 再置 abandoned」可交错 → 已宣布作废的请求仍晚到执行。改为配对次序（worker 先 started 再查 abandoned；调用方先 abandoned 再查 started）+ `_result_or_internal_error` 兜底恒返回 dict | 既有 9 例回归全绿（作废语义测试覆盖排队路径） | ✅ 交错空间已闭合：调用方判"未开始"时 worker 必然（或尚未 started→必见作废退出，或已 started→调用方转入超时路径），不存在第三态 |

**修复代码终审结论**（逐 diff 复核）：8 个提交无回归。唯一遗留注意点：`_redirect_stdout` 替换的是进程级 `sys.stdout`，同进程多个 executor 实例（server_http / server_mqtt / rpc_tcp 各一）并发执行时存在跨通道 stdout 串扰——**这是既有问题**（MQTT 分发线程与 HTTP worker 重叠执行在本轮之前同样可能），5639e2c 未引入也未恶化；建议后续把 stdout 捕获改为 per-call 上下文而非进程级替换（见二-1）。

---

## 二、新发现问题（首轮未列出）

### 中

**1. `_redirect_stdout` 是进程级 `sys.stdout` 替换，多 executor 并发互相踩踏**
`rpc_executor.py:514-525`。同进程 server_http(:406)/server_mqtt(:409)/rpc_tcp(:63) 三个 executor 各自串行，但互相之间不串行：MQTT 分发线程执行命令期间并发来一个 HTTP/TCP RPC 时，嵌套的 `__enter__/__exit__` 把对方 StringIO 当 "previous" 保存/恢复，最坏把 `sys.stdout` 永久留在已死 StringIO 上——stdout 串台 / print 进黑洞。既有问题，本轮终审定性。**修法方向**：捕获改为向 executor 实例的 buffer 写（用 `contextlib.redirect_stdout` 仍一样；应改为在执行命名空间注入自定义 `print`/或对全进程唯一 executor 加全局互斥，或干脆 per-channel executor 单例化）。

**2. PTY 服务端模板没同步 socks5 的"占坑"修复：同 sid 并发握手仍是 check-then-act**
`client/remote_cmd.py:874-877`（锁内读 `_sessions.get`）与 `:1236-1237`（锁内写回成品）之间夹着开 PTY/起线程/订阅等长串锁外动作。socks5 模板已为此引入 Event 占位（其 208-249 注释记载该竞态真实发生过），PTY 模板只留一次性缓存检查。触发：同一条 PTY 握手代码被并发执行两次（多 broker 重复投递穿透 dedup 窗口）。后果即其注释自认的"双流按 seq 拼出错位重影"+孤儿线程到 TTL。**修法方向**：照抄 socks5 的 `_sess_ready` Event 占坑。

**3. PTY 模板 TTL/心跳用墙钟 `_t.time()`，时钟步进误杀长会话或永不超时**
`client/remote_cmd.py:1109,1115-1117`（`_start=_t.time()`、`_now-_start>=_ttl2`），`_hb_loop` 调度（1208-1216）同样。socks5 已换 `_t.monotonic()`（276-279 注释逐条列了 NTP 步进/休眠唤醒的后果），PTY 漏改。触发：会话期间系统校时。后果：长会话被误杀（远端 shell 被 kill）或孤儿 PTY 永不超时。**修法方向**：与 socks5 相同换 monotonic（hb 载荷里的墙上时钟对齐字段保留 time()）。

**4. 部署链路 HTTPS `verify=False`：pythonanywhere 更新即 MITM 代码注入面**
`client/client_http.py:104-105,129-131`（所有 `_rpc_call`/`rpc_set_file` 均 `verify=False`）+ `client/pythonanywhere_update.py` 全链路复用（向 `https://<user>.pythonanywhere.com/` 上传源码并 SIGHUP 重载 master）。中间人可替换上传内容或伪造应答，把任意代码送进部署目标并热生效。与服务端明文口（首轮 S4）不同，这是客户端侧主动放弃 TLS 校验。**修法方向**：`verify=True`（pythonanywhere 证书正常，无理由关）。

**5. realtime_editor 是第 4 个零鉴权 0.0.0.0 RPC 面（:1133）且 `/ws` 任何人可写文件**
`realtime_editor.py:242-243,255-263`。与首轮 S4 同类但未登记的新端口实例：任意代码执行 + `save()` 可写任意内容进被编辑文件。**修法方向**：默认 127.0.0.1 + key。

### 低

**6.** `client/remote_cmd.py:1105-1198` — PTY `_out_loop` 只有正常路径收尾（pop router/弹注册表/发 end 都在 try 尾部），中途异常（如 `_poll.poll` 抛）只关 mfd：`_in_loop` 继续往已关 fd 写（EBADF 被吞），`_sessions[_sid]` 残留死会话缓存。对照 socks5 本轮的 try/finally 守卫，PTY 侧缺同款结构。
**7.** `client/socks5_client_mqtt.py:952` 与 `client/remote_cmd.py:1179` — teardown 无条件 `_cmq_pty_router.pop(_in_topic)`，不校验弹出的队列 `is _inq`：30s 接管竞态下影子会话自杀会误摘接管者刚注册的同 topic 队列，活会话上行黑洞。
**8.** `rpc_tcp.py:34-46,118-126` — 帧协议 `body_len` uint32 无上限、`buffer` 无界累积（客户端 `_read_loop` 同）：单对端内存 DoS；坏帧只记日志跳过，对端 `call()` 只能干等满超时，无错误回帧。
**9.** `client/client_http.py:25-29` — `_hsize` 进 KiB 分支后重复除 1024：`_hsize(2048)` 输出 "0.002 KiB"（应为 2.000 KiB）。仅影响 `rpc_set_file` 诊断行。同文件 `_fmt_size` 是正确实现。
**10.** `realtime_editor.py:158` — `write_text` 缺 `newline=''`（注释"低版本py不支持"不成立，Path.write_text 自 3.10 支持）：Windows 上 LF 文件保存一次整体变 CRLF，且内存 `self.content`（LF）与磁盘从此不一致，watch/快照对账错乱。
**11.** `realtime_editor.py:93 vs 182-199` — 客户端上报的 `revision` 被完全忽略：并发编辑后保存者整文件覆盖前者、广播乱序时旧 content 盖新 content，无冲突提示。
**12.** `server_http.py:71-79` — WebSocket 帧长 64 位无上限、分片列表无上限：慢速滴字节即可内存 DoS（/wslog 日志台、realtime_editor /ws 均走此实现）。
**13.** `multi_mqtt.py:1035-1036,1083-1090` — `clients`/`_host_port_map` 以 host 为唯一键：配置同 host 双条目（不同端口/凭据，如 broker_list_add 追加 emqx 的 8883）时后者顶掉前者，stats/恢复看门狗按 host 折叠且用错端口探测。**修法方向**：键改 `(host, port)`。
**14.** `server_mqtt.py:316` — CLI `--broker` 用 `rpartition(":")` 解析：IPv6 `[::1]:1883` 得带方括号 host 连不上；裸 `::1:1883` 更糟。**修法方向**：rsplit 一次后剥方括号，或要求 `[v6]:port` 格式。
**15.** `realtime_editor.py:163` — 每次 save 把全文 `repr(content)` 打进 INFO：逐击保存（input 事件即发 edit）→ 日志里出现文件完整副本且每键重复，敏感内容+体积爆炸。
**16.** `_run_bounded`（5639e2c 引入，**已由 b475d5a 修复**）— 作废判定的 TOCTOU。列出仅为完整性；终审确认已闭合。

### 边缘备注（与首轮已知项同类的新字面量/小子，未计入总数）

- `.trae/skills/apk-device-deploy/scripts/mqtt_put.py:26`：`-k` 默认 `"2**128"`（可预测默认私钥，同首轮"硬编码密钥"类）。
- `cmd_client_mqtt.py:805-808`：`%edit` 把 EDITOR 整串当单可执行名，`EDITOR="code -w"` 类带参写法失败。
- `cmd_client_mqtt.py:709-711`：`%replace` 的 NEW 以 `%` 结尾被误当"全部替换"标志吞掉末字符（已有文档化但无转义）。
- `tests/socks5/test_socks5_e2e.py` 抽查：测试自身质量高，未发现伪通过用例；仅 tearDown 不等 s5-in/hb/bulk 线程，理论上存在跨用例线程残留导致 t32 偶发误报的可能（未见实际判错证据）。

### 新发现的分布规律

本轮修复暴露出明显的"**修一处漏一处**"模式：PTY 模板没有同步 socks5 已修的占坑（二-2）、monotonic 时钟（二-3）、收尾守卫（二-6）三件套——三处都是 socks5 修过、PTY 同病未治。真正从未审过的角落里，realtime_editor.py 与 pythonanywhere 更新链路是两块质量洼地（二-4/5/10/11/15），问题密度显著高于主干。

---

## 三、本轮有意的行为变更清单（非回归，使用方需知）

1. **global_topic_list 发布 QoS 恒为 1**：自定义 `mqtt_global_topic_list_publish_kwargs` 中的 qos（缺省/0/2）一律被钉 1；其余键（retain 等）照旧透传。
2. **`PythonExecutor.execute` 默认 `timeout=999`**：超过 999s 的单次执行会得到明确超时错误而不再无限阻塞；`timeout=None` 保留旧同步语义。被执行代码内**嵌套**调用同一 executor.execute 不再依赖 RLock 同线程重入（会按作废/超时返回明确错误）。
3. **pty Ctrl-]/命令栏 detach 不再杀远端会话**：与 banner/帮助承诺一致；需要结束远端会话仍走远端 `exit`/Ctrl-D、判死退出或窗口关闭信号路径（后两者维持原 stop 行为）。
4. **`%watch df -h` 现在真的执行 `df -h`**（旧实现执行 `-h`）；`%grep --include` 缺参打印用法而不再崩掉 REPL。
5. **心跳帧新增 `fe` 字段**（socks5 服务端帧错误计数），纯增量，旧客户端忽略。
6. **multi_mqtt 构造新增 `timestamp_window=120.0`**：已验签消息 timestamp 偏差超窗即拒；时钟偏差 >120s 的客户端/服务端组合会被拒（可用参数调宽或置 0 关闭）。**拒收不静默**：网络层带 `_cmq_timestamp_reject` 标记继续投递，server_mqtt.handle_message 按标准回包格式向 reply_topic 发布明确错误（含 `frame_ts`/`server_now_ms` 与处置建议，回包 req_id 已剥签名后缀）——客户端立即拿到诊断，不再死等到超时。

---

## 四、遗留（按指示本轮不修）

首轮 S1（dedup 先于验签可投毒）、S3（DEFAULT_PUB_KEY 共享信任锚 / pub_key 空串不验签 / AES_KEY）、S4（HTTP RPC 零鉴权 0.0.0.0：1166/1177/1188）、S5（仓库内 runmqtt/flespi 凭据）、S6（socks5/pty 数据帧无认证无加密）维持原状；本报告第二部分 16 条新问题同样未修，建议下一轮按"中→低"顺序处理（二-2/3/6 三件套可合并为一次"PTY 模板对齐 socks5"的专项）。

## 五、回归与提交索引

- 测试：`python -m unittest discover -s tests`（266 项）与 `python -m unittest discover -s tests/socks5`（51 项）全绿；新增测试 47 项（replay 9 + verify 5 + qos 1 + executor timeout 9 + socks5 2 + cmd_client 13 + pty 3 + rpc_tcp 9，另更新 1 处旧契约断言）。
- 提交：e389797 → 8ed4f27 → 97507b0 → 5639e2c → 7ad472b → 3e5575e → 988e0c2 → b475d5a（→ 86bb00e 重放窗口拒收回包明确错误），均含完整原因与修改说明，未推送远端。

---

## 附录：2026-10-24 真机（docker "hi"，时钟慢 4 年）验证补遗

### 已验证的行为（符合设计）

1. **未签名 + 服务端无公钥**：窗口校验完全不参与（它只在"验签通过"分支内），执行正常——"k=None 访问无公钥服务器"属正常行为。
2. **签名 + 服务端有公钥 + 时钟偏差 ≈4 年**：请求被窗口明确拒收，客户端在 ~100ms 内收到标准格式错误回包（`rejected=timestamp_window`，error 含 `frame_ts=1792853529512 server_now_ms=1666623130570` 与处置建议）——86bb00e 的"不静默"行为在真实大幅偏差机器上验证通过。

### 新发现（中）

**17.〔已修 c416464〕`rpc_config()` 更换 broker/公钥时，本次请求自身的回包会丢失**：`apply_rpc_config` 在执行中途把 `self.mqtt_net` 换成新 manager，`handle_message` 随后的回包经**尚未连上任何 broker 的新 manager** 发布（`publish_broadcast` 只发给 `is_connected()` 的 client，冷启动瞬间 `published=0`）→ 配置实际已生效并已持久化，但调用方超时。实测复现：`rpc_config(pub_key=...)` 客户端超时、服务端配置生效。修复：handle_message 入口快照 manager，回包统一经 `_reply_publish` 发出——先经快照；`published==0` 且已换网时 `wait_connected` 后经新 manager 重发一次，仍 0 记 error；未换网 0 成功只记 error 不重发（顺带覆盖全 broker 瞬断丢回包）。仅改 apply_rpc_config 无法修复（回包路径不经过它）；"回退旧 manager"不可行（旧 manager 在 execute 返回前就 stop）。回归：ReplyPublishFallbackTests 4 例。

**18.（部分落地 f63bb12）运维风险（2026-10-25 修正：比初版结论轻）**：验签开启 + 时钟严重偏差时，**持私钥的客户端总可自救**——从窗口拒收的错误回包（或任何成功响应）读 `server_time`，用服务器时钟作为 timestamp 重新签名（签名覆盖 ts，可签任意时间），`|now-ts|≈0` 双检通过后执行 `rpc_config(pub_key=None)`。**已落地**：`rpc()/request()/request_bg_gather()` 新增 `timestamp_ms`（别名 `ts`/`server_ms`）注入参数，req_id 与 timestamp 一并由注入值派生，救援配方写进 rpc docstring；只注入 req_id 无效（窗口读的是 timestamp 字段）。真正不可自救的情形只剩：**私钥丢失**（验签在窗口之前失败，时间伪造无效，只能改 rpc.json/重建容器）——2026-10-24 hi 锁定事故即此例。遗留：① "缺少签名结构"的静默拒绝仍无回包诊断（与 timestamp 拒收不一致）；② 配公钥前先校时仍是最佳实践。
