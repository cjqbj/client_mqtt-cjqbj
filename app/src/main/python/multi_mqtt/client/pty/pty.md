# PTY 提示符精简
长提示符 `root@r-huggingface1q-...:~#` 来自远端登录 shell 的 PS1，与客户端无关；本客户端不提供参数去除，用 SSH 式前置命令（位置参数，连上自动敲一行）或直接改 PS1。
## 外层 shell
- 临时生效（当前会话）：`PS1=#`
- 连接时自动生效：
```sh
python client/pty_client_mqtt.py -k <key> -t q PS1=#
python client/pty_client_mqtt.py -k <key> -t q "PS1='# '"
python client/pty_client_mqtt.py -k <key> -t q "PS1='\w # '"
```
- 永久（新开 shell 生效）：`echo PS1=# >> ~/.bashrc`
- 注意：`--no-login` 无效，交互 bash 仍读 `.bashrc` 设置 PS1。
## tmux
tmux pane 的 shell 由 tmux server 启动，外层前置命令改不到它。
- attach 后手动敲 `PS1=#`（最安全）
- 一键：仅当 pane 停在 shell 提示符时可用，pane 里开着 vim/top 会被注入字符
```sh
python client/pty_client_mqtt.py -k <key> -t q "tmux send-keys PS1=# Enter; tmux at"
```
- 永久：`echo PS1=# >> ~/.bashrc`，之后新 window/pane 自动为 `#`；已有 pane 不重读，空闲 pane 可批量刷新
```sh
tmux list-panes -a -F '#S:#I.#P' | xargs -I P tmux send-keys -t P 'PS1=#' Enter
```
## 多层嵌套（tmux 内再 ssh）
PS1 是每台机器各自 shell 的属性，客户端只透传按键，无法穿透；每跳一台机器都要重做一次。
- 临时：逐层敲 `PS1=#`
- 永久：每台机器执行一次 `echo PS1=# >> ~/.bashrc`






# 多 AI 共用同一个 tmux：现状审核与改造规划
审核对象 `client/pty`（分片 `parts/` 与生成的 `client/pty_client_mqtt.py` 一致）；基线 `main` 3be14f1。本次只审核与规划，未改代码。
代号约定：`090/097/120/110/010/098` 指 `client/pty/parts/` 下的同名分片；`remote_cmd` 指 `client/remote_cmd.py`；`multi_mqtt`/`server_mqtt` 指仓库根目录同名文件。
## 审核基线
- 工作区不干净：`server_mqtt.py`、`tests/test_server_mqtt_config.py` 有未提交改动（certifi SSL 上下文、`_publish_global_topic_list` 默认 `confirm_timeout=2.0`、周期 tick 传 0），与 pty 无关，本次未触碰。
- 分片与单文件一致：`python client/pty/generate_single_file.py --check` → `[OK] 逐字节一致（230685 bytes, sha256=5a8dbe15…）`；改 pty 必须改 `parts/` 后 `-f` 重组。
- 测试基线（`C:\QGB\anaconda3\python.exe -m pytest tests/test_pty_mqtt.py -q`）：135 passed / 2 failed / 1 skipped；两个失败均为环境问题（pytest 替换 `sys.stdout` 的断言、`/wslog` e2e 超时），与 pty 逻辑无关。
## 结论
| 形态 | 现在能否 | 结论 |
| --- | --- | --- |
| 两个独立 pty 客户端进程，各自挂同一 tmux 的不同窗口 | 能，但必须手工指定 pane | 可临时用，需补 P0 隔离 |
| 一个 pty 客户端、两个 AI 共用同一 MCP/RPC 口 | 不能 | 单 busy 锁 + 单实时缓冲 + 单 active_pane + 进程级默认窗口，必须按 pane 分槽 |
“不同窗口”本身不难：投递 `send-keys -t <pane>`、收流 `pipe-pane -t <pane>`、每次 run 独立 sid 与临时文件，都是 pane 级隔离；难在**窗口怎么分、进程内状态怎么分、远端共享资源怎么防撞**。
## 数据通路
```
AI ──HTTP/MCP──> pty_client_mqtt（单例 AIBridge）
   run(cmd, pane) → busy 锁 → 选 pane → RPC tmux_stream(sid, pane, line, path, topic, end)
远端 server_mqtt（单进程 / 单分发线程）
   pipe-pane -t <pane> "cat >> /tmp/.pty_ai_<tag>.log"
   send-keys -t <pane> -l -- <line> + 单独 Enter
   _tmux_worker 增量读文件 → publish_broadcast(topic, {stream: sid, seq, d})
客户端订阅 topic → _on_push_frame 按 sid 分发 → _PtyReorderBuffer 重排 → _pack_result
```
每进程一份的状态：`_busy`（090:220、390–392）、`_live`（090:244、402–404、425–451）、`_active_pane`（090:242、344–351）、`_pane_id`（090:227、740–803）、`_push_topic`（090:238、568–582）、`_push_runs`（090:239、584–597）。
远端共享资源：`_cmq_tmux` 按 sid 去重（remote_cmd:748–760）、每条流结束无条件关该 pane 管道（remote_cmd:409–412）、`tmux_stream` 同步段在单分发线程里起 3 个 tmux 子进程（remote_cmd:772–788、multi_mqtt:1418–1472、server_mqtt:750）。
## 阻断点（按严重度）
### P0-1 默认落到同一个 pane
`_resolve_pane()` 固定返回“最小 window_index 的 active pane”（090:753–803）。两个客户端都不传 `--ai-pane` 时落在同一 pane，两条命令插进同一行缓冲，BEGIN/END 交错，`_pack_result`（090:841–858）会取到别人的 END，rc/out 全错。
### P0-2 一个 pane 只能有一条 pipe-pane
`_pipe_start` 用 `tmux pipe-pane -t pane "cat >> path"`（090:905、remote_cmd:772–777），不带 `-o` 时新管道会关掉旧管道；两端收尾又都会 `pipe-pane -t pane`（090:908–910、remote_cmd:409–412）。同 pane 并发 → 先起的流丢输出，且先结束的一方关掉另一方的管道；不同 pane 无此问题。
### P0-3 sid / tag / 临时文件撞车
`tag` 是客户端本地时间 + 毫秒（090:397–401），`sid="ai-<tag>"`（090:610），文件 `/tmp/.pty_ai_<tag>.log`（090:611）。两个 AI 同一毫秒开跑 → 远端 `_cmq_tmux` 报 `sid already active`（remote_cmd:757–760）；客户端只对 `unknown op` 回落（090:673–677），其他错误直接 `ok=False`，命令根本不会执行。
### P0-4 单进程 busy 锁
090:390–392 拿不到立即 `{"busy": True}`；MCP `pty_run` 直通（097:403–452），文档明确“同一 shell 串行执行”（097:349）。A 跑长任务时 B 完全无法执行。
### P0-5 实时缓冲 / Ctrl-C / 状态都是单份
`tail()` 只有一份 `_live`（090:436–451），MCP `pty_tail` 无 pane 参数（097:467–476）；`send("\x03")` 打 `_active_pane`（090:344–351），B 的 Ctrl-C 会杀 A 的命令；`status()` 只报一个 busy、一个 pane（090:306–334）；无活跃 pane 时 `send()` 写本 PTY shell（090:350–351），多 AI 场景会敲进人正在看的 shell。
### P0-6 本地端口冲突
`--port` 默认 1188（120:48–54），同机第二个客户端必须换口，否则起 RPC 口直接抛异常。
### P1-1 `--ai-pane` 不解析、可能漂移
`configure()`/`set_pane()` 只存字符串（090:268–279、957–975），传 `0:2` 或窗口名时，`renumber-windows`/`move-window`/改名后会指错 pane；应只收 `%N`，或首次使用时解析成 `%N` 再固化。帮助文案“百分号前缀，如 2”（120:73–75）自相矛盾，应为 `%2`。
### P1-2 tmux 多客户端共享视图与尺寸
两个 pty 客户端 attach 同一 session 时，current window 与 `window-size` 是 session 级的；`resize_watch` 每 0.25s 发 winsz（110:295–320），不同尺寸终端会来回触发布局变更，全屏 TUI 重绘抖动（命令本身不受影响）。缓解：`set -g window-size largest`，或 `manual` + 固定 `resize-window`；或让两个 AI 用同一 tmux server 的不同 session。前置命令别用 `tmux attach -d`，会把另一个客户端踢下线。
### P1-3 远端单分发线程
`multi_mqtt:1418–1472` 串行消费，`server_mqtt:750` 在回调里直接 exec。两个客户端的 `tmux_stream` 启动、轮询通道的 `tail -c`、`fs_*`（单次最大 5MB，098:29–30）共用一条队列；推送通道只占一次启动 RPC，影响小，轮询回落与大文件读写会互相拖慢。历史提交 `46d9d3e` 有“第二个 pty 客户端并发握手 RemoteTimeout”的实证。
### P1-4 `_push_topic` 创建无锁
`_push_ready()`（090:568–582）无锁；按 pane 分槽并发后两个 run 可能各建一个 topic。分发按 sid 走，不会串结果，但多一份订阅与带宽浪费。
### P1-5 编码按外层 shell 判断
`set_shell()` 取的是 pty 登录 shell（110:67–69），pane 的 shell 由 tmux `default-shell` 决定，两者可能不同；`$'...'` 在 dash 里会静默变字面量（090:144–147）。两个窗口 shell 不同时，多行命令有静默跑错的风险。
### P2 缺少窗口分配与租约
没有 `new-window` 分配、没有“谁占了哪个 pane”的登记、没有 `#{pane_pipe}` 校验，多 AI 只能人肉分配，窗口与管道都可能残留。
## 两种部署形态对比
| 项 | 一个客户端 + 按 pane 分槽 | 两个客户端各挂各的 |
| --- | --- | --- |
| 进程内状态隔离 | 需改造后成立 | 天然成立 |
| 远端握手/RPC | 1 份 PTY、1 份连接 | 2 份，启动需错峰，共享单分发线程 |
| 本地端口 | 1 个 | 必须两个（如 1188/1189） |
| 远端共享资源 | 同进程注册表，好治理 | 仍需 claim 与 pipe 互斥 |
| 适用 | 两个 AI 同机、同一 MCP 入口 | 两个 AI 两地/两套工具，各连各的 |
## 改造规划
### P0 唯一标识与同 pane 互斥（两种形态都要）
- 保留可读 `stamp` 只用于人看，另加 `rid=os.urandom(3).hex()`：`begin/end="__AI_BEGIN_<stamp>_<rid>__"`、`sid="ai-<stamp>-<rid>"`、`path="/tmp/.pty_ai_<stamp>_<rid>.log"`。
- 远端 `tmux_stream` 启动前查 `#{pane_pipe}`（tmux ≥2.6），已占用即拒绝；在 `_cmq_tmux` 维护 `pane -> sid`，同 pane 第二条流拒绝；收尾只在 `pane -> sid` 仍属自己时关管道。
- 客户端把“同 pane 已被占用”变成可读错误，且不要误判成可回落。
### P1 进程内按 pane 分槽（推荐形态）
- `AIBridge._slots: dict[pane|"pty"] -> {busy, live, active, enc, shell, sid, created}`，配 `_slots_lock`；`run()` 先解析 target 再取该槽的锁，不同 pane 完全并发，同 pane 仍串行。
- `_live_*`/`tail(pane=None)` 按槽隔离；不传 pane 返回默认槽，保持旧调用兼容。
- `send(data, pane=None)`：槽在跑则 `send-keys -t <槽 pane>`，否则沿用写本 PTY；多租户下要求显式 pane。
- `status()` 保留旧字段，新增 `slots:[{pane,busy,cmd,elapsed,bytes,channel}]`。
- `_push_ready()` 加锁；`_push_runs` 已按 sid 分发，结构不变。
### P2 窗口认领与体验
- 远端新增 `tmux_pane_claim(owner, session?, name?, create, ttl)`：按 name 找窗口，没有则 `tmux new-window -d -t <session> -n <name> -P -F '#{pane_id}'`；登记 `{pane:{owner,ts,ttl}}`，被其他活 owner 持有则拒绝；配套 `tmux_pane_release` / `tmux_pane_list`，注册表与锁放 `gms` 全局。
- MCP：`pty_tail`/`pty_send`/`pty_status` 增加 `pane`；新增 `pty_panes()` 列出本进程槽；`pty_pane(pane="new:<name>")` 暴露认领。
- CLI：`--ai-pane` 增加 `new:<name>`/`claim:<name>`，`auto` 保持旧行为；`set_pane` 首次解析成 `%N`。
- 每 pane 探测 shell：用 claim 返回的 `pane_pid` 读 `/proc/<pid>/comm` 或 `ps -o comm= -p`，缓存到槽，`_pick_encoding` 用槽的 shell；探测不到走 base64。
## 接口与文件清单
| 文件 | 改动 |
| --- | --- |
| `client/pty/parts/090_aibridge.py` | 分槽状态、唯一 tag/sid/path、按槽 live/tail/send/status、`_push_ready` 加锁、pane shell 缓存 |
| `client/pty/parts/097_mcp_server.py` | `pty_tail`/`pty_send`/`pty_status` 加 pane，新增 `pty_panes`，更新 instructions |
| `client/pty/parts/120_cli.py` | `--ai-pane` 新增 new/claim 语义，修正 `%2` 文案 |
| `client/pty/parts/010_alias.py` | 新参数别名 |
| `client/pty/parts/110_session.py` | 把会话 sid 注入桥，作为 claim owner |
| `client/remote_cmd.py` | 远端模板新增 `tmux_pane_claim/release/list`、`tmux_stream` 同 pane 互斥与 `#{pane_pipe}` 校验、收尾只关自己的管道 |
| `tests/test_pty_mqtt.py` | 分槽并发/隔离、唯一 id、同 pane 拒绝、claim 复用、回落语义 |
| `client/pty_client_mqtt.py` | 生成物，`-f` 重组，不手改 |
| `client/pty/README.md`、`parts/000_head.py` | 多租户说明 |
## 测试与验证
- 单测：两个 pane 并发 run 都非 busy 且输出互不含对方 marker；`tail(pane=A)`/`tail(pane=B)` 分离；`send("\x03", pane=A)` 只打断 A；mock 时间验证同毫秒 sid/tag/path 唯一；同 pane 第二条流被拒；claim 复用同名窗口、释放后可再认领；`unknown op` 仍回落、其他错误仍不重复投递。
- 重组校验：`python client/pty/generate_single_file.py -f` 后 `--check` 字节一致。
- 真机：A 跑 `sleep 40; echo A`（`wait=0` 后轮询 `pty_tail(pane=A)`），同时 B 跑 `echo B` 应立即返回；`tmux_stream_list` 两条 sid、两个 pane；收尾后 `tmux list-panes -F '#{pane_id} #{pane_pipe}'` 为 0；`/tmp/.pty_ai_*` 已清理；人切两个窗口分别看到各自输出。
## 临时可用方案（不改代码）
远端先建两个专用窗口并取 pane_id：
```sh
tmux new-window -d -n aiA ; tmux new-window -d -n aiB
tmux list-panes -a -F '#{session_name}:#{window_name} #{pane_id}'
```
本机两个终端，端口与 pane 都必须分开，第二台错峰几秒启动：
```powershell
python client/pty_client_mqtt.py -k KEY -t q --port 1188 --ai-pane %3 "tmux attach -t work"
python client/pty_client_mqtt.py -k KEY -t q --port 1189 --ai-pane %5 "tmux attach -t work"
```
仍存在 P0-2/P0-3/P1-2 的残余风险，只建议临时用。
## 风险与待确认
- “同一个 tmux”是同一 session 的不同 window（视图/尺寸共享），还是允许同一 tmux server 的不同 session（隔离更彻底）？
- 两个 AI 是同机同一 MCP 入口（走 P1 分槽），还是各自一条 pty 客户端（走 P0 + claim）？
- tmux 版本需支持 `#{pane_pipe}`（≥2.6）；更老版本只能用 `pipe-pane -o` + 注册表近似。
- MCP 是 stateless（提交 `0ea8442`），服务端不区分调用者：两个 AI 必须每次显式带 pane，或先 claim 再记住返回值；`pty_pane` 改的是进程级默认值，不能当多租户默认。若要让 AI 完全无感，可考虑挂两个 MCP 路径（`/mcp/a`、`/mcp/b`）各自绑定槽。
