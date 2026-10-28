# ================= 统一别名表（小写变量名） =================
# 与 client_mqtt.py 同一套规矩：语义别名不带 "--"/"%" 前缀，三处共用一份：
#   1) CLI 参数   -> _cm._cli_opts(*alias_xxx) 自动生成 --x/-x/x_x/x-x
#   2) 本地魔术栏 -> cmd in alias_xxx（输入里的 - 先归一化成 _）
#   3) 其他引用   -> 直接 import 本模块的 alias_xxx
# 连接类（request_topic/reply_topic/private_key/allow/timeout）直接复用
# client_mqtt 的表，由 cmd_client_mqtt.add_connection_args 注册，这里不重复；
# status/help/exit 也直接引用 client_mqtt 的别名，禁止在 PTY 侧另立名字。

# ---- PTY 会话参数（CLI 选项 + 可被魔术栏热调/引用） ----
alias_shell        = ('shell', 'sh')
alias_term         = ('term', 'terminal')
alias_cwd          = ('cwd', 'dir', 'workdir')
alias_interval     = ('interval', 'i', 'flush_interval', 'flush')
alias_heartbeat    = ('heartbeat', 'hb')
alias_ttl          = ('max_shell_live_time','ttl','DEFAULT_PTY_TTL')
alias_dead_timeout = ('dead_timeout', 'deadtime', 'dead')
alias_size         = ('size', 'geometry')
alias_no_login     = ('no_login', 'nologin')
alias_detach_key   = ('detach_key', 'detachkey')
alias_menu_key     = ('menu_key', 'menukey', 'magic_key')
alias_rpc_port     = ('port', 'rpc_port', 'p')
alias_rpc_host     = ('host', 'rpc_host')
# MCP 工具服务（pip install "mcp[cli]" 后默认挂在 --port 的 RPC 口上）
alias_mcp_port     = ('mcp_port', 'mcp')
alias_mcp_transport= ('mcp_transport', 'mcptrans')
alias_no_mcp       = ('no_mcp', 'nomcp')
# MCP 文件工具的全局只读开关（默认关闭，写家族作用于任意路径）
alias_fs_read_only = ('fs_read_only', 'fsro')
# AI 目标窗口：默认「session 内 window_index 最小的窗口」，可 CLI/RPC 指定
alias_ai_pane     = ('ai_pane', 'aipane', 'aip')
# AI 命令编码：raw=窗口里直接看到命令原文（默认）；base64=旧严格编码兜底
alias_ai_encoding = ('ai_encoding', 'aienc')
alias_command      = ('command', 'cmd')
# 本地命令栏查看诊断日志（只读，不碰会话参数）
alias_log          = ('log', 'logs')
# 整屏重绘 / 写线程卡滞自愈（不会把命令发给远端 shell，只发控制帧）
alias_redraw       = ('redraw', 'refresh', 'rd')

# 魔术栏的"脱离"：语义等同退出，exit/quit 直接复用 client_mqtt 的别名表，
# PTY 语境再补 detach 系列（顺序无所谓，匹配一律用 in）。
alias_detach = tuple(dict.fromkeys(
    ('detach', 'd', 'bye', 'q', 'x') + tuple(_cm.alias_exit)))


