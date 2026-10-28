这是一个以 Python 为主的 MQTT RPC 项目，辅助逻辑分散在根目录、`client/` 和 `.trae/skills/` 中，没有单独的 `utils/` 模块。

- 核心通信层：`multi_mqtt.py` 管理 MQTT Broker 连接、消息分发、去重、签名和连接统计。
- RPC 执行与服务端：`rpc_executor.py` 提供持久化 Python 执行器；`server_mqtt.py`、`server_http.py`、`server_http_wsgi.py` 和 `rpc_tcp.py` 分别提供 MQTT、HTTP、WSGI 和 TCP 服务。
- 客户端：`client/` 包含 MQTT REPL、远端命令/PTY、HTTP 文件同步和 SOCKS5 客户端。
- 其他：`U.py` 查询设备信息；`realtime_editor.py` 是实时协作编辑工具；`tests/` 放测试；`.trae/skills/` 放独立自动化脚本。

## 辅助工具函数清单

### 通信核心与通用基础

- `ensure_dependencies`：检查并安装运行依赖。
- `get_duplicated_kargs`：从多个别名参数中读取同一选项，处理冲突及是否移除原键。
- `stime`、`utc_ms`、`get_req_id`：时间戳格式化、毫秒时间和请求 ID。
- `process_cipher`：JSON 序列化及可选 AES-GCM 加解密。
- `TTLCache`：有 TTL 和容量上限的内存去重缓存。
- `_eval_safe_int_expression`：安全计算整数表达式，用于密钥解析。
- `get_standard_pem_bytes`、`_describe_public_key`、`get_standard_public_pem_bytes`：私钥/公钥格式标准化和公钥信息描述。
- `BrokerStat`、`ConnectionQualityStats`：Broker 状态和连接质量统计组件。

### RPC 执行与服务端

- `PythonExecutor`：持有共享命名空间、执行 RPC 代码并捕获输出；内部包含普通表达式求值和 `await` 执行逻辑。
- `_redirect_stdout`：临时重定向标准输出。
- `format_result`：把执行结果转成可传输的文本形式。
- `http_import`：从 URL 导入内容。
- `get_bmp_bytes`：生成 BMP 图片字节；`pretty_format` 格式化对象；`stime` 生成时间字符串。
- `ThreadedHTTPServer`、`WebSocket`：HTTP 并发服务器和轻量 WebSocket 支持组件。
- `_is_verify_enabled`：检查 MQTT 服务端是否已正确启用验签。
- `_wsgi_unescape`：还原 WSGI 环境中按 Latin-1 承载的原始路径字节。
- `TCPFrameProtocol.encode`、`TCPFrameProtocol.decode_frames`：TCP 长度前缀帧的编码和解码。

### 客户端通用、远端命令与文件传输

- `client_mqtt._cli_opts`：根据别名生成 CLI 选项。
- `client_mqtt._get_clipboard_text`、`client_mqtt._normalize_history_path`：读取剪贴板、规范化历史文件路径。
- `client_mqtt._build_prompt`、`client_mqtt._make_print`：构造交互式输入和兼容 prompt_toolkit 的打印函数。
- `client_mqtt._handle_magic`：处理 REPL 的 `%topic`、`%key`、`%status` 等命令；`client_mqtt._fallback_code_input`：无 prompt_toolkit 时的输入回退。
- `build_code`、`build_pty_start_code`：构造远端执行及 PTY 启动代码。
- `bytes_to_wire`、`wire_to_bytes`、`_wire_len`、`_iter_wire_chunks`：字节/报文转换、估算传输长度和按预算分块。
- `_is_within`、`_validate_tar_member`、`safe_extract_tar`：校验路径边界并安全解压 tar。
- `_parse_pty_responders`：解析 PTY 响应方信息；`_PtyReorderBuffer` 用于 PTY 数据重排。
- `client_http._req_repr`、`client_http._fmt_size`：生成请求调试表示和格式化文件大小。
- `client_http._rpc_call`、`client_http._server_code`、`client_http._rpc_json`：HTTP RPC 请求、远端代码包装和 JSON 响应解析。
- `client_http._rpc_list_dir`、`client_http._rpc_upload_chunk`、`client_http._rpc_delete_file`、`client_http._rpc_sha256_remote`、`client_http._upload_one_file`：远端目录枚举、分块上传、删除、校验和单文件上传。

### 终端、PTY 与 SOCKS5 工具

- `cmd_client_mqtt._enable_win_vt`、`cmd_client_mqtt._fmt_stats`：启用 Windows VT 输出、格式化连接统计。
- `cmd_client_mqtt._parse_getdir_args`、`cmd_client_mqtt._do_pull_dir`：解析并执行目录拉取。
- `cmd_client_mqtt._parse_tmux_args`、`cmd_client_mqtt._parse_watch`、`cmd_client_mqtt._build_shell_prompt`：解析终端命令参数和构造 shell 提示符。
- `pty_client_mqtt._enable_output_vt`、`pty_client_mqtt._new_input_transcoder`、`pty_client_mqtt._make_raw_console`：终端模式、输入编码和原始控制台创建。
- `pty_client_mqtt._normalize_paste_text`、`pty_client_mqtt._read_clipboard_text_win`、`pty_client_mqtt._bin_stdout`、`pty_client_mqtt._bin_stderr`：粘贴文本、剪贴板和二进制标准流处理。
- `pty_client_mqtt._terminal_cleanup`、`pty_client_mqtt.install_console_guards`：终端恢复和控制台信号保护。
- `pty_client_mqtt._parse_key_spec`、`pty_client_mqtt._build_key_display_map`、`pty_client_mqtt._describe_key`：按键序列解析及显示。
- `pty_client_mqtt._stderr_writer`、`pty_client_mqtt._stderr_write`、`pty_client_mqtt._emit_local`、`pty_client_mqtt.get_log`、`pty_client_mqtt.clear_log`：终端诊断输出和内存日志读写；相关组件有 `_LogRing`、`_RingTextIO`、`_RingLogHandler`、`LogHub`。
- `pty_client_mqtt._lan_ip`、`pty_client_mqtt.log_html`、`pty_client_mqtt._install_ring_logging`、`pty_client_mqtt._install_log_capture`：本机 IP、日志网页及日志捕获初始化。
- `pty_client_mqtt._writer_state`、`pty_client_mqtt.writer_diag`、`pty_client_mqtt.reset_out_writer`：输出 writer 状态诊断和重置。
- `pty_client_mqtt._arm_esc_guard`、`pty_client_mqtt._filter_injected_esc`、`pty_client_mqtt._cancel_console_selection`、`pty_client_mqtt._strip_ansi`：ESC 输入防护、控制台选择处理和 ANSI 清理。
- `pty_client_mqtt.local_rpc`、`pty_client_mqtt._call_ai_bridge`、`pty_client_mqtt.ai_pty_run`、`pty_client_mqtt.ai_pty_send`、`pty_client_mqtt.ai_pty_status`：AI bridge 本地 RPC/PTY 调用。
- `socks5_client_mqtt._recv_exact`、`socks5_client_mqtt._socks_reply`：SOCKS5 TCP 精确读取和协议响应封装；`build_socks5_start_code` 生成远端启动代码。

### 设备信息与编辑器

- `_cmd`、`_file`、`_prop`：执行系统命令、读取文件和 Android 属性。
- `_is_android`、`_safe_getter`：平台检测和安全字符串返回包装器。
- `get_manufacturer`、`get_model`、`get_system_id`、`get_id`：跨 Android、Windows、macOS、Linux 获取设备厂商、型号和系统 ID。
- `Editor._signature`、`Editor._message`、`Editor.broadcast`：文件签名、消息封装和 WebSocket 客户端广播。`Editor` 还负责文件保存、外部变更监视与客户端同步。

### `.trae/skills/` 下的独立脚本工具

这些脚本属于技能/自动化工具，不是 MQTT RPC 核心模块：

- `pull_apk.py`：`human`、`human_speed` 格式化大小/速度；`build_rpc_url` 生成 RPC URL。
- `remote_build.py`：`run_serial` 运行带超时和等待逻辑的命令。
- `relay_pull.py`：`human`、`normalize_raw_url`、`dial_detail`、`head_info`、`download_once`，负责 URL 规范化、连接探测和续传下载。
- `relay_repo.py`：`token_file`、`get_token`、`api`，处理 token 读取和 GitHub API 请求。
- `relay_push.py`：`run` 包装命令运行。

## 简要结论

复用性最高的工具集中在 `multi_mqtt.py`、`rpc_executor.py` 和 `client/remote_cmd.py`。终端与日志工具主要聚集在 `client/pty_client_mqtt.py`，HTTP 文件传输工具集中在 `client/client_http.py`。

目前工具函数与各模块的交互流程混在一起，尚未抽成独立工具层；例如时间格式化分别出现在核心 MQTT 模块和 HTTP 服务模块。测试文件中的测试辅助函数未列入上面的运行时代码清单。


这句话只是个人笔记，直接忽略：
  #我希望每个文件都只保持 这个文件名所描述的功能。  和本文件无关的共用工具函数  都提取到 U.py . 保持整个项目所有功能 不变。你先运行测试。然后提取工具函数。所有工具函数都在函数内 import。  不要公共模块级别import。保持独立性。紧凑代码，不要多余空行，必要注释写代码后面，不要另外一行。  完成后再次 测试 保持功能完全一致









# MQTT RPC 请求/响应流程与安全边界
+
+## 1. 真实的消息流
+
+### 1.1 客户端发送请求
+
+客户端在发起 `code` 请求时，如果自己配置了私钥（例如 `--key ...`），会在 `MultiMQTTManager.publish_broadcast()` 中为 `req_id` 追加签名：
+
+- 原始 `req_id`：`20260911_173656.404 8d795f`
+- 签名后：`20260911_173656.404 8d795f|6a0520...`
+
+签名内容是：
+
+- `base_req_id`
+- `code`
+- `timestamp`
+
+也就是：
+
+`sign_msg = f"{base_req_id}|{code}|{timestamp}"`
+
+这一步是“客户端发起命令”时的签名，不是服务端返回结果的签名。
+
+### 1.2 服务端接收请求
+
+服务端收到请求时，`MultiMQTTManager._make_on_message()` 会先检查是否存在 `code` 字段；如果服务端已配置了 `server_public_key_bytes`，那么才会执行验签。
+
+关键点：
+
+- `server_public_key_bytes` 是服务端自己的公钥
+- `client_private_key_bytes` 是客户端自己的私钥
+- 两者是不同侧的密钥材料
+
+因此：
+
+- 客户端启动时是否带私钥，决定它是否签名发请求
+- 服务端启动时是否带公钥，决定它是否验签收到的请求
+
+`server_mqtt.py --pub ...` 只是“服务端启用验签功能”的启动参数，不是客户端默认自动注入的公钥。
+
+### 1.3 服务端返回结果
+
+服务端处理 `code` 后，返回的是普通 JSON：
+
+```python
+{
+    "req_id": "...",
+    "r": ...,
+    "stdout": ...,
+    "ok": True,
+    "server_time": ...,
+    "server_from": ...,
+}
+```
+
+这里没有 `code` 字段，因此它不是“新的命令请求”，而是“对原请求的执行结果”。
+
+也就是说，服务端回包不会再次走“签名发起请求”的逻辑，它只是普通广播回包。
+
+### 1.4 客户端接收回包
+
+客户端收到回包之后，会拿到 `req_id` 去匹配当前 pending request。
+
+如果客户端启用了私钥签名，而当前没有已知服务端公钥，并且没有显式允许 `allow_no_server_pubkey_response=True`，那么它应该拒绝回包，避免“无公钥/无信任锚点的服务端”伪装成可信响应。
+
+这条规则在客户端代码中必须做，且默认值应该是拒绝，而不是“静默放行”。
+
+壳层逻辑是：
+
+```python
+signed_req = bool(client_private_key_bytes)
+no_server_pubkey = not bool(self.mqtt_net.server_public_key_bytes)
+
+if signed_req and no_server_pubkey and not allow_no_server_pubkey_response:
+    # 拦截无公钥服务器返回
+    return
+```
+
+这不是“客户端搞公钥”，而是“在私钥模式下，对未知服务器的返回做安全门槛”。
+
+## 2. 这次错误的根源
+
+问题不是“客户端不该碰公钥”，而是“之前把默认公钥注入到了客户端，导致流程被绕乱了”。
+
+这会导致：
+
+- 一个正常的默认 server 被当成“带公钥的可信服务器”
+- 结果绕过了本来应该出现的拒绝门槛
+- 违背了“在私钥模式下，如果不显式允许，就不接受无公钥服务器返回”的设计
+
+因此，正确修法是：
+
+- 不要写死默认 server 公钥
+- 让用户显式决定是否允许无公钥回包
+- 当私钥模式 + 无已知服务端公钥 + 未允许时，直接丢弃回包
+
+## 3. 代码中真正的分工
+
+- `client/client_mqtt.py`：
+  - 发送签名请求
+  - 接收响应
+  - 在私钥模式下对“无公钥服务器回包”应用安全开关
+
+- `server_mqtt.py`：
+  - 只负责本机作为 server
+  - 通过 `--pub` 启用/配置自己的公钥
+  - 处理请求并返回结果
+
+- `multi_mqtt.py`：
+  - 真正的 MQTT 网络层
+  - 处理签名封装、验签、去重和广播
+
+## 4. 一句话总结
+
+客户端签名发出 `code`，服务端验签执行；服务端返回普通结果；客户端在私钥模式下只在“无已知公钥 + 未显式允许”时拒收回包。
+
+这才是符合当前协议设计的主线流程。
+
+
+如果两个服务端 ./server_mqtt.py  同时运行，  client 只会显示最先到达的那个
+
+
+降低 CPU 与耗电 (Low-Resource & Power Efficiency):

去磁盘 IO：丢弃了频繁写 SQLite 的操作，换用内存 TTLCache 实现 O(1) 时间复杂度的消息查重，显著降低 CPU 负载与磁盘读写耗电。

事件驱动唤醒：客户端使用 threading.Event() 实现零 CPU 占用率的等待机制，只要有一个 Broker 返回消息就立刻唤醒主线程。

多路抗丢包与首胜机制 (Racing Protocol):

客户端发送时：一条请求并发投递给 5 个不同的 MQTT Broker。

服务端收到后：仅处理最先到达的那一条，后续到达的相同 msg_id 被 TTLCache 直接抛弃。

服务端响应时：同样广播至所有 Broker；客户端仅接收最先到达的 Response 并解除阻塞。




mqtt_server.py 收到 code
调用 PythonExecutor.execute(code)
ast.parse() 解析代码
发现最后一条语句是表达式 3
把前面的语句执行完
对最后表达式执行 eval
eval("3") 返回整数 3
服务端 format_result(3) 转成字符串 "3"
返回：


r=3这不是最后表达式，而是赋值语句：
进入普通 exec
执行后 locals 中出现 r: 3
执行器读取 locals["r"]
返回结果 3
server 格式化后返回 "r": "3"

所以 MQTT 支持两种结果来源


#TODO
加入非对称加密与解密支持。