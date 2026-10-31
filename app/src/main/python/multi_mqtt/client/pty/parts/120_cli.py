# ============================ CLI ============================

def _pre_command_index(parser, argv):
    """SSH 式前置命令的起始下标：跳过选项及其值后的第一个非选项 token。

    返回 None = 整行都是选项（没有前置命令）；返回下标 i 表示 argv[i] 起
    都该按位置参数处理。``--`` 是显式的命令分隔符，返回它后面的下标。
    选项值识别规则：``--opt=value`` 自带值；``store_true`` 等 nargs=0 的
    动作不带走下一个 token；其余动作（nargs=None/int>0）认为值在下一个
    token。认不出的选项（未知名或歧义缩写）按“不取值”处理，把下一个非
    选项 token 当命令起点，让 argparse 自己报选项错误——扫描阶段绝不替
    argparse 猜测，更不吞掉用户真实的命令。
    """
    i, n = 0, len(argv)
    while i < n:
        tok = argv[i]
        if tok == "--":
            return i + 1
        if not tok or tok == "-" or not tok.startswith("-"):
            return i                       # 第一个位置参数 = 命令起点
        if tok[1:2].isdigit():
            return i                       # 负数字面量，argparse 同样当位置参数
        i += 1
        takes_value = _option_takes_value(parser, tok)
        if takes_value:
            i += 1                         # 跳过该选项的值（可能不存在，越界即到头）
    return None


def _option_takes_value(parser, token) -> bool:
    """该选项是否把下一个 token 当自己的值（只读 argparse 已注册的动作表）。"""
    action = parser._option_string_actions.get(token)
    explicit_arg = None
    if action is None:  # 缩写/歧义：用 argparse 自己的前缀匹配结果判定
        try:
            tuples = parser._get_option_tuples(token)
        except Exception:
            return False
        if len(tuples) != 1:
            return False
        action, _opt, explicit_arg = tuples[0]
    if "=" in token or explicit_arg is not None:
        return False                       # --opt=value 已内联，不再吃下一个
    nargs = action.nargs
    if nargs in (None, "?", "+"):
        return True
    return isinstance(nargs, int) and nargs > 0


class _PreCommandParser(argparse.ArgumentParser):
    """自动为 SSH 式前置命令插入 ``--`` 的 ArgumentParser。

    本客户端的选项别名里有单横线长名（-dir/-dead_timeout/-detach_key），
    argparse 会把命令自身的 ``-d``（如 ``tmux attach -d``）当成这些长名的
    缩写并报 ``ambiguous option: -d``（allow_abbrev=False 只关双横线长名的
    缩写，管不住单横线长名）。在命令起点前插入 ``--`` 后，选项解析到此为止，
    其后每个 token（含 -d/-t 和它们的值）都按位置参数交给 nargs=REMAINDER，
    与 ssh 的“命令放最后”语义一致；用户在命令前主动写的 ``--`` 不会被重复插入。
    """

    def parse_known_args(self, args=None, namespace=None):
        argv = list(sys.argv[1:] if args is None else args)
        idx = _pre_command_index(self, argv)
        if idx is not None and (idx == 0 or argv[idx - 1] != "--"):
            argv.insert(idx, "--")
        return super().parse_known_args(argv, namespace)


def build_parser() -> argparse.ArgumentParser:
    # 选项名一律由文件前面的 alias_xxx 别名表 + client_mqtt._cli_opts 生成，
    # 与本地魔术栏命令、rpc(**ka) 别名三处永远一致；禁止再硬编码 "--xxx"。
    # dest 保持历史字段名（args.interval/args.port 等），调用方无需改动。
    p = _PreCommandParser(
        prog="pty_client_mqtt",
        description="PTY over MQTT：SSH 式远程交互终端（常驻 shell，逐键过 broker）")
    # 连接参数（topic/key/allow/timeout）：直接复用 client_mqtt 别名表
    add_connection_args(p, default_timeout=30.0)
    # PTY 会话参数
    p.add_argument(*_cm._cli_opts(*alias_shell), dest="shell", default="",
                   help="远端 shell，默认服务端用户登录 shell（$SHELL/passwd）")
    p.add_argument(*_cm._cli_opts(*alias_term), dest="term", default="",
                   help="TERM，默认本地 $TERM 或 xterm-256color")
    p.add_argument(*_cm._cli_opts(*alias_cwd), dest="cwd", default=None,
                   help="启动目录，默认远端 HOME")
    p.add_argument(*_cm._cli_opts(*alias_interval), dest="interval",
                   type=float, default=0.0,
                   help="服务端主动推送最小间隔秒：0=实时（默认），>0 攒批")
    p.add_argument(*_cm._cli_opts(*alias_no_login), dest="no_login",
                   action="store_true",
                   help="不使用 login shell（默认 argv0 带 - 前缀）")
    p.add_argument(*_cm._cli_opts(*alias_ttl), dest="ttl",
                   type=float, default=DEFAULT_PTY_TTL,
                   help="孤儿会话最长存活秒（默认 12h，上限 24h）")
    p.add_argument(*_cm._cli_opts(*alias_heartbeat), dest="heartbeat",
                   type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument(*_cm._cli_opts(*alias_dead_timeout), dest="dead_timeout",
                   type=float, default=15.0,
                   help="多久收不到任何远端帧（输出/心跳）即判定服务器已死"
                        "并直接退出（默认 15s，实际不小于 3 倍心跳；"
                        "0=不检测，心跳关闭时自动失效）")
    p.add_argument(*_cm._cli_opts(*alias_size), dest="size", default=None,
                   help="强制窗口 ROWSxCOLS，如 24x100；默认取本地终端大小")
    p.add_argument(*_cm._cli_opts(*alias_detach_key), dest="detach_key",
                   default="ctrl-]",
                   help="本地脱离键：只断开 client，远端 tmux/shell 继续运行"
                        "（默认 ctrl-]，沿用 telnet 惯例，不与 tmux 前缀冲突；"
                        "可写 ctrl-a/f9/insert 等名字，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_menu_key), dest="menu_key",
                   default="ctrl-alt-insert",
                   help="本地命令栏热键：detach、status、热调 interval/"
                        "heartbeat/ttl/dead，不会把按键发到远端（默认 "
                        "ctrl-alt-insert；名字写法同脱离键，none 禁用）")
    p.add_argument(*_cm._cli_opts(*alias_rpc_port), dest="port",
                   type=int, default=1188,
                   help="本地 AI 控制口 HTTP RPC 端口（默认 1188）；"
                        "外部进程经它调用 ai_bridge 复用本 PTY，不再重连 broker；0=关闭")
    p.add_argument(*_cm._cli_opts(*alias_rpc_host), dest="host",
                   default="0.0.0.0",
                   help="本地 AI 控制口绑定地址（默认 0.0.0.0）；仅本机调用建议 127.0.0.1")
    p.add_argument(*_cm._cli_opts(*alias_mcp_port), dest="mcp_port",
                   type=int, default=0,
                   help="MCP 工具服务：0=自动（默认，挂在 --port 的 RPC 口 /mcp 路径，"
                        "不新开端口）；>0=另起独立端口（--port 0 关闭 RPC 口时仍要 "
                        "MCP 用）；需 pip install \"mcp[cli]\"，未装包仅记日志")
    p.add_argument(*_cm._cli_opts(*alias_no_mcp), dest="no_mcp",
                   action="store_true",
                   help="完全关闭 MCP 工具服务（默认开启并复用 --port 的口）")
    p.add_argument(*_cm._cli_opts(*alias_mcp_transport), dest="mcp_transport",
                   default="",
                   help="MCP 传输类型：留空自动（优先 streamable-http，旧版 mcp 包 "
                        "退回 sse），可显式 streamable-http / sse")
    p.add_argument(*_cm._cli_opts(*alias_fs_read_only), dest="fs_read_only",
                   action="store_true",
                   help="MCP 文件工具全只读：禁用 fs_write/fs_edit/fs_mkdir/"
                        "fs_move/fs_delete（默认关闭，允许写任意路径）")
    p.add_argument(*_cm._cli_opts(*alias_ai_pane), dest="ai_pane",
                   default="",
                   help="AI 命令默认投递到哪个 tmux 窗口：留空 = 自动按「session "
                        "内 window_index 最小的窗口」解析并固化；也可传 pane_id "
                        "（百分号前缀，如 2）或 0:2 / 窗口名（RPC 的 pty_pane 可运行时改）")
    p.add_argument(*_cm._cli_opts(*alias_ai_encoding), dest="ai_encoding",
                   default="raw",
                   help="AI 命令编码：raw = 自动（默认，无控制字符原文直发；有裸"
                        "换行且 shell 为 bash/zsh 时用 $'...' 保持可读；否则 "
                        "base64）；dollar = 强制 ANSI-C 引用；base64 = 强制严格"
                        "编码")
    # SSH 式可选位置参数：连接成功后自动敲进常驻 shell 的前置命令，如
    # ``pty_client_mqtt.py -t q -k *** "tmux at"``；REMAINDER 保证命令自身
    # 的 -x 选项（tmux attach -d）不会被本客户端解析。命令结束后会话继续，
    # 人仍留在远端 shell / 全屏程序里。
    p.add_argument(alias_command[0], nargs=argparse.REMAINDER,
                   help="可选：连接后自动执行的前置命令（SSH 式），如 \"tmux at\"")

    return p


def main(argv=None) -> int:
    global _LOG_MIRROR_STDERR,transport
    args = build_parser().parse_args(argv)

    if args.size:
        try:
            r_txt, c_txt = args.size.lower().split("x", 1)
            rows, cols = int(r_txt), int(c_txt)
        except ValueError:
            sys.stderr.write("[ERROR] --size 格式应为 ROWSxCOLS，如 24x100\n")
            return 2
    else:
        sz = shutil.get_terminal_size((80, 24))
        rows, cols = sz.lines, sz.columns

    if not sys.stdin.isatty():
        sys.stderr.write("[ERROR] PTY 需要一个交互式本地终端（stdin 不是 TTY）\n")
        return 2
    
    _enable_output_vt()
    # 启动自检：本文件是否仍是 pty/parts 分片重组出来的最新版（不同步只告警）
    check_parts_sync()
    signed = bool(str(args.key or "").strip())
    t_conn = time.monotonic()
    _info("正在并发连接 %d 个公共 MQTT broker（多路径冗余，首个连上即继续，通常不足 1 秒）..."
          % len(BROKER_LIST))
    _info("request_topic=%s  reply_topic=%s  请求签名=%s  允许未验签回包=%s"
          % (args.request_topic, args.reply_topic,
             "是" if signed else "否", "是" if args.allow else "否"))
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)
    online, total, hosts = _broker_status(transport)
    _info("MQTT 就绪：在线 broker %d/%d，建连耗时 %.1f 秒"
      % (online, total, time.monotonic() - t_conn)
      + ("；在线节点：" + ", ".join(hosts) if hosts else ""))
    # if hosts:
        # _info("在线节点：" + ", ".join(hosts))
    # el
    if not hosts and total:
        _emit_local("[WARN] 当前没有任何 broker 在线，握手大概率超时\n")
        
    if args.port:
        import server_http
        # 注意：持久命名空间在此刻快照一次，ai_bridge/get_log 都是模块级
        # 对象引用，会话建立后 attach/feed 对 HTTP 调用方立即生效。
        # 起口成功即切换为"本地日志只进环形缓冲"：包括下面的启动 _info、
        # 每笔 [RPC] 请求行和 root 上的 MultiMQTT/paho logging，终端零写入；
        # 起口失败则保留 stderr 镜像，致命错误用户照样看得见。
        _LOG_MIRROR_STDERR = False
        try:
            ghs = server_http.start_rpc_server(
                port=args.port, ip=args.host, globals=globals(), locals=locals(),
                log_sink=_emit_local,
                websocket_handler=_LOG_HUB.serve, websocket_path="/wslog",
                redirect_root="/log_html(p)")
        except Exception:
            _LOG_MIRROR_STDERR = True
            raise
        # 起口成功后再接管 stdout/stderr/logging：浏览器日志台与 RPC 取日志
        # 两个查看通道都已就绪，PTY 终端从此只渲染远端 shell。
        _install_log_capture()
        lan = _lan_ip()
        _info(f"本地 AI 控制口已开启：http://{lan}:{args.port}/"
              f"根路径自动跳转 /log_html(p)")

    # MCP 文件工具权限：默认读写任意路径，--fs-read-only 才全禁写。
    # 必须在挂载 MCP 前配置好。
    _fs_configure(getattr(args, "fs_read_only", False))

    # MCP 工具服务：默认复用 --port 的 HTTP 口（路径 /mcp，与 RPC/日志台
    # 同口并存，不新开端口）；--mcp-port>0 才另起独立端口（--port 0 时仍要
    # MCP 的场景）；--no-mcp 全关。没装 mcp 包只记日志，PTY 照常工作。
    if not getattr(args, "no_mcp", False):
        _prefer = getattr(args, "mcp_transport", "") or ""
        if int(getattr(args, "mcp_port", 0) or 0) > 0:
            start_mcp_server(port=int(args.mcp_port), host=args.host,
                             prefer=_prefer)
        elif args.port:
            _mcp = mount_mcp_on_rpc(prefer=_prefer, host=args.host)
            if _mcp.get("ok") and not _mcp.get("already"):
                _info(f"MCP 工具服务已挂到 RPC 口：http://{_lan_ip()}:{args.port}/mcp  "
                      f"AI 客户端：claude mcp add --transport http pty "
                      f"http://{_lan_ip()}:{args.port}/mcp")

    try:
        return run_session(transport, args, rows, cols)
    except RemoteError as exc:
        _stderr_write(f"[ERROR] {type(exc).__name__}: {exc}\n")
        # main 即将 return 退出进程，给 stderr 写线程 1s 把错误落盘
        if _ERR_WRITER is not None:
            _ERR_WRITER.close(timeout=1.0)
        return 2
    finally:
        transport.close()


if __name__ == "__main__":
    sys.exit(main())
