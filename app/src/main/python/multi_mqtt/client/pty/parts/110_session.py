def run_session(transport: MqttTransport, args, rows: int, cols: int) -> int:
    global _OUT_WRITER
    pty = RemotePty(transport, timeout=args.timeout)
    # 挂到 AI 桥：外部经本地 HTTP RPC 调用 ai_bridge 即复用本 PTY，
    # attach 放在 open 之前，保证最早的握手回显也能被 feed 分流到。
    ai_bridge.attach(pty, transport)
    # 注入 AI 投递配置：--ai-pane 为空时保持 auto——这里**不能**立刻解析默认
    # 窗口，因为 attach 发生在 pty.open 之前，远端 tmux 还没被前置命令
    # （如 "tmux at"）启动；首次 run() 时才解析并按需固化。
    # --ai-encoding 默认 raw：命令原文直接在窗口里可见。
    ai_bridge.configure(getattr(args, "ai_pane", ""),
                        getattr(args, "ai_encoding", "raw"))
    outq: "queue.Queue[bytes]" = queue.Queue()
    stop_ev = threading.Event()
    term = args.term or os.environ.get("TERM") or "xterm-256color"
    detach_key = _parse_key_spec(getattr(args, "detach_key", "\x1d"))
    menu_key = _parse_key_spec(getattr(args, "menu_key", "ctrl-alt-insert"))

    # ---- 存活检测：输出帧和心跳帧都算"服务器还活着"的信号 ----
    heartbeat = max(0.0, float(args.heartbeat))
    dead_timeout = max(0.0, float(args.dead_timeout))
    if heartbeat <= 0:
        # 没有心跳时，shell 长时间静默和"服务器已死"无法区分，不做误杀
        dead_timeout = 0.0
    elif dead_timeout > 0:
        # 至少容忍连续 3 次心跳缺失，避免公共 broker 短暂重连被误判
        dead_timeout = max(dead_timeout, heartbeat * 3)
    signal_state = {"last": time.monotonic()}

    def on_data(chunk: bytes):
        signal_state["last"] = time.monotonic()
        outq.put(chunk)
        ai_bridge.feed(chunk)

    def on_heartbeat(_ts):
        signal_state["last"] = time.monotonic()

    def _responders_warning(responders) -> str:
        # 多应答者（持相同 key 的多个服务端进程同时应答）告警文本；
        # 后台 gather 到齐后走本地日志，不阻塞首屏。
        winner = next((r for r in responders if r.get("winner")),
                      responders[0])
        lines = [
            f"[pty][WARN] 检测到 {len(responders)} 个持相同 key 的服务端同时应答！"
            f"仅保留 {winner.get('host')} pid={winner.get('pid')}，"
            f"其余影子 PTY 已被通知立即关闭。"]
        for r in responders:
            if not r.get("winner"):
                lines.append("           影子: %s pid=%s owner=%s"
                             % (r.get("host"), r.get("pid"), r.get("owner")))
        lines.append("           请停掉多余机器/容器上的旧 server_mqtt 进程，"
                     "否则每次连接都会重复拉起并短暂干扰首屏。")
        return "\n".join(lines) + "\n"

    # 协商：订阅 out topic + 下发 PTY 启动代码（shell 在服务端只启动这一次）
    _info("正在远端启动 PTY：shell=%s cwd=%s 窗口=%dx%d，等待握手回包（超时 %.0fs）..."
          % (args.shell or "服务端登录 shell", args.cwd or "远端 HOME",
             rows, cols, args.timeout))
    env = pty.open(rows, cols, shell=(args.shell or None), term=term,
                   cwd=args.cwd, login=not args.no_login,
                   flush_interval=args.interval, ttl=args.ttl,
                   heartbeat=heartbeat, on_heartbeat=on_heartbeat,
                   on_data=on_data,
                   on_responders=lambda rs: _emit_local(
                       _responders_warning(rs)))

    # 会话启动时把远端登录 shell 记入 AI 桥：多行命令能否用可读的 $'...'
    # 编码取决于它，探测不到（dash/未知）就由 _pick_encoding 退回 base64。
    ai_bridge.set_shell(env.get("shell") if env else None)

    banner = (
        f"[{stime()}] connected shell={env['shell']} pid={env['pid']} "
        f"({env['rows']}x{env['cols']}, interval={env['flush_interval']})\t"
        f"{env['in_topic']}\t{env['out_topic']}\n"
        f"[pty] {_describe_key(detach_key)} 本地脱离（远端 shell/tmux 继续运行）；"
        f"{_describe_key(menu_key)} 本地命令栏（detach/status/时间参数）；"
        f"远端 exit/Ctrl-D 结束会话\n")
    if env.get("cwd_warning"):
        # 服务端对不存在的 cwd 已自行回退（HOME→/），会话照常用；只提示不退出
        banner += (f"[pty] 注意: {env['cwd_warning']}，"
                   f"已回退到 {env['cwd']}\n")
    # 多应答者告警由 on_responders 后台回调输出（首包不阻塞 gather 窗）。
    # 同步 gather 回退路径（transport 不支持后台收集）下这里仍兜底拼一次。
    responders = getattr(pty, "responders", None) or []
    if len(responders) > 1:
        banner += _responders_warning(responders)
    if dead_timeout > 0:
        banner += (f"[pty] 心跳 {heartbeat:g}s：服务器关闭/断连后最多 "
                   f"{dead_timeout:g}s 自动退出\n")
    if getattr(args, "port", 0):
        banner += ("[pty] 本地日志与本画面完全分离：浏览器开 "
                   "http://<本机IP>:%d/ 即全屏实时日志台；"
                   "命令栏输 log，或 curl 同口 /r=get_log()\n"
                   % args.port)
    if sys.platform == "win32":
        banner += "[pty] 粘贴：右键 或 Shift+Insert，中文按 UTF-8 发送\n"
    # banner 及之后的本地日志只进环形缓冲（RPC 口开启时终端零污染），
    # 不再 _stderr_write 插进远端画面。
    _emit_local(banner)

    # 会话期可变参数（本地命令栏热调；dead_timeout 纯本地，其余经 set 帧下发）
    live = {
        "interval": float(args.interval),
        "heartbeat": heartbeat,
        "ttl": float(args.ttl),
        "dead_timeout": dead_timeout,
    }
    magic_ctx = {"pty": pty, "transport": transport, "live": live}

    console = _make_raw_console()
    # 终端输出全部交给异步写线程：唯一可能阻塞的原生控制台 IO 只发生在
    # 那个可牺牲线程上，渲染主线程 / RPC 线程再也不会被控制台堵死。
    # 通道用 _ConsoleSink（Win32 WriteConsoleW + 自有锁），彻底绕开
    # sys.stdout.buffer 那个共享 BufferedWriter——它的 flush 在遇到半截
    # UTF-8 时会零进度死循环并占死锁（2026-10-25 实证）。
    if _bin_stdout() is not None:
        _OUT_WRITER = _TerminalWriter(_new_console_sink("pty-out"),
                                      name="pty-out")

    def _out_write(data: bytes) -> None:
        if _OUT_WRITER is not None:
            _OUT_WRITER.write(data)

    # 关窗口/Ctrl-Break/kill 信号时也要恢复本地终端模式，否则 cmd 残留在
    # raw/VT 输入模式（方向键失灵）；顺手 fire-and-forget 一帧 stop。
    install_console_guards(console, on_signal=lambda: pty.detach())

    keyq: "queue.Queue[bytes]" = queue.Queue()
    menu_ev = threading.Event()

    def reader_loop():
        # 唯一触碰本地终端输入的线程：只负责读出来塞进 keyq，热键匹配/命令栏/
        # 透传全部在 input_loop 做，避免热键字节被拆读时错过。
        try:
            console.enter()
            while not stop_ev.is_set():
                try:
                    data = console.read()
                except OSError:
                    break
                if not data:
                    break
                data = _filter_injected_esc(data)
                if not data:
                    continue
                keyq.put(data)
        finally:
            stop_ev.set()

    def _local_out(text: str):
        _out_write(b"\x1b[0m" + text.encode("utf-8", "replace"))

    def _read_local_line():
        """命令栏本地行编辑（提示符由调用方画）：回显不发给远端；
        返回 str，Esc/Ctrl-C 返回 None。回车/取消都不换行——整行随后由
        调用方统一归位擦除，避免任何菜单字符残留在终端上。"""
        line = bytearray()
        while not stop_ev.is_set():
            try:
                data = keyq.get(timeout=0.3)
            except queue.Empty:
                continue
            for ch in data:
                if ch in (13, 10):
                    return line.decode("utf-8", "replace")
                if ch == 3 or ch == 27:
                    return None  # Ctrl-C / Esc：本地取消，绝不发给远端
                if ch in (127, 8):
                    if line:
                        del line[-1]
                        _local_out("\b \b")
                elif ch >= 32:
                    line.append(ch)
                    _out_write(bytes([ch]))
        return None

    def _open_menu():
        # 打开期间主线程暂停渲染远端输出（暂存在 held 里），避免 tmux 刷新
        # 打花本地命令行；关闭后先彻底擦掉菜单再补画。
        # ESC 7 保存远端光标（含 shell 提示符位置），ESC 8 归位：
        # 每轮提示符都归位并 ESC[0J 清掉下方，所以 help 那 9 行、^C 等
        # 不会累积；退出时同样归位+清下方，菜单零残留，随后 held 补画，
        # tmux 全屏重绘 / shell 接着原来的提示符位置输出。
        menu_ev.set()
        _local_out("\x1b7\r\n\x1b[2K[pty] >> ")
        try:
            while True:
                line = _read_local_line()
                if line is None:
                    return
                if not line.strip():
                    return
                do_detach, lines = _run_magic(line, magic_ctx)
                if do_detach:
                    stop_ev.set()
                    return
                # 归位擦除旧提示符/上次输出，在同一区域重画（高度不累积）
                _local_out("\x1b8\x1b[0J\r\n")
                for ln in lines:
                    _local_out(ln + "\r\n")
                _local_out("[pty] >> ")
        finally:
            # 必须先擦干净再放行渲染，否则补画的远端流会和菜单字符拼成花屏。
            # ESC8/0J 覆盖普通 shell（光标归位+清下方）；菜单行数超过光标
            # 下方空间时备用屏幕可能已滚屏，再让 tmux/vim 收到 SIGWINCH
            # 整屏重绘兜底（进程即将退出的 detach 路径就不必发了）。
            _local_out("\x1b8\x1b[0J")
            menu_ev.clear()
            if not stop_ev.is_set():
                try:
                    pty.configure(redraw=True)
                except Exception:
                    pass

    def _do_paste() -> None:
        # Shift+Insert：conhost 已把按键交给本进程，直接读本地剪贴板发远端，
        # 不经过 conhost 的粘贴路径，任何语言文本都是干净 UTF-8。
        if sys.platform != "win32":
            return
        try:
            text = _read_clipboard_text_win()
        except Exception:
            text = ""
        if not text:
            return
        data = _normalize_paste_text(text)
        if data:
            try:
                pty.send(data)
            except Exception:
                pass

    def _do_hotkey(name) -> bool:
        if name == "detach":
            _local_out("\r\n[pty] 本地脱离\r\n")
            stop_ev.set()
            return True
        if name == "menu":
            _open_menu()
        elif name == "paste":
            _do_paste()
        return False

    def input_loop():
        # 热键精确匹配：一个读入突发恰好等于热键序列才触发（粘贴文本里
        # 恰好含同样字节不会误触）。多字节热键以 ESC 开头且被拆读时，
        # 按 _HOTKEY_GAP 等剩余字节；超时则把 ESC 照常放给远端。
        seqs = [(s, n) for s, n in
                ((detach_key, "detach"), (menu_key, "menu")) if s]
        # Shift+Insert 本地粘贴（仅 Windows：POSIX 终端一般自行完成粘贴，
        # 不会把序列交给应用；万一收到也照旧透传）。
        if sys.platform == "win32":
            seqs.append((_PASTE_KEY_SEQ, "paste"))
        multi = [s for s, _ in seqs if len(s) > 1]
        try:
            while not stop_ev.is_set():
                try:
                    chunk = keyq.get(timeout=0.3)
                except queue.Empty:
                    continue
                burst = bytearray(chunk)
                while True:  # 把同一次按键已到齐的字节合并成一个突发
                    try:
                        burst += keyq.get_nowait()
                    except queue.Empty:
                        break
                burst = bytes(burst)
                hit = next((n for s, n in seqs if burst == s), None)
                if hit is None and multi and burst[:1] == b"\x1b" \
                        and any(s.startswith(burst) for s in multi):
                    deadline = time.monotonic() + _HOTKEY_GAP
                    extra = b""
                    while time.monotonic() < deadline:
                        try:
                            c = keyq.get(
                                timeout=max(0.0, deadline - time.monotonic()))
                            extra += c
                            deadline = time.monotonic() + _HOTKEY_GAP
                        except queue.Empty:
                            break
                    if extra:
                        burst += extra
                        hit = next((n for s, n in seqs if burst == s), None)
                if hit is not None:
                    if _do_hotkey(hit):
                        return
                    continue
                try:
                    pty.send(burst)
                except Exception:
                    pass
        finally:
            stop_ev.set()

    def resize_watch():
        # last=None：连 broker / 握手期间窗口可能已被拖动过，线程一启动
        # 先无条件同步一次真实尺寸。
        # 尺寸变化立即发 winsz；停稳后的几个 tick 再补发同尺寸、更高 iseq
        # 的新帧兜底——多 broker 路径乱序时，配合服务端的 iseq 单调闸门，
        # 保证最终生效的一定是当前尺寸：拖动时经慢 broker 迟到的旧（更大）
        # 尺寸要么被服务端丢弃，要么被这里的补发覆盖，远端 PTY 不会被卡在
        # 比本地窗口宽的尺寸上（否则 pip/gradle 的 \r 进度条会全线错位）。
        last = None
        resend = 0
        while not stop_ev.wait(0.25):
            cur_sz = shutil.get_terminal_size((80, 24))
            cur = (cur_sz.lines, cur_sz.columns)
            if cur != last:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                last = cur
                resend = 3
            elif resend > 0:
                try:
                    pty.resize(cur[0], cur[1])
                except Exception:
                    pass
                resend -= 1

    t_read = threading.Thread(target=reader_loop, name="pty-read", daemon=True)
    t_in = threading.Thread(target=input_loop, name="pty-input", daemon=True)
    t_resize = threading.Thread(target=resize_watch, name="pty-resize",
                                daemon=True)
    t_read.start()
    t_in.start()
    t_resize.start()

    pre_cmd = _join_pre_command(getattr(args, "command", None))
    if pre_cmd:
        # SSH 式前置命令（ssh host "tmux at"）：握手一完成就把整行敲进常驻
        # shell。tty 行规程会先把字节缓存在内核输入队列里等 shell 读取，
        # 无需 sleep 等提示符。加 \r 提交；命令结束后会话不关闭，人继续
        # 留在 shell / tmux 里。
        try:
            pty.send(pre_cmd + "\r")
            _emit_local("[pty] 已自动执行前置命令: %s\n" % pre_cmd)
        except Exception as exc:
            _emit_local("[pty][WARN] 前置命令发送失败: %s\n" % exc)

    # 主线程：远端输出原样渲染；任何退出路径都走 _hard_exit 立即收场
    held = bytearray()  # 本地命令栏打开期间暂存远端输出，关闭后补画
    last_stall_warn = [0.0]  # 卡滞告警节流（每 30s 一条）
    last_unstick_ts = [0.0]  # 上次尝试自动解除的时间（每 30s 重试直到成功）
    last_writer_reset_ts = [0.0]  # 上次自动切换写线程的时间（每 60s 一次）
    try:
        while not stop_ev.is_set():
            try:
                chunk = outq.get(timeout=0.3)
            except queue.Empty:
                chunk = None
            # 菜单刚关闭、远端又静默时，暂存的输出也要立刻补画，不能等新帧
            if chunk is None and held and not menu_ev.is_set():
                _out_write(bytes(held))
                held.clear()
            if chunk is None:
                # 写线程卡滞：屏幕虽冻结但日志 ring 照写，浏览器日志台 / AI
                # 经 r=writer_diag() 可见，并给出处置路径
                _stall = (_OUT_WRITER.stall_seconds()
                          if _OUT_WRITER is not None else 0.0)
                if _stall > 10 and (time.monotonic() - last_stall_warn[0]) > 30:
                    last_stall_warn[0] = time.monotonic()
                    _snap = _writer_sink_snapshot()
                    _emit_local(
                        "[pty][WARN] 终端写卡滞 %.0fs（通道=%s 零进度=%s次 "
                        "carry=%sB 持锁线程=%s）：窗口选择/标记模式请先点"
                        "本窗口再按 ESC；否则 60s 后自动换新输出通道\n"
                        % (_stall, _snap.get("kind"),
                           _snap.get("zero_progress"),
                           _snap.get("carried_bytes"),
                           _snap.get("holder")))
                # 长时间冻结（通常是选了文本忘记退出标记模式）：前台时
                # 合成 ESC 自动解除，鼠标左键选择/右键粘贴照常保留。
                # 每 30s 重试直到成功：解除仅在窗口前台时生效，用户切到
                # 别的窗口时一次尝试必然失败，旧的一次性设计会让冻结
                # 一直挂到手工干预（实证：卡滞 7 分钟未自愈）。
                if _stall >= _SELECTION_STALL_AUTO_UNSTICK \
                        and (time.monotonic() - last_unstick_ts[0]) > 30:
                    last_unstick_ts[0] = time.monotonic()
                    if _cancel_console_selection():
                        _emit_local(
                            "[pty] 窗口选择模式冻结输出超过 %.0f 秒，"
                            "已自动解除\n" % _SELECTION_STALL_AUTO_UNSTICK)
                # 写线程活锁兜底（2026-10-25 实证）：源头是半截 UTF-8 让
                # BufferedWriter.flush 零进度死循环并占死共享锁。现在新通道
                # 换的是新锁 + 新 CONOUT$ 句柄，旧实例卡死也拖不住新写线程。
                if _stall >= 60.0 \
                        and (time.monotonic() - last_writer_reset_ts[0]) > 60:
                    last_writer_reset_ts[0] = time.monotonic()
                    # 换新通道（新锁 + 新 CONOUT$ 句柄）才真正有效；次数
                    # 封顶防止"换了也没用"的极端场景无限堆线程。
                    if _WRITER_RESETS >= _AUTO_RESET_MAX:
                        _emit_local("[pty] 写卡滞 %.0fs，自愈已尝试 %d 次，"
                                    "停止自动换通道（需人工介入）\n"
                                    % (_stall, _WRITER_RESETS))
                    else:
                        _r = reset_out_writer()
                        _emit_local("[pty] 写卡滞 %.0fs，自动换输出通道: "
                                    "ok=%s resets=%s零进度=%s\n"
                                    % (_stall, _r.get("ok"),
                                       _r.get("resets"),
                                       (_r.get("old") or {}).get("sink")))
                if pty.end_reason is not None:
                    _hard_exit(
                        console, 0,
                        f"\r\n[pty] session ended: {pty.end_reason}\r\n", pty)
                _dt = float(live["dead_timeout"])
                if _dt > 0 and (time.monotonic()
                                - signal_state["last"]) > _dt:
                    _hard_exit(
                        console, 3,
                        f"\r\n[pty] {_dt:g}s 收不到任何远端帧"
                        f"（输出/心跳），服务器可能已关闭，直接断开\r\n", pty)
                continue
            if menu_ev.is_set():
                held.extend(chunk)
                continue
            if held:
                _out_write(bytes(held))
                held.clear()
            _out_write(chunk)
    except KeyboardInterrupt:
        _hard_exit(console, 130, "\r\n[pty] interrupted\r\n", pty)

    # input_loop 结束：Ctrl-] 本地脱离 / 本地 stdin 关闭。
    # leave=True：只清本地订阅不发 stop，远端 shell/tmux 继续运行
    # （与 banner/帮助文案的承诺一致；裸 shell 前台作业不再被误杀）。
    _hard_exit(console, 0, "\r\n[pty] session ended: detached\r\n", pty,
               leave=True)
    return 0  # 不可达：_hard_exit 不返回；仅为类型/静态检查保留


