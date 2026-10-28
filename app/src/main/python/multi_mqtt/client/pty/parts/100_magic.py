# ============================ PTY 会话 ============================

# 可设置参数规格表（唯一真相源；别名表在文件前面统一声明）：
#   (规范名/live 键, 别名组, RemotePty.configure 关键字[None=纯本地],
#    (下限, 上限), 说明)
_PTY_MAGIC_SPECS = (
    ("interval", alias_interval, "interval", (0.0, 60.0),
     "服务端输出攒批间隔秒，0=实时，上限 60"),
    ("heartbeat", alias_heartbeat, "heartbeat", (0.0, 3600.0),
     "心跳间隔秒，0=关闭，上限 3600"),
    ("ttl", alias_ttl, "ttl", (60.0, 86400.0),
     "孤儿会话存活秒，60~86400"),
    ("dead_timeout", alias_dead_timeout, None, (0.0, 3600.0),
     "本地判死超时秒，0=不检测（立即生效）"),
)


def _alias_usage(aliases) -> str:
    return "|".join(aliases)


def _magic_help_lines():
    """帮助文本由别名表/规格表生成，命令增删改时不可能和实现脱节。"""
    lines = ["本地命令（不会发到远端）："]
    lines.append("  %s   脱离（远端 tmux/shell 继续运行）"
                 % _alias_usage(alias_detach))
    lines.append("  %s   会话与 broker 状态"
                 % _alias_usage(_cm.alias_status))
    lines.append("  %s [n]   查看最近本地日志（不写终端，默认 20 行，上限 100；"
                 "更多走 HTTP RPC r=get_log(n)）"
                 % _alias_usage(alias_log))
    lines.append("  %s   写线程卡顿时切换写线程，并通知远端整屏重绘"
                 % _alias_usage(alias_redraw))
    for canon, aliases, _remote, _bounds, desc in _PTY_MAGIC_SPECS:
        lines.append("  %s <秒>   %s"
                     % (_alias_usage((canon,) + aliases[1:]), desc))
    lines.append("  %s   本帮助；空行/Esc/Ctrl-C 取消"
                 % _alias_usage(_cm.alias_help))
    return lines


def _run_magic(line, ctx):
    """执行一行本地魔术命令，返回 ``(是否请求脱离, 要本地打印的行列表)``。"""
    body = (line or "").strip()
    if not body:
        return False, []
    parts = body.split(None, 1)
    # - 与 _ 等价（dead-timeout / dead_timeout 同一命令）
    cmd = parts[0].lower().replace("-", "_")
    arg = parts[1].strip() if len(parts) > 1 else ""
    pty = ctx["pty"]
    live = ctx["live"]

    if cmd in _cm.alias_help:
        return False, _magic_help_lines()
    if cmd in alias_detach:
        return True, ["正在脱离（远端会话保持运行）…"]
    if cmd in _cm.alias_status:
        info = getattr(pty, "server_info", None) or {}
        online, total, _hosts = _broker_status(ctx.get("transport"))
        lines = [
            "sid        = %s" % getattr(pty, "sid", None),
            "远端       = %s pid=%s owner=%s"
            % (info.get("host"), info.get("pid"), getattr(pty, "owner", None)),
            "broker     = 在线 %d/%d" % (online, total),
            "interval   = %s 秒（服务端攒批）" % live.get("interval"),
            "heartbeat  = %s 秒" % live.get("heartbeat"),
            "ttl        = %s 秒" % live.get("ttl"),
            "dead       = %s 秒（0=不检测）" % live.get("dead_timeout"),
            "丢弃影子帧 = %d" % getattr(pty, "foreign_frames", 0),
            "写线程卡滞 = %s 秒（>2 可执行 redraw）"
            % (round(_OUT_WRITER.stall_seconds(), 1)
               if _OUT_WRITER is not None else 0.0),
            "结束原因   = %s" % getattr(pty, "end_reason", None),
        ]
        return False, lines
    if cmd in alias_log:
        # 只读查看：覆盖层自擦，终端不留痕；行数夹 1~100，防止整屏滚动
        if arg:
            try:
                n = int(float(arg))
            except ValueError:
                return False, ["log 参数应为行数（数字），收到: %r" % arg]
        else:
            n = 20
        n = max(1, min(n, 100))
        text = get_log(n)
        body = text.split("\n") if text else ["（暂无本地日志）"]
        header = ("本地日志（最近 %d 行，缓冲共 %d 行；"
                  "更多走 HTTP RPC：r=get_log(n)）："
                  % (len(body) if text else 0, len(_LOCAL_LOG)))
        return False, [header] + body
    if cmd in alias_redraw:
        lines = []
        # 写线程真卡滞（>2s）时先切换 writer，避免解冻后旧流与重绘流拼接
        if _OUT_WRITER is not None and _OUT_WRITER.stall_seconds() > 2.0:
            res = reset_out_writer()
            if res.get("ok"):
                lines.append("检测到终端写卡滞 %.1fs，已切换写线程"
                             % res["stall_seconds"])
            else:
                lines.append("切换写线程失败: %s" % res.get("error"))
        try:
            pty.configure(redraw=True)  # killpg(SIGWINCH)：tmux/vim 整屏重绘
            lines.append("已通知远端整屏重绘")
        except RemoteError as exc:
            return False, ["redraw 失败: %s" % exc]
        return False, lines
    for canon, aliases, remote_key, (lo, hi), _desc in _PTY_MAGIC_SPECS:
        if cmd not in aliases:
            continue
        if not arg:
            return False, ["%s = %s" % (canon, live.get(canon))]
        try:
            val = float(arg)
        except ValueError:
            return False, ["%s 需要一个数字（秒），收到: %r" % (canon, arg)]
        val = min(max(lo, val), hi)
        if remote_key is None:
            live[canon] = val
            return False, ["%s <- %s 秒（立即生效）" % (canon, val)]
        try:
            applied = pty.configure(**{remote_key: val})
        except RemoteError as exc:
            return False, ["设置失败: %s" % exc]
        live[canon] = applied[remote_key]
        lines = ["%s <- %s 秒（已通知服务端热调）"
                 % (canon, applied[remote_key])]
        if canon == "heartbeat":
            # 与会话建立时（run_session 开局）同一规则联动 dead_timeout：
            # hb<=0 → 停止判死（静默 shell 与服务器已死无法区分，不误杀）；
            # hb>0 且判死已开启 → 下限 3×心跳。旧实现不联动：热调
            # heartbeat 0 或调大后，静默 shell 超过陈旧的 dead_timeout 即
            # 被误判"服务器已死"强制退出，退出路径还顺手发 stop 杀远端。
            _hb = live["heartbeat"]
            if _hb <= 0:
                live["dead_timeout"] = 0.0
                lines.append("心跳已关闭：dead_timeout 自动置 0（不再判死）")
            elif live["dead_timeout"] > 0:
                live["dead_timeout"] = max(live["dead_timeout"], _hb * 3)
                lines.append("dead_timeout 联动 <- %g 秒（不小于 3×心跳）"
                             % live["dead_timeout"])
        return False, lines
    return False, ["未知本地命令: %s（输入 help 查看）" % cmd]


