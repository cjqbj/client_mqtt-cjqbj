#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CLI：参数构建与主流程（等价单文件版 build_parser / main）。"""
from __future__ import annotations

import argparse

from .. import client_mqtt as _cm
from ..cmd_client_mqtt import MqttTransport, add_connection_args
from ..remote_cmd import PTY_FRAME_MAX, RemoteError
from multi_mqtt import BROKER_LIST
from server_http import start_rpc_server

from .config import (
    _GAP_DEFAULT, alias_connect_timeout, alias_dead_timeout, alias_frame_max,
    alias_gap_timeout, alias_heartbeat, alias_listen_host, alias_listen_port,
    alias_rpc_host, alias_rpc_port, alias_ttl,
)
from .local_server import _Socks5Server
from .session import RemoteSocks5
from .util import _info


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 2584-2730 行（冻结基线，校验见 verify_parity.py） ——
def build_parser() -> argparse.ArgumentParser:
    # 选项名一律由文件前面的 alias_xxx 别名表 + client_mqtt._cli_opts 生成，
    # 与 client_mqtt / pty_client_mqtt 同一套规矩，禁止再硬编码 "--xxx"。
    p = argparse.ArgumentParser(
        prog="socks5_client_mqtt",
        description="SOCKS5 over MQTT：本地 SOCKS5 代理，流量经 broker 从远端"
                    "服务端网络出口（服务端只需运行 server_mqtt.py）")
    # 连接参数（topic/key/allow/timeout）：直接复用 client_mqtt 别名表
    add_connection_args(p, default_timeout=30.0)
    p.add_argument(*_cm._cli_opts(*alias_listen_port), dest="port",
                   type=int, default=1080,
                   help="本地 SOCKS5 监听端口（默认 1080）")
    p.add_argument(*_cm._cli_opts(*alias_listen_host), dest="host",
                   default="127.0.0.1",
                   help="本地 SOCKS5 绑定地址（默认 127.0.0.1）；"
                        "共享给局域网用 0.0.0.0（无认证，慎用）")
    p.add_argument(*_cm._cli_opts(*alias_heartbeat), dest="heartbeat",
                   type=float, default=5.0,
                   help="服务端心跳间隔秒：0=关闭（默认 5s）")
    p.add_argument(*_cm._cli_opts(*alias_dead_timeout), dest="dead_timeout",
                   type=float, default=15.0,
                   help="多久收不到任何远端帧（数据/心跳）即判定服务器已死并退出"
                        "（默认 15s，实际不小于 3 倍心跳；0=不检测）")
    p.add_argument(*_cm._cli_opts(*alias_ttl), dest="ttl",
                   type=float, default=86400.0,
                   help="孤儿会话最长存活秒（默认 24h，上限 7d）")
    p.add_argument(*_cm._cli_opts(*alias_connect_timeout), dest="connect_timeout",
                   type=float, default=10.0,
                   help="服务端 connect 目标超时秒（默认 10s）")
    p.add_argument(*_cm._cli_opts(*alias_frame_max), dest="frame_max",
                   type=int, default=PTY_FRAME_MAX,
                   help="单帧原始字节上限（默认 16KiB，与 PTY 相同）")
    p.add_argument(*_cm._cli_opts(*alias_gap_timeout), dest="gap_timeout",
                   type=float, default=_GAP_DEFAULT,
                   help="per-cid seq 缺口硬熔断秒：缺口先经 NACK 重传补发，"
                        "多轮仍补不齐才断开（默认 15s；TCP 流不跳号）")
    p.add_argument(*_cm._cli_opts(*alias_rpc_port), dest="rpc_port",
                   type=int, default=2288,
                   help="本地 HTTP RPC 口（默认 2288，0=关闭）："
                        "/r=<python 表达式> 直接调进程内对象，如 "
                        "/r=transport.node.mqtt_net.stats.get_report() 或 "
                        "/r=sess.net_report()")
    p.add_argument(*_cm._cli_opts(*alias_rpc_host), dest="rpc_host",
                   default="127.0.0.1",
                   help="HTTP RPC 绑定地址（默认 127.0.0.1）；/r= 可执行任意"
                        " Python，绑 0.0.0.0 等于把本机代码执行暴露给局域网，"
                        "仅在可信网络使用")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    heartbeat = max(0.0, float(args.heartbeat))
    dead_timeout = max(0.0, float(args.dead_timeout))
    if heartbeat <= 0:
        dead_timeout = 0.0
    elif dead_timeout > 0:
        dead_timeout = max(dead_timeout, heartbeat * 3)

    signed = bool(str(args.key or "").strip())
    _info("正在并发连接 %d 个公共 MQTT broker（首个连上即继续）..."
          % len(BROKER_LIST))
    _info("request_topic=%s  reply_topic=%s  请求签名=%s  允许未验签回包=%s"
          % (args.request_topic, args.reply_topic,
             "是" if signed else "否", "是" if args.allow else "否"))
    transport = MqttTransport(
        request_topic=args.request_topic, reply_topic=args.reply_topic,
        private_key=args.key, allow_no_pub=args.allow)

    sess = None
    try:
        sess = RemoteSocks5(transport, timeout=args.timeout, on_log=_info)
        sess.dead_timeout = dead_timeout
        _info("正在下发并启动远端 SOCKS5 出口（心跳 %gs，等待握手回包）..."
              % heartbeat)
        env = sess.open(ttl=args.ttl, heartbeat=heartbeat,
                        frame_max=args.frame_max,
                        connect_timeout=args.connect_timeout,
                        gap_timeout=args.gap_timeout)
        banner = ("[s5] 会话已建立: server=%s owner=%s  %s  %s"
                  % (env.get("host"), env.get("owner"),
                     env.get("in_topic"), env.get("out_topic")))
        responders = sess.responders or []
        if len(responders) > 1:
            winner = next((r for r in responders if r.get("winner")),
                          responders[0])
            banner += ("\n[s5][WARN] 检测到 %d 个持相同 key 的服务端同时应答！"
                       "仅保留 %s pid=%s，其余影子会话已被通知自杀；"
                       "请停掉多余机器/容器上的旧 server_mqtt 进程。"
                       % (len(responders), winner.get("host"),
                          winner.get("pid")))
        _info(banner)

        # ---- 本地 HTTP RPC：/r=<python 表达式> 直接调进程内对象 ----
        # 命名空间零拷贝持有 sess/transport，便于：
        #   /r=sess.net_report()                                  网络分析快照
        #   /r=transport.node.mqtt_net.stats.get_report()         系统自带报告
        #   /r=sess.net_report()["downlink_race"]                 broker 竞速
        if args.rpc_port > 0:
            if args.rpc_host not in ("127.0.0.1", "localhost", "::1"):
                _info("[s5][WARN] RPC 口绑定 %s：/r= 可执行任意 Python，"
                      "确认当前网络可信" % args.rpc_host)
            _rpc_ns = {"transport": transport, "sess": sess,
                       "node": transport.node, "mqtt_net": transport.node.mqtt_net,
                       "net_report": sess.net_report}
            try:
                start_rpc_server(port=args.rpc_port, ip=args.rpc_host,
                                 globals=_rpc_ns)
                _info("[s5] HTTP RPC: http://%s:%d/r=sess.net_report()"
                      % (args.rpc_host, args.rpc_port))
            except OSError as exc:
                _info("[s5][WARN] RPC 口 %s:%d 绑定失败（不影响代理）: %s"
                      % (args.rpc_host, args.rpc_port, exc))

        srv = _Socks5Server(sess, args.host, args.port, on_log=_info)
        try:
            srv.start()
        except OSError as exc:
            _info("[ERROR] 本地监听 %s:%d 失败: %s" % (args.host, args.port, exc))
            sess.close()
            return 2
        _info("[s5] SOCKS5 代理已就绪: %s:%d -> 远端出口 %s"
              "（仅 CONNECT，域名由远端解析；curl -x socks5h://%s:%d）"
              % (args.host, args.port, env.get("host"), args.host, args.port))
        if dead_timeout > 0:
            _info("[s5] 心跳 %gs：服务器关闭/断连后最多 %gs 自动退出"
                  % (heartbeat, dead_timeout))

        try:
            while not sess.wait_end(0.5):
                pass
        except KeyboardInterrupt:
            _info("[s5] Ctrl-C，正在断开会话...")
            return 130
        _info("[s5] 会话结束: %s" % (sess.end_reason or "end"))
        return 3 if sess.end_reason == "dead" else 0
    except RemoteError as exc:
        _info("[ERROR] %s: %s" % (type(exc).__name__, exc))
        return 2
    finally:
        if sess is not None:
            try:
                sess.close()
            except Exception:
                pass
        transport.close()
