#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""socks5_client_mqtt —— SOCKS5 over 公共 MQTT broker（映射目标网络到本地）。

模型（和 pty_client_mqtt 一样）
===============================
- 仿 pty_client_mqtt 建立 MQTT 连接（默认 ``sys/device/request`` ↔ response）；
- 连接成功后 client 生成会话 id 并提出 in/out 两个**新 topic**，握手时把
  SOCKS5 服务端全部功能代码（按 cid 的连接管理 + 读/写线程）整段下发执行——
  服务端只要求运行通用 server_mqtt.py，无需预装任何 agent 代码；
- 本地监听一个 SOCKS5 口（默认 127.0.0.1:1080，仅 CONNECT，无认证）。
  每个进入的 CONNECT 让服务端进程从它所在的网络位置去 connect 目标
  host:port，字节流分帧经 broker 双向转发；域名一律发给服务端解析
  （等价 socks5h），本地应用因此"站在"服务端的网络里访问目标。

用法
====
    python client/socks5_client_mqtt.py                      # 127.0.0.1:1080
    python client/socks5_client_mqtt.py -p 1081 --host 0.0.0.0
    curl -x socks5h://127.0.0.1:1080 http://目标内网服务/

本地 HTTP RPC（默认 127.0.0.1:2288，--rpc-port 0 关闭）
======================================================
/r=<python 表达式> 直接调进程内对象（sess/transport/node/mqtt_net）：

    curl "http://127.0.0.1:2288/r=sess.net_report()"      # 网络分析快照
    curl "http://127.0.0.1:2288/r=transport.node.mqtt_net.stats.get_report()"
    curl "http://127.0.0.1:2288/r=sess.net_report()['downlink_race']"

net_report 全部来自**被动观察已收到的帧**，零额外网络请求：per-broker
首到胜率（同一帧经全部 broker 广播，谁投递的副本被重组缓冲采信谁赢）、
副本丢弃数、hb 延迟分布（hb 帧内服务器时间戳逐 broker 比对，横向有效；
跨机器含时钟偏差），以及服务端心跳捎带的**服务端一侧**各 broker 连接
快照（内置 ConnectionQualityStats 的存量记账）。

多 broker 冗余的三条血泪经验（与 PTY 相同，全部内建）
====================================================
1. 同一帧会被每个 broker 各投递一次，且各路径延迟抖动导致乱序：双向每个
   连接（cid）各自按 seq 重排，重复副本丢弃。PTY 缺口可以跳号牺牲显示，
   但 SOCKS5 承载的是 TCP 字节流，跳号=数据损坏——缺口超过 gap_timeout
   判定真丢帧，直接断开该条连接（应用重试），绝不放行坏流；
2. 多个持相同 key 的 server_mqtt 进程会同时应答握手、各自往同一 topic
   推流：握手后 gather 收集全部应答者并告警，owner 仲裁——客户端只认
   首个应答者，上行帧带 owner 让影子会话在 iseq 闸门之前立即自杀；
3. 服务端同进程同 sid 幂等：重复握手直接返回缓存 env，不会重复拉起
   第二套会话线程；路由回调安装幂等且与 PTY 共用同一份 _cmq_pty_*
   基建（按 topic 分发，两种会话可共存于同一服务端进程）。

帧约定
======
- 上行（client→server，in topic）：每帧带 ``owner`` 与 per-cid 单调
  ``seq``（open=0，其后数据/close 递增）：
  ``{"s5": sid, "cid": n, "seq": 0, "owner": uid, "open": {"host","port"}}`` /
  ``{"s5": sid, "cid": n, "seq": n, "owner": uid, "d": latin-1}`` /
  ``{"s5": sid, "cid": n, "seq": n, "owner": uid, "close": true}``（半关写）/
  ``{"s5": sid, "cid": n, "owner": uid, "nack": [seq,...]}``（缺帧补发请求）/
  ``{"s5": sid, "owner": uid, "pb": [broker,...]}``（数据帧首选路径名单）/
  ``{"s5": sid, "owner": uid, "stop": true}`` / ``{"s5": sid, "owner": uid, "claim": true}``
- 下行（server→client，out topic）：per-cid ``seq``（opened=0）：
  ``{"s5": sid, "cid": n, "seq": 0, "owner": uid, "opened": {"ok":bool,"error"?}}`` /
  ``{"s5": sid, "cid": n, "seq": n, "owner": uid, "d": latin-1}`` /
  ``{"s5": sid, "cid": n, "seq": n, "owner": uid, "closed": true, "reason": ...}`` /
  ``{"s5": sid, "cid": n, "owner": uid, "nack": [seq,...]}``（上行缺帧补发请求）/
  ``{"s5": sid, "owner": uid, "hb": ms}`` / ``{"s5": sid, "owner": uid, "end": true, "reason": ...}``

qos0 没有 broker 层重传：双向各自缓存最近 256 帧，缺口先 NACK 补发
（0.8s 首轮、每 1.2s 一轮），硬超时（默认 6s）才断连。数据大帧只走
客户端下发的首选 broker（默认 3 条），控制帧仍全 broker 扇出，把跨洋
出口放大从 13× 降到 3×、同时保留 NACK 全路径补发的容灾。
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import socket
import struct
import sys
import threading
import time

# 本文件已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/socks5_client_mqtt.py」直接运行与包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# 抑制 multi_mqtt import 时挂到 root 的 stderr StreamHandler（broker/paho
# 的 INFO 噪音）：本进程是代理守护，自己的 _info 输出足够，日志走 stderr。
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")
# 直接按脚本启动时没有包上下文，相对导入会失败，显式补上。
if not __package__:
    __package__ = "client"

from . import client_mqtt as _cm            # noqa: E402  复用统一别名表/_cli_opts
from .cmd_client_mqtt import (              # noqa: E402
    MqttTransport, add_connection_args,
)
from .remote_cmd import (                   # noqa: E402
    Transport, RemoteError, RemoteTimeout, RemoteRpcError, RemoteOpError,
    bytes_to_wire, wire_to_bytes, DEFAULT_TIMEOUT, PTY_FRAME_MAX,
    _parse_pty_responders,
)

from multi_mqtt import stime, BROKER_LIST, MultiMQTTManager  # noqa: E402
from server_http import start_rpc_server   # noqa: E402  复用 /r= 表达式求值 RPC

logger = logging.getLogger("socks5_client_mqtt")

# ================= 统一别名表（小写变量名） =================
# 与 client_mqtt.py 同一套规矩：CLI 参数经 _cm._cli_opts(*alias_xxx) 生成；
# 连接类（request_topic/reply_topic/key/allow/timeout）直接复用 client_mqtt
# 的表，由 add_connection_args 注册，这里不重复。
alias_listen_port     = ('port', 'p', 'listen_port', 'socks_port')
alias_listen_host     = ('host', 'listen_host', 'bind')
alias_heartbeat       = ('heartbeat', 'hb')
alias_ttl             = ('ttl', 'max_live_time')
alias_dead_timeout    = ('dead_timeout', 'deadtime', 'dead')
alias_connect_timeout = ('connect_timeout', 'ctimeout', 'dial_timeout')
alias_frame_max       = ('frame_max', 'frame')
alias_gap_timeout     = ('gap_timeout', 'gap')
alias_rpc_port        = ('rpc_port', 'rpc')
alias_rpc_host        = ('rpc_host', 'rpc_ip')

_GAP_DEFAULT = 15.0       # per-cid seq 缺口硬熔断秒数（先经 NACK 多轮重传）
_NACK_FIRST = 0.8         # 缺口出现后多久发首轮重传请求
_NACK_INTV = 1.2          # 后续重传请求轮询间隔（硬熔断前约 4 轮）
_CACHE_MAX = 256          # per-conn 重传缓存帧数（≈256×16KiB=4MiB）
_PB_MAX = 3               # 大帧下行首选 broker 数（控制帧仍全 broker）
# 会话建立即下发的静态种子名单：竞速统计热身（约 45-60s）完成前，大帧若
# 回退主节点会变成全 13 broker 扇出——热身下载本身就能触发账号级惩罚。
# 选历史胜率/隔离性最好的三个，热身完成后由累计胜率名单接管。
_PB_SEED = ("demo.tbmq.io", "broker-cn.emqx.io", "broker.emqx.io")
_PB_INTV = 5.0            # 首选 broker 名单刷新/下发间隔秒
_BULK_INTV = 0.04         # 大帧全局平滑节奏下限（秒/帧 ≈25 帧/s≈400KB/s）
_PACE_START = 0.08        # 会话起跑节奏（≈200KB/s）：宁可慢，不触发惩罚
_PACE_MAX = 0.5           # 最严节奏上限（2 帧/s≈32KB/s，仅惩罚期短暂进入）
_WQ_MAX = 256             # per-conn 写队列帧数上限（≈256×16KiB=4MiB 背压红线）
_INGRESS_MAX = 128        # per-cid 重组暂存帧数上限（防 closed 连接迟到帧堆积）


def _info(msg):
    sys.stderr.write("[%s] %s\n" % (stime(), msg))
    try:
        sys.stderr.flush()
    except Exception:
        pass


# ============================ 服务端模板（自包含，握手时整段下发） ============================
# 与 _PTY_START_TEMPLATE 同一模型：executor 全局命名空间跨握手持久；
# 函数体内不得 print；最后一条裸表达式调用，REPL 语义把返回的 JSON 放进 r。
_SOCKS5_START_TEMPLATE = r'''
def _cmq_socks5_start():
    import json as _j
    _a = _j.loads(__PAYLOAD__)
    _res = {}
    try:
        import os as _o, time as _t, threading as _th, traceback as _tb
        import socket as _sk, queue as _qe

        # 本服务端进程的稳定身份（executor 全局命名空间跨握手持久）。多个持
        # 相同 key 的 server_mqtt 进程会同时应答同一个握手、各自拉起一套
        # 会话往同一 out topic 推流；客户端只认首个应答者，上行帧里的
        # owner 让其他"影子会话"自行了断。
        _g = globals()
        _uid = _g.get("_cmq_server_uid")
        if not _uid:
            _uid = _o.urandom(6).hex()
            _g["_cmq_server_uid"] = _uid
        try:
            _host = _o.uname().nodename
        except Exception:
            try:
                import platform as _pf
                _host = _pf.node() or "?"
            except Exception:
                _host = "?"

        _sid = str(_a["sid"])
        _in_topic = str(_a["in_topic"])
        _out_topic = str(_a["out_topic"])

        # 同进程内同 sid 幂等：重复握手直接返回缓存 env，不拉起第二套线程。
        _sess_lock = _g.get("_cmq_s5_sess_lock")
        if _sess_lock is None:
            _sess_lock = _th.Lock()
            _g["_cmq_s5_sess_lock"] = _sess_lock
        _sessions = _g.setdefault("_cmq_s5_sessions", {})
        with _sess_lock:
            _cached_env = _sessions.get(_sid)
        if _cached_env is not None:
            return _cached_env

        def _find_net():
            # 复用服务端进程里现成的 MQTT 网络层（gms.mqtt_net），不新建连接。
            for _v in list(globals().values()):
                _mn = getattr(_v, "mqtt_net", None)
                if _mn is not None and hasattr(_mn, "publish_broadcast"):
                    _cb = getattr(_v, "handle_message", None)
                    return _mn, (_cb if callable(_cb) else None)
            return None, None

        _net, _srv_cb = _find_net()
        if _net is None:
            raise RuntimeError("服务端没有可复用的 MQTT 管理器（期望 gms.mqtt_net）")

        _frame_max = min(max(512, int(_a.get("frame_max", 16384))), 65536)
        _ttl = min(max(60.0, float(_a.get("ttl", 86400.0))), 604800.0)
        _hb = min(max(0.0, float(_a.get("heartbeat", 5.0))), 3600.0)
        _ctimeout = min(max(1.0, float(_a.get("connect_timeout", 10.0))), 120.0)
        _GAP = min(max(0.5, float(_a.get("gap_timeout", 15.0))), 60.0)
        _NACK_FIRST = 0.8
        _NACK_INTV = 1.2
        _PB_MAX = 3
        _BULK_INTV = 0.04     # 大帧全局平滑节奏（秒/帧 ≈25 帧/s≈400KB/s）
        _pace_intv = 0.08     # 起跑节奏（客户端首帧 ps 前也保守，≈200KB/s）
        _WQ_MAX = 256
        _start = _t.time()

        _end = _th.Event()
        _st0 = {"reason": "stop"}
        _conns = {}          # cid -> conn dict
        _ingress = {}        # cid -> {"next","pending","deadline","nd"}（上行重组）
        _lock = _th.Lock()   # 保护 _conns / _ingress
        _pb = []             # 客户端下发的首选 broker（数据大帧子集扇出）
        _bnet = None         # 数据大帧专用第二路 MQTT 管理器（独立 paho FIFO）
        _bhosts = frozenset()
        _block = _th.Lock()

        # 与 publish_broadcast 相同的序列化（兼容进程开了加密的情况），
        # 子集扇出时只序列化一次再逐个 client.publish。
        import sys as _sys
        _pc = _g.get("process_cipher")
        if _pc is None:
            _mmod = _sys.modules.get(getattr(type(_net), "__module__", ""))
            if _mmod is not None:
                _pc = getattr(_mmod, "process_cipher", None)
        if _pc is None:
            _pc = lambda _d, decrypt=False, enabled=False: \
                _j.dumps(_d, ensure_ascii=False)

        def _pub(_fr, _subset=None):
            # 下行帧唯一出口。_subset=None：全 broker（控制帧/未学到名单前）；
            # 否则只发首选 broker（数据大帧），丢失由客户端 NACK 触发全 broker
            # 补发。多 broker 全量扇出的出口放大约 13 倍，跨洋公共 broker
            # 在突发下会排队/限流并拖死交互流（WS 心跳卡顿）。
            _fr["s5"] = _sid
            _fr["owner"] = _uid
            try:
                if not _subset:
                    _net.publish_broadcast(_out_topic, _fr)
                    return
                _payload = _pc(_fr, decrypt=False,
                               enabled=getattr(_net, "enable_crypto", False))
                with _net.lock:
                    _items = list(_net.clients.items())
                for _h, _cl in _items:
                    if _h in _subset:
                        try:
                            if _cl.is_connected():
                                _cl.publish(_out_topic, _payload, qos=0)
                        except Exception:
                            pass
            except BaseException as _e:
                # paho 断连/队列满等：帧没出去但 seq 已消费会造成客户端缺口。
                # 计数随心跳暴露，供网络分析定位；丢失帧可经 NACK 补发。
                try:
                    _g["_cmq_s5_pub_err"] = _g.get("_cmq_s5_pub_err", 0) + 1
                    _g["_cmq_s5_pub_last"] = "%s: %s" % (
                        type(_e).__name__, _e)
                except Exception:
                    pass

        def _bulk_pub(_fr):
            # 大数据帧专用出口：只连首选 broker 的**独立第二路 paho 连接**。
            # 实测同一条 paho 连接上大帧突发会把 1s 的小帧在发送 FIFO 里压
            # 十几秒（broker/链路按连接排队）；大帧走独立连接后控制/交互
            # 帧（走 _net 主连接，全 broker）不再被队头阻塞。名单变化时
            # 惰性重建，重建期间退回主连接子集发送。
            global _bnet, _bhosts
            _want = frozenset(_pb[:_PB_MAX])
            _node = None
            if _want:
                with _block:
                    if _want != _bhosts:
                        _old = _bnet
                        try:
                            _cls = _g.get("MultiMQTTManager") or type(_net)
                            _bl = _g.get("BROKER_LIST")
                            _bsub = [_b for _b in _bl if _b[0] in _want] \
                                if _bl else []
                            if len(_bsub) == len(_want):
                                # 先建新再停旧：名单有在位者保护，重建罕见，
                                # 零数据窗口优先；切换后立即异步停旧。
                                _nn = _cls(
                                    brokers=_bsub, enable_stats=False,
                                    keepalive=getattr(_net, "keepalive", 300))
                                _nn.start()
                                try:
                                    _nn.wait_connected(min_count=1, timeout=4.0)
                                except Exception:
                                    pass
                                _bnet = _nn
                                _bhosts = _want
                                _node = _nn
                                if _old is not None:
                                    def _stop_old(_o=_old):
                                        try:
                                            _o.stop()
                                        except Exception:
                                            pass
                                    _th.Thread(target=_stop_old,
                                               daemon=True).start()
                        except Exception:
                            _bnet = None
                            _bhosts = frozenset()
                    else:
                        _node = _bnet
            if _node is not None and getattr(_node, "clients", None):
                _fr["s5"] = _sid
                _fr["owner"] = _uid
                try:
                    _payload = _pc(_fr, decrypt=False,
                                   enabled=getattr(_node, "enable_crypto", False))
                    with _node.lock:
                        _items = list(_node.clients.items())
                    for _h, _cl in _items:
                        try:
                            if _cl.is_connected():
                                _cl.publish(_out_topic, _payload, qos=0)
                        except Exception:
                            pass
                    return
                except BaseException as _e:
                    try:
                        _g["_cmq_s5_pub_err"] = _g.get("_cmq_s5_pub_err", 0) + 1
                        _g["_cmq_s5_pub_last"] = "%s: %s" % (
                            type(_e).__name__, _e)
                    except Exception:
                        pass
            # bulk 连接未就绪/发送异常：退回主连接（子集或全 broker）
            _pub(_fr, set(_want) if _want else None)

        # 大帧平滑队列：所有连接的大帧先入队（有界），单线程按 _BULK_INTV
        # 匀速投出。实测公共 broker 对微突发会按账号施加十几秒级惩罚（连
        # 同账号第二连接上的小帧 hb 都被拖 15s）；匀速后既削惩罚又让 paho
        # FIFO 不堆积。队列满反压到 _conn_main → 读目标 socket 变慢 →
        # TCP 窗口自然闭合，背压链完整。小帧绝不入队，直接全 broker。
        _bq = _qe.Queue(maxsize=64)
        _rt_last = {}   # NACK 大帧补发去重 (cid,seq)->monotonic

        def _bsend_loop():
            global _pace_intv
            _last = 0.0
            while True:
                try:
                    _fr = _bq.get(timeout=0.5)
                except _qe.Empty:
                    if _end.is_set():
                        return
                    continue
                if _fr is None:
                    return
                _w = _pace_intv - (_t.monotonic() - _last)
                if _w > 0:
                    _t.sleep(_w)
                _last = _t.monotonic()
                try:
                    _bulk_pub(_fr)
                except Exception:
                    pass

        def _bq_put(_fr):
            # 会话结束时不阻塞收尾线程
            while not _end.is_set():
                try:
                    _bq.put(_fr, timeout=0.5)
                    return
                except _qe.Full:
                    pass

        def _cache_put(_c, _fr):
            # per-conn 下行重传缓存（NACK 补发用，qos0 无 broker 层重传）。
            try:
                _rc = _c["rcache"]
                _rc[int(_fr["seq"])] = _fr
                if len(_rc) > 256:
                    for _k in sorted(_rc)[:64]:
                        _rc.pop(_k, None)
            except Exception:
                pass

        def _emit(_cid, _c, _fr, _bulk=False):
            # 缓存 + 发送。大帧进**平滑队列**（匀速走 bulk 专用连接）；
            # 小帧（WS ping/pong、命令行交互等）立即走主连接全 broker，
            # 永远不被大帧节奏拖住。
            _cache_put(_c, _fr)
            if _bulk:
                _d2 = _fr.get("d")
                if isinstance(_d2, str) and len(_d2) > 2048:
                    _bq_put(_fr)
                    return
            _pub(_fr)

        def _retrans(_cid, _seqs):
            # 客户端上行 NACK 的对称处理（这里是下行缺帧补发）：从下行缓存
            # 原样补发（seq 不变，客户端去重）。大帧必须走 bulk 平滑队列且
            # 每 (cid,seq) 2s 至多一次——多轮 NACK 把同一批 16KiB 帧反复
            # 全 broker 扇出，会直接触发公共 broker 账号级限流惩罚。
            with _lock:
                _c = _conns.get(_cid)
                _frs = list(_c["rcache"].values()) if _c is not None else []
            _want = set()
            for _x in (_seqs or []):
                try:
                    _want.add(int(_x))
                except Exception:
                    pass
            _now2 = _t.monotonic()
            for _fr in _frs:
                try:
                    if int(_fr.get("seq", -1)) not in _want:
                        continue
                    if isinstance(_fr.get("d"), str) and len(_fr["d"]) > 2048:
                        _key = (_cid, int(_fr.get("seq", -1)))
                        if _now2 - _rt_last.get(_key, 0.0) < 2.0:
                            continue
                        _rt_last[_key] = _now2
                        _bq_put(dict(_fr))
                    else:
                        _pub(dict(_fr))
                except Exception:
                    pass

        def _wake_writer(_c):
            # 读侧结束（目标拒连/EOF）后唤醒 writer：否则只读长连接
            # （WebSocket/SSE，客户端永不发 close）的 writer 会永久阻塞在
            # wq.get()，_conns 条目也永不摘除——每条此类连接泄漏一个线程
            # + 一个字典条目。writer 会先排空 wq 残余数据再 SHUT_WR 退出。
            if _c.get("w_done"):
                return
            try:
                _c["wq"].put_nowait(None)
            except Exception:
                pass

        def _gc(_cid, _c):
            # 读/写两侧都结束后才真正关 socket 并摘除连接：
            # 半关语义（客户端 close → 只对目标 SHUT_WR，目标响应继续回传）
            # 要求一侧 EOF 时另一侧还能把队列里的数据发完。
            if not (_c["r_done"] and _c["w_done"]):
                return
            with _lock:
                _conns.pop(_cid, None)
                _ingress.pop(_cid, None)
            try:
                if _c.get("sock") is not None:
                    _c["sock"].close()
            except Exception:
                pass

        def _close_conn(_cid, _reason, _send=True):
            # 强制收尾（上行缺口/会话结束/背压/写失败）：立即关双侧。
            with _lock:
                _c = _conns.pop(_cid, None)
                _ingress.pop(_cid, None)
            if _c is None:
                return
            _c["closed"] = True
            _c["r_done"] = True
            _c["w_done"] = True
            try:
                _c["wq"].put_nowait(None)
            except Exception:
                pass
            try:
                if _c.get("sock") is not None:
                    _c["sock"].close()
            except Exception:
                pass
            if _send:
                try:
                    with _c["seq_lock"]:
                        _sq = _c["out_seq"]
                        _c["out_seq"] += 1
                        _fr = {"cid": _cid, "seq": _sq,
                               "closed": True, "reason": _reason}
                    _cache_put(_c, _fr)
                    _pub(_fr)  # 收尾控制帧必须全 broker
                except Exception:
                    pass

        def _writer(_cid, _c):
            # 写队列 -> 目标 socket。收到 None（客户端 in-seq close 或强收尾）
            # 后排空已尽，半关写端让目标看到 EOF；读线程继续收响应。
            try:
                while True:
                    _item = _c["wq"].get()
                    if _item is None:
                        break
                    _s = _c.get("sock")
                    if _s is None:
                        break
                    _s.sendall(_item)
            except Exception:
                _close_conn(_cid, "write_error")
                return
            _c["w_done"] = True
            try:
                _s = _c.get("sock")
                if _s is not None and not _c["closed"]:
                    _s.shutdown(_sk.SHUT_WR)
            except Exception:
                pass
            _gc(_cid, _c)

        def _conn_main(_cid, _c, _host2, _port):
            # 连接目标 + 读循环：socket -> 下行帧（opened=0，数据 1..）。
            try:
                _s = _sk.create_connection((_host2, _port), timeout=_ctimeout)
                _s.settimeout(None)
            except Exception as _e:
                with _c["seq_lock"]:
                    _fr0 = {"cid": _cid, "seq": 0, "opened": {
                        "ok": False,
                        "error": "%s: %s" % (type(_e).__name__, _e)}}
                    _cache_put(_c, _fr0)
                    _pub(_fr0)
                    _c["out_seq"] = 1
                    _fr1 = {"cid": _cid, "seq": _c["out_seq"],
                            "closed": True, "reason": "connect_failed"}
                    _c["out_seq"] += 1
                    _cache_put(_c, _fr1)
                    _pub(_fr1)
                _c["r_done"] = True
                # sock 根本没建出来：必须唤醒 writer，否则它阻塞在 wq.get()
                # 上永不退出，_conns 条目也永不摘除（线程+条目双泄漏）。
                _wake_writer(_c)
                _gc(_cid, _c)
                return
            _c["sock"] = _s
            with _c["seq_lock"]:
                _fr0 = {"cid": _cid, "seq": 0, "opened": {"ok": True}}
                _cache_put(_c, _fr0)
                _pub(_fr0)  # opened 控制帧全 broker
                _c["out_seq"] = 1
            while not _end.is_set() and not _c["closed"]:
                try:
                    _data = _s.recv(_frame_max)
                except OSError:
                    break
                except Exception:
                    break
                if not _data:
                    break
                with _c["seq_lock"]:
                    _fr = {"cid": _cid, "seq": _c["out_seq"],
                           "d": _data.decode("latin-1")}
                    _c["out_seq"] += 1
                # 数据大帧走首选 broker 子集（降低出口放大），丢失走 NACK 补发
                _emit(_cid, _c, _fr, _bulk=True)
            _c["r_done"] = True
            if not _c["closed"]:
                try:
                    with _c["seq_lock"]:
                        _fr = {"cid": _cid, "seq": _c["out_seq"],
                               "closed": True, "reason": "eof"}
                        _c["out_seq"] += 1
                    _cache_put(_c, _fr)
                    _pub(_fr)  # closed 控制帧全 broker
                except Exception:
                    pass
                # 客户端若是只读不写的长连接（WS/SSE）可能永不发 close，
                # 不唤醒 writer 它就一直挂在 wq.get() 上直到会话结束。
                _wake_writer(_c)
            _gc(_cid, _c)

        def _deliver(_cid, _fr):
            # 按序放行一帧（由 in_loop 的重组缓冲调用，单线程）。
            _op = _fr.get("open")
            if isinstance(_op, dict):
                with _lock:
                    if _cid in _conns:
                        return  # seq 闸门已挡重复 open，这里双保险
                    _c = {"sock": None, "wq": _qe.Queue(_WQ_MAX),
                          "closed": False, "r_done": False, "w_done": False,
                          "seq_lock": _th.Lock(), "out_seq": 0,
                          "rcache": {}}
                    _conns[_cid] = _c
                _th.Thread(target=_conn_main,
                           args=(_cid, _c, str(_op.get("host")),
                                 int(_op.get("port"))),
                           name="s5-conn-%s" % _cid, daemon=True).start()
                _th.Thread(target=_writer, args=(_cid, _c),
                           name="s5-wr-%s" % _cid, daemon=True).start()
                return
            with _lock:
                _c = _conns.get(_cid)
            if _c is None or _c["closed"]:
                return
            if _fr.get("d") is not None:
                try:
                    _c["wq"].put_nowait(_fr["d"].encode("latin-1"))
                except _qe.Full:
                    _close_conn(_cid, "backpressure")
                except Exception:
                    pass
            elif _fr.get("close"):
                try:
                    _c["wq"].put_nowait(None)
                except Exception:
                    _close_conn(_cid, "backpressure")

        def _in_loop():
            # 上行重组：同一帧被每个 broker 各投递一次且会乱序。按 per-cid
            # seq 连续放行；缺口超 _GAP 秒判真丢帧——SOCKS5 承载 TCP 流，
            # 跳号=数据损坏，必须断连而不是像 PTY 那样跳号放行。
            global _pb, _pace_intv
            while not _end.is_set():
                try:
                    _fr = _inq.get(timeout=0.25)
                except _qe.Empty:
                    _fr = None
                if _t.time() - _start >= _ttl:
                    _st0["reason"] = "ttl"
                    _end.set()
                    break
                _now = _t.monotonic()
                _nacks = []
                with _lock:
                    _gaps = []
                    for _c2, _s2 in _ingress.items():
                        if not _s2["pending"]:
                            continue
                        if _s2["deadline"] and _now > _s2["deadline"]:
                            _gaps.append(_c2)
                        elif _s2["nd"] is not None and _now > _s2["nd"]:
                            _hi = min(_s2["pending"])
                            _nacks.append((_c2, list(range(
                                _s2["next"],
                                min(_hi + 1, _s2["next"] + 256)))))
                            _s2["nd"] = _now + min(
                                _NACK_INTV * (1.5 ** _s2.get("nr", 0)), 3.0)
                            _s2["nr"] = _s2.get("nr", 0) + 1
                for _c2 in _gaps:
                    _close_conn(_c2, "uplink_gap")
                for _c2, _seqs in _nacks:
                    # 上行缺口：让客户端从它的 per-conn 缓存补发（全 broker
                    # 控制帧）；硬超时仍补不齐才 uplink_gap 断连。
                    try:
                        _pub({"cid": _c2, "nack": _seqs})
                    except Exception:
                        pass
                if _fr is None:
                    continue
                if not isinstance(_fr, dict) or _fr.get("s5") != _sid:
                    continue
                _ow = _fr.get("owner")
                if _ow is not None and _ow != _uid:
                    # 归属仲裁：客户端只认首个握手应答者；收到不匹配的帧
                    # 说明本进程是影子会话，立即自杀，必须在放行之前判断。
                    _st0["reason"] = "claim_lost"
                    _end.set()
                    break
                if _fr.get("stop"):
                    _st0["reason"] = "stopped"
                    _end.set()
                    break
                if _fr.get("pb") is not None:
                    # 客户端首选 broker 名单：数据大帧子集扇出
                    _pbv = _fr.get("pb")
                    if isinstance(_pbv, (list, tuple)):
                        _pb = [str(_x) for _x in _pbv][:_PB_MAX]
                    continue
                if _fr.get("ps") is not None:
                    # 客户端自适应 pacing 指令（秒/帧，0.02~1.0）
                    try:
                        _pv = float(_fr.get("ps"))
                        if 0.02 <= _pv <= 1.0:
                            _pace_intv = _pv
                    except Exception:
                        pass
                    continue
                _cid = _fr.get("cid")
                if _cid is None:
                    continue  # claim 等无 cid 控制帧：owner 校验后无事可做
                try:
                    _cid = int(_cid)
                except Exception:
                    continue
                _nkv = _fr.get("nack")
                if isinstance(_nkv, (list, tuple)):
                    _retrans(_cid, _nkv)  # 下行缺帧补发请求
                    continue
                _sq = _fr.get("seq")
                if _sq is None:
                    _deliver(_cid, _fr)  # 无 seq 帧宽松放行（兼容）
                    continue
                _ready = []
                with _lock:
                    _st = _ingress.get(_cid)
                    if _st is None:
                        _st = {"next": 0, "pending": {}, "deadline": None,
                               "nd": None, "nr": 0}
                        _ingress[_cid] = _st
                    _sq = int(_sq)
                    if _sq >= _st["next"]:
                        if len(_st["pending"]) < 128:
                            _st["pending"][_sq] = _fr
                        while _st["next"] in _st["pending"]:
                            _ready.append(_st["pending"].pop(_st["next"]))
                            _st["next"] += 1
                        if _st["pending"]:
                            _st["deadline"] = _now + _GAP
                            if _st["nd"] is None:
                                _st["nd"] = _now + _NACK_FIRST
                                _st["nr"] = 0
                        else:
                            _st["deadline"] = None
                            _st["nd"] = None
                            _st["nr"] = 0
                    # _sq < next：多 broker 重复副本，丢弃
                for _f2 in _ready:
                    _deliver(_cid, _f2)

            # 会话结束：关全部连接、退订、弹注册表、发 end 帧
            with _lock:
                _all = list(_conns.keys())
            for _c3 in _all:
                _close_conn(_c3, "session_end")
            try:
                _net._cmq_pty_router.pop(_in_topic, None)
            except Exception:
                pass
            try:
                with _sess_lock:
                    _sessions.pop(_sid, None)
            except Exception:
                pass
            try:
                _net.publish_broadcast(
                    _out_topic,
                    {"s5": _sid, "end": True,
                     "reason": _st0["reason"], "owner": _uid})
            except Exception:
                pass
            try:
                if _bnet is not None:
                    _bnet.stop()
            except Exception:
                pass
            try:
                while True:
                    _bq.get_nowait()       # 丢弃连接已关后的残余大帧
            except Exception:
                pass
            try:
                _bq.put_nowait(None)   # 停平滑发送线程
            except Exception:
                pass

        # ---- 路由安装：与 PTY 共用同一份 _cmq_pty_* 基建（按 topic 分发到
        # 各自队列）。哪个模板先握手就由谁安装，后到者只注册 topic；
        # 幂等标记保证全程只安装一次，链底永不被二次握手覆盖（含旧版
        # 自裹闭环的自愈：链底固定用服务端实例的 handle_message）。
        _rlocal = _th.local()

        def _router(_topic, _data, _broker):
            if getattr(_rlocal, "depth", 0) >= 8:
                return None
            _rlocal.depth = getattr(_rlocal, "depth", 0) + 1
            try:
                _q = _net._cmq_pty_router.get(_topic)
                if _q is not None and isinstance(_data, dict):
                    _q.put(_data)
                    return None
                return _net._cmq_pty_orig(_topic, _data, _broker)
            finally:
                _rlocal.depth -= 1

        if not getattr(_net, "_cmq_pty_installed", False):
            _base = _srv_cb if _srv_cb is not None else _net.message_callback
            _net._cmq_pty_orig = _base
            _net._cmq_pty_router = {}
            _net._cmq_pty_installed = True
            _net.set_on_message(_router)

        _inq = _qe.Queue()
        _net._cmq_pty_router[_in_topic] = _inq
        _net.subscribe(_in_topic)

        _th.Thread(target=_in_loop, name="s5-in", daemon=True).start()

        # 服务端一侧各 broker 连接快照（读内置 ConnectionQualityStats 的
        # 存量字段：is_connected/disconnect_count/avg_latency，后者来自
        # 内置 MQTT ping，均为已有记账，零额外网络请求）。随心跳捎带给客户端。
        def _srv_brokers():
            try:
                _st = getattr(_net, "stats", None)
                if _st is None:
                    return None
                with _st.lock:
                    _items = list(_st.stats.items())
                _out = {}
                for _h, _bs in _items:
                    try:
                        _out[_h] = [1 if _bs.is_connected else 0,
                                    _bs.disconnect_count,
                                    int(_bs.avg_latency)]
                    except Exception:
                        pass
                return _out
            except Exception:
                return None

        # 会话内 per-cid 传输水位（随心跳暴露，供丢帧定位）：
        # [下行已发最大seq(out_seq-1), 上行已放行最大seq, r_done, w_done, closed]
        def _srv_conns():
            try:
                with _lock:
                    _items = list(_conns.items())
                    _ing = dict(_ingress)
                _out = {}
                for _cid, _c in _items:
                    with _c["seq_lock"]:
                        _dmax = _c["out_seq"] - 1
                    _st = _ing.get(_cid)
                    _umax = (_st["next"] - 1) if _st is not None else None
                    _out[str(_cid)] = [_dmax, _umax,
                                       1 if _c["r_done"] else 0,
                                       1 if _c["w_done"] else 0,
                                       1 if _c["closed"] else 0]
                return _out
            except Exception:
                return None

        # 心跳：服务端进程活着就周期发一帧，客户端据此区分"没流量"和
        # "服务器已死"。hb=0 时空转（无线程发不出热调心跳，本协议不支持热调）。
        def _hb_loop():
            _next = _t.time()
            while not _end.wait(0.5):
                if _hb <= 0.0:
                    continue
                _now = _t.time()
                if _now < _next:
                    continue
                _next = _now + _hb
                try:
                    _pub({"hb": int(_t.time() * 1000),
                          "srv": _srv_brokers(),
                          "c": _srv_conns(),
                          "pe": _g.get("_cmq_s5_pub_err", 0),
                          "pl": _g.get("_cmq_s5_pub_last")})
                except Exception:
                    pass

        _th.Thread(target=_hb_loop, name="s5-hb", daemon=True).start()
        _th.Thread(target=_bsend_loop, name="s5-bulk", daemon=True).start()

        _res = {"ok": True, "sid": _sid, "owner": _uid, "host": _host,
                "in_topic": _in_topic, "out_topic": _out_topic,
                "heartbeat": _hb, "ttl": _ttl, "frame_max": _frame_max,
                "connect_timeout": _ctimeout, "gap_timeout": _GAP}
        _env_json = _j.dumps(_res, ensure_ascii=False)
        with _sess_lock:
            _sessions[_sid] = _env_json
    except Exception:
        _res = {"ok": False, "error": _tb.format_exc()}
        _env_json = _j.dumps(_res, ensure_ascii=False)
    return _env_json
_cmq_socks5_start()
'''


def build_socks5_start_code(payload: dict) -> str:
    """把 SOCKS5 启动信封编译成远端可直接执行的自包含 Python 代码。"""
    lit = json.dumps(json.dumps(payload, ensure_ascii=False))
    return _SOCKS5_START_TEMPLATE.replace("__PAYLOAD__", lit)


# ============================ 客户端会话 ============================

class _ConnReassembly:
    """per-cid 单向字节流重组缓冲。

    多 broker 冗余：同一帧到多份（seq<next 丢弃），各路径乱序（暂存等前序）。
    与 PTY 的 _PtyReorderBuffer 唯一但关键的区别：缺口**绝不跳号**——
    TCP 流丢一段就是损坏。缺口后先经 NACK 请求服务端从 per-conn 重传缓存
    补发（多轮），硬超时仍补不齐才熔断该连接。
    """

    def __init__(self, deliver, on_gap, on_nack, gap=_GAP_DEFAULT,
                 nack_first=_NACK_FIRST, nack_intv=_NACK_INTV):
        self.next = 0
        self.pending = {}
        self.deadline = None       # 硬熔断时刻
        self.ndeadline = None      # 下一轮 NACK 时刻
        self.gap = float(gap)
        self.nack_first = float(nack_first)
        self.nack_intv = float(nack_intv)
        self.nround = 0           # NACK 轮次（间隔指数退避，封顶 3s）
        self.broken = False
        self._deliver = deliver
        self._on_gap = on_gap
        self._on_nack = on_nack
        self._lock = threading.Lock()

    def add(self, seq: int, frame: dict) -> bool:
        """返回 True=该 (cid,seq) 的首个副本（投递它的 broker 赢了竞速），
        False=迟到重复副本/已熔断。供 per-broker 竞速统计用。"""
        ready = []
        with self._lock:
            if self.broken:
                return False
            seq = int(seq)
            if seq < self.next or seq in self.pending:
                return False  # 重复副本
            if len(self.pending) < _INGRESS_MAX:
                self.pending[seq] = frame
            while self.next in self.pending:
                ready.append(self.pending.pop(self.next))
                self.next += 1
            if self.pending:
                self.deadline = time.monotonic() + self.gap
                if self.ndeadline is None:
                    self.ndeadline = time.monotonic() + self.nack_first
                    self.nround = 0
            else:
                self.deadline = None
                self.ndeadline = None
                self.nround = 0
        for fr in ready:
            self._deliver(fr)
        return True

    def check(self) -> None:
        """由会话清扫线程周期调用：到点发 NACK；硬超时才熔断。"""
        nack_seqs = None
        fire_gap = False
        with self._lock:
            if self.broken or not self.pending:
                return
            now = time.monotonic()
            if self.ndeadline is not None and now > self.ndeadline:
                # 只要连续前缀缺口（next .. 首个已缓存帧-1），补发即可解锁
                hi = min(self.pending)
                nack_seqs = list(
                    range(self.next, min(hi + 1, self.next + _CACHE_MAX)))
                # 间隔指数退避（1.2, 1.8, 2.7...封顶 3s）：缺帧往往只是
                # 慢副本在路上，高频全 broker 重传会触发账号级限流惩罚。
                self.ndeadline = now + min(
                    self.nack_intv * (1.5 ** self.nround), 3.0)
                self.nround += 1
            if self.deadline is not None and now > self.deadline:
                self.broken = True
                self.pending.clear()
                fire_gap = True
        if nack_seqs:
            try:
                self._on_nack(nack_seqs)
            except Exception:
                pass
        if fire_gap:
            try:
                self._on_gap()
            except Exception:
                pass

    def close(self) -> None:
        with self._lock:
            self.broken = True
            self.pending.clear()


class _ClientConn:
    """一条本地 SOCKS5 连接对应的会话内状态。"""

    __slots__ = ("cid", "desc", "wq", "opened", "open_result", "reassembly",
                 "local_sock", "closed", "remote_closed", "seq_lock",
                 "out_seq", "bytes_up", "bytes_down", "created",
                 "ucache", "local_read_done")

    def __init__(self, cid, desc, reassembly):
        self.cid = cid
        self.desc = desc
        self.wq = queue.Queue(_WQ_MAX)
        self.opened = threading.Event()
        self.open_result = None
        self.reassembly = reassembly
        self.local_sock = None
        self.closed = False
        self.remote_closed = False
        self.seq_lock = threading.Lock()
        self.out_seq = 0          # 上行 per-cid seq（open=0，其后递增）
        self.bytes_up = 0
        self.bytes_down = 0
        self.created = time.time()
        # 上行重传缓存：seq -> 已发完整帧（服务端 NACK 时原样重发）
        self.ucache = {}
        # 本地读线程已退出（应用已关本地连接）：writer 发 FIN 后据此安全 close
        self.local_read_done = threading.Event()


class RemoteSocks5:
    """远端 SOCKS5 出口会话客户端，网络层由注入的 transport 决定。

    生命周期：:meth:`open` → :meth:`open_connection` / :meth:`send_data` /
    :meth:`eof_connection` / :meth:`drop_connection` → :meth:`close`。

    transport 需具备可选能力：``publish`` / ``stream_subscribe`` /
    ``stream_unsubscribe``（MQTT 版见 cmd_client_mqtt.MqttTransport）。
    """

    def __init__(self, transport: Transport, timeout: float = DEFAULT_TIMEOUT,
                 on_log=None):
        if not isinstance(transport, Transport):
            raise TypeError("transport 必须是 remote_cmd.Transport 实例")
        self.tr = transport
        self.timeout = timeout
        self.on_log = on_log or (lambda m: None)
        self.sid = None
        self.in_topic = None
        self.out_topic = None
        self.server_info = None
        self.owner = None
        self.responders = []
        self.foreign_frames = 0   # 被丢弃的影子会话帧数（诊断用）
        self.end_reason = None
        self.last_signal = time.monotonic()
        self.dead_timeout = 0.0
        self.gap_timeout = _GAP_DEFAULT
        self.frame_max = int(PTY_FRAME_MAX)
        self._conns = {}
        self._cid_next = 0
        self._lock = threading.Lock()
        self._end_event = threading.Event()
        self._handler = None
        # ---- per-broker 到达统计（纯被动观察已收到的帧，零额外网络请求）----
        # broker -> {"rx","wins","dups","hb_n","hb_min","hb_sum","hb_max","last"}
        self.broker_stats = {}
        self.first_frame_broker = None     # 本会话第一帧（含握手后 hb）的到达 broker
        self.server_brokers = None         # 最近一次 hb 捎带的服务端 broker 快照
        self.server_conns = None           # 最近一次 hb 捎带的服务端 per-cid 水位
        self.server_pub_err = 0            # 服务端 publish 异常计数
        self.server_pub_last = None
        self._created_at = time.time()
        self.total_bytes_up = 0            # 会话级累计（含已关闭连接）
        self.total_bytes_down = 0
        # 上行大帧专用第二路 MQTT 连接（与服务端 _bulk_pub 对称）：避免上传
        # 突发把 ping/NACK 等小帧压在同一条 paho FIFO 队尾。
        self._bnet = None
        self._bhosts = frozenset()
        self._bnet_lock = threading.Lock()
        self._bulk_q = queue.Queue(maxsize=64)  # 上行大帧平滑队列
        self._rt_last = {}                        # NACK 大帧补发去重 (cid,seq)->t
        self.pace_intv = _PACE_START              # 当前大帧节奏（闭环热调）
        self.hb_floor = None                      # 全 broker 实测 hb 延迟地板（含时钟偏差）
        self._pace_ok = 0                         # 连续健康轮次（恢复要慢）
        self._last_ps = 0.0                       # 上次下发 ps 指令时刻
        self._open_mono = time.monotonic()      # 会话起点（pb 热身计时用）

    # ---- 握手 ----

    def _check_caps(self):
        missing = [n for n in ("publish", "stream_subscribe", "stream_unsubscribe")
                   if not callable(getattr(self.tr, n, None))]
        if missing:
            raise RemoteError("当前 transport 不支持 SOCKS5 会话（缺少: %s）"
                              % ", ".join(missing))

    def open(self, *, ttl=86400.0, heartbeat=5.0, frame_max=PTY_FRAME_MAX,
             connect_timeout=10.0, gap_timeout=_GAP_DEFAULT, sid=None,
             in_topic=None, out_topic=None, req_timeout=None,
             owner_gather=0.8) -> dict:
        """协商并下发服务端代码，返回服务端确认信息（含 owner/topic）。"""
        self._check_caps()
        sid = sid or ("s5-%d-%s" % (int(time.time() * 1000),
                                    os.urandom(2).hex()))
        in_topic = in_topic or ("s5/%s/in" % sid)
        out_topic = out_topic or ("s5/%s/out" % sid)
        req_timeout = req_timeout or max(float(self.timeout), 30.0)
        self.frame_max = int(frame_max)
        self.gap_timeout = float(gap_timeout)

        def handler(data, broker=None):
            self._on_frame(data, broker)

        self._handler = handler
        # 先订阅再启动，避免丢失最早的下行帧
        self.tr.stream_subscribe(out_topic, handler)
        payload = {"sid": sid, "in_topic": in_topic, "out_topic": out_topic,
                   "ttl": float(ttl), "heartbeat": max(0.0, float(heartbeat)),
                   "frame_max": int(frame_max),
                   "connect_timeout": float(connect_timeout),
                   "gap_timeout": float(gap_timeout)}
        code = build_socks5_start_code(payload)
        try:
            request_many = getattr(self.tr, "request_many", None)
            if callable(request_many) and owner_gather > 0:
                resp, extras = request_many(code, req_timeout, owner_gather)
            else:
                resp, extras = self.tr.request(code, req_timeout), []
            if not resp:
                raise RemoteTimeout("SOCKS5 会话启动请求超时无回包")
            try:
                env = json.loads(resp.get("r") or "")
            except (ValueError, TypeError) as e:
                raise RemoteRpcError("无法解析 SOCKS5 启动回包: %s" % e, resp)
            if not env.get("ok"):
                raise RemoteOpError(env.get("error", "socks5 start failed"),
                                    env, resp)
        except Exception:
            try:
                self.tr.stream_unsubscribe(out_topic, handler)
            except Exception:
                pass
            raise
        self.sid = sid
        self.in_topic = in_topic
        self.out_topic = out_topic
        self._open_mono = time.monotonic()
        self.server_info = env
        self.owner = env.get("owner")
        self.responders = _parse_pty_responders(resp, extras)
        self.last_signal = time.monotonic()
        if self.owner:
            # 立即发 claim：影子服务端在第一时间自杀（owner 不匹配即结束）
            try:
                self.tr.publish(self.in_topic,
                                {"s5": self.sid, "claim": True,
                                 "owner": self.owner})
            except Exception:
                pass
        threading.Thread(target=self._sweep_loop, name="s5-sweep",
                         daemon=True).start()
        threading.Thread(target=self._pb_loop, name="s5-pb",
                         daemon=True).start()
        threading.Thread(target=self._bulk_send_loop, name="s5-bulk",
                         daemon=True).start()
        # 立刻下发种子名单并建立本端 bulk 连接：竞速热身完成前大帧也只走
        # 3 条 broker，绝不回退成全 13 broker 扇出。
        try:
            seed = [b for b in _PB_SEED]
            self.tr.publish(self.in_topic,
                            {"s5": self.sid, "owner": self.owner,
                             "pb": seed})
            threading.Thread(target=self._ensure_bnet,
                             args=(seed,), daemon=True).start()
        except Exception:
            pass
        return env

    # ---- 帧收发 ----

    @property
    def ended(self) -> bool:
        return self._end_event.is_set()

    def _send(self, conn: _ClientConn, extra: dict) -> None:
        """上行帧唯一出口：per-cid 打号 + 盖 owner 后发出。

        打号与 publish 在同一把 per-conn 锁内原子完成（与 PTY 的 _publish_input
        同因）：否则读线程/收尾线程并发时序号与投递顺序不一致，服务端重组
        会把晚到的低 seq 当真丢帧，NACK/超时后误断连接。帧副本进 per-conn
        重传缓存，服务端上行缺口回 NACK 时原样补发（qos0 无 broker 重传）。
        """
        with conn.seq_lock:
            frame = {"s5": self.sid, "cid": conn.cid, "seq": conn.out_seq}
            conn.out_seq += 1
            if self.owner:
                frame["owner"] = self.owner
            frame.update(extra)
            conn.ucache[frame["seq"]] = frame
            if len(conn.ucache) > _CACHE_MAX:
                for _k in sorted(conn.ucache)[:64]:
                    conn.ucache.pop(_k, None)
            _d = frame.get("d")
            if isinstance(_d, str) and len(_d) > 2048:
                self._bulk_enqueue(frame)    # 大帧：平滑队列→专用连接
            else:
                self.tr.publish(self.in_topic, frame)

    def _send_nack(self, cid: int, seqs) -> None:
        """上行缺口重传请求（控制帧，无 seq，走全 broker）。"""
        try:
            self.tr.publish(self.in_topic,
                            {"s5": self.sid, "cid": int(cid),
                             "owner": self.owner, "nack": [int(s) for s in seqs]})
        except Exception:
            pass

    def _resend_uplink(self, conn: _ClientConn, seqs) -> None:
        """服务端回 NACK：从上行重传缓存原样补发（seq 不变，服务端去重）。
        大帧走 bulk 平滑队列并做 2s 去重——多轮 NACK 若把同一批 16KiB 帧
        反复全 broker 扇出，会立刻触发公共 broker 的账号级限流惩罚。"""
        now = time.monotonic()
        for s in seqs:
            try:
                si = int(s)
            except (TypeError, ValueError):
                continue
            fr = conn.ucache.get(si)
            if fr is None:
                continue
            big = isinstance(fr.get("d"), str) and len(fr["d"]) > 2048
            if big:
                key = (conn.cid, si)
                last = self._rt_last.get(key, 0.0)
                if now - last < 2.0:
                    continue
                self._rt_last[key] = now
                self._bulk_enqueue(fr)
            else:
                try:
                    self.tr.publish(self.in_topic, fr)
                except Exception:
                    pass

    def _track_broker(self, broker, data, is_new=None) -> None:
        """per-broker 到达统计。is_new：seq 帧的首到/重复判定（由重组缓冲
        返回）；hb 帧顺带测该 broker 的下行延迟（同一 hb 经全部 broker
        广播，帧内服务器时间戳相同，横向比较有效；跨机器含时钟偏差，仅
        相对比较有意义）。hb 还捎带服务端各 broker 连接快照。"""
        if broker is None:
            return
        st = self.broker_stats.get(broker)
        if st is None:
            st = {"rx": 0, "wins": 0, "dups": 0,
                  "hb_n": 0, "hb_min": None, "hb_sum": 0, "hb_max": None,
                  "hb_last_ms": None, "hb_last_t": 0.0}
            self.broker_stats[broker] = st
        st["rx"] += 1
        if self.first_frame_broker is None:
            self.first_frame_broker = broker
        if is_new is True:
            st["wins"] += 1
        elif is_new is False:
            st["dups"] += 1
        hb = data.get("hb")
        if hb is not None:
            try:
                d = int(time.time() * 1000) - int(hb)
            except (TypeError, ValueError):
                return
            st["hb_n"] += 1
            st["hb_sum"] += d
            st["hb_last_ms"] = d
            st["hb_last_t"] = time.monotonic()
            if self.hb_floor is None:
                self.hb_floor = d
            elif d < self.hb_floor:
                self.hb_floor = d
            st["hb_min"] = d if st["hb_min"] is None else min(st["hb_min"], d)
            st["hb_max"] = d if st["hb_max"] is None else max(st["hb_max"], d)
            srv = data.get("srv")
            if isinstance(srv, dict):
                self.server_brokers = {"ts": int(time.time()), "brokers": srv}
            # 服务端 per-cid 传输水位（诊断真丢帧：服务端已发 seq vs 客户端实收）
            if isinstance(data.get("c"), dict):
                self.server_conns = {"ts": int(time.time()), "conns": data["c"]}
            if data.get("pe"):
                self.server_pub_err = data.get("pe")
                self.server_pub_last = data.get("pl")

    def _on_frame(self, data, broker=None) -> None:
        if not isinstance(data, dict) or data.get("s5") != self.sid:
            return
        ow = data.get("owner")
        if ow is not None and self.owner and ow != self.owner:
            self.foreign_frames += 1
            return
        self.last_signal = time.monotonic()
        if data.get("end"):
            self._track_broker(broker, data)
            self.end_reason = data.get("reason") or "end"
            self._end_event.set()
            return
        if data.get("hb") is not None:
            self._track_broker(broker, data)
            return
        cid = data.get("cid")
        if cid is None:
            self._track_broker(broker, data)
            return
        try:
            cid = int(cid)
        except Exception:
            return
        with self._lock:
            conn = self._conns.get(cid)
        if conn is None:
            self._track_broker(broker, data, is_new=False)
            return  # 已关闭连接的迟到副本帧
        nk = data.get("nack")
        if nk is not None:
            # 服务端上行缺口：从 per-conn 上行缓存原样补发
            self._track_broker(broker, data, is_new=False)
            if isinstance(nk, (list, tuple)):
                self._resend_uplink(conn, nk)
            return
        seq = data.get("seq")
        if seq is not None:
            is_new = conn.reassembly.add(seq, data)
            self._track_broker(broker, data, is_new=is_new)
        else:
            self._track_broker(broker, data)
            self._deliver(conn, data)  # 无 seq 宽松放行（兼容）

    def net_report(self) -> dict:
        """网络分析快照（JSON 可序列化）。全部来自被动观察，无额外请求：
        - downlink_race：下行 per-broker 首到胜率/副本率 + hb 延迟分布
        - server_side：服务端 hb 捎带的它自己各 broker 连接快照
        系统级每 broker 连接质量（在线率/掉线/重连）用内置的
        ``transport.node.mqtt_net.stats.get_report(probe=False)``（别动它，
        probe=True 会发起主动探测产生额外流量）。
        """
        with self._lock:
            conns = list(self._conns.values())
        total_wins = sum(st["wins"] for st in self.broker_stats.values())
        total_dups = sum(st["dups"] for st in self.broker_stats.values())
        race = []
        for b, st in sorted(self.broker_stats.items(),
                            key=lambda kv: -kv[1]["wins"]):
            row = {"broker": b, "rx": st["rx"],
                   "wins": st["wins"], "dups": st["dups"],
                   "win_pct": round(100.0 * st["wins"] / total_wins, 1)
                              if total_wins else 0.0}
            if st["hb_n"]:
                row["hb_delay_ms"] = {
                    "n": st["hb_n"], "min": st["hb_min"],
                    "avg": round(st["hb_sum"] / st["hb_n"], 1),
                    "max": st["hb_max"]}
            race.append(row)
        return {
            "sid": self.sid, "owner": self.owner,
            "server": (self.server_info or {}).get("host"),
            "uptime_s": round(time.time() - self._created_at, 1),
            "first_frame_broker": self.first_frame_broker,
            "foreign_frames_dropped": self.foreign_frames,
            "unique_seq_frames": total_wins, "dup_frames_dropped": total_dups,
            "downlink_race": race,
            "server_side_brokers": self.server_brokers,
            "server_side_conns": self.server_conns,
            "server_publish_errors": {"count": self.server_pub_err,
                                      "last": self.server_pub_last},
            "conns": {"active": len(conns), "total": self._cid_next,
                      "active_bytes_up": sum(c.bytes_up for c in conns),
                      "active_bytes_down": sum(c.bytes_down for c in conns),
                      "total_bytes_up": self.total_bytes_up,
                      "total_bytes_down": self.total_bytes_down,
                      "detail": [
                          {"cid": c.cid, "desc": c.desc,
                           "down_delivered_next": c.reassembly.next,
                           "down_waiting": sorted(c.reassembly.pending)[:8],
                           "up_next_seq": c.out_seq}
                          for c in conns]},
            "note": "hb_delay_ms 含两端时钟偏差，仅跨 broker 横向比较有"
                    "效；系统级 broker 连接质量另见 get_report(probe=False)",
        }

    def _deliver(self, conn: _ClientConn, fr: dict) -> None:
        op = fr.get("opened")
        if isinstance(op, dict):
            conn.open_result = op
            conn.opened.set()
            return
        d = fr.get("d")
        if d is not None:
            b = wire_to_bytes(d)
            if not b:
                return
            conn.bytes_down += len(b)
            try:
                conn.wq.put_nowait(b)
            except queue.Full:
                self.on_log("[cid=%d] 本地写队列背压红线，断开 %s"
                            % (conn.cid, conn.desc))
                self.drop_connection(conn.cid, reason="backpressure")
            return
        if fr.get("closed"):
            conn.remote_closed = True
            try:
                conn.wq.put_nowait(None)
            except Exception:
                pass

    def _on_conn_gap(self, cid: int) -> None:
        self.on_log("[cid=%d] 下行 seq 缺口超过 %.1fs（NACK 多轮补发仍失败），"
                    "断开该连接（TCP 流不允许跳号）" % (cid, self.gap_timeout))
        self.drop_connection(cid, reason="downlink_gap")

    def _pace_control(self) -> None:
        """自适应 pacing 闭环（每 3s）。探针=最近 10s 内各 broker 下行 hb
        小帧延迟的最小值（即当前最快小帧路径；时钟偏差只取 min，天然稳）。
        最快小帧都 >3s：大帧已触发账号级惩罚，节奏放慢 1.5×；持续 <1.2s：
        逐步恢复。指令 ps 随 1 字节级控制帧全 broker 上行。"""
        now = time.monotonic()
        if now - self._last_ps < 3.0:
            return
        self._last_ps = now
        recent = [st["hb_last_ms"] for st in self.broker_stats.values()
                  if st.get("hb_last_ms") is not None and st.get("hb_n", 0) >= 3
                  and now - st.get("hb_last_t", 0.0) < 10.0]
        if recent and self.hb_floor is not None:
            # 信号=最新鲜 hb 的「年龄」（相对全局实测地板，扣除时钟偏差与
            # RTT）。注意不能用各 broker 自身基线：恒慢 19s 的 broker 相对
            # 自己偏移很小，但它送来的帧永远是 19s 前的——交互流真正在意
            # 的是绝对新鲜度。
            best = min(recent) - self.hb_floor
            if best > 2500:
                # 单次超龄立即收油，超 5s 收得更狠：惩罚一旦形成要持续
                # 5-15s，反应慢一个 hb 周期交互流就断了。
                self.pace_intv = min(_PACE_MAX,
                                     self.pace_intv * (2.0 if best > 5000 else 1.5))
                self._pace_ok = 0
            elif best < 800:
                self._pace_ok += 1
                if self._pace_ok >= 3:
                    # 恢复要慢：连续 3 轮（≈9s）健康才松一格，避免在惩罚
                    # 边缘来回振荡反而反复触发。
                    self.pace_intv = max(_BULK_INTV, self.pace_intv / 1.25)
            else:
                self._pace_ok = 0
        try:
            self.tr.publish(self.in_topic,
                            {"s5": self.sid, "owner": self.owner,
                             "ps": round(self.pace_intv, 4)})
        except Exception:
            pass

    def _sweep_loop(self) -> None:
        while not self._end_event.wait(0.25):
            with self._lock:
                conns = list(self._conns.values())
            for c in conns:
                c.reassembly.check()
            self._pace_control()
            dt = float(self.dead_timeout or 0.0)
            if dt > 0 and (time.monotonic() - self.last_signal) > dt:
                self.end_reason = "dead"
                self.on_log("[s5] %.0fs 收不到任何远端帧（输出/心跳），"
                            "服务器可能已关闭" % dt)
                self._end_event.set()
                return

    def _ensure_bnet(self, hosts) -> None:
        """按首选名单（重）建上行大帧专用连接；变化才重建，旧连接后台关闭。"""
        want = frozenset(str(h) for h in (hosts or ()))
        with self._bnet_lock:
            if want == self._bhosts:
                return
            old = self._bnet
            try:
                sub = [b for b in BROKER_LIST if b[0] in want]
                if len(sub) != len(want):
                    return
                # 先建新再停旧：重建罕见（名单有在位者保护），零数据窗口
                # 比同账号瞬时多连接更重要；切换后立即异步停旧，缩短叠加。
                nn = MultiMQTTManager(brokers=sub, enable_stats=False)
                nn.start()
                try:
                    nn.wait_connected(min_count=1, timeout=4.0)
                except Exception:
                    pass
                self._bnet = nn
                self._bhosts = want
                if old is not None:
                    def _stop_old(o=old):
                        try:
                            o.stop()
                        except Exception:
                            pass
                    threading.Thread(target=_stop_old, daemon=True).start()
                self.on_log("[s5] 上行大帧专用连接就绪: %s"
                            % ", ".join(sorted(want)))
            except Exception as e:
                self._bnet = None
                self._bhosts = frozenset()
                self.on_log("[s5] 上行大帧专用连接建立失败，回退主连接: %r" % e)

    def _bulk_publish(self, frame: dict) -> None:
        """大帧走专用连接；未就绪/异常时回退主连接全 broker。"""
        node = self._bnet
        if node is not None and getattr(node, "clients", None):
            try:
                node.publish_broadcast(self.in_topic, frame)
                return
            except Exception:
                pass
        self.tr.publish(self.in_topic, frame)

    def _bulk_enqueue(self, frame: dict) -> None:
        """大帧入平滑队列；队列满反压本地读线程（TCP 窗口闭合）。"""
        while not self._end_event.is_set():
            try:
                self._bulk_q.put(frame, timeout=0.5)
                return
            except queue.Full:
                pass

    def _bulk_send_loop(self) -> None:
        last = 0.0
        while True:
            try:
                frame = self._bulk_q.get(timeout=0.5)
            except queue.Empty:
                if self._end_event.is_set():
                    return
                continue
            if frame is None:
                return
            wait = self.pace_intv - (time.monotonic() - last)
            if wait > 0:
                time.sleep(wait)
            last = time.monotonic()
            try:
                self._bulk_publish(frame)
            except Exception:
                pass

    def _pb_loop(self) -> None:
        """周期把「首到胜率最高的 N 个 broker」名单发给服务端。

        多 broker 冗余的代价是服务端出口 ×13 放大：跨洋公共 broker 在大帧
        突发下会排队/限流，把交互流（WS ping 等）拖出数秒卡顿。服务端拿
        到名单后，数据大帧只走首选的几条路径（丢了由 NACK 补发），控制
        帧（opened/closed/hb/nack）仍走全 broker，兼顾吞吐与容灾。
        """
        last_pb = list(_PB_SEED)
        last_change = self._open_mono
        warmed = False
        while not self._end_event.wait(_PB_INTV):
            try:
                ranked = sorted(self.broker_stats.items(),
                                key=lambda kv: -kv[1]["wins"])
                # 只挑「真快」的：hb 平均延迟 ≤3s 且样本 ≥5，防止热身期
                # 偶发首到的慢 broker 混进 bulk 名单（会把下载拖死）。
                cand = [b for b, st in ranked
                        if st["wins"] > 0 and st["hb_n"] >= 5
                        and (st["hb_sum"] / max(1, st["hb_n"])) <= 3000]
                # wins 是会话内累计计数：样本越大排名越稳。热身不足（开局
                # 45s 或总首到 <40）不发名单，避免慢 broker 抖动入选。
                total_wins = sum(self.broker_stats[b]["wins"] for b in cand)
                if not warmed:
                    if time.monotonic() - self._open_mono < 45 or total_wins < 40:
                        continue
                    warmed = True
                top = cand[:_PB_MAX]
                if not top:
                    continue
                # 在位者保护：名单只「增补/被显著更强者替换」，不因一次
                # hb 抖动缩小——重建本身有几秒数据回退窗口，频繁换血比
                # 留着一条稍慢的路径代价大得多。
                if last_pb:
                    inc = [b for b in last_pb if b in self.broker_stats]
                    win = {b: self.broker_stats[b]["wins"] for b in inc}
                    new = list(inc)
                    rest = [b for b in cand if b not in new]
                    while len(new) < _PB_MAX and rest:
                        new.append(rest.pop(0))
                    # 累计胜率 3 倍于在位最弱：才替换
                    while rest:
                        new.sort(key=lambda b: win.get(b, 0))
                        weak_w = win.get(new[0], 0)
                        better = rest[0]
                        if self.broker_stats[better]["wins"] >= max(3, weak_w * 3):
                            new[0] = better
                            rest.pop(0)
                            continue
                        break
                    top = new[:_PB_MAX]
                newset = frozenset(top)
                cur = frozenset(last_pb)
                if newset == cur:
                    continue
                nowm = time.monotonic()
                # 名单最短驻留 60s；累计计数下持续落后才换，杜绝 A/B 抖动
                # 带来的反复建连（公共 broker 对同账号连接数有限制）。
                if last_pb and nowm - last_change < 60:
                    continue
                last_pb = top
                last_change = nowm
                self.tr.publish(self.in_topic,
                                {"s5": self.sid, "owner": self.owner,
                                 "pb": top})
                self._ensure_bnet(top)
            except Exception:
                pass

    # ---- 连接管理（供本地 SOCKS5 服务调用） ----

    def open_connection(self, host: str, port: int) -> int:
        """向服务端发起一条到 host:port 的 CONNECT，返回 cid。"""
        if self.sid is None or self.ended:
            raise RemoteError("SOCKS5 会话不可用")
        with self._lock:
            cid = self._cid_next
            self._cid_next += 1
            desc = "%s:%d" % (host, int(port))
            conn = _ClientConn(
                cid, desc,
                _ConnReassembly(
                    lambda fr, c=cid: self._deliver_by_cid(c, fr),
                    lambda c=cid: self._on_conn_gap(c),
                    lambda seqs, c=cid: self._send_nack(c, seqs),
                    gap=self.gap_timeout))
            self._conns[cid] = conn
        try:
            self._send(conn, {"open": {"host": str(host), "port": int(port)}})
        except Exception:
            with self._lock:
                self._conns.pop(cid, None)
            raise
        return cid

    def _deliver_by_cid(self, cid: int, fr: dict) -> None:
        with self._lock:
            conn = self._conns.get(cid)
        if conn is not None:
            self._deliver(conn, fr)

    def wait_opened(self, cid: int, timeout: float) -> dict:
        """等服务端 CONNECT 结果；超时/失败抛 RemoteError。"""
        with self._lock:
            conn = self._conns.get(cid)
        if conn is None:
            raise RemoteError("连接已关闭")
        if not conn.opened.wait(timeout):
            self.drop_connection(cid, reason="open_timeout")
            raise RemoteTimeout("等待远端 connect 超时（%.0fs）: %s"
                                % (timeout, conn.desc))
        res = conn.open_result or {"ok": False, "error": "empty opened frame"}
        return res

    def send_data(self, cid: int, data: bytes) -> None:
        with self._lock:
            conn = self._conns.get(cid)
        if conn is None or conn.closed:
            raise RemoteError("连接已关闭")
        conn.bytes_up += len(data)
        self._send(conn, {"d": bytes_to_wire(bytes(data))})

    def eof_connection(self, cid: int) -> None:
        """本地读到 EOF：in-seq 发 close，服务端对目标半关写（SHUT_WR），
        目标剩余响应照常经下行回传（HTTP POST 等场景必须半关）。"""
        with self._lock:
            conn = self._conns.get(cid)
        if conn is None or conn.closed:
            return
        try:
            self._send(conn, {"close": True})
        except Exception:
            pass

    def drop_connection(self, cid: int, reason: str = "closed") -> None:
        """本地收尾：关本地 socket、停重组、best-effort 通知服务端。"""
        with self._lock:
            conn = self._conns.pop(cid, None)
        if conn is None:
            return
        conn.closed = True
        conn.reassembly.close()
        conn.local_read_done.set()
        self.total_bytes_up += conn.bytes_up
        self.total_bytes_down += conn.bytes_down
        try:
            conn.wq.put_nowait(None)
        except Exception:
            pass
        if conn.local_sock is not None:
            try:
                conn.local_sock.close()
            except Exception:
                pass
        if not self.ended and not conn.remote_closed:
            try:
                self._send(conn, {"close": True})
            except Exception:
                pass
        self.on_log("[cid=%d] %s 关闭（%s）：上行 %dB / 下行 %dB，存活 %.1fs"
                    % (cid, conn.desc, reason, conn.bytes_up, conn.bytes_down,
                       time.time() - conn.created))

    def abort_all(self, reason: str = "session_end") -> None:
        with self._lock:
            cids = list(self._conns.keys())
        for cid in cids:
            self.drop_connection(cid, reason=reason)

    def wait_end(self, timeout=None) -> bool:
        return self._end_event.wait(timeout)

    def close(self) -> None:
        """断开会话：通知服务端停止、退订、关掉所有本地连接；不停 transport。"""
        if self.sid is not None and not self.ended:
            try:
                self.tr.publish(self.in_topic,
                                {"s5": self.sid, "stop": True,
                                 "owner": self.owner})
            except Exception:
                pass
            self._end_event.wait(1.5)
        if self._handler is not None and self.out_topic is not None:
            try:
                self.tr.stream_unsubscribe(self.out_topic, self._handler)
            except Exception:
                pass
        self.abort_all()
        try:
            while True:
                self._bulk_q.get_nowait()
        except Exception:
            pass
        try:
            self._bulk_q.put_nowait(None)
        except Exception:
            pass
        if self._bnet is not None:
            _bn = self._bnet
            self._bnet = None
            self._bhosts = frozenset()
            try:
                _bn.stop()
            except Exception:
                pass
        self.sid = None
        self.owner = None


# ============================ 本地 SOCKS5 监听服务 ============================

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("eof during socks handshake")
        buf += chunk
    return buf


def _socks_reply(sock: socket.socket, rep: int) -> None:
    # BND.ADDR/BND.PORT 对 CONNECT 成功语义不重要，填 0.0.0.0:0 即可
    sock.sendall(b"\x05" + bytes([rep]) + b"\x00\x01\x00\x00\x00\x00\x00\x00")


class _Socks5Server:
    """本地 SOCKS5 监听（仅 CONNECT、无认证；域名交远端解析 = socks5h）。"""

    def __init__(self, session: RemoteSocks5, host: str, port: int,
                 open_timeout: float = 30.0, on_log=None):
        self.session = session
        self.bind_host = host
        self.bind_port = int(port)
        self.open_timeout = float(open_timeout)
        self.on_log = on_log or (lambda m: None)
        self._lsock = None
        self._closed = threading.Event()

    def start(self) -> None:
        ls = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ls.bind((self.bind_host, self.bind_port))
        ls.listen(128)
        self._lsock = ls
        threading.Thread(target=self._accept_loop, name="s5-accept",
                         daemon=True).start()

    def close(self) -> None:
        self._closed.set()
        if self._lsock is not None:
            try:
                self._lsock.close()
            except Exception:
                pass

    def _accept_loop(self) -> None:
        while not self._closed.is_set() and not self.session.ended:
            try:
                csock, addr = self._lsock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(csock, addr),
                             name="s5-local", daemon=True).start()

    def _handle(self, csock: socket.socket, addr) -> None:
        cid = None
        entered_pump = False
        try:
            csock.settimeout(30)
            ver = _recv_exact(csock, 1)[0]
            nmethods = _recv_exact(csock, 1)[0]
            _recv_exact(csock, nmethods)
            if ver != 5:
                csock.close()
                return
            csock.sendall(b"\x05\x00")  # 无认证
            ver, cmd, _rsv, atyp = _recv_exact(csock, 4)
            if atyp == 1:
                host = socket.inet_ntoa(_recv_exact(csock, 4))
            elif atyp == 3:
                n = _recv_exact(csock, 1)[0]
                host = _recv_exact(csock, n).decode("latin-1")
            elif atyp == 4:
                host = socket.inet_ntop(socket.AF_INET6, _recv_exact(csock, 16))
            else:
                _socks_reply(csock, 8)   # address type not supported
                csock.close()
                return
            port = struct.unpack(">H", _recv_exact(csock, 2))[0]
            if cmd != 1:
                _socks_reply(csock, 7)   # command not supported（不支持 UDP 等）
                csock.close()
                return
            csock.settimeout(None)

            sess = self.session
            cid = sess.open_connection(host, port)
            try:
                res = sess.wait_opened(cid, self.open_timeout)
            except RemoteError as exc:
                self.on_log("[cid=%d] 远端 connect %s:%d 失败: %s"
                            % (cid, host, port, exc))
                _socks_reply(csock, 5)   # connection refused
                return
            if not res.get("ok"):
                self.on_log("[cid=%d] 远端 connect %s:%d 被拒: %s"
                            % (cid, host, port, res.get("error")))
                _socks_reply(csock, 5)
                return
            _socks_reply(csock, 0)
            self.on_log("[cid=%d] %s:%d 已建立（经 %s）"
                        % (cid, host, port, addr[0]))

            with sess._lock:
                conn = sess._conns.get(cid)
            if conn is None:
                return
            conn.local_sock = csock
            threading.Thread(target=self._local_writer, args=(cid, csock),
                             name="s5-lw-%d" % cid, daemon=True).start()
            # 进入泵送阶段后连接收尾移交给 reader/writer：
            # 本地 EOF 只是半关（远端响应还在路上），绝不能在这里关 csock。
            entered_pump = True
            self._local_reader(cid, csock)
        except (ConnectionError, OSError):
            pass
        except Exception as exc:
            self.on_log("[s5] 本地连接处理异常: %s: %s"
                        % (type(exc).__name__, exc))
        finally:
            if not entered_pump:
                if cid is not None:
                    self.session.drop_connection(cid, reason="setup_failed")
                try:
                    csock.close()
                except Exception:
                    pass

    def _local_reader(self, cid: int, csock: socket.socket) -> None:
        """本地 socket -> 上行帧。EOF 时发 in-seq close（半关），连接其余
        收尾由 writer 收到远端 closed 后完成；本地读出错则整条连接收尾。"""
        sess = self.session
        try:
            while not sess.ended:
                data = csock.recv(sess.frame_max)
                if not data:
                    sess.eof_connection(cid)
                    return
                sess.send_data(cid, data)
        except (ConnectionError, OSError, RemoteError):
            with sess._lock:
                conn = sess._conns.get(cid)
            if conn is not None and conn.remote_closed:
                # csock 是 writer 在远端 closed 后主动关的，收尾归 writer
                return
            sess.drop_connection(cid, reason="local_error")
        finally:
            # 读线程退出 = 应用端已关闭（FIN/RST 已到），writer 可安全回收
            with sess._lock:
                conn = sess._conns.get(cid)
            if conn is not None:
                conn.local_read_done.set()

    def _local_writer(self, cid: int, csock: socket.socket) -> None:
        """下行帧 -> 本地 socket。收到 None（远端 closed 或本地收尾）即结束；
        正常路径（远端 closed）由这里做整条连接的最终收尾。"""
        sess = self.session
        with sess._lock:
            conn = sess._conns.get(cid)
        if conn is None:
            return
        clean = False
        try:
            while True:
                item = conn.wq.get()
                if item is None:
                    # 区分：远端 EOF（clean）还是 drop_connection 强收尾
                    clean = conn.remote_closed and not conn.closed
                    break
                csock.sendall(item)
        except (ConnectionError, OSError):
            clean = False
        if clean:
            # 远端半关：只 shutdown 写方向给应用发 FIN，绝不能直接 close()——
            # Windows 下 socket 接收缓冲里还有应用未读完的数据时，close()
            # 会直接发 RST，把已完整送达的下载/响应在应用侧打成连接重置。
            # 等读线程（应用关闭后）退出再 close，fd 由 drop_connection 回收。
            try:
                csock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            conn.local_read_done.wait(120.0)
        try:
            csock.close()
        except Exception:
            pass
        if clean:
            sess.drop_connection(cid, reason="remote_closed")
        elif not conn.closed:
            sess.drop_connection(cid, reason="local_error")


# ============================ CLI ============================

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


if __name__ == "__main__":
    sys.exit(main())
