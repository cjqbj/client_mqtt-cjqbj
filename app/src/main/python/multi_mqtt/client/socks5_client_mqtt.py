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
from collections import deque
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

_PB_SEED = ("demo.tbmq.io", "broker-cn.emqx.io", "broker.emqx.io")
# 会话建立即下发的静态种子名单：竞速统计热身（约 45-60s）完成前，大帧若
# 回退主节点会变成全 13 broker 扇出——热身下载本身就能触发账号级惩罚。
# 选历史胜率/隔离性最好的三个，热身完成后由累计胜率名单接管。
_PB_MAX = 3               # 大帧下行首选 broker 数（控制帧仍全 broker）
_PB_INTV = 5.0            # 首选 broker 名单刷新/下发间隔秒

_GAP_DEFAULT = 15.0       # per-cid seq 缺口硬熔断秒数（先经 NACK 多轮重传）
_HB_FLOOR_WIN = 60.0      # hb 延迟地板的滚动窗口秒数（单调 min 会被一次 NTP/GC/调度抖动永久钉死，pace 余生误判惩罚中）
_FIN_CLOSE_GRACE = 120.0  # 远端 clean EOF 后等本地应用读完缓冲再 close socket 的上限（Windows 防 RST）；会话条目不再被它拖住
_NACK_FIRST = 0.8         # 缺口出现后多久发首轮重传请求
_NACK_INTV = 1.2          # 后续重传请求轮询间隔（硬熔断前约 4 轮）
_CACHE_MAX = 256          # per-conn 重传缓存帧数（≈256×16KiB=4MiB）

_BULK_INTV = 0.04         # 大帧全局平滑节奏下限（秒/帧 ≈25 帧/s≈400KB/s）
_PACE_START = 0.08        # 会话起跑节奏（≈200KB/s）：宁可慢，不触发惩罚
_PACE_MAX = 0.5           # 最严节奏上限（2 帧/s≈32KB/s，仅惩罚期短暂进入）
_WQ_MAX = 256             # per-conn 写队列帧数上限（≈256×16KiB=4MiB 背压红线）
_INGRESS_MAX = 128        # per-cid 重组暂存帧数上限（防 closed 连接迟到帧堆积）
# NACK 补发待办队列硬上限：元素只是 ucache 里既有帧的引用，不拷贝载荷；
# 满量（NACK 风暴/补发长期被 bulk 队列反压）直接拒收新待办并留痕，补发
# 由服务端下一轮 NACK（指数退避）重新驱动，不在回调线程堆积内存。
_RESEND_Q_MAX = 512
# NACK 大帧补发去重表的淘汰线：超过此条目数才做一轮清扫（避免每帧扫表），
# 清扫只删超过 TTL 的键。去重窗口才 2s，30s 前的键绝无再用价值；cid 已
# 死的键也随年龄一起走，防止 HTTP 短连接海量轮换下表只增不删。
_RT_LAST_MAX = 4096
_RT_LAST_TTL = 30.0
_RT_LAST_PRUNE_INTV = 10.0
# s5-bulk 线程等上行大帧专用连接首次就绪的上限：覆盖建连 4s 窗口并留余量；
# 超时仍不可用则丢帧留痕，由服务端 NACK（缺口超时 15s 前会轮询）补发。
_BULK_READY_WAIT = 8.0


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
    # 会话占坑三件套先于 try 定义：异常分支需要它们安全释放"启动中"占位。
    _sess_lock = None
    _sessions = None
    _sess_ready = None
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

        # 同进程内同 sid 幂等：原子占坑再初始化。旧实现"锁内查缓存→锁外
        # 起线程/订阅→末尾写回"是先查再写：两个并发同 sid 握手（paho 回调
        # 线程或不同 broker 触发的同段代码并发执行）会同时缓存未命中，双双
        # 拉起 _in_loop/_hb_loop/_bsend_loop、重复注册路由，先完成者的整套
        # 线程成孤儿（只能熬到 TTL 或等 owner 仲裁帧自杀）。用"启动中"
        # 事件占位：只有占坑成功者干活，后来者等同一份成品 env。
        _sess_lock = _g.get("_cmq_s5_sess_lock")
        if _sess_lock is None:
            _sess_lock = _th.Lock()
            _g["_cmq_s5_sess_lock"] = _sess_lock
        _sessions = _g.setdefault("_cmq_s5_sessions", {})
        _sess_ready = _th.Event()
        _starter = True
        with _sess_lock:
            _prev = _sessions.get(_sid)
            if isinstance(_prev, str):
                return _prev               # 已有成品会话，原样复用
            if _prev is None:
                _sessions[_sid] = _sess_ready
            else:
                _starter = False
        if not _starter:
            # 另一握手正在初始化。初始化只做本地建表/订阅/起线程，秒级完成；
            # 30s 上限防赢家异常挂死时后来者永久阻塞。
            _prev.wait(30.0)
            with _sess_lock:
                _v = _sessions.get(_sid)
            if isinstance(_v, str):
                return _v
            # 占位已被赢家异常分支摘除 → 锁内抢坑接管（多等待者也只有一个
            # 能抢到）；占位仍在（赢家疑似卡死）→ 绝不另起一套制造孤儿，
            # 返回 busy 让本次调用方重试。
            with _sess_lock:
                _v = _sessions.get(_sid)
                if isinstance(_v, str):
                    return _v
                if _v is not None:
                    return _j.dumps(
                        {"ok": False,
                         "error": "session initialization in progress"},
                        ensure_ascii=False)
                _sessions[_sid] = _sess_ready
                _starter = True

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
        # TTL 计时必须用单调时钟：墙上时钟会被 NTP/手动校时步进（虚拟机
        # 休眠恢复更常见跨小时跳变），_t.time() 差值可能瞬间越过 TTL 误杀
        # 长会话，也可能倒走让 TTL 永不生效。
        _start = _t.monotonic()

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

        # 下行失败记账必须**会话私有**：executor 命名空间跨会话持久，旧实现
        # 写 _g["_cmq_s5_pub_err"]，同进程起第二个 sid 时新会话心跳会捎带
        # 上一会话的累计错误数和 last 文案，客户端据此误判新链路质量。
        _pub_err = 0
        _pub_last = None

        def _pub_fail(_msg):
            # 注意 paho 在断连 client 上 publish() 通常**不抛异常**，而是
            # 返回 rc!=0（MQTT_ERR_NO_CONN 等）把帧静默丢弃——只 try/except
            # 抓不到这种"seq 已消费但帧没出去"，缺口只能靠客户端 NACK
            # 补发，但计数必须如实反映。
            nonlocal _pub_err, _pub_last
            try:
                _pub_err += 1
                _pub_last = _msg
            except Exception:
                pass

        def _cl_pub(_cl, _topic, _payload):
            # 单个 paho client 发一帧：异常或 rc!=0 都算没出去，返回成败。
            try:
                if not _cl.is_connected():
                    return False, "not connected"
                _mi = _cl.publish(_topic, _payload, qos=0)
                _rc = getattr(_mi, "rc", 0)
                if _rc:
                    return False, "paho rc=%s" % _rc
                return True, None
            except BaseException as _e:
                return False, "%s: %s" % (type(_e).__name__, _e)

        def _pub(_fr, _subset=None):
            # 下行帧唯一出口。_subset=None：全 broker（控制帧/未学到名单前）；
            # 否则只发首选 broker（数据大帧），丢失由客户端 NACK 触发全 broker
            # 补发。多 broker 全量扇出的出口放大约 13 倍，跨洋公共 broker
            # 在突发下会排队/限流并拖死交互流（WS 心跳卡顿）。
            _fr["s5"] = _sid
            _fr["owner"] = _uid
            try:
                if not _subset:
                    # publish_broadcast 内部逐个 client 发送且不回传 rc，
                    # 这里只能兜异常；子集路径的静默失败由下方 rc 检查记账。
                    _net.publish_broadcast(_out_topic, _fr)
                    return
                _payload = _pc(_fr, decrypt=False,
                               enabled=getattr(_net, "enable_crypto", False))
                with _net.lock:
                    _items = list(_net.clients.items())
                _tried = 0
                for _h, _cl in _items:
                    if _h in _subset:
                        _tried += 1
                        _sent, _why = _cl_pub(_cl, _out_topic, _payload)
                        if not _sent:
                            # 不主动改走全 broker 兜底：broker 过载时全扇出
                            # 只会加剧 13× 放大，缺帧统一由客户端 NACK 补发。
                            _pub_fail("subset %s: %s" % (_h, _why))
                if _tried == 0:
                    # 子集 broker 在主连接上一个 client 都没有：帧静默没
                    # 出去（上面循环无任何记账），seq 已消费，等 NACK 补发。
                    _pub_fail("subset empty: no matching broker client")
            except BaseException as _e:
                # 序列化/网络层异常：帧没出去但 seq 已消费会造成客户端缺口。
                _pub_fail("%s: %s" % (type(_e).__name__, _e))

        def _bulk_pub(_fr):
            # 大数据帧专用出口：只连首选 broker 的**独立第二路 paho 连接**。
            # 实测同一条 paho 连接上大帧突发会把 1s 的小帧在发送 FIFO 里压
            # 十几秒（broker/链路按连接排队）；大帧走独立连接后控制/交互
            # 帧（走 _net 主连接，全 broker）不再被队头阻塞。名单变化时
            # 惰性重建，重建期间退回主连接子集发送。
            # 必须 nonlocal：_bnet/_bhosts 是 _cmq_socks5_start 的局部闭包
            # 变量。误用 global 会绑到 executor 模块全局——多会话共享且
            # 初值不存在，_want!=_bhosts 首次读取即 NameError。
            nonlocal _bnet, _bhosts
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
                _bok = 0
                try:
                    _payload = _pc(_fr, decrypt=False,
                                   enabled=getattr(_node, "enable_crypto", False))
                    with _node.lock:
                        _items = list(_node.clients.items())
                    for _h, _cl in _items:
                        _sent, _why = _cl_pub(_cl, _out_topic, _payload)
                        if _sent:
                            _bok += 1
                        else:
                            _pub_fail("bulk %s: %s" % (_h, _why))
                except BaseException as _e:
                    _pub_fail("bulk %s: %s" % (type(_e).__name__, _e))
                if _bok:
                    return  # 至少一份入队：全丢再由客户端 NACK 触发补发
                # bulk client 全断/rc 全失败：落到主连接兜底（子集或全
                # broker），不能像旧代码那样 return 假装已发出。
            # bulk 连接未就绪/发送异常：退回主连接（子集或全 broker）
            _pub(_fr, set(_want) if _want else None)

        # 大帧平滑队列：所有连接的大帧先入队（有界），单线程按 _BULK_INTV
        # 匀速投出。实测公共 broker 对微突发会按账号施加十几秒级惩罚（连
        # 同账号第二连接上的小帧 hb 都被拖 15s）；匀速后既削惩罚又让 paho
        # FIFO 不堆积。队列满反压到 _conn_main → 读目标 socket 变慢 →
        # TCP 窗口自然闭合，背压链完整。小帧绝不入队，直接全 broker。
        _bq = _qe.Queue(maxsize=64)
        _rt_last = {}   # NACK 大帧补发去重 (cid,seq)->monotonic
        _rt_prune = [0.0]  # 上次清扫时刻（list 载体供 _retrans 闭包改写）
        _RT_LAST_MAX = 4096   # 与客户端侧同名常量保持一致
        _RT_LAST_TTL = 30.0   # 去重窗口仅 2s，30s 前的键绝无复用价值
        _RT_LAST_PRUNE_INTV = 10.0

        def _bsend_loop():
            # nonlocal：_pace_intv 是闭包变量，由 _in_loop 收 ps 帧热调。
            # global 会让首帧（ps 到达前）读模块全局未定义名 → NameError
            # 打死本线程，此后 _bq 无人消费、堆满反压，读循环整个卡死。
            nonlocal _pace_intv
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
            # 与客户端 _resend_loop 对称的年龄清扫：key=(短命 cid,seq)，
            # NACK 平息后永不复用；HTTP 短连接海量轮换下不清扫会单调膨胀。
            # 超 4096 条且距上次清扫 >10s 时删 30s 以上老键。
            if (len(_rt_last) > _RT_LAST_MAX
                    and _now2 - _rt_prune[0] > _RT_LAST_PRUNE_INTV):
                _rt_prune[0] = _now2
                for _k in list(_rt_last.keys()):
                    if _now2 - _rt_last[_k] > _RT_LAST_TTL:
                        _rt_last.pop(_k, None)
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
            # connect 可能仍在进行：释放正等 sock_ev 的 writer，避免它白等
            try:
                _c["sock_ev"].set()
            except Exception:
                pass
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
                        # sock 未就绪：connect 还在进行（慢目标可达 _ctimeout
                        # 秒）。正常客户端会等 opened 才发数据，但上行 NACK
                        # 补发/乱序重放可能让数据帧早于建连完成到达此处。
                        # 直接 break 会让 writer 死亡、之后所有上行字节被
                        # wq 静默吞掉（连接表面正常，单向断流，极难排查）。
                        _c["sock_ev"].wait(_ctimeout + 5.0)
                        _s = _c.get("sock")
                        if _s is None:
                            # 理论上 conn_main 成功/失败都会 set；等到超时
                            # 说明收尾信号丢失。静默 break 会让 wq 残余数据
                            # 随线程死亡无声丢弃、conn 条目也无人摘除——走
                            # 统一收尾并在 closed 帧留下原因。
                            _close_conn(_cid, "writer_sock_timeout")
                            return
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
                    # writer_sock_timeout/线程启动失败/会话 stop 可能已持
                    # 本锁发过 closed(seq=0)。此处不复查就再发 opened
                    # (ok=False,seq=0) 会让客户端收到**同一 seq 两种内容**
                    # 的帧——协议契约被破坏，且第二条 closed 还会平白多占
                    # 一个 seq。closed 后唯一权威帧是 _close_conn 那条。
                    if not _c["closed"]:
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
                _c["sock_ev"].set()
                _wake_writer(_c)
                _gc(_cid, _c)
                return
            _c["sock"] = _s
            # sock 先落字典再 set：writer 醒来时保证 _c["sock"] 可见。
            _c["sock_ev"].set()
            with _c["seq_lock"]:
                _dead = _c["closed"]
                if not _dead:
                    _fr0 = {"cid": _cid, "seq": 0, "opened": {"ok": True}}
                    _cache_put(_c, _fr0)
                    _pub(_fr0)  # opened 控制帧全 broker
                    _c["out_seq"] = 1
            if _dead:
                # 建连期间已被收尾（writer_sock_timeout/线程启动失败/
                # session stop）：sock 晚于 _close_conn 才建出，它当时关
                # 的是 None，不补关就永久泄漏；也绝不能在 closed 帧之后
                # 倒序补发 seq=0。
                try:
                    _s.close()
                except Exception:
                    pass
                _gc(_cid, _c)
                return
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
                    # _close_conn 可能在本线程 recv 期间已摘条目、持锁发了
                    # closed(seq=N)。这里不复查就占号会把数据帧排成 N+1，
                    # 落在 closed 之后（倒序），必须放弃此帧并退出读循环。
                    if _c["closed"]:
                        break
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
                          "rcache": {},
                          # connect 结果事件：成功时先放 sock 再 set，失败
                          # 路径 r_done+唤醒后 set。writer 在 sock 尚未就绪
                          # 时必须等它，绝不能直接退出（见 _writer）。
                          "sock_ev": _th.Event()}
                    _conns[_cid] = _c
                _t1 = _th.Thread(target=_conn_main,
                                 args=(_cid, _c, str(_op.get("host")),
                                       int(_op.get("port"))),
                                 name="s5-conn-%s" % _cid, daemon=True)
                _t2 = _th.Thread(target=_writer, args=(_cid, _c),
                                 name="s5-wr-%s" % _cid, daemon=True)
                try:
                    _t1.start()
                    _t2.start()
                except (RuntimeError, OSError):
                    # 系统线程/资源耗尽：_c 已入 _conns，但一个或两个 worker
                    # 没起来（sock_ev 永不 set、writer 永不消费 wq），不兜
                    # 会挂到会话结束才被清。按建连失败回 opened+closed；
                    # conn_main 若已起，closed 标志会让它建连后立即退出并
                    # 由 _gc 关 sock（opened 重复 seq=0 客户端按副本去重）。
                    try:
                        with _c["seq_lock"]:
                            if _c["out_seq"] == 0:
                                _fr0 = {"cid": _cid, "seq": 0, "opened": {
                                    "ok": False,
                                    "error": "server cannot start worker "
                                             "thread"}}
                                _cache_put(_c, _fr0)
                                _pub(_fr0)
                                _c["out_seq"] = 1
                    except Exception:
                        pass
                    _close_conn(_cid, "thread_start_failed")
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
            # nonlocal：_pb/_pace_intv 必须与 _bulk_pub/_bsend_loop 读的
            # 是同一个闭包单元；global 会让 pb 写进模块全局而 bulk 路径
            # 永远读到空名单（大帧全部退回 13 broker 广播）。
            nonlocal _pb, _pace_intv
            while not _end.is_set():
                try:
                    _fr = _inq.get(timeout=0.25)
                except _qe.Empty:
                    _fr = None
                if _t.monotonic() - _start >= _ttl:
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
            # 调度用单调时钟（NTP 步进不连发/不漏跳）；载荷里的 hb 时间戳
            # 是给客户端做墙上时钟对齐用的 epoch 毫秒，保持 _t.time()。
            nonlocal _pub_err, _pub_last
            _next = _t.monotonic()
            while not _end.wait(0.5):
                if _hb <= 0.0:
                    continue
                _now = _t.monotonic()
                if _now < _next:
                    continue
                _next = _now + _hb
                try:
                    _pub({"hb": int(_t.time() * 1000),
                          "srv": _srv_brokers(),
                          "c": _srv_conns(),
                          "pe": _pub_err,
                          "pl": _pub_last})
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
            # 只在占位仍是自己时写成品：初始化期间会话可能已被 stop 帧
            # 摘除（死会话不得复活），或占位已被超时等待者接管。
            if _sessions.get(_sid) is _sess_ready:
                _sessions[_sid] = _env_json
        _sess_ready.set()
    except Exception:
        _res = {"ok": False, "error": _tb.format_exc()}
        _env_json = _j.dumps(_res, ensure_ascii=False)
        try:
            # 赢家初始化失败：摘除自己的"启动中"占位并通知等待者，它们
            # 会锁内抢坑接管；占位已易主（被接管）时绝不动别人的坑。
            if _sess_lock is not None and _sessions is not None \
                    and _sess_ready is not None:
                with _sess_lock:
                    if _sessions.get(_sid) is _sess_ready:
                        _sessions.pop(_sid, None)
                _sess_ready.set()
        except Exception:
            pass
    return _env_json
_cmq_socks5_start()
'''


def build_socks5_start_code(payload: dict) -> str:
    """把 SOCKS5 启动信封编译成远端可直接执行的自包含 Python 代码。"""
    placeholder = "__PAYLOAD__"
    if _SOCKS5_START_TEMPLATE.count(placeholder) != 1:
        # 全量 replace 依赖"占位符全模板唯一"：多一处会把载荷重复注入。
        raise RuntimeError("socks5 启动模板的 %s 占位符不唯一" % placeholder)
    lit = json.dumps(json.dumps(payload, ensure_ascii=False))
    # payload 是用户可控内容（sid/topic 等都能塞任意子串）。旧实现无条件
    # str.replace：载荷 JSON 里若恰好出现 __PAYLOAD__，注入结果会被二次
    # 替换，拼出语法损坏或载荷被改写的远端代码（且无任何报错）。先换成
    # 一个模板与载荷里都不存在的确定性锚点，再注入。
    if placeholder in lit:
        i = 0
        while True:
            cand = "__PAYLOAD_%d__" % i
            if cand not in lit and cand not in _SOCKS5_START_TEMPLATE:
                code = _SOCKS5_START_TEMPLATE.replace(placeholder, cand)
                return code.replace(cand, lit)
            i += 1
    return _SOCKS5_START_TEMPLATE.replace(placeholder, lit)


# ============================ 客户端会话 ============================

class _ConnReassembly:
    """per-cid 单向字节流重组缓冲。

    多 broker 冗余：同一帧到多份（seq<next 丢弃），各路径乱序（暂存等前序）。
    与 PTY 的 _PtyReorderBuffer 唯一但关键的区别：缺口**绝不跳号**——
    TCP 流丢一段就是损坏。缺口后先经 NACK 请求服务端从 per-conn 重传缓存
    补发（多轮），硬超时仍补不齐才熔断该连接。

    交付串行化：add 被每个 broker 各自的 paho 网络线程并发调用，在锁内只能
    保证"放行集合按 seq 连续"，锁外若直接回调 deliver，两个线程可能把
    seq=6 先于 seq=5 写进本地 socket——TCP 字节序静默错乱（长度合法、内容
    错位），比缺口熔断危险得多。故每条连接配一个专职交付线程：add 锁内
    drain 后只把帧 FIFO 入队，单线程严格按放行顺序 deliver（PTY 侧也要求
    同样的顺序语义）。
    """

    def __init__(self, deliver, on_gap, on_nack, gap=_GAP_DEFAULT,
                 nack_first=_NACK_FIRST, nack_intv=_NACK_INTV, name="",
                 on_log=None):
        self.next = 0
        self.pending = {}
        self.deadline = None       # 硬熔断时刻
        self.ndeadline = None      # 下一轮 NACK 时刻
        self.gap = float(gap)
        self.nack_first = float(nack_first)
        self.nack_intv = float(nack_intv)
        self.nround = 0           # NACK 轮次（间隔指数退避，封顶 3s）
        self.broken = False
        self.dropped_full = 0     # pending 堆满被拒收的首到帧（靠 NACK 补）
        self._deliver = deliver
        self._on_gap = on_gap
        self._on_nack = on_nack
        self._on_log = on_log or (lambda m: None)
        self._name = str(name)
        self._lock = threading.Lock()
        # 待交付帧 FIFO：唯一写者是各 broker 回调（持 _lock 时入队），
        # 唯一读者是本连接的交付线程。无界：入队候选受 pending<_INGRESS_MAX
        # 约束，且 deliver 路径全部非阻塞（put_nowait/event.set），唯一的
        # 背压出口（wq 满）会立即 drop_connection→close→哨兵停线程。
        self._dq = queue.Queue()
        self._closing = False
        t = threading.Thread(target=self._deliver_loop,
                             name="s5-deliver-%s" % name, daemon=True)
        t.start()

    def _deliver_loop(self):
        while True:
            fr = self._dq.get()
            if fr is None:
                return
            try:
                self._deliver(fr)
            except Exception as e:
                # 单帧异常绝不能打死交付线程，否则后续字节全部滞留、顺序
                # 与存活水位一起失真；但静默吞掉会让线上彻底黑障，必须留痕。
                try:
                    self._on_log("[cid=%s] 下行帧交付异常（已跳过）: %s: %s"
                                 % (self._name, type(e).__name__, e))
                except Exception:
                    pass

    def add(self, seq: int, frame: dict) -> bool:
        """返回 True=该 (cid,seq) 的首个副本且已被接收（投递它的 broker 赢了
        竞速），False=迟到重复副本/已熔断/**pending 堆满拒收**。满队拒收不
        能算竞速胜利：帧没进缓存、没入交付队列，要靠后续 NACK 补发，计成
        win 会让 broker 竞速胜率与实际接收量永久对不上。"""
        with self._lock:
            if self.broken:
                return False
            seq = int(seq)
            if seq < self.next or seq in self.pending:
                return False  # 重复副本
            if len(self.pending) >= _INGRESS_MAX:
                self.dropped_full += 1
                try:
                    self._on_log(
                        "[cid=%s] 下行缺口暂存已满 %d 帧，拒收 seq=%d"
                        "（等 NACK 补发/缺口熔断）"
                        % (self._name, _INGRESS_MAX, seq))
                except Exception:
                    pass
                return False
            self.pending[seq] = frame
            ready = []
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
            # 入队必须仍在锁内：FIFO 只保证单次 put 原子，两个并发 add
            # 若在锁外各自 put，seq=6 仍可能先于 seq=5 入队。drain 与
            # enqueue 同临界区，队列顺序就与 next 推进严格一致。Queue 无界，
            # put 立即返回，不增加锁持有时间。
            if not self._closing:
                for fr in ready:
                    self._dq.put(fr)
        return True

    def deliver_now(self, frame: dict) -> None:
        """无 seq 兼容帧也走交付 FIFO：否则它在 broker 回调线程直接交付，
        会与交付线程上的 seq 数据帧并发写本地 socket，绕过保序。"""
        with self._lock:
            if self._closing:
                return
            self._dq.put(frame)

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
            if self._closing:
                return
            self._closing = True
            self.broken = True
            self.pending.clear()
            # 哨兵入队：已在 FIFO 里的放行帧先交付（连接已从 _conns 摘除，
            # _deliver_by_cid 会丢弃），随后交付线程退出，不残留线程。
            self._dq.put(None)


class _ClientConn:
    """一条本地 SOCKS5 连接对应的会话内状态。"""

    __slots__ = ("cid", "desc", "wq", "opened", "open_result", "reassembly",
                 "local_sock", "closed", "remote_closed", "seq_lock",
                 "out_seq", "bytes_up", "bytes_down", "created",
                 "ucache", "state_lock",
                 "reader_done", "fin_close_pending")

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
        # closed/remote_closed 生命周期状态位的唯一保护锁：_deliver（MQTT
        # 分发线程）与 drop_connection（reader/writer/收尾线程）会并发写，
        # writer 还要把两个标志当一对来判定 clean EOF。裸 bool 虽有 GIL
        # 不致损坏，但组合读会撕裂——正常远端 EOF 可能被误记 local_error。
        # seq_lock 只保护打号/缓存，语义不同，不复用。
        self.state_lock = threading.Lock()
        self.seq_lock = threading.Lock()
        self.out_seq = 0          # 上行 per-cid seq（open=0，其后递增）
        self.bytes_up = 0
        self.bytes_down = 0
        self.created = time.time()
        # 上行重传缓存：seq -> 已发完整帧（服务端 NACK 时原样重发）
        self.ucache = {}
        # reader_done：只由本连接的本地读线程自身在退出时 set（cid 单调
        # 不回收，线程持 conn 对象直接置位，与条目是否已被 drop 弹出
        # _conns 无关）；fin 收尾线程据此判断应用是否真的读完，避免
        # "drop 已弹条目→reader finally 查字典为 None→信号永不置位→
        # fin 死等 120s" 的互等。
        self.reader_done = threading.Event()
        # clean EOF 时 socket 的关闭权已移交 s5-fin 收尾线程，drop_connection
        # 不得再直接 close（Windows 下会把接收缓冲里应用未读的数据打成 RST）
        self.fin_close_pending = False


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
        # bulk 连接首次就绪事件：s5-bulk 线程在热身窗口内等它而不是回退
        # 全 broker（13× 扇出会在大帧热点期触发账号级限流）。
        self._bulk_ready = threading.Event()
        self._bulk_drop_n = 0
        self._bulk_drop_log_t = 0.0
        self._bulk_q = queue.Queue(maxsize=64)  # 上行大帧平滑队列
        # NACK 补发待办（conn, seqs）：paho 回调线程只入队，由 s5-rsnd
        # 单线程消费——补发的大帧要进 _bulk_q（满则按 0.5s 粒度反压，
        # 64 格最坏挡数十秒），在回调线程同步等会卡死该 broker 的整条
        # 下行 FIFO（其它 cid/hb/NACK 全部头阻塞）。
        self._resend_q = queue.Queue(maxsize=_RESEND_Q_MAX)
        self._rt_last = {}                        # NACK 大帧补发去重 (cid,seq)->t
        self._rt_last_prune = 0.0               # 上次清扫去重表时刻
        self.pace_intv = _PACE_START              # 当前大帧节奏（闭环热调）
        # hb 延迟地板：最近 _HB_FLOOR_WIN 秒内全 broker 样本的最小值。
        # 单调 min 会被一次 NTP 步进/GC stop-the-world/OS 调度抖动钉出的
        # 极小值永久锁死，此后 best 恒偏大、pace 单调爬到 _PACE_MAX，会话
        # 余生被误限速。滚动窗口让异常样本 60s 后自然淘汰（多线程回调，
        # 用专用锁保护 deque 与 min 计算）。
        self._hb_floor_samples = deque()
        self._hb_lock = threading.Lock()
        self._closing = False                     # close() 幂等/防 bnet 复活
        self._pace_ok = 0                         # 连续健康轮次（恢复要慢）
        self._last_ps = 0.0                       # 上次下发 ps 指令时刻
        self._open_mono = time.monotonic()      # 会话起点（pb 热身计时用）

    @property
    def hb_floor(self):
        """最近 _HB_FLOOR_WIN 秒 hb 延迟（ms）的滚动最小值；窗口内无样本
        返回 None。deque 的物理淘汰只发生在收到新 hb 时（_track_broker），
        长断网期间没有新 hb、旧样本会滞留；读取处再按当前 monotonic 过滤
        一遍，保证"无近期 hb → 地板消失"，不依赖新样本驱动淘汰。"""
        now = time.monotonic()
        with self._hb_lock:
            live = [d for t, d in self._hb_floor_samples
                    if now - t <= _HB_FLOOR_WIN]
            return min(live) if live else None

    def _hb_floor_inject(self, when: float, d: float,
                         left: bool = False) -> None:
        """测试钩子：在 _hb_lock 保护下写样本，不绕过锁封装契约。

        left=True 时插到队首（模拟"比当前所有样本更老"的历史样本，
        产品侧样本按 monotonic 单调 append，真实老样本恒在队首）。
        """
        with self._hb_lock:
            if left:
                self._hb_floor_samples.appendleft((when, d))
            else:
                self._hb_floor_samples.append((when, d))

    def _hb_floor_reset(self) -> None:
        """测试钩子：锁内清空滚动窗口。"""
        with self._hb_lock:
            self._hb_floor_samples.clear()

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
        threading.Thread(target=self._resend_loop, name="s5-rsnd",
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

        打号与入重传缓存在同一把 per-conn 锁内原子完成：序号必须连续无竞，
        否则服务端重组把晚到低 seq 当真缺帧、NACK/超时误断连接。**实际
        发送必须在锁外**：大帧走 _bulk_enqueue（队列满时按 0.5s 粒度反压，
        64 格最坏要挡数十秒），主连接 publish 在 paho 内部队列紧张时也会
        短暂阻塞；持着 seq_lock 阻塞会把同连接的 close 帧（eof_connection/
        drop_connection）一起堵在锁外，半关延迟数十秒。锁外乱序到达无害——
        服务端按 per-cid seq 重组，乱序只进 pending 不会错放（大帧走 bulk、
        小帧走主连接本就是两条路，到达顺序从来不保证）。
        """
        # extra 只允许业务键（open/d/close）：s5/cid/seq/owner 是打号与
        # 身份字段，必须由本函数统一生成，被 extra 覆盖会拼出非法序号/身份
        # 的帧且静默成功——编程错误，在占号之前直接抛出，不吞序号。
        bad = [k for k in ("s5", "cid", "seq", "owner") if k in extra]
        if bad:
            raise ValueError("_send 的 extra 不得携带保留帧字段 %r" % bad)
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
            big = isinstance(_d, str) and len(_d) > 2048
        if big:
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

    def _resend_loop(self) -> None:
        """NACK 补发的唯一消费线程（s5-rsnd）：把所有可能阻塞的补发动作
        （大帧 _bulk_q 反压、publish）移出 paho 回调线程。单线程串行，
        同一轮 NACK 列表内的补发顺序保持。"""
        while True:
            try:
                item = self._resend_q.get(timeout=0.5)
            except queue.Empty:
                if self._end_event.is_set():
                    return
                continue
            if item is None:
                return
            conn, seqs = item
            # 待办排队期间连接可能已被 drop：对象虽还在，但帧发出去服务端
            # 也会按未知 cid 丢弃，直接跳过（ucache 随 conn 对象一起回收）。
            with self._lock:
                alive = self._conns.get(conn.cid) is conn
            if not alive or self._end_event.is_set():
                continue
            try:
                self._resend_uplink(conn, seqs)
            except Exception as e:
                # 与交付线程同理：单轮补发异常不能打死消费线程
                try:
                    self.on_log("[cid=%s] NACK 补发异常（已跳过本轮）: %s: %s"
                                % (conn.cid, type(e).__name__, e))
                except Exception:
                    pass
            # 低频清扫补发去重表：只在超量后每 10s 扫一次，删 30s 前的键
            now = time.monotonic()
            if (len(self._rt_last) > _RT_LAST_MAX
                    and now - self._rt_last_prune > _RT_LAST_PRUNE_INTV):
                self._rt_last_prune = now
                stale = [k for k, t in self._rt_last.items()
                         if now - t > _RT_LAST_TTL]
                for k in stale:
                    self._rt_last.pop(k, None)

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
            now_mono = time.monotonic()
            st["hb_n"] += 1
            st["hb_sum"] += d
            st["hb_last_ms"] = d
            st["hb_last_t"] = now_mono
            # 滚动窗口地板：淘汰 60s 前的样本，异常极小值不会永久锁死 pacing
            with self._hb_lock:
                self._hb_floor_samples.append((now_mono, d))
                while (self._hb_floor_samples
                       and now_mono - self._hb_floor_samples[0][0]
                       > _HB_FLOOR_WIN):
                    self._hb_floor_samples.popleft()
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
            # 服务端上行缺口：补发只入待办队列，绝不在 paho 回调线程同步
            # 执行——大帧补发进 _bulk_q 时队列满可反压数十秒，会把该
            # broker 承载的全部会话/cid 下行（含 hb 与后续 NACK）头阻塞。
            self._track_broker(broker, data, is_new=False)
            if isinstance(nk, (list, tuple)) and nk:
                try:
                    # 有界队列 put_nowait：多 broker 回调并发下也是严格硬
                    # 上限（无 qsize 检查与入队之间的竞态窗口）。
                    self._resend_q.put_nowait((conn, list(nk)))
                except queue.Full:
                    self.on_log("[cid=%d] NACK 补发待办已满 %d，"
                                "丢弃本轮 %d 个待办（等下轮 NACK）"
                                % (cid, _RESEND_Q_MAX, len(nk)))
                except Exception:
                    pass
            return
        seq = data.get("seq")
        if seq is not None:
            is_new = conn.reassembly.add(seq, data)
            self._track_broker(broker, data, is_new=is_new)
        else:
            self._track_broker(broker, data)
            # 无 seq 宽松放行（兼容）：仍走该连接的交付 FIFO，不允许在
            # broker 回调线程直接写本地 socket 而绕过保序。
            conn.reassembly.deliver_now(data)

    def net_report(self) -> dict:
        """网络分析快照（JSON 可序列化）。全部来自被动观察，无额外请求：
        - downlink_race：下行 per-broker 首到胜率/副本率 + hb 延迟分布
        - server_side：服务端 hb 捎带的它自己各 broker 连接快照
        系统级每 broker 连接质量（在线率/掉线/重连）用内置的
        ``transport.node.mqtt_net.stats.get_report(probe=False)``（别动它，
        probe=True 会发起主动探测产生额外流量）。

        注意：这是**无锁非一致快照，仅供诊断参考**——conns 列表先在锁内
        拷出，但各 conn 的 reassembly.next/pending/bytes 在交付线程并发
        推进下逐字段读取，同一 cid 的 down_delivered_next 与 down_waiting
        可能瞬时对不齐（如 next=5 而 waiting 里还看得到 4），不保证也不
        需要事务一致性，不要据此刻画正确性判断。
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
                           "down_dropped_full": c.reassembly.dropped_full,
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
            with conn.state_lock:
                conn.remote_closed = True
            try:
                conn.wq.put_nowait(None)
            except queue.Full:
                # close 语义绝不能丢：wq 堆满（writer 正被慢本地端 sendall
                # 背压）时 None 投不进去，writer 排空后会永久阻塞在 get()，
                # 连 fd/线程/条目一起泄漏。强收尾：close(local_sock) 会
                # 打断 writer 的 sendall 使其退出（与服务端 _close_conn 对称）。
                self.on_log("[cid=%d] 本地写队列满，closed 信号无法入队，"
                            "强制收尾 %s" % (conn.cid, conn.desc))
                self.drop_connection(conn.cid,
                                     reason="close_signal_dropped")

    def _on_conn_gap(self, cid: int) -> None:
        self.on_log("[cid=%d] 下行 seq 缺口超过 %.1fs（NACK 多轮补发仍失败），"
                    "断开该连接（TCP 流不允许跳号）" % (cid, self.gap_timeout))
        self.drop_connection(cid, reason="downlink_gap")

    def _pace_control(self) -> None:
        """自适应 pacing 闭环（每 3s）。探针=最近 10s 内各 broker 下行 hb
        小帧延迟的最小值（即当前最快小帧路径；时钟偏差只取 min，天然稳）。
        最快小帧都 >3s：大帧已触发账号级惩罚，节奏放慢 1.5×；持续 <1.2s：
        逐步恢复。指令 ps 随 1 字节级控制帧全 broker 上行。"""
        if self._end_event.is_set() or self.in_topic is None:
            return  # 会话关闭中：不再产生任何上行帧
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
            # 与 close() 互斥：会话关闭中/已关闭绝不建连，否则新建的管理器
            # 不在 close() 的清理快照里，会带着一条真实 MQTT 连接泄漏
            # （"_bnet 复活"竞态）。
            if self._closing or self._end_event.is_set():
                return
            if want == self._bhosts:
                return
            old = self._bnet
            nn = None
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
                # wait_connected 最长阻塞 4s，期间 close() 可能已经拿走并
                # 停掉旧 _bnet：赋值前复查，关闭中则当场掐死新连接，绝不
                # 写回 self._bnet 造成复活。
                if self._closing or self._end_event.is_set():
                    try:
                        nn.stop()
                    except Exception:
                        pass
                    return
                self._bnet = nn
                self._bhosts = want
                self._bulk_ready.set()
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
                # self._bnet 只在上面的成功路径整体替换，异常发生时它从未
                # 被摘走——旧连接原样保留（若有），这里只停掉半成品 nn，
                # 绝不能误停仍在服务大帧的活连接。
                if nn is not None:
                    try:
                        nn.stop()
                    except Exception:
                        pass
                self.on_log("[s5] 上行大帧专用连接建立失败，保持现有连接: %r" % e)

    def _bulk_publish(self, frame: dict) -> None:
        """大帧走专用连接。

        未就绪（open 后 ~4s 建连窗口/重建中）时在本线程等就绪事件：
        _bulk_q 有界（64），等待会让队列自然填满并反压本地读线程、闭合
        TCP 窗口——这是正确的背压。**绝不回退主连接全 broker**：16KiB
        大帧在热身热点期 13× 扇出会直接触发公共 broker 账号级限流惩罚
        （与服务端 _pub 子集失败不回退同一策略）。等够仍不可用则丢帧
        留痕，缺口由服务端 NACK 触发 _resend_uplink 从 ucache 重入本队列。
        """
        if self._end_event.is_set():
            return  # 会话收尾：队列已排空，丢弃不记噪声日志
        node = self._bnet
        if node is None or not getattr(node, "clients", None):
            self._bulk_ready.wait(timeout=_BULK_READY_WAIT)
            if self._end_event.is_set():
                return
            node = self._bnet
        if node is not None and getattr(node, "clients", None):
            try:
                node.publish_broadcast(self.in_topic, frame)
                return
            except Exception as e:
                try:
                    self.on_log("[s5] 上行大帧专用连接发送异常，丢帧待NACK补发: %r" % e)
                except Exception:
                    pass
        # 未就绪或发送异常：丢帧但必须留痕（序号可对照 ucache），限流
        # 日志防大帧热点期刷屏；本方法只在 s5-bulk 单线程调用，计数无需锁。
        now = time.monotonic()
        self._bulk_drop_n += 1
        if (self._bulk_drop_n == 1
                or now - self._bulk_drop_log_t > 5.0):
            self._bulk_drop_log_t = now
            try:
                self.on_log(
                    "[s5] 上行大帧专用连接未就绪，丢帧待NACK补发 "
                    "(累计%d): cid=%s seq=%s"
                    % (self._bulk_drop_n, frame.get("cid"), frame.get("seq")))
            except Exception:
                pass

    def _bulk_enqueue(self, frame: dict) -> None:
        """大帧入平滑队列；队列满反压本地读线程（TCP 窗口闭合）。"""
        while not self._end_event.is_set():
            try:
                self._bulk_q.put(frame, timeout=0.5)
                return
            except queue.Full:
                pass
        # 会话结束导致未入队：以前静默返回，发送方以为帧已进管线，线上
        # 定位"最后几 KB 没到"时完全无线索，必须留痕（序号可对照缓存）。
        try:
            self.on_log("[s5] 会话结束，上行大帧未入平滑队列而丢弃: cid=%s seq=%s"
                        % (frame.get("cid"), frame.get("seq")))
        except Exception:
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
                    gap=self.gap_timeout, name=str(cid),
                    on_log=self.on_log))
            self._conns[cid] = conn
        try:
            self._send(conn, {"open": {"host": str(host), "port": int(port)}})
        except Exception:
            with self._lock:
                self._conns.pop(cid, None)
            # 发送已移到 seq_lock 外：失败回滚时必须停掉交付线程，否则它
            # 永久阻塞在空 FIFO 上（daemon 不挡退出，但每条失败连接漏一个）。
            conn.reassembly.close()
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
        if conn is None:
            raise RemoteError("连接已关闭")
        with conn.state_lock:
            closed = conn.closed
        if closed:
            raise RemoteError("连接已关闭")
        conn.bytes_up += len(data)
        self._send(conn, {"d": bytes_to_wire(bytes(data))})

    def eof_connection(self, cid: int) -> None:
        """本地读到 EOF：in-seq 发 close，服务端对目标半关写（SHUT_WR），
        目标剩余响应照常经下行回传（HTTP POST 等场景必须半关）。"""
        with self._lock:
            conn = self._conns.get(cid)
        if conn is None:
            return
        with conn.state_lock:
            closed = conn.closed
        if closed:
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
        # closed 置位与 remote_closed 快照必须原子：与 _deliver 的远端
        # closed 帧同瞬到达时，据此一致决定要不要补发上行 close，writer
        # 也据此对 clean EOF / local_error 做不撕裂的判定。
        with conn.state_lock:
            conn.closed = True
            remote_closed = conn.remote_closed
        conn.reassembly.close()
        self.total_bytes_up += conn.bytes_up
        self.total_bytes_down += conn.bytes_down
        try:
            conn.wq.put_nowait(None)
        except Exception:
            pass
        # clean EOF 的 socket 归 s5-fin 线程按"应用读完/grace"节奏关，这里
        # 直接 close 会在 Windows 上把未读数据打成 RST
        if conn.local_sock is not None and not conn.fin_close_pending:
            try:
                conn.local_sock.close()
            except Exception:
                pass
        if not self.ended and not remote_closed:
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
        """断开会话：通知服务端停止、退订、关掉所有本地连接；不停 transport。

        顺序关键：必须**先**置结束位再做任何清理，最后才清会话身份。旧顺序
        先干活、末尾才清 sid，而服务端不回 end（UDP 式 qos0 丢包/1.5s 超时）
        时 _end_event 永远不置位——sweep/pb/bulk 线程与 open() 异步拉起的
        _ensure_bnet 全程活着：会继续 publish（sid 清空后拼出垃圾帧），还
        可能在 bnet 已 stop 之后新建并写回 _bnet（真实 MQTT 连接无人停）。
        """
        if self._closing:
            return
        self._closing = True
        # 1. 先停本地所有循环：sweep/pb/bulk/ensure 立即不再产生帧与新连接
        self._end_event.set()
        # 唤醒可能阻塞在"等 bulk 就绪"上的 s5-bulk 线程，让它立刻看到
        # 结束位而不是熬满 _BULK_READY_WAIT 才排空队列、退哨兵。
        self._bulk_ready.set()
        # 2. best-effort 通知服务端停止（此刻 sid/topic 仍在，帧合法）；
        #    不阻塞等待 end 回包：本地资源回收不依赖服务端确认，它收不到
        #    stop 也有 TTL 兜底。
        sid, in_topic, owner = self.sid, self.in_topic, self.owner
        if sid is not None and in_topic is not None:
            try:
                self.tr.publish(in_topic,
                                {"s5": sid, "stop": True, "owner": owner})
            except Exception:
                pass
        # 3. 退订：杜绝收尾期间又有下行帧进来触发收尾/日志
        if self._handler is not None and self.out_topic is not None:
            try:
                self.tr.stream_unsubscribe(self.out_topic, self._handler)
            except Exception:
                pass
        # 4. 关全部本地连接（ended=True，drop 不再补发上行 close 垃圾帧）
        self.abort_all()
        # 5. 排空大帧队列并投哨兵停 bulk 发送线程；NACK 补发队列同样
        #    排空+哨兵，避免 s5-rsnd 持着已关闭会话的待办空转
        try:
            while True:
                self._bulk_q.get_nowait()
        except Exception:
            pass
        try:
            self._bulk_q.put_nowait(None)
        except Exception:
            pass
        try:
            while True:
                self._resend_q.get_nowait()
        except Exception:
            pass
        try:
            self._resend_q.put_nowait(None)
        except Exception:
            pass
        # 6. 停 bulk 管理器：持 _bnet_lock 停，与 _ensure_bnet 互斥，杜绝
        #    "已 stop 又被异步线程重建"的复活泄漏
        with self._bnet_lock:
            bn = self._bnet
            self._bnet = None
            self._bhosts = frozenset()
        if bn is not None:
            try:
                bn.stop()
            except Exception:
                pass
        # 7. 最后清会话身份：任何漏网路径也拼不出带本会话 sid 的帧
        self.sid = None
        self.owner = None
        self.in_topic = None
        self.out_topic = None


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
            # 泵送阶段的非预期异常：reader 自身已对已知错误收尾，这里是最后
            # 一道保险（防止未来新增逃逸路径让 conn/writer/fd 静默泄漏）。
            # fin 已接管 socket（clean EOF）时不抢关：reader_done 已置位，
            # fin 线程会自己立刻 close。
            if entered_pump and cid is not None:
                try:
                    with self.session._lock:
                        cc = self.session._conns.get(cid)
                    if cc is not None and not cc.fin_close_pending:
                        self.session.drop_connection(
                            cid, reason="handle_fatal")
                except Exception:
                    pass
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
        # 入口即持有本连接对象：cid 单调不回收，之后即便条目被 drop 弹出
        # _conns，本线程的信号/状态也必须落到自己的 conn 上，不能再查字典。
        with sess._lock:
            own = sess._conns.get(cid)
        try:
            while not sess.ended:
                data = csock.recv(sess.frame_max)
                if not data:
                    sess.eof_connection(cid)
                    return
                sess.send_data(cid, data)
        except (ConnectionError, OSError, RemoteError):
            if own is not None:
                with own.state_lock:
                    remote_closed = own.remote_closed
                if remote_closed:
                    # csock 是 writer/fin 在远端 closed 后主动 shutdown/close
                    # 的，收尾归它们，这里只退出
                    return
            sess.drop_connection(cid, reason="local_error")
        except Exception:
            # 网络三异常以外的非预期错误（库内部 RuntimeError/MemoryError
            # 等）：正常路径不会走到。不兜住的话异常只冒泡到 _handle 记一行
            # 日志，而 entered_pump 后 _handle 不关 csock、writer 永远阻塞
            # 在 wq.get()——fd、conn 条目、writer 线程全部静默泄漏。
            try:
                sess.drop_connection(cid, reason="reader_fatal")
            except Exception:
                pass
        finally:
            # 读线程退出 = 应用端已关闭（FIN/RST 已到），fin 可安全 close
            # fd。必须无条件置位：clean EOF 时 drop_connection 已先于应用
            # 关闭把条目弹出 _conns，靠字典回查会让 fin 白等 120s grace。
            if own is not None:
                own.reader_done.set()

    def _local_writer(self, cid: int, csock: socket.socket) -> None:
        """下行帧 -> 本地 socket。收到 None（远端 closed 或本地收尾）即结束；
        正常路径（远端 closed）由这里做整条连接的最终收尾。"""
        sess = self.session
        with sess._lock:
            conn = sess._conns.get(cid)
        if conn is None:
            return
        clean = False
        already_closed = False
        try:
            while True:
                item = conn.wq.get()
                if item is None:
                    # 区分：远端 EOF（clean）还是 drop_connection 强收尾。
                    # 两个标志必须在同一把锁内成对快照，否则 None 入队与
                    # drop 同瞬发生时，正常远端 EOF 会被误判成 local_error。
                    with conn.state_lock:
                        already_closed = conn.closed
                        clean = conn.remote_closed and not already_closed
                    break
                csock.sendall(item)
        except (ConnectionError, OSError):
            clean = False
            with conn.state_lock:
                already_closed = conn.closed
        if clean:
            # 远端半关：只 shutdown 写方向给应用发 FIN，绝不能直接 close()——
            # Windows 下 socket 接收缓冲里还有应用未读完的数据时，close()
            # 会直接发 RST，把已完整送达的下载/响应在应用侧打成连接重置。
            try:
                csock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            # socket 最终 close 权移交专职 fin 线程：它等本地读线程退出
            # （应用读完）或 grace 上限；会话被 abort 时 0.5s 内兜底关 fd。
            # 会话侧资源（_conns 条目/重组/缓存/统计）则**立即**回收，
            # writer 线程不再为一个只读长连接白挂 120s。
            conn.fin_close_pending = True

            def _fin_close(_c=conn, _s=csock):
                deadline = time.monotonic() + _FIN_CLOSE_GRACE
                while not sess.ended:
                    remain = deadline - time.monotonic()
                    if remain <= 0:
                        break
                    if _c.reader_done.wait(min(0.5, remain)):
                        break
                if not _c.reader_done.is_set():
                    # 走到这里说明 reader 仍阻塞在 recv（应用拖着写端不关，
                    # 或会话 abort）。Windows 上另一线程 close() 叫不醒阻塞
                    # 的 recv（fd 被一直占着），必须先 shutdown 双向：recv
                    # 随即返回 EOF/异常，reader 退出并置 reader_done。
                    # 连接条目此时已被 drop 弹出，reader 的 eof/drop 均 no-op。
                    try:
                        _s.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                try:
                    _s.close()
                except Exception:
                    pass

            threading.Thread(target=_fin_close,
                             name="s5-fin-%d" % cid, daemon=True).start()
            sess.drop_connection(cid, reason="remote_closed")
        else:
            try:
                csock.close()
            except Exception:
                pass
            if not already_closed:
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
