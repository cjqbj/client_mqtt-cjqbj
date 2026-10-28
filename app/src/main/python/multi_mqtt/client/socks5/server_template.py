#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""服务端启动代码模板：握手时整段下发、在远端执行的自包含 Python 源码文本。

与 pty 的 _PTY_START_TEMPLATE 同一模型：executor 全局命名空间跨握手
持久；模板内函数体不得 print；最后一条裸表达式调用，REPL 语义把返回
的 JSON 放进 r。改动模板后必须跑 verify_parity.py 与冻结单文件版比对。
"""
from __future__ import annotations

import json


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 170-1144 行（冻结基线，校验见 verify_parity.py） ——
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
        # _in_loop 单帧处理异常计数（畸形帧防护，见 _in_loop_run）：经心跳
        # fe 字段暴露给客户端，>0 说明有上行帧被丢弃，应排查数据完整性。
        _fr_errs = 0

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

        def _in_loop_run():
            # 上行重组：同一帧被每个 broker 各投递一次且会乱序。按 per-cid
            # seq 连续放行；缺口超 _GAP 秒判真丢帧——SOCKS5 承载 TCP 流，
            # 跳号=数据损坏，必须断连而不是像 PTY 那样跳号放行。
            # nonlocal：_pb/_pace_intv 必须与 _bulk_pub/_bsend_loop 读的
            # 是同一个闭包单元；global 会让 pb 写进模块全局而 bulk 路径
            # 永远读到空名单（大帧全部退回 13 broker 广播）。
            nonlocal _pb, _pace_intv, _fr_errs
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
                    # 收尾路径异常不允许带死会话单点线程：计数后继续
                    try:
                        _close_conn(_c2, "uplink_gap")
                    except Exception:
                        _fr_errs += 1
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
                    try:
                        _retrans(_cid, _nkv)  # 下行缺帧补发请求
                    except Exception:
                        _fr_errs += 1
                    continue
                _sq = _fr.get("seq")
                if _sq is None:
                    try:
                        _deliver(_cid, _fr)  # 无 seq 帧宽松放行（兼容）
                    except Exception:
                        _fr_errs += 1
                    continue
                _ready = []
                with _lock:
                    _st = _ingress.get(_cid)
                    if _st is None:
                        _st = {"next": 0, "pending": {}, "deadline": None,
                               "nd": None, "nr": 0}
                        _ingress[_cid] = _st
                    try:
                        _sq = int(_sq)
                    except Exception:
                        # 畸形 seq（非数字字符串等）：丢帧计数，绝不带死会话
                        _fr_errs += 1
                        continue
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
                    try:
                        _deliver(_cid, _f2)
                    except Exception:
                        # open 帧的 int(port) 解析等可抛路径发生在 _deliver
                        # 内：丢帧计数，并按连接级错误收尾该 cid（_conns 条
                        # 目此时可能已建而 worker 线程没起来，不收尾会挂到
                        # 会话结束）
                        _fr_errs += 1
                        try:
                            _close_conn(_cid, "deliver_error")
                        except Exception:
                            pass

        def _teardown():
            # 会话结束：关全部连接、退订、弹注册表、发 end 帧。
            # 单步全部兜异常：收尾路径的任何失败不允许短路后续步骤。
            with _lock:
                _all = list(_conns.keys())
            for _c3 in _all:
                try:
                    _close_conn(_c3, "session_end")
                except Exception:
                    pass
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

        def _in_loop():
            # 会话单点守卫：_in_loop_run 抛任何未预见异常也必须走完收尾。
            # 原先收尾块跟在 while 后面，线程一死全部跳过——心跳照发、
            # 会话表面健康，上行却彻底黑洞，_inq 无界堆积直到 24h TTL。
            # 线程 target 名保持 _in_loop：e2e 测试按函数名从其闭包向下
            # 钻取会话私有状态（_in_loop_run/_teardown 的 cell 可达）。
            try:
                _in_loop_run()
            finally:
                _teardown()

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
                          "fe": _fr_errs,
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
