#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RemoteSocks5：远端 SOCKS5 出口会话客户端（网络层由注入的 transport 决定）。

生命周期：open → open_connection / send_data / eof_connection /
drop_connection → close；帧收发、per-broker 竞速统计、NACK 补发与
自适应 pacing 都在本模块。
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections import deque

from ..remote_cmd import (
    Transport, RemoteError, RemoteTimeout, RemoteRpcError, RemoteOpError,
    bytes_to_wire, wire_to_bytes, DEFAULT_TIMEOUT, PTY_FRAME_MAX,
    _parse_pty_responders,
)
from multi_mqtt import BROKER_LIST, MultiMQTTManager

from .config import (
    _BULK_INTV, _BULK_READY_WAIT, _CACHE_MAX, _GAP_DEFAULT, _HB_FLOOR_WIN,
    _PACE_MAX, _PACE_START, _PB_INTV, _PB_MAX, _PB_SEED, _RESEND_Q_MAX,
    _RT_LAST_MAX, _RT_LAST_PRUNE_INTV, _RT_LAST_TTL,
)
from .conn import _ClientConn
from .reassembly import _ConnReassembly
from .server_template import build_socks5_start_code


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 1350-2321 行（冻结基线，校验见 verify_parity.py） ——
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
