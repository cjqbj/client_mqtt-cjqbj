#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""_ConnReassembly：per-cid 单向下行字节流重组缓冲（缺口绝不跳号）。"""
from __future__ import annotations

import queue
import threading
import time

from .config import _CACHE_MAX, _GAP_DEFAULT, _INGRESS_MAX, _NACK_FIRST, _NACK_INTV


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 1149-1304 行（冻结基线，校验见 verify_parity.py） ——
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
