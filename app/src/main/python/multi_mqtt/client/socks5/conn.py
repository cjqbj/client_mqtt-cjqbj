#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""_ClientConn：一条本地 SOCKS5 连接对应的会话内状态条目。"""
from __future__ import annotations

import queue
import threading
import time

from .config import _WQ_MAX


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 1307-1347 行（冻结基线，校验见 verify_parity.py） ——
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
