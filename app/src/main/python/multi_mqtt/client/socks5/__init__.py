#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client.socks5 —— SOCKS5 over 公共 MQTT broker 的多文件包版本。

与冻结单文件版 client/socks5_client_mqtt.py 的关系
==================================================
本包是其**真模块化拆分**：函数/类/常量逐字节搬运（来源行号标注在各
模块头部注释），``verify_parity.py`` 可机器校验两侧逐符号源码一致。
单文件版冻结期间一个字节不许改；本包即编辑入口。

与 client/pty（parts 字节切片 + __init__ 共享命名空间 exec）方案不同：
原文件全程没有 ``global`` 重绑定、模块级常量均为不可变值，因此可以拆
成互相 import 的**真模块**——IDE 可导航、grep 可达、无 exec 魔法、无
重组器，也不存在跨模块全局拷贝漂移问题。

结构
====
- config.py          统一别名表 + 调优常量（纯数据）
- util.py            _info：stderr 直写日志
- server_template.py 服务端启动代码模板（握手整段下发）+ build_socks5_start_code
- reassembly.py      _ConnReassembly：per-cid 下行重组（缺口绝不跳号）
- conn.py            _ClientConn：会话内单连接状态
- session.py         RemoteSocks5：远端出口会话（帧收发/竞速统计/NACK/pacing）
- local_server.py    _recv_exact/_socks_reply/_Socks5Server：本地 SOCKS5 监听
- cli.py             build_parser / main
- verify_parity.py   与冻结单文件版的逐符号校验（命名空间/源码/模板/CLI help）

用法
====
    from client.socks5 import RemoteSocks5, main   # 等价单文件版同名 API
    python -m client.socks5                        # 等价 python client/socks5_client_mqtt.py
    python client/socks5/verify_parity.py          # 校验与单文件版逐符号一致
"""
import logging
import os
import sys

# ---- sys.path 引导（与单文件版第 85-96 行语义一致）----
# 1) 项目根 + client/ 入 path：multi_mqtt / server_http 在项目根，扁平模块
#    名在 client/ 目录，两种启动/导入方式都要能找到；
# 2) CMQ_NO_STDERR_LOG 必须先于 multi_mqtt 的首次导入：抑制其挂到 root
#    logger 的 stderr StreamHandler（broker/paho 的 INFO 噪音），本进程
#    日志走自己的 _info。
_HERE = os.path.dirname(os.path.abspath(__file__))
_CLIENT_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_CLIENT_DIR)
for _p in (_ROOT, _CLIENT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")

logger = logging.getLogger("socks5_client_mqtt")

from .config import (                     # noqa: E402
    alias_listen_port, alias_listen_host, alias_heartbeat, alias_ttl,
    alias_dead_timeout, alias_connect_timeout, alias_frame_max,
    alias_gap_timeout, alias_rpc_port, alias_rpc_host,
    _PB_SEED, _PB_MAX, _PB_INTV, _GAP_DEFAULT, _HB_FLOOR_WIN,
    _FIN_CLOSE_GRACE, _NACK_FIRST, _NACK_INTV, _CACHE_MAX, _BULK_INTV,
    _PACE_START, _PACE_MAX, _WQ_MAX, _INGRESS_MAX, _RESEND_Q_MAX,
    _RT_LAST_MAX, _RT_LAST_TTL, _RT_LAST_PRUNE_INTV, _BULK_READY_WAIT,
)
from .util import _info                                   # noqa: E402
from .server_template import (                            # noqa: E402
    _SOCKS5_START_TEMPLATE, build_socks5_start_code,
)
from .reassembly import _ConnReassembly                   # noqa: E402
from .conn import _ClientConn                             # noqa: E402
from .session import RemoteSocks5                         # noqa: E402
from .local_server import (                               # noqa: E402
    _recv_exact, _socks_reply, _Socks5Server,
)
from .cli import build_parser, main                       # noqa: E402

__all__ = [
    # 会话与本地服务
    "RemoteSocks5", "_Socks5Server", "_ClientConn", "_ConnReassembly",
    # 函数
    "build_socks5_start_code", "build_parser", "main", "_info",
    "_recv_exact", "_socks_reply",
    # 日志
    "logger",
    # 模板
    "_SOCKS5_START_TEMPLATE",
    # 别名表（CLI 选项名）
    "alias_listen_port", "alias_listen_host", "alias_heartbeat", "alias_ttl",
    "alias_dead_timeout", "alias_connect_timeout", "alias_frame_max",
    "alias_gap_timeout", "alias_rpc_port", "alias_rpc_host",
    # 调优常量
    "_PB_SEED", "_PB_MAX", "_PB_INTV", "_GAP_DEFAULT", "_HB_FLOOR_WIN",
    "_FIN_CLOSE_GRACE", "_NACK_FIRST", "_NACK_INTV", "_CACHE_MAX",
    "_BULK_INTV", "_PACE_START", "_PACE_MAX", "_WQ_MAX", "_INGRESS_MAX",
    "_RESEND_Q_MAX", "_RT_LAST_MAX", "_RT_LAST_TTL",
    "_RT_LAST_PRUNE_INTV", "_BULK_READY_WAIT",
]
