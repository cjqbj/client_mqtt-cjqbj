#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""stderr 直写小工具（不走 logging，保证时序与进程退出前可见）。"""
from __future__ import annotations

import sys

from multi_mqtt import stime


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 162-167 行（冻结基线，校验见 verify_parity.py） ——
def _info(msg):
    sys.stderr.write("[%s] %s\n" % (stime(), msg))
    try:
        sys.stderr.flush()
    except Exception:
        pass
