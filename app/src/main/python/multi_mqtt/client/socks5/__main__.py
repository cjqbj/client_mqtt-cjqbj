#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""``python -m client.socks5`` 等价 ``python client/socks5_client_mqtt.py``。

也兜底兼容 ``python client/socks5/__main__.py`` 直接按脚本运行：无包上下
文时补项目根进 sys.path 后按绝对包名导入。
"""
import os
import sys

if __package__ in (None, ""):
    _root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from client.socks5.cli import main
else:
    from .cli import main

if __name__ == "__main__":
    sys.exit(main())
