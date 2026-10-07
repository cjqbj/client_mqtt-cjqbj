#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client 子包 —— 所有客户端相关代码。

- :mod:`client.client_mqtt`     基础 MQTT RPC 客户端（统一别名表 / REPL）
- :mod:`client.cmd_client_mqtt` remote_cmd 的 MQTT 绑定 + 命令行/交互 shell
- :mod:`client.pty_client_mqtt` PTY over MQTT（SSH 式交互终端）
- :mod:`client.client_http`     HTTP 版文件上传客户端
- :mod:`client.remote_cmd`      与网络层无关的远端 Shell/PTY 命令封装层

同时兼容两种用法：
    python client/client_mqtt.py        # 直接按脚本运行
    from client import client_mqtt      # 作为包导入
"""
import os
import sys

# 作为包被导入时，子模块仍使用扁平绝对导入（multi_mqtt / server_mqtt /
# server_http 位于项目根，client_mqtt / remote_cmd 位于本目录），
# 这里把两个目录都补进 sys.path 兜底。
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
for _p in (_ROOT, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
