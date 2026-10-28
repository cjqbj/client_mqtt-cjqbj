#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pty_client_mqtt —— PTY over 公共 MQTT broker（SSH 式交互终端）。

模型（和 SSH 一样）
===================
- 仿 cmd_client_mqtt 建立 MQTT 连接（默认 ``sys/device/request`` ↔ response）；
- 连接成功后，client 生成会话 id 并提出 in/out 两个**新 topic**，协商成功后
  用于实时数据传输（底层仍完全复用 multi_mqtt.MultiMQTTManager，不新建连接）；
- 服务端只要求运行通用 server_mqtt.py：PTY 全部功能代码（openpty + 常驻
  用户 shell + 读写线程）由本 client 在握手时**整段下发执行**；shell 只启动
  一次，之后每个按键都经 broker 写入同一个 PTY，绝不在每条命令上重启 bash；
- 服务端主动发送频率可用 ``--interval`` 设定（默认 0=有输出立刻实时回传；
  >0 时按该秒数攒批合并输出，适合高延迟网络减少帧数）。

用法
====
    python client/pty_client_mqtt.py                    # 交互式 PTY（默认参数）
    python client/pty_client_mqtt.py -i 0.2             # 服务端最多 0.2s 攒批
    python client/pty_client_mqtt.py --shell /bin/bash --cwd /root
    python client/pty_client_mqtt.py -t q -k 2333 "tmux at"
                                                        # SSH 式：连上自动执行
                                                        # 前置命令，随后留在会话里

AI 常驻调用（监控窗口模型，避免每条命令重连 broker）
====================================================
本进程启动时内置一个本地 HTTP RPC 口（默认 1188）。人类先在一个终端里
开着本窗口，AI / 其他进程不新建 MQTT 连接，直接 HTTP 复用同一个 PTY，
命令回显与输出照样在本窗口实时可见：

    from client.pty_client_mqtt import ai_pty_run, ai_pty_status, ai_pty_send
    ai_pty_status()                 # {'attached': True, 'brokers_online': 13, ...}
    ai_pty_run("uname -a")          # -> {'ok': True, 'rc': 0, 'out': '...'}
    ai_pty_run("apt install -y htop", timeout=300)
    ai_pty_send("y\\n")             # 回答交互提示（密码/确认）

裸 HTTP 等价写法（POST 一段 Python 到本地口）：

    curl "http://127.0.0.1:1188/$(python -c \
"import urllib.parse;print(urllib.parse.quote('import json;p.set_data(json.dumps(ai_bridge.run(\\\"uname -a\\\"),ensure_ascii=False))'))")"

MCP 工具服务（Model Context Protocol，AI 客户端标准工具协议）
====================================================
pip install "mcp[cli]" 后，常驻 PTY 自动以标准 MCP 工具形式持续暴露：
默认**复用 --port 的 HTTP 口**（路径 /mcp，与 RPC/浏览器日志台同口并存，
不新开端口；新版 SDK 走 streamable-http，旧版退回 /sse+/messages）：

    claude mcp add --transport http pty http://192.168.1.3:1188/mcp

工具集：pty_run 跑命令回收 rc/输出、pty_tail 查长命令实时进度、
pty_send 喂交互键、pty_pane 查询/设置 AI 命令的 tmux 窗口、pty_status
会话与 broker 状态、pty_log 本地诊断日志、pty_redraw 写卡滞自愈+整屏
重绘；另有 9 个远端文件工具：fs_read/fs_ls/fs_glob/fs_grep 只读，
fs_write/fs_edit/fs_mkdir/fs_move/fs_delete 默认允许写任意路径
（pty_run 本就能执行任意代码，写入根限制没有实际意义；--fs-read-only 可
整体禁写）。文件工具走 ai_bridge.transport.request 把 Python 源码发到
远端 server_mqtt.py 进程里 exec，作用于**远端机器**的文件系统（路径语义
是远端的，如 /root/...），既不经过交互 shell、也不占 busy 锁，与
client_mqtt.rpc 同一条通道。pty_* 与 HTTP RPC 口同源同锁。
--no-mcp 关闭；--mcp-port>0 改为另起独立端口（--port 0 关闭 RPC 口时仍
要 MCP 的场景）。未装 mcp 包只记日志，PTY 照常工作。

AI 命令的三条投递通道（run() 自动选择，逐级回落）
==================================================
① tmux 推送（默认）：客户端一次 RPC 让远端装 pipe-pane + send-keys，
远端起后台线程本地增量读输出并逐帧 publish，客户端只订阅收流——没有
"每读一次一往返"，长命令输出可以边产生边到达（pty_tail 即看这份实时
缓冲）。② tmux 轮询：远端不认识推送 op（旧版本）时退回客户端按 offset
增量读。③ 原生 PTY：tmux 不可用时直写本 PTY，行为与改造前一致。

每次 MCP 工具调用都会分配一个 6 位调用 id（os.urandom(3).hex()），开始行
带完整参数，完成行整行缩进、带参数短摘要 + 状态 + 耗时，两行各自可独立
阅读，并发交错也不影响归属：
    [MCP] 3f9a2b 开始 fs_read file_path='/tmp/a.txt' offset=0 limit=2000
       [MCP] 3f9a2b 完成 fs_read file_path='/tmp/a.txt' offset=0 limit=2000 ok 0.36s
业务失败是 FAIL + 原因，异常是 FAIL + 异常类型；参数按 repr 转义成单行、
开始行单值超 400 字符截断、完成行单值只留前 60 字符。不用等远端返回，
远端卡死/超时时也能从日志台看出是哪个调用没收尾。/mcp 的 2xx HTTP 访问行
（每次调用伴随多条 POST、既无工具名也无参数）已被过滤，只保留 4xx/5xx；
无状态 SDK 的 "Terminating session: None" INFO 已压到 WARNING，避免冲散日志。

本地诊断日志与 PTY 画面彻底分离
====================================
本终端窗口**只渲染远端 shell**；broker 连接/重连、每笔 [RPC] 请求、
告警、任何库的 print / stdout / stderr 一律不写终端，全部进进程内
环形缓冲，查看通道有三个：

  ① 浏览器全屏实时日志台（首选，和普通控制台一样，无工具栏）：

        http://192.168.1.3:1188/        # 根路径自动跳 log_html 页面
        # 页面经 WebSocket /wslog 先收全量快照再实时增量推送，
        # 滚到底自动跟随，向上翻则暂停，带极简 ANSI 着色；
        # 手机/同局域网其他机器也能直接开

  ② HTTP RPC 取文本：

        curl "http://192.168.1.3:1188/r=get_log()"     # 最近 200 行
        curl "http://192.168.1.3:1188/r=get_log(50)"   # 最近 50 行
        curl "http://192.168.1.3:1188/r=clear_log()"   # 清空

  ③ 窗口前的人按命令栏热键输 log（自擦覆盖层，不留痕迹）。

--port 0 关闭 HTTP 口时没有查看通道，退回旧行为：日志镜像到 stderr。

注意：AI 命令串行执行（一个 shell），重叠调用立刻返回 busy；run() 只
回收非交互命令的输出，vim/top 等全屏程序请人工在窗口里操作。仅本机
调用建议 --host 127.0.0.1。

断开
====
- 在远端 shell 里 ``exit`` / Ctrl-D（会话自然结束，客户端立即退出）；
- 本地脱离：Ctrl-]（默认，``--detach-key`` 可改），只断 client，远端
  tmux/shell 原封不动继续跑，稍后重连再 ``tmux at`` 即可；
- 本地命令栏：Ctrl+Alt+Insert（默认，``--menu-key`` 可改），可执行
  detach、status，并热调 interval/heartbeat/ttl/dead 等时间参数，
  输入的内容不会发到远端；
- 服务器进程被关 / 网络中断：心跳超时（``--heartbeat`` / ``--dead-timeout``）
  后客户端自动立即退出，绝不在 broker 仍在线时无限干等。

所有退出路径都学 client_mqtt.py 直接 ``os._exit``：先恢复本地终端模式、
fire-and-forget 发一帧 stop，不做 pty.close/transport.stop 那套慢清理。
"""
from __future__ import annotations

import argparse
import base64
import codecs
import io
import json
import logging
import os
import queue
import re
import shlex
import shutil
import socket
import sys
import threading
import time
from collections import deque

# 本文件已迁移到 client/ 子目录：把项目根目录与本目录加入 sys.path，
# 同时兼容「python client/pty_client_mqtt.py」直接运行与包导入。
_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in (os.path.dirname(_HERE), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# 必须在 import multi_mqtt 之前：multi_mqtt 在 import 时会 basicConfig 往 root
# 挂写 stderr 的 StreamHandler，paho/broker 的 INFO 会插进 PTY 远端画面。
# 本进程的终端只渲染远端 shell，一切本地日志走环形缓冲 + 浏览器实时日志台。
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")
# 直接按脚本启动时没有包上下文，相对导入会失败，显式补上。
if not __package__:
    __package__ = "client"

from . import client_mqtt as _cm            # noqa: E402  复用统一别名表/_cli_opts
from .cmd_client_mqtt import (              # noqa: E402
    MqttTransport, add_connection_args,
)
from .remote_cmd import (                   # noqa: E402
    RemotePty, RemoteShell, RemoteError, DEFAULT_PTY_TTL,
    RemoteOpError, _PtyReorderBuffer,
)

from multi_mqtt import stime, BROKER_LIST

