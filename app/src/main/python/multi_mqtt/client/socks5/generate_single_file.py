#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client/socks5 包 -> 单文件重组器。

把包内 8 个模块「来源标记」之后的逐字节切片，按冻结基线的版面（头部、
空行、分节注释、__main__ 守卫均为内嵌冻结常量）拼回一个自包含单文件。

**默认只生成、不与任何现有文件比对**：结果写入正式单文件
client/socks5_client_mqtt.py（已存在需 -f 覆盖）。需要校验时显式 --check，
与冻结基线 client/old=socks5_client_mqtt.py 做字节 + AST 双重比对——包
未编辑时应逐字节一致（sha256 相同）；编辑过则 AST 比对可区分"纯版面
差异"与"语义差异"。

用法：
    python client/socks5/generate_single_file.py -f         # 只生成：写 ../socks5_client_mqtt.py（不比对）
    python client/socks5/generate_single_file.py --check    # 只比对：对照 old=socks5_client_mqtt.py（字节+AST）
    python client/socks5/generate_single_file.py -o 路径 -f # 只生成：写到指定文件
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import io
import os
import re
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
# 冻结基线（原 socks5_client_mqtt.py 改名而来，只读，--check 的比对对象）
FROZEN_SINGLE = os.path.join(os.path.dirname(_HERE), "old=socks5_client_mqtt.py")
# 正式单文件：默认输出，由本生成器从包重组写回
DEFAULT_OUT = os.path.join(os.path.dirname(_HERE), "socks5_client_mqtt.py")

# ---- 冻结版面常量（构建时从原文件逐字节提取，修改它们等于篡改基线） ----

# 原文件第 1-111 行：shebang / 模块 docstring / imports / sys.path 引导 / logger
HEAD = r'''#!/usr/bin/env python3
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
'''

# 原文件第 1147 / 2324 / 2582 行的分节注释
SECTION_SESSION = '# ============================ 客户端会话 ============================\n'
SECTION_LOCAL = '# ============================ 本地 SOCKS5 监听服务 ============================\n'
SECTION_CLI = '# ============================ CLI ============================\n'

# 原文件第 2733-2734 行：__main__ 守卫
GUARD = r'''if __name__ == "__main__":
    sys.exit(main())
'''

# 包模块 -> 冻结原文件切片行号的既定版面（模块名, 起行, 止行）。
# 切片之间的空行/分节注释由本生成器按原版面回填。
LAYOUT = [
    ("config.py", 113, 159, ""),
    ("util.py", 162, 167, "\n" * 2),
    ("server_template.py", 170, 1144, "\n" * 2),
    ("reassembly.py", 1149, 1304, "\n" * 2 + SECTION_SESSION + "\n"),
    ("conn.py", 1307, 1347, "\n" * 2),
    ("session.py", 1350, 2321, "\n" * 2),
    ("local_server.py", 2326, 2579, "\n" * 2 + SECTION_LOCAL + "\n"),
    ("cli.py", 2584, 2730, "\n" * 2 + SECTION_CLI + "\n"),
]

MARKER_RE = re.compile(
    r"^# —— 以下内容逐字节摘自 client/socks5_client_mqtt\.py 第 (\d+)-(\d+) 行.*?$",
    re.M,
)


def extract_slice(path: str) -> tuple[int, int, str]:
    """取模块中来源标记之后的全部内容，并返回标记声明的行号区间。"""
    text = io.open(path, "r", encoding="utf-8", newline="").read()
    marks = list(MARKER_RE.finditer(text))
    if len(marks) != 1:
        raise RuntimeError("%s 应恰好含 1 条来源标记，实际 %d 条" % (path, len(marks)))
    m = marks[0]
    body = text[m.end():]
    # $ 匹配在换行符之前结束：剥掉标记行自身的那一个换行，剩下的才是切片
    if body.startswith("\r\n"):
        body = body[2:]
    elif body.startswith("\n"):
        body = body[1:]
    if not body.endswith("\n"):
        body += "\n"
    return int(m.group(1)), int(m.group(2)), body


def build() -> str:
    out = [HEAD, "\n"]          # 原文件第 112 行：logger 与别名表之间的空行
    prev_end = 112
    for i, (name, start, end, glue) in enumerate(LAYOUT):
        # 切片之间的空行/分节注释是"版面胶水"：行数必须恰好填满缺口
        gap = start - prev_end - 1
        if gap < 0:
            raise RuntimeError("版面重叠：%s 起始 %d，上一段止于 %d" % (name, start, prev_end))
        if gap != glue.count("\n"):
            raise RuntimeError(
                "版面不连续：%s 之前缺口 %d 行，LAYOUT 胶水只有 %d 行"
                % (name, gap, glue.count("\n")))
        s_start, s_end, body = extract_slice(os.path.join(_HERE, name))
        if (s_start, s_end) != (start, end):
            raise RuntimeError(
                "%s 标记声明 %d-%d，与既定版面 %d-%d 不符（模块被移动/重排？）"
                % (name, s_start, s_end, start, end))
        out.append(glue)
        out.append(body)
        prev_end = end
    if prev_end != 2730:
        raise RuntimeError("版面终点 %d != 2730" % prev_end)
    out.append("\n" * 2)
    out.append(GUARD)
    return "".join(out)


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def ast_equal(a: str, b: str) -> bool:
    try:
        return ast.dump(ast.parse(a)) == ast.dump(ast.parse(b))
    except SyntaxError:
        return False


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="client/socks5 包 -> 单文件重组器")
    p.add_argument("-o", "--output", default=DEFAULT_OUT,
                   help="输出路径（默认: client/socks5_client_mqtt.py）")
    p.add_argument("-f", "--force", action="store_true", help="允许覆盖已存在的输出文件")
    p.add_argument("--check", action="store_true",
                   help="不写文件：只与冻结基线 old=socks5_client_mqtt.py 比对（字节+AST）")
    args = p.parse_args(argv)

    text = build()

    if args.check:
        if not os.path.exists(FROZEN_SINGLE):
            print("找不到冻结基线：%s" % FROZEN_SINGLE)
            return 1
        frozen = io.open(FROZEN_SINGLE, "r", encoding="utf-8", newline="").read()
        if text == frozen:
            print("PASS 字节一致：重组产物与冻结基线逐字节相同")
            print("     sha256=%s" % sha256(text))
            return 0
        print("FAIL 字节不一致：重组 sha256=%s / 冻结基线 sha256=%s"
              % (sha256(text), sha256(frozen)))
        if ast_equal(text, frozen):
            print("     AST 一致：差异纯属版面（空行/注释），语义相同")
            return 2
        print("     AST 也不一致：存在语义差异，先跑 verify_parity.py 定位是哪个符号变了")
        return 3

    out_path = os.path.abspath(args.output)
    if out_path == os.path.abspath(FROZEN_SINGLE):
        print("拒绝：old=socks5_client_mqtt.py 是冻结基线，一个字节都不许改。")
        print("     正式单文件请用默认输出 %s。" % DEFAULT_OUT)
        return 1
    if os.path.exists(out_path) and not args.force:
        print("已存在（用 -f 覆盖）：%s" % out_path)
        return 1
    with open(out_path, "wb") as f:
        f.write(text.encode("utf-8"))
    print("已写出 %s（%d 字节，sha256=%s）" % (out_path, len(text.encode("utf-8")), sha256(text)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
