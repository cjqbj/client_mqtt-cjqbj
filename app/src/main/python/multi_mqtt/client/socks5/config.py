#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""统一别名表 + 网络调优常量（纯数据模块，无任何逻辑依赖）。

alias_xxx 供 cli.build_parser 经 client_mqtt._cli_opts 生成 CLI 选项；
_ 前缀常量是重组/NACK/pacing 等机制的调优参数。全模块运行期只读、
从不重绑定、无 global——这是本包能拆成真模块（而非 exec 共享命名
空间）的前提。
"""


# —— 以下内容逐字节摘自 client/socks5_client_mqtt.py 第 113-159 行（冻结基线，校验见 verify_parity.py） ——
# ================= 统一别名表（小写变量名） =================
# 与 client_mqtt.py 同一套规矩：CLI 参数经 _cm._cli_opts(*alias_xxx) 生成；
# 连接类（request_topic/reply_topic/key/allow/timeout）直接复用 client_mqtt
# 的表，由 add_connection_args 注册，这里不重复。
alias_listen_port     = ('port', 'p', 'listen_port', 'socks_port')
alias_listen_host     = ('host', 'listen_host', 'bind')
alias_heartbeat       = ('heartbeat', 'hb')
alias_ttl             = ('ttl', 'max_live_time')
alias_dead_timeout    = ('dead_timeout', 'deadtime', 'dead')
alias_connect_timeout = ('connect_timeout', 'ctimeout', 'dial_timeout')
alias_frame_max       = ('frame_max', 'frame')
alias_gap_timeout     = ('gap_timeout', 'gap')
alias_rpc_port        = ('rpc_port', 'rpc')
alias_rpc_host        = ('rpc_host', 'rpc_ip')

_PB_SEED = ("demo.tbmq.io", "broker-cn.emqx.io", "broker.emqx.io")
# 会话建立即下发的静态种子名单：竞速统计热身（约 45-60s）完成前，大帧若
# 回退主节点会变成全 13 broker 扇出——热身下载本身就能触发账号级惩罚。
# 选历史胜率/隔离性最好的三个，热身完成后由累计胜率名单接管。
_PB_MAX = 3               # 大帧下行首选 broker 数（控制帧仍全 broker）
_PB_INTV = 5.0            # 首选 broker 名单刷新/下发间隔秒

_GAP_DEFAULT = 15.0       # per-cid seq 缺口硬熔断秒数（先经 NACK 多轮重传）
_HB_FLOOR_WIN = 60.0      # hb 延迟地板的滚动窗口秒数（单调 min 会被一次 NTP/GC/调度抖动永久钉死，pace 余生误判惩罚中）
_FIN_CLOSE_GRACE = 120.0  # 远端 clean EOF 后等本地应用读完缓冲再 close socket 的上限（Windows 防 RST）；会话条目不再被它拖住
_NACK_FIRST = 0.8         # 缺口出现后多久发首轮重传请求
_NACK_INTV = 1.2          # 后续重传请求轮询间隔（硬熔断前约 4 轮）
_CACHE_MAX = 256          # per-conn 重传缓存帧数（≈256×16KiB=4MiB）

_BULK_INTV = 0.04         # 大帧全局平滑节奏下限（秒/帧 ≈25 帧/s≈400KB/s）
_PACE_START = 0.08        # 会话起跑节奏（≈200KB/s）：宁可慢，不触发惩罚
_PACE_MAX = 0.5           # 最严节奏上限（2 帧/s≈32KB/s，仅惩罚期短暂进入）
_WQ_MAX = 256             # per-conn 写队列帧数上限（≈256×16KiB=4MiB 背压红线）
_INGRESS_MAX = 128        # per-cid 重组暂存帧数上限（防 closed 连接迟到帧堆积）
# NACK 补发待办队列硬上限：元素只是 ucache 里既有帧的引用，不拷贝载荷；
# 满量（NACK 风暴/补发长期被 bulk 队列反压）直接拒收新待办并留痕，补发
# 由服务端下一轮 NACK（指数退避）重新驱动，不在回调线程堆积内存。
_RESEND_Q_MAX = 512
# NACK 大帧补发去重表的淘汰线：超过此条目数才做一轮清扫（避免每帧扫表），
# 清扫只删超过 TTL 的键。去重窗口才 2s，30s 前的键绝无再用价值；cid 已
# 死的键也随年龄一起走，防止 HTTP 短连接海量轮换下表只增不删。
_RT_LAST_MAX = 4096
_RT_LAST_TTL = 30.0
_RT_LAST_PRUNE_INTV = 10.0
# s5-bulk 线程等上行大帧专用连接首次就绪的上限：覆盖建连 4s 窗口并留余量；
# 超时仍不可用则丢帧留痕，由服务端 NACK（缺口超时 15s 前会轮询）补发。
_BULK_READY_WAIT = 8.0
