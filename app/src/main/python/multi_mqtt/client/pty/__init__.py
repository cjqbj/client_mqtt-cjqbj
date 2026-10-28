#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client.pty —— pty_client_mqtt.py 的多文件版本。

结构
====
- ``parts/``：单文件按顶层语句边界切出的**逐字节分片**（文件名编号即
  拼接顺序）。分片不是独立模块，不要单独 import；**只编辑分片**，
  改完用生成器重组合。
- ``generate_single_file.py``：把 ``parts/`` 全部分片按序拼回单文件；
  ``-o ../pty_client_mqtt.py -f`` 写回正式单文件（parts 是唯一编辑入口），
  默认输出 ``client/pty_client_mqtt_generated.py`` 仅作预览/比对。
- 本 ``__init__.py``：按序把全部分片 exec 进**同一个命名空间**——与单文件
  版语义完全一致（跨分片的模块级可变全局 ``_OUT_WRITER`` / ``_ERR_WRITER``
  / ``_LOG_MIRROR_STDERR`` 等仍是一份，不会出现跨模块拷贝漂移）。

用法
====
    from client.pty import ai_pty_run, ai_pty_status   # 等价单文件版的同名 API
    from client.pty import ai_bridge                   # 常驻会话挂载点
    python -m client.pty                               # 等价 python client/pty_client_mqtt.py

为何不用「每个分片一个真模块 + 相互 import」：原文件的会话主循环
``run_session`` 会 ``global _OUT_WRITER`` 重新绑定写线程，多处函数跨段读写
同一批模块级可变全局；拆成真模块后各持有独立绑定，语义必然漂移。共享
命名空间 exec 是唯一能同时满足「分片可编辑」与「语义不变」的拆法，重组
侧也因此可以做到与原文件逐字节一致。
"""
import os as _os


def _load_flat():
    here = _os.path.dirname(_os.path.abspath(__file__))
    parts_dir = _os.path.join(here, "parts")
    # 与单文件版逐点对齐：
    # 1) __file__ 指向 client/pty_client_mqtt.py——000_head 的 sys.path 引导
    #    靠它推算 client/ 与项目根；
    # 2) __package__ 临时置为 "client"——000_head 里的 ``from . import
    #    client_mqtt`` 等包内相对导入按单文件版的层级解析；
    # 3) 全部分片共用本模块 globals()：扁平语义，无跨模块全局拷贝。
    g = globals()
    saved = (g.get("__package__"), g.get("__spec__"), g.get("__file__"))
    g["__file__"] = _os.path.join(_os.path.dirname(here), "pty_client_mqtt.py")
    g["__package__"] = "client"
    g["__spec__"] = None
    try:
        for name in sorted(_os.listdir(parts_dir)):
            if not name.endswith(".py") or name.startswith("__"):
                continue
            path = _os.path.join(parts_dir, name)
            with open(path, "rb") as f:
                src = f.read()
            exec(compile(src, path, "exec"), g)
    finally:
        g["__package__"], g["__spec__"], g["__file__"] = saved


_load_flat()
del _load_flat, _os
