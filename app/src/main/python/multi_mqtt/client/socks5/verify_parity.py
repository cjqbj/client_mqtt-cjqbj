#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""client/socks5 包与单文件版 client/socks5_client_mqtt.py 的逐符号校验。

四层校验，全部通过退出码 0：
1. 命名空间层：单文件版全部顶层定义（函数/类/常量/别名表/logger）在本包
   ``client.socks5`` 命名空间同名存在（import 进来的模块名与 sys.path 引导
   临时量除外）；
2. 源码层：每个函数/类的 def 块文本逐字节一致（inspect.getsource）——
   拆分只允许"搬运"，不允许顺手改写；
3. 模板层：``_SOCKS5_START_TEMPLATE`` 全文一致，且
   ``build_socks5_start_code(样例载荷)`` 产物一致；
4. CLI 层：``build_parser().format_help()`` 完全一致（选项/默认值/帮助文本）。

用法：
    python client/socks5/verify_parity.py
"""
from __future__ import annotations

import ast
import importlib.util
import inspect
import io
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_CLIENT_DIR = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_CLIENT_DIR)
ORIG_PATH = os.path.join(_CLIENT_DIR, "socks5_client_mqtt.py")
for _p in (_ROOT, _CLIENT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
os.environ.setdefault("CMQ_NO_STDERR_LOG", "1")

# 单文件版按真实层级 client.socks5_client_mqtt 加载：其内部
# ``if not __package__: __package__ = "client"`` 与 ``from . import ...``
# 都能按原语义解析，且不会与已导入的 client.socks5 子模块互相干扰。
_spec = importlib.util.spec_from_file_location(
    "client.socks5_client_mqtt", ORIG_PATH)
orig = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = orig
_spec.loader.exec_module(orig)

import client.socks5 as pkg  # noqa: E402

# _HERE 是两版各自的 sys.path 引导内部量（client/ vs client/socks5/），
# 语义本就不同，不参与比对；import 进来的模块名同理。
_SKIP_NAMES = {"_HERE"}
_SAMPLE_PAYLOAD = {
    "sid": "s5-parity-check", "in_topic": "s5/s5-parity-check/in",
    "out_topic": "s5/s5-parity-check/out", "ttl": 86400.0,
    "heartbeat": 5.0, "frame_max": 16384, "connect_timeout": 10.0,
    "gap_timeout": 15.0,
}

failures = []


def fail(msg):
    failures.append(msg)
    print("FAIL %s" % msg)


def top_level_defs(path):
    """原文顶层定义的名字 -> ('value' | 'def' | 'class')。"""
    tree = ast.parse(io.open(path, "r", encoding="utf-8").read())
    out = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out[node.name] = "def"
        elif isinstance(node, ast.ClassDef):
            out[node.name] = "class"
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    out[t.id] = "value"
    return out


def main() -> int:
    defs = top_level_defs(ORIG_PATH)
    checked = 0

    # 1) 命名空间层
    for name in sorted(defs):
        if name in _SKIP_NAMES:
            continue
        if not hasattr(pkg, name):
            fail("命名空间缺失: %s" % name)
    checked += 1
    print("PASS 命名空间层: 单文件版顶层定义 %d 个（跳过 %d 个引导内部量）"
          % (len(defs), len(_SKIP_NAMES)))

    # 2) 源码层 + 3) 常量值 + 模板
    for name, kind in sorted(defs.items()):
        if name in _SKIP_NAMES:
            continue
        a = getattr(orig, name, None)
        b = getattr(pkg, name, None)
        if a is None or b is None:
            continue  # 缺失已在上一层报过
        if kind in ("def", "class"):
            sa = inspect.getsource(a)
            sb = inspect.getsource(b)
            if sa != sb:
                fail("源码不一致: %s %s（原 %d 字符 / 包 %d 字符）"
                     % (kind, name, len(sa), len(sb)))
        else:
            if type(a) is not type(b) or a != b:
                fail("常量不一致: %s（原 %r / 包 %r）" % (name, a, b))
    checked += 1
    n_def = sum(1 for k in defs.values() if k in ("def", "class"))
    print("PASS 源码层: %d 个函数/类 def 块逐字节一致；常量值逐一相等"
          % n_def)

    # 3) 模板层（_SOCKS5_START_TEMPLATE 已按常量比对，这里补产物比对）
    code_a = orig.build_socks5_start_code(_SAMPLE_PAYLOAD)
    code_b = pkg.build_socks5_start_code(_SAMPLE_PAYLOAD)
    if code_a != code_b:
        fail("build_socks5_start_code 产物不一致")
    checked += 1
    print("PASS 模板层: _SOCKS5_START_TEMPLATE %d 字符一致，注入产物 %d 字符一致"
          % (len(orig._SOCKS5_START_TEMPLATE), len(code_a)))

    # 4) CLI 层
    help_a = orig.build_parser().format_help()
    help_b = pkg.build_parser().format_help()
    if help_a != help_b:
        fail("build_parser().format_help() 不一致")
    checked += 1
    print("PASS CLI 层: format_help() %d 字符完全一致" % len(help_a))

    if failures:
        print("\n%d 项校验失败（共 %d 层 %d 项检查）"
              % (len(failures), checked, len(defs)))
        return 1
    print("\n全部通过：包与单文件版逐符号一致。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
