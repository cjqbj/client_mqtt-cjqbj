#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把本目录 parts/ 下的全部分片按序重新组合成单文件 pty_client_mqtt.py。

parts/ 是**唯一编辑入口**：每个分片保持顶层语句边界、可独立解析，重组是
纯字节拼接。**默认只生成、不与任何现有文件比对**；需要校验分片与磁盘上
正式单文件是否同步时显式加 ``--check``（字节 + AST，不写文件）。

默认输出即正式单文件 ``client/pty_client_mqtt.py``：目标是它时必须加
``--force`` 确认；其他已存在的输出同样需要 ``--force``。

用法
====
    python client/pty/generate_single_file.py -f         # 只生成：写 ../pty_client_mqtt.py（不比对）
    python client/pty/generate_single_file.py --check    # 只比对：与 ../pty_client_mqtt.py（字节+AST）
    python client/pty/generate_single_file.py -o 路径 -f # 只生成：写到指定文件
    python client/pty/generate_single_file.py --stdout   # 拼接到标准输出
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PARTS_DIR = HERE / "parts"
ORIG = HERE.parent / "pty_client_mqtt.py"
DEFAULT_OUTPUT = HERE.parent / "pty_client_mqtt.py"   # 正式单文件：默认输出


def assemble() -> bytes:
    """按文件名顺序拼接 parts/ 下全部 .py 分片（编号即拼接序）。"""
    parts = sorted(PARTS_DIR.glob("*.py"))
    if not parts:
        raise SystemExit("no part files found in %s" % PARTS_DIR)
    data = b"".join(p.read_bytes() for p in parts)
    ast.parse(data, filename="<assembled>")  # 输出必须是合法 Python 源码
    return data


def compare_with_orig(data: bytes) -> int:
    """与原文件比对：字节一致 -> 0；仅 AST 一致 -> 10；语义漂移 -> 20。"""
    if not ORIG.is_file():
        print("[MISS] 原文件不存在，无法比对: %s" % ORIG)
        return 20
    orig = ORIG.read_bytes()
    if orig == data:
        print("[OK] 与原文件逐字节一致（%d bytes, sha256=%s）"
              % (len(data), hashlib.sha256(data).hexdigest()))
        return 0
    same_ast = (ast.dump(ast.parse(orig, filename=str(ORIG)),
                         include_attributes=False)
                == ast.dump(ast.parse(data, filename="<assembled>"),
                            include_attributes=False))
    print("[%s] 字节不同（原 %d B / 生成 %d B）；AST %s"
          % ("WARN" if same_ast else "FAIL", len(orig), len(data),
             "一致（语义相同，仅注释/空白漂移）" if same_ast
             else "不同（语义已漂移！）"))
    return 10 if same_ast else 20


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="把 pty/parts/ 分片按序拼回单文件（默认只生成，不比对）。")
    parser.add_argument("-o", "--output", type=Path, default=DEFAULT_OUTPUT,
                        help="输出路径（默认: %(default)s）")
    parser.add_argument("-f", "--force", action="store_true",
                        help="允许覆盖已存在的输出文件")
    parser.add_argument("--check", action="store_true",
                        help="不写文件：只与磁盘上的 pty_client_mqtt.py 比对（字节 + AST）")
    parser.add_argument("--stdout", action="store_true",
                        help="拼接到标准输出（不比对、不写文件）")
    args = parser.parse_args(argv)

    data = assemble()
    if args.stdout:
        sys.stdout.buffer.write(data)
        return 0
    if args.check:
        return compare_with_orig(data)

    output = args.output.expanduser().resolve()
    if output == ORIG.resolve() and not args.force:
        print("[REFUSED] 目标是正式单文件 %s；确认把重组结果写回请加 --force"
              % output)
        return 2
    if output.exists() and not args.force:
        print("[SKIP] 输出已存在，如确认覆盖请加 --force: %s" % output)
        return 2
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(data)
    print("Generated: %s (%d bytes)" % (output, len(data)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
