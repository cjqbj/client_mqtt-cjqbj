#!/usr/bin/env python3
r"""快速路径：借助 Sysinternals handle.exe 找出绑定指定端口的句柄持有者。

与 scan_port_holder.py 等价但快一个数量级：handle.exe 一次性导出全系统
\Device\Afd socket 句柄（几百个，而不是全量八万+），再逐个探测。
依赖 handle64.exe：放本目录、%TEMP% 或 PATH 中均可，
下载 https://live.sysinternals.com/handle64.exe

用法: python probe_afd.py [端口，默认1122]
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER = os.path.join(THIS_DIR, "probe_worker.py")
TARGET_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 1122
CHUNK, TIMEOUT = 250, 120


def find_handle_exe():
    temp = os.environ.get("TEMP", "")
    for cand in (
        os.path.join(THIS_DIR, "handle64.exe"),
        os.path.join(temp, "handle64.exe"),
        shutil.which("handle64.exe") or "",
        shutil.which("handle.exe") or "",
    ):
        if cand and os.path.isfile(cand):
            return cand
    return None


exe = find_handle_exe()
if not exe:
    sys.exit("未找到 handle64.exe/handle.exe：请放到本目录或 PATH，"
             "或改用无需外部工具的 scan_port_holder.py")

dump = subprocess.run(
    [exe, "-accepteula", "-a", "-nobanner"],
    capture_output=True, text=True, timeout=300).stdout

entries = []
pid = None
header_re = re.compile(r"pid:\s*(\d+)")
for line in dump.splitlines():
    m = header_re.search(line)
    if m and not line.startswith((" ", "\t")):
        pid = int(m.group(1))
        continue
    if "\\Device\\Afd" in line and pid is not None:
        hm = re.match(r"\s*([0-9a-fA-F]+):", line)
        if hm:
            entries.append((pid, int(hm.group(1), 16)))
print(f"从 handle.exe 解析出 {len(entries)} 个 Afd socket 句柄", flush=True)


def proc_name(pid):
    try:
        out = subprocess.run(
            ["tasklist", "/fi", f"PID eq {pid}", "/fo", "csv", "/nh"],
            capture_output=True, text=True, timeout=15).stdout.strip()
        return out.split('","')[0].strip('"') or "?"
    except Exception:
        return "?"


hits = []
for start in range(0, len(entries), CHUNK):
    chunk = entries[start:start + CHUNK]
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(chunk, f)
    try:
        out = subprocess.run(
            [sys.executable, "-u", WORKER, path],
            capture_output=True, text=True, timeout=TIMEOUT).stdout
        for ln in out.splitlines():
            if not ln.strip():
                continue
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            if len(rec.get("name", [])) >= 2 and rec["name"][1] == TARGET_PORT:
                hits.append(rec)
    except subprocess.TimeoutExpired:
        print(f"块 {start} 超时跳过，可改用 scan_port_holder.py 二分定位",
              flush=True)
    finally:
        os.unlink(path)
    print(f"进度 {min(start + CHUNK, len(entries))}/{len(entries)}", flush=True)

if not hits:
    print(f"没有存活进程持有绑定 {TARGET_PORT} 的 socket")
else:
    for rec in hits:
        print(f"PID={rec['pid']} ({proc_name(rec['pid'])}) "
              f"handle={rec['handle']:#x} {rec['name']}")
