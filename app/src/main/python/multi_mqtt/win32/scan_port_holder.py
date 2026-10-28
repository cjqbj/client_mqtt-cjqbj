#!/usr/bin/env python3
"""纯 Python 全量扫描：找出绑定指定端口的 socket 被哪个存活进程持有。

适用场景：netstat 显示某端口 LISTENING，但属主 PID 已不存在（僵尸 socket）。
taskkill 对死 PID 无效，必须找到持有其句柄的活进程再结束它。

用法: python scan_port_holder.py [端口，默认1122]
依赖: 无（不依赖 Sysinternals handle.exe，probe_afd.py 是更快的替代路径）

原理:
  1. NtQuerySystemInformation(SystemExtendedHandleInformation) 枚举全系统
     句柄表。实测 Win10 x64 的表头是 16 字节（NumberOfHandles 8 字节 +
     保留 8 字节），条目 40 字节：
     Object(8) PID(8) Handle(8) Access(4) CreatorBackTraceIndex(2)
     ObjectTypeIndex(2) HandleAttributes(4) Reserved(4)
  2. 用本进程自建 socket 自检条目偏移，并取得 socket 的 ObjectTypeIndex，
     只探测该类型的句柄（socket 属 File 对象），大幅缩小范围。
  3. 分块交给子进程 probe_worker.py 探测（复制句柄 + getsockname）。
     个别句柄会把子进程卡死在内核调用里，因此每块带超时看门狗，
     超时则二分定位并跳过肇事句柄，保证整体必然跑完。
"""
import ctypes
import json
import os
import socket
import subprocess
import sys
import tempfile
import ctypes.wintypes as wt

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "probe_worker.py")
PYTHON = sys.executable
TARGET_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 1122
CHUNK = 300
TIMEOUT = 90

ntdll = ctypes.WinDLL("ntdll")
# 对照 socket 必须先于枚举创建，快照里才会包含它
control = socket.socket()
control.bind(("0.0.0.0", 0))
control.listen(1)
my_pid = os.getpid()
ctrl_fd = control.fileno()

size = 4 * 1024 * 1024
while True:
    buf = ctypes.create_string_buffer(size)
    needed = ctypes.c_ulong(0)
    status = ntdll.NtQuerySystemInformation(
        64, buf, size, ctypes.byref(needed))
    if status == 0:
        break
    if status != 0xC0000004:  # STATUS_INFO_LENGTH_MISMATCH
        raise OSError(hex(status))
    size = max(size * 2, needed.value + 65536)

data = buf.raw
count = int.from_bytes(data[0:8], "little")
HEADER, ENTRY = 16, 40
assert HEADER + count * ENTRY == needed.value, "句柄表长度校验失败"
print(f"句柄总数 {count}", flush=True)

sock_type_idx = None
for i in range(count):
    off = HEADER + i * ENTRY
    if int.from_bytes(data[off+8:off+16], "little") == my_pid and \
       int.from_bytes(data[off+16:off+24], "little") == ctrl_fd:
        sock_type_idx = int.from_bytes(data[off+30:off+32], "little")
        break
print(f"socket ObjectTypeIndex = {sock_type_idx}", flush=True)
assert sock_type_idx is not None, "自检失败：找不到自己的 socket 条目"

cands = []
for i in range(count):
    off = HEADER + i * ENTRY
    if int.from_bytes(data[off+30:off+32], "little") != sock_type_idx:
        continue
    pid = int.from_bytes(data[off+8:off+16], "little")
    if pid <= 4:
        continue
    cands.append((pid, int.from_bytes(data[off+16:off+24], "little")))
print(f"类型过滤后待探测: {len(cands)}", flush=True)


def proc_name(pid):
    try:
        out = subprocess.run(
            ["tasklist", "/fi", f"PID eq {pid}", "/fo", "csv", "/nh"],
            capture_output=True, text=True, timeout=15).stdout.strip()
        return out.split('","')[0].strip('"') or "?"
    except Exception:
        return "?"


hits = []
skipped = []
probe_count = 0


def run_chunk(chunk):
    """探测一块，返回 (结果行列表, 是否超时)。"""
    global probe_count
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(chunk, f)
    try:
        proc = subprocess.run(
            [PYTHON, "-u", WORKER, path],
            capture_output=True, text=True, timeout=TIMEOUT)
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        probe_count += len(chunk)
        return lines, False
    except subprocess.TimeoutExpired as e:
        out = e.stdout or ""
        if isinstance(out, bytes):
            out = out.decode("utf-8", "replace")
        lines = [ln for ln in out.splitlines() if ln.strip().startswith("{")]
        return lines, True
    finally:
        os.unlink(path)


def process_chunk(chunk):
    lines, timed_out = run_chunk(chunk)
    for ln in lines:
        try:
            rec = json.loads(ln)
        except Exception:
            continue
        name = rec.get("name") or []
        if len(name) >= 2 and name[1] == TARGET_PORT:
            hits.append(rec)
            print(f"  命中: pid={rec['pid']} ({proc_name(rec['pid'])}) "
                  f"handle={rec['handle']:#x} {name}", flush=True)
    if not timed_out:
        return
    if len(chunk) == 1:
        skipped.append(chunk[0])
        print(f"  跳过卡死句柄: pid={chunk[0][0]} handle={chunk[0][1]:#x}",
              flush=True)
        return
    mid = len(chunk) // 2
    process_chunk(chunk[:mid])
    process_chunk(chunk[mid:])


for start in range(0, len(cands), CHUNK):
    process_chunk(cands[start:start + CHUNK])
    print(f"进度 {min(start + CHUNK, len(cands))}/{len(cands)}", flush=True)

print(f"探测完成: {probe_count} 个, 跳过 {len(skipped)} 个卡死句柄", flush=True)
if not hits:
    print(f"结论：没有任何存活进程持有绑定 {TARGET_PORT} 的 socket", flush=True)
else:
    print(f"绑定 {TARGET_PORT} 的持有者共 {len(hits)} 个:", flush=True)
    for rec in hits:
        print(f"  PID={rec['pid']} ({proc_name(rec['pid'])}) "
              f"handle={rec['handle']:#x} {rec['name']}", flush=True)
