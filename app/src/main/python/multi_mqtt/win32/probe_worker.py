#!/usr/bin/env python3
"""探测一批 (pid, handle)：输出 JSON 行结果，供带看门狗的父进程调用。

由 scan_port_holder.py / probe_afd.py 以子进程方式调用，一般不直接使用。
原理：OpenProcess(PROCESS_DUP_HANDLE) + DuplicateHandle 把目标进程的句柄
复制到本进程，再包装成 socket 调 getsockname() 读出绑定的本地地址。
"""
import ctypes
import json
import socket
import sys
import ctypes.wintypes as wt

kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
kernel32.OpenProcess.restype = wt.HANDLE
kernel32.OpenProcess.argtypes = [wt.DWORD, wt.BOOL, wt.DWORD]
kernel32.GetCurrentProcess.restype = wt.HANDLE
kernel32.DuplicateHandle.restype = wt.BOOL
kernel32.DuplicateHandle.argtypes = [
    wt.HANDLE, wt.HANDLE, wt.HANDLE,
    ctypes.POINTER(wt.HANDLE), wt.DWORD, wt.BOOL, wt.DWORD,
]
PROC_DUP_HANDLE = 0x40
DUPLICATE_SAME_ACCESS = 2

entries = json.load(open(sys.argv[1], encoding="utf-8"))
opened = {}
for pid, handle in entries:
    try:
        if pid not in opened:
            opened[pid] = kernel32.OpenProcess(PROC_DUP_HANDLE, False, pid)
        hproc = opened[pid]
        if not hproc:
            continue
        dup = wt.HANDLE()
        if not kernel32.DuplicateHandle(
                hproc, wt.HANDLE(handle), kernel32.GetCurrentProcess(),
                ctypes.byref(dup), 0, False, DUPLICATE_SAME_ACCESS):
            continue
        try:
            s = socket.socket(fileno=dup.value)
            try:
                name = s.getsockname()
                print(json.dumps(
                    {"pid": pid, "handle": handle, "name": list(name)}),
                    flush=True)
            finally:
                s.detach()
        except Exception:
            pass
        finally:
            kernel32.CloseHandle(dup)
    except Exception:
        pass
