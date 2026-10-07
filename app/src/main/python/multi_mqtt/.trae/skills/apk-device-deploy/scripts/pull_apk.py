#!/usr/bin/env python3
"""Stream-download the qgb client APK from the HF Space HTTPS RPC endpoint.

Why this exists:
- The APK (~37MB) cannot be pulled through the MQTT PTY channel; the terminal
  stream corrupts large binary/base64 payloads.
- The Space exposes an unauthenticated GET RPC: /rpc/<urlencoded python code>.
  Server-side code streams the file in 1 MiB blocks (multiline loop MUST be
  wrapped in exec, and a Content-Disposition header makes the router 404).

The file is saved into the system temp dir by default (never the workspace),
prints full connection/response details, live speed/ETA, and verifies SHA256.

Exit codes: 0 ok | 1 connection/HTTP error | 2 SHA256 mismatch
"""
import argparse
import hashlib
import os
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_HOST = "https://huggingface1q-q.hf.space"
DEFAULT_REMOTE = "/root/build_xime_home/client/out/com.qgb.client-1-arm64-v8a.apk"
DEFAULT_OUT = os.path.join(tempfile.gettempdir(), "qgb_client_latest.apk")

# Windows consoles often default to gbk/cp936; the progress bar and box chars
# are unicode. Best-effort reconfigure (no-op on non-tty/older Python).
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:6.2f} {unit}"
        n /= 1024
    return f"{n:6.2f} PB"


def human_speed(n):
    return human(n) + "/s"


def build_rpc_url(host, remote):
    # set_header + open + exec(multiline read/write loop). Do NOT add a
    # Content-Disposition header here — it caused HTTP 404 on the Space.
    code = (
        "p.set_header('Content-Type','application/octet-stream'); "
        f"f=open({remote!r},'rb'); "
        "exec(\"while True:\\n b=f.read(1048576)\\n if not b: break\\n p.write(b)\")"
    )
    return f"{host}/rpc/" + urllib.parse.quote(code, safe=""), code


def main():
    parser = argparse.ArgumentParser(description="Stream-download qgb client APK via Space HTTPS RPC")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Space HTTPS origin")
    parser.add_argument("--remote", default=DEFAULT_REMOTE, help="Absolute APK path on the Space")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Local path (default: %%TEMP%%/qgb_client_latest.apk)")
    parser.add_argument("--sha256", default="", help="Expected SHA256; download fails on mismatch when given")
    parser.add_argument("--timeout", type=float, default=300.0, help="Socket timeout seconds")
    parser.add_argument("--block", type=int, default=1 << 20, help="Read block size bytes")
    args = parser.parse_args()

    url, _code = build_rpc_url(args.host, args.remote)
    expect = args.sha256.strip().lower()

    print("=" * 60)
    print("连接详情")
    print("=" * 60)
    print(f"  Host         : {args.host}")
    print(f"  远端路径     : {args.remote}")
    print(f"  本地保存     : {args.out}")
    print(f"  请求URL长度  : {len(url)} 字符")
    print(f"  URL前缀      : {url[:80]}...")
    print(f"  期望SHA256   : {(expect[:16] + '...' + expect[-8:]) if expect else '(未提供，跳过校验)'}")
    print("=" * 60)
    print()

    digest = hashlib.sha256()
    total = 0
    start = time.time()
    last_t = start
    last_bytes = 0

    print("正在连接并开始下载...\n")
    try:
        with urllib.request.urlopen(url, timeout=args.timeout) as resp:
            headers = resp.headers
            status = getattr(resp, "status", None) or resp.getcode()

            print("响应详情")
            print("-" * 60)
            print(f"  HTTP 状态码   : {status} {resp.reason}")
            print(f"  服务器        : {headers.get('Server', '-')}")
            print(f"  内容类型      : {headers.get('Content-Type', '-')}")
            cl = headers.get("Content-Length")
            print(f"  Content-Length: {cl if cl else '(未提供 / chunked)'}")
            print(f"  传输编码      : {headers.get('Transfer-Encoding', '-')}")
            print(f"  连接状态      : {headers.get('Connection', '-')}")
            print("-" * 60)
            print()

            os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
            with open(args.out, "wb") as out:
                while True:
                    block = resp.read(args.block)
                    if not block:
                        break
                    out.write(block)
                    digest.update(block)
                    total += len(block)

                    now = time.time()
                    elapsed = now - start
                    dt = now - last_t
                    if dt >= 0.5:
                        inst_speed = (total - last_bytes) / dt
                        avg_speed = total / elapsed if elapsed > 0 else 0
                        if cl and cl.isdigit():
                            pct = total / int(cl) * 100
                            eta = (int(cl) - total) / avg_speed if avg_speed > 0 else 0
                            bar_len = 30
                            filled = int(bar_len * pct / 100)
                            bar = "█" * filled + "░" * (bar_len - filled)
                            line = (
                                f"\r  [{bar}] {pct:5.1f}%  "
                                f"{human(total)} / {human(int(cl))}  "
                                f"↓ {human_speed(inst_speed):>12}  "
                                f"avg {human_speed(avg_speed):>12}  "
                                f"ETA {int(eta):4d}s"
                            )
                        else:
                            line = (
                                f"\r  已下载 {human(total)}  "
                                f"↓ {human_speed(inst_speed):>12}  "
                                f"avg {human_speed(avg_speed):>12}  "
                                f"用时 {elapsed:6.1f}s"
                            )
                        print(line, end="", flush=True)
                        last_t = now
                        last_bytes = total

            now = time.time()
            elapsed = now - start
            dt = now - last_t
            if dt > 0:
                inst_speed = (total - last_bytes) / dt
                print(
                    f"\r  完成 {human(total)}  "
                    f"↓ {human_speed(inst_speed):>12}  "
                    f"avg {human_speed(total / elapsed if elapsed > 0 else 0):>12}"
                    + " " * 20
                )
            print()

    except urllib.error.HTTPError as e:
        # Layered failure evidence: HTTP reached the server, status + body.
        print(f"\n[HTTP 错误] {e.code} {e.reason}")
        try:
            print(f"响应体: {e.read()[:500]!r}")
        except Exception:
            pass
        return 1
    except urllib.error.URLError as e:
        # DNS / TCP / TLS layer failure — do NOT blindly retry; check the
        # reason (name resolution vs connection reset vs timeout) first.
        print(f"\n[连接失败] {e.reason}")
        return 1

    end = time.time()
    elapsed = end - start
    actual_sha = digest.hexdigest()
    ok = (not expect) or actual_sha == expect

    print("=" * 60)
    print("下载结果")
    print("=" * 60)
    print(f"  本地文件     : {args.out}")
    print(f"  总字节数     : {total}  ({human(total).strip()})")
    print(f"  总耗时       : {elapsed:.2f} s")
    print(f"  平均速度     : {human_speed(total / elapsed if elapsed else 0)}")
    print(f"  实际 SHA256  : {actual_sha}")
    if expect:
        print(f"  期望 SHA256  : {expect}")
    print(f"  校验结果     : {'✅ 一致' if ok else '❌ 不一致（禁止安装，请重跑下载）'}")
    print("=" * 60)
    print("bytes", total, "sha ok:", ok)
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
