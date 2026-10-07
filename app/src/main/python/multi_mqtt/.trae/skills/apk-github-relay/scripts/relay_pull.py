#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国内下载：经 ghfast.top 代理拉取 GitHub 周转仓库里的 APK。

输出与 pull_apk.py 同级别的网络详情：DNS / TCP / TLS / TTFB /
响应头（含 x-cache 命中状态）、进度条、瞬时与平均速度、峰值速度、
SHA256 校验。代理偶发断流时用 Range 断点续传自动重试。

Exit codes: 0 ok | 1 连接/HTTP 错误 | 2 SHA256 不一致
仅用标准库，任意 cwd 可运行；文件落系统临时目录，不进工作区。
"""
import argparse
import hashlib
import http.client
import os
import socket
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

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


def normalize_raw_url(url_or_spec, owner=None, repo=None, branch=None, path=None):
    if url_or_spec:
        u = url_or_spec.strip()
        # 去掉可能已带的代理前缀
        for pre in ("https://ghfast.top/", "https://ghproxy.net/", "https://gh-proxy.com/"):
            if u.startswith(pre):
                u = u[len(pre):]
        p = urllib.parse.urlsplit(u)
        parts = [x for x in p.path.split("/") if x]
        # /owner/repo/raw/refs/heads/branch/...
        if len(parts) >= 6 and parts[2] == "raw":
            return f"https://github.com/{parts[0]}/{parts[1]}/raw/refs/heads/{parts[5]}/{'/'.join(parts[6:])}"
        if len(parts) >= 5 and parts[2] in ("blob", "raw"):
            return f"https://github.com/{parts[0]}/{parts[1]}/raw/refs/heads/{parts[4]}/{'/'.join(parts[5:])}"
        raise SystemExit(f"无法解析 GitHub URL: {url_or_spec}")
    return (f"https://github.com/{owner}/{repo}/raw/refs/heads/"
            f"{branch}/{path}")


def dial_detail(host, port=443, timeout=15):
    t0 = time.time()
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    ip = infos[0][4][0]
    t_dns = time.time() - t0
    t1 = time.time()
    raw = socket.create_connection((host, port), timeout=timeout)
    t_tcp = time.time() - t1
    t2 = time.time()
    ctx = ssl.create_default_context()
    s = ctx.wrap_socket(raw, server_hostname=host)
    t_tls = time.time() - t2
    s.close()
    return ip, t_dns, t_tcp, t_tls


def head_info(url, timeout):
    req = urllib.request.Request(url, method="HEAD",
                                 headers={"User-Agent": "apk-relay-pull"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, dict(r.headers)


def download_once(url, part, start, timeout, expect_len):
    """从 start 续传写入 .part，返回 (新字节数, 是否完整, headers)。"""
    headers = {"User-Agent": "apk-relay-pull"}
    mode = "ab"
    if start:
        headers["Range"] = f"bytes={start}-"
    else:
        mode = "wb"
    req = urllib.request.Request(url, headers=headers)
    got = 0
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        hdr = dict(resp.headers)
        with open(part, mode) as f:
            while True:
                b = resp.read(1 << 20)
                if not b:
                    break
                f.write(b)
                got += len(b)
                cur = os.path.getsize(part)
                el = time.time() - t0
                if expect_len:
                    pct = cur / expect_len * 100
                    eta = (expect_len - cur) / (cur / el) if el > 0 and cur else 0
                    print(f"\r  {pct:5.1f}%  {human(cur)}/{human(expect_len)}  "
                          f"本轮 {got / max(el, .01) / 1048576:5.2f}MB/s  ETA {eta:4.0f}s   ",
                          end="", flush=True)
                else:
                    print(f"\r  {human(cur)}  本轮 {got / max(el, .01) / 1048576:5.2f}MB/s   ",
                          end="", flush=True)
        print()
    final = os.path.getsize(part)
    complete = (not expect_len) or final >= expect_len
    return got, complete, hdr


def main():
    ap = argparse.ArgumentParser(description="ghfast.top 代理下载 GitHub 周转 APK（含网络详情/续传/SHA）")
    ap.add_argument("--url", default="", help="GitHub raw/blob URL（自动去掉代理前缀并规范化）")
    ap.add_argument("--owner", default="eightobox")
    ap.add_argument("--repo", default="xime-relay-test")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--path", default="", help="仓库内路径，与 --url 二选一")
    ap.add_argument("--proxy", default="https://ghfast.top", help="代理前缀，留空走 GitHub 直连")
    ap.add_argument("--out", default="", help="默认 %%TEMP%%/<仓库内文件名>")
    ap.add_argument("--sha256", default="")
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    if not args.url and not args.path:
        raise SystemExit("必须给 --url 或 --path")
    raw = normalize_raw_url(args.url, args.owner, args.repo, args.branch, args.path)
    final_url = f"{args.proxy.rstrip('/')}/{raw}" if args.proxy else raw
    name = urllib.parse.unquote(raw.rsplit("/", 1)[-1])
    out = args.out or os.path.join(tempfile.gettempdir(), name)
    part = out + ".part"
    expect = args.sha256.strip().lower()

    host = urllib.parse.urlsplit(final_url).netloc
    print("=" * 64)
    print("连接详情")
    print("=" * 64)
    print(f"  代理通道     : {args.proxy or '(直连 GitHub)'}")
    print(f"  目标 raw     : {raw}")
    print(f"  请求 Host    : {host}")
    print(f"  本地保存     : {out}")
    print(f"  期望 SHA256  : {expect[:16] + '...' + expect[-8:] if expect else '(未提供，跳过)'}")
    try:
        ip, dns, tcp, tls = dial_detail(host)
        print(f"  解析IP       : {ip}")
        print(f"  DNS {dns*1000:5.0f}ms | TCP {tcp*1000:5.0f}ms | TLS {tls*1000:5.0f}ms")
    except Exception as e:
        print(f"  [建连探测失败] {type(e).__name__}: {e}")
    print("=" * 64)

    # HEAD 拿长度与缓存状态（失败不致命）
    expect_len = None
    try:
        st, hh = head_info(final_url, args.timeout)
        expect_len = int(hh.get("Content-Length", 0)) or None
        print(f"  HEAD {st}  Server={hh.get('Server','-')}  "
              f"x-cache={hh.get('x-cache') or hh.get('X-Cache') or '-'}  "
              f"via={hh.get('via','-')}")
        print(f"  Content-Length={expect_len or '(chunked)'}")
    except Exception as e:
        print(f"  [HEAD 失败，继续 GET] {e}")
    print()

    if os.path.exists(part) and expect_len and os.path.getsize(part) >= expect_len:
        os.remove(part)
    total_start = time.time()
    attempt = 0
    ok = False
    last_err = ""
    while attempt < args.retries and not ok:
        attempt += 1
        start = os.path.getsize(part) if os.path.exists(part) else 0
        if start:
            print(f"第 {attempt} 次尝试，从 {start} 字节断点续传 ...")
        else:
            print(f"第 {attempt} 次尝试 ...")
        try:
            _, ok, _ = download_once(final_url, part, start, args.timeout, expect_len)
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last_err = f"{type(e).__name__}: {e}"
            print(f"\n  [断流] {last_err}，1.5s 后续传")
            time.sleep(1.5)

    if not ok:
        print(f"\n[失败] {args.retries} 次尝试未完成: {last_err}")
        return 1

    os.replace(part, out)
    elapsed = time.time() - total_start
    size = os.path.getsize(out)
    h = hashlib.sha256()
    with open(out, "rb") as f:
        for blk in iter(lambda: f.read(1 << 20), b""):
            h.update(blk)
    sha = h.hexdigest()
    sha_ok = (not expect) or sha == expect

    print("=" * 64)
    print("下载结果")
    print("=" * 64)
    print(f"  本地文件     : {out}")
    print(f"  字节数       : {size} ({human(size).strip()})")
    print(f"  尝试次数     : {attempt}（含断点续传）")
    print(f"  总耗时       : {elapsed:.1f} s")
    print(f"  平均速度     : {size / max(elapsed, .01) / 1048576:.2f} MB/s")
    print(f"  实际 SHA256  : {sha}")
    print(f"  校验结果     : {'一致' if sha_ok else '不一致，禁止安装'}")
    print("=" * 64)
    return 0 if sha_ok else 2


if __name__ == "__main__":
    sys.exit(main())
