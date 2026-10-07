#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""远端执行：把 APK 经 git pack 协议推送到 GitHub 周转仓库。

为什么不走 GitHub Contents / Blob API：
- Contents API 单文件上限 1MB；
- Blob API 对 ~87MB 的 base64 JSON 实测返回 422；
- git 智能协议 push 62MB 实测 5.4s / 11.5MB/s，无大小坑。

token 从文件读取（默认 /tmp/relay/.tok，由 mqtt_put 上传），经
http.extraHeader 注入，不写入 .git/config，不出现在远端 URL 里。
仓库必须 public，否则 ghfast 代理拉 raw 会 404。
仅依赖 python3 标准库 + 远端 git 二进制。
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time


def run(cmd, **kw):
    p = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if p.returncode != 0:
        sys.stderr.write(p.stdout)
        sys.stderr.write(p.stderr)
        raise SystemExit(f"命令失败 rc={p.returncode}: {' '.join(cmd[:3])}")
    return p.stdout


def main():
    ap = argparse.ArgumentParser(description="远端 APK → GitHub 周转仓库（git push）")
    ap.add_argument("--apk", required=True, help="远端 APK 绝对路径")
    ap.add_argument("--repo", default="eightobox/xime-relay-test", help="owner/name")
    ap.add_argument("--branch", default="main")
    ap.add_argument("--path", default="", help="仓库内路径，默认 out/<apk文件名>")
    ap.add_argument("--token-file", default="/tmp/relay/.tok")
    ap.add_argument("--workdir", default="/tmp/relay/work")
    ap.add_argument("--message", default="")
    args = ap.parse_args()

    token = open(args.token_file, encoding="utf-8").read().strip()
    import base64
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()

    apk = os.path.abspath(args.apk)
    if not os.path.isfile(apk):
        raise SystemExit(f"APK 不存在: {apk}")
    size = os.path.getsize(apk)
    in_repo = args.path or f"out/{os.path.basename(apk)}"

    url = f"https://github.com/{args.repo}"
    if os.path.isdir(os.path.join(args.workdir, ".git")):
        shutil.rmtree(args.workdir)
    os.makedirs(os.path.dirname(args.workdir) or tempfile.gettempdir(), exist_ok=True)

    def g(*gargs):
        return run(["git", "-c", f"http.extraHeader=Authorization: Basic {auth}",
                    "-C", args.workdir, *gargs])

    print(f"[1/3] clone {args.repo}（depth=1）", flush=True)
    run(["git", "-c", f"http.extraHeader=Authorization: Basic {auth}",
         "clone", "--depth", "1", "--branch", args.branch, url, args.workdir])

    print(f"[2/3] 加入 {in_repo}（{size} 字节）", flush=True)
    dst = os.path.join(args.workdir, in_repo)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(apk, dst)
    g("config", "user.email", "relay@local")
    g("config", "user.name", "relay")
    g("add", in_repo)
    msg = args.message or f"relay {time.strftime('%Y%m%d%H%M%S')}"
    r = subprocess.run(["git", "-C", args.workdir, "commit", "-m", msg],
                       capture_output=True, text=True)
    if r.returncode != 0 and "nothing to commit" not in (r.stdout + r.stderr):
        sys.stderr.write(r.stdout + r.stderr)
        raise SystemExit("git commit 失败")

    print("[3/3] push（计时）", flush=True)
    t0 = time.time()
    g("push", "origin", args.branch)
    dt = time.time() - t0

    raw = f"https://github.com/{args.repo}/raw/refs/heads/{args.branch}/{in_repo}"
    print("=" * 60)
    print(f"PUSH_SECONDS={dt:.1f}")
    print(f"PUSH_SPEED_MBPS={size / dt / 1048576:.2f}")
    print(f"RAW_URL={raw}")
    print(f"GHFAST_URL=https://ghfast.top/{raw}")
    print("=" * 60)


if __name__ == "__main__":
    main()
