#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""管理 GitHub 周转仓库（ensure / delete）。

token 来源（按顺序）：环境变量 RELAY_GH_TOKEN，或文件
%USERPROFILE%\\.config\\apk-relay\\github.token。
绝不把 token 写进仓库目录或 skill 目录。

ensure 会在空仓库时补一个 README 初始化 main 分支——空仓库调
git/tree/blob API 会 409、git push 也无默认分支，必须先初始化。
"""
import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request

API = "https://api.github.com"


def token_file():
    base = os.environ.get("USERPROFILE") or os.path.expanduser("~")
    return os.path.join(base, ".config", "apk-relay", "github.token")


def get_token():
    t = os.environ.get("RELAY_GH_TOKEN", "").strip()
    if t:
        return t
    p = token_file()
    if os.path.isfile(p):
        return open(p, encoding="utf-8").read().strip()
    raise SystemExit(f"未找到 token：设置 RELAY_GH_TOKEN 或写入 {p}")


def api(method, url, token, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": "token " + token,
        "Accept": "application/vnd.github+json",
        "User-Agent": "apk-relay",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def main():
    ap = argparse.ArgumentParser(description="GitHub 周转仓库 ensure/delete")
    ap.add_argument("action", choices=("ensure", "delete"))
    ap.add_argument("--repo", default="xime-relay-test")
    ap.add_argument("--private", action="store_true", help="建私有仓（代理无法拉私有仓，周转场景别用）")
    args = ap.parse_args()

    token = get_token()
    st, me = api("GET", f"{API}/user", token)
    if st != 200:
        raise SystemExit(f"token 无效: HTTP {st}")
    owner = me["login"]
    full = f"{owner}/{args.repo}"
    print(f"账号: {owner}")

    st, _ = api("GET", f"{API}/repos/{full}", token)
    exists = st == 200

    if args.action == "delete":
        if not exists:
            print(f"仓库不存在，无需删除: {full}")
            return
        st, body = api("DELETE", f"{API}/repos/{full}", token)
        if st == 204:
            print(f"已删除 {full}")
        else:
            raise SystemExit(f"删除失败 HTTP {st}: {body.get('message')}")
        return

    if exists:
        print(f"仓库已存在: {full}")
    else:
        st, body = api("POST", f"{API}/user/repos", token, {
            "name": args.repo,
            "private": bool(args.private),
            "auto_init": True,
            "description": "temporary apk relay",
        })
        if st not in (200, 201):
            raise SystemExit(f"创建失败 HTTP {st}: {body.get('message')}")
        print(f"已创建并初始化 main: {full}（private={bool(args.private)}）")

    # 确认 main 已初始化（空 main 会让后续 git/tree API 409）
    st, _ = api("GET", f"{API}/repos/{full}/contents/README.md", token)
    if st == 404:
        content = base64.b64encode(b"temporary apk relay repo").decode()
        st2, body2 = api("PUT", f"{API}/repos/{full}/contents/README.md", token, {
            "message": "init", "content": content,
        })
        if st2 not in (200, 201):
            raise SystemExit(f"初始化 README 失败 HTTP {st2}: {body2.get('message')}")
        print("已补 README 初始化 main 分支")
    print(f"RAW 前缀: https://github.com/{full}/raw/refs/heads/main/")


if __name__ == "__main__":
    main()
