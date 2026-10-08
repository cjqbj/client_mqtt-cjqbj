#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, time, json, base64, hashlib, requests, sys
requests.packages.urllib3.disable_warnings()

_py_repr = repr  # 保存内置 repr，防止后面形参遮蔽

# ============================================================
# 配置加载
# 优先级：sys._qgb_dict 注入 > 命令行参数 > 脚本同目录 !config.json
# 命令行两种写法（空格分隔或等号均可；同时出现多次时以最后一次为准）：
#   --config PATH          从任意路径读取 JSON 配置文件
#   --config-json 'JSON'   直接传 JSON 原文，无需落地配置文件
# 例（cmd / .bat）：
#   python aliyun_git.py --config D:\secrets\aliyun.json
#   python aliyun_git.py --config-json "{\"DEFAULT_TOKEN\":\"xxx\",\"DEFAULT_DOMAIN\":\"codeup.aliyuncs.com\"}"
# 例（PowerShell 5.1，内嵌双引号必须加反斜杠转义；嫌麻烦直接用 --config 文件）：
#   python aliyun_git.py --config-json '{\"DEFAULT_TOKEN\":\"xxx\",\"DEFAULT_DOMAIN\":\"codeup.aliyuncs.com\"}'
# 注意：这里手工扫描 sys.argv 而不用 argparse，且不改写 sys.argv，
# 避免本模块被 chaquopy mqtt server 等宿主程序 import 时干扰宿主自身的参数解析。
# ============================================================
def _parse_config_args(argv):
    """从 argv 中提取 --config / --config-json，返回 (kind, value)；
    kind 为 'file'（配置路径）或 'json'（JSON 原文），未提供返回 (None, None)。"""
    sources = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--config", "--config-json"):
            if i + 1 >= len(argv):
                raise SystemExit(f"[FATAL] {a} 缺少参数值")  # chaquopy 下直接停止服务
            sources.append(("file" if a == "--config" else "json", argv[i + 1]))
            i += 2
            continue
        if a.startswith("--config="):
            sources.append(("file", a[len("--config="):]))
        elif a.startswith("--config-json="):
            sources.append(("json", a[len("--config-json="):]))
        i += 1
    return sources[-1] if sources else (None, None)


def _load_config_dict(kind, value):
    """kind='file'：value 为 JSON 配置文件路径；kind='json'：value 为 JSON 原文。"""
    if kind == "json":
        try:
            cfg = json.loads(value)
        except Exception as e:
            raise SystemExit(f"[FATAL] --config-json 不是合法 JSON: {e}")
    else:
        if not os.path.isfile(value):
            raise SystemExit(f"[FATAL] 找不到配置文件: {value}")
        # utf-8-sig 可同时兼容带/不带 BOM 的 JSON（Windows 记事本常写出 BOM）
        with open(value, "r", encoding="utf-8-sig") as f:
            try:
                cfg = json.load(f)
            except Exception as e:
                raise SystemExit(f"[FATAL] 配置文件不是合法 JSON ({value}): {e}")
    if not isinstance(cfg, dict):
        raise SystemExit("[FATAL] 配置顶层必须是 JSON 对象: {...}")
    return cfg


_cfg = getattr(sys, '_qgb_dict', {}).get('aliyun_git', {})
if not _cfg:
    _cfg_kind, _cfg_value = _parse_config_args(sys.argv[1:])
    if _cfg_kind is None:
        _cfg_kind = "file"
        _cfg_value = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "!config.json")
    _cfg = _load_config_dict(_cfg_kind, _cfg_value)
    if _cfg:
        sys._qgb_dict = getattr(sys, '_qgb_dict', {})
        sys._qgb_dict['aliyun_git'] = getattr(sys, '_qgb_dict', {}).get('aliyun_git', {})
        sys._qgb_dict['aliyun_git'].update(_cfg)

DEFAULT_TOKEN = _cfg.get("DEFAULT_TOKEN")
DEFAULT_DOMAIN = _cfg.get("DEFAULT_DOMAIN")
if not DEFAULT_TOKEN or not DEFAULT_DOMAIN:
    raise SystemExit("[FATAL] 配置必须包含非空的 DEFAULT_TOKEN 和 DEFAULT_DOMAIN"
                     "（来源：!config.json / --config / --config-json）")

DEFAULT_ORG_ID = DEFAULT_DOMAIN.split('-')[0]
DEFAULT_REPO = "qpsu-repo"
DEFAULT_BRANCH = "master"
DEFAULT_VISIBILITY = "private"
DEFAULT_TIMEOUT = 600
MAX_OPENAPI_SIZE = 1024 * 1024 * 50  # 50MB OpenAPI 硬上限

# ---- LFS Basic Auth（可选）----
# 密码恒为 DEFAULT_TOKEN，无需单独配置。
# 若 BASIC_AUTH_USERNAME 非空，则跳过自动嗅探，直接用它做 Basic Auth 用户名。
BASIC_AUTH_USERNAME = (_cfg.get("BASIC_AUTH_USERNAME")
                       or _cfg.get("MANUAL_CLONE_USERNAME") or "")


# ============================================================
# 请求打印 & 统一请求入口
# ============================================================
_PRINT_REQ = False


class _PrintReqCtx:
    """上下文：临时开启/关闭请求打印，支持嵌套调用全局生效。"""
    def __init__(self, enable):
        self.enable = bool(enable)

    def __enter__(self):
        global _PRINT_REQ
        self._old = _PRINT_REQ
        _PRINT_REQ = self.enable
        return self

    def __exit__(self, *exc):
        global _PRINT_REQ
        _PRINT_REQ = self._old
        return False


def _fmt_request_arg(v):
    """把请求参数格式化为可读的、可复现的字符串。"""
    if v is None:
        return 'None'
    if isinstance(v, (str, bytes, bytearray, int, float, bool)):
        return _py_repr(v)
    if isinstance(v, (dict, list, tuple, set)):
        return _py_repr(v)
    if hasattr(v, 'read') and hasattr(v, '__len__'):
        try:
            n = len(v)
        except Exception:
            n = '?'
        return f'<stream {type(v).__name__} len={n}>'
    return _py_repr(v)


def _req(method, url, **kwargs):
    """统一 requests 入口：_PRINT_REQ=True 时打印一行可复现的 requests.* 调用。"""
    if _PRINT_REQ:
        parts = []
        for k, v in kwargs.items():
            if k.startswith('_'):
                continue
            parts.append(f'{k}={_fmt_request_arg(v)}')
        args_str = (',' + ','.join(parts)) if parts else ''
        print(f"requests.{method.lower()}({_py_repr(url)}{args_str},)")
    return requests.request(method, url, **kwargs)


# ============================================================
# progress 钩子辅助（下载链路透传：download / download_lfs / download_openapi）
# 约定：progress(got, total)，total 未知时传 0；回调异常绝不能影响传输本身。
# 旧版本 aliyun_git 没有 progress 形参，调用方需自行做能力探测（见 client_service）。
# ============================================================
def _fire_progress(progress, current, total):
    if progress is None:
        return
    try:
        progress(current, total)
    except Exception:
        pass


# ============================================================
# 通用工具
# ============================================================
def readable_size(n, ndigits=2):
    """把字节数转换成人类可读字符串，例如 390876536 -> '372.77MB'"""
    units = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB')
    f = float(n)
    i = 0
    while f >= 1024.0 and i < len(units) - 1:
        f /= 1024.0
        i += 1
    if i == 0:
        return f'{int(f)}B'
    return f'{f:.{ndigits}f}{units[i]}'


def object_custom_repr(obj, max_show_bytes_size=None, preview_len=99, **kwargs):
    """为 bytes 返回一个 __repr__ 被自定义的 bytes 子类实例（仅影响显示）。"""
    kwargs.pop('size', None)
    _ = (kwargs.pop('repr', None)
         or kwargs.pop('target', None)
         or kwargs.pop('f', None))
    if kwargs:
        raise TypeError(
            f"object_custom_repr() got unexpected keyword arguments: {list(kwargs)}")

    if isinstance(obj, bytearray):
        obj = bytes(obj)
    if not isinstance(obj, bytes):
        return obj

    if not max_show_bytes_size or len(obj) <= max_show_bytes_size:
        return obj

    preview = _py_repr(obj[:preview_len])
    custom = f'{preview}...#{readable_size(len(obj))}'

    class _TruncatedBytes(bytes):
        __slots__ = ()
        def __repr__(self):
            return custom
        def __str__(self):
            return custom

    return bytes.__new__(_TruncatedBytes, obj)


class CodeupError(Exception):
    pass


_repo_cache = {}
_lfs_identity_cache = {}


class _ProgressStream:
    """升级版流读取器：支持 Bytes，也支持直接读取本地文件以节省内存"""
    def __init__(self, data, prefix="↑[上行]"):
        self.is_file = isinstance(data, str) and os.path.isfile(data)
        if self.is_file:
            self.fd = open(data, "rb")
            self.length = os.path.getsize(data)
        else:
            self.payload = data
            self.length = len(data)

        self.offset = 0
        self.start_time = time.time()
        self.finish_time = self.start_time
        self.prefix = prefix

    def read(self, size=-1):
        if self.offset >= self.length:
            return b""
        if size < 0:
            size = self.length - self.offset

        if self.is_file:
            chunk = self.fd.read(size)
        else:
            chunk = self.payload[self.offset:self.offset + size]

        self.offset += len(chunk)
        elapsed = time.time() - self.start_time
        speed = (self.offset / 1024 / 1024) / elapsed if elapsed > 0 else 0
        print(f"\r{self.prefix}:{self.offset/1024/1024:.2f}/{self.length/1024/1024:.2f}MB "
              f"{speed:.2f}MB/s", end="", flush=True)

        if self.offset >= self.length:
            self.finish_time = time.time()
            if self.is_file:
                self.fd.close()
        return chunk

    def __len__(self):
        return self.length


def _auto_msg_from_bytes(payload_or_path):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + \
         f".{int(time.time()*1000)%1000:03d}"
    if isinstance(payload_or_path, str) and os.path.isfile(payload_or_path):
        size = os.path.getsize(payload_or_path)
        return f"{ts}={size}B=LFS_FILE"
    else:
        h = hashlib.sha256(payload_or_path).hexdigest()[:16]
        return f"{ts}={len(payload_or_path)}B={h}"


def _base(token=None, domain=None, org_id=None):
    token = token or DEFAULT_TOKEN
    domain = domain or DEFAULT_DOMAIN
    org_id = org_id or DEFAULT_ORG_ID
    if domain == "openapi-rdc.aliyuncs.com":
        if not org_id:
            raise CodeupError("中心版需要 organization_id")
        base = f"https://{domain}/oapi/v1/codeup/organizations/{org_id}"
    else:
        base = f"https://{domain}/oapi/v1/codeup"
    headers = {"Content-Type": "application/json", "x-yunxiao-token": token}
    return base, headers, domain


def _ensure_repo(repo_name, token=None, domain=None, org_id=None,
                 visibility=DEFAULT_VISIBILITY, timeout=DEFAULT_TIMEOUT,return_id=False):
    """
    确保仓库存在（仅上传路径使用）。
    会话内 _repo_cache 命中后不再产生任何请求。
    """
    key = (repo_name, domain or DEFAULT_DOMAIN)
    if key in _repo_cache:
        return _repo_cache[key]
    base, headers, _ = _base(token, domain, org_id)
    body = {"name": repo_name, "path": repo_name,
            "visibility": visibility, "readMeType": "EMPTY"}
    r = _req("POST", f"{base}/repositories?createParentPath=true",
             headers=headers, json=body, verify=False, timeout=timeout)
    if r.status_code in (200, 201,):
        rid = r.json().get("id")
        if rid:
            _repo_cache[key] = rid
            return rid
    if r.status_code == 409:
        if not return_id:return
        r2 = _req("GET", f"{base}/repositories", headers=headers,
                  params={"search": repo_name, "page": 1, "perPage": 10},
                  verify=False, timeout=timeout)
        if r2.status_code == 200:
            data = r2.json()
            repos = data if isinstance(data, list) else data.get("result", [])
            for repo in repos:
                if repo.get("name") == repo_name or repo.get("path") == repo_name:
                    rid = repo.get("id")
                    _repo_cache[key] = rid
                    return rid
        raise CodeupError(f"仓库 '{repo_name}' 已存在但无法找到")
    raise CodeupError(f"创建仓库失败 [{r.status_code}]: {r.text[:300]}")


def _make_url(domain, rid, file_path, branch):
    return (f"https://{domain}/oapi/v1/codeup/repositories/{rid}/files/"
            f"{requests.utils.quote(file_path, safe='')}?ref={branch}")


def _parse_codeup_file_url(url):
    """
    解析 Codeup OpenAPI 文件 URL：
    https://{domain}/oapi/v1/codeup/repositories/{org}%2F{repo}/files/{file_path}?ref={branch}
    """
    from urllib.parse import urlparse, unquote, parse_qs
    try:
        p = urlparse(url)
    except Exception:
        return None
    if not p.netloc:
        return None
    path = p.path
    marker = "/repositories/"
    idx = path.find(marker)
    if idx < 0:
        return None
    rest = path[idx + len(marker):]
    if "/files/" not in rest:
        return None
    rid_enc, file_enc = rest.split("/files/", 1)
    rid = unquote(rid_enc)
    file_path = unquote(file_enc)
    if not file_path:
        return None
    if "/" in rid:
        org_id, repo_name = rid.split("/", 1)
    else:
        org_id, repo_name = None, rid
    branch = parse_qs(p.query).get("ref", [None])[0]
    return {
        "domain": p.netloc,
        "org_id": org_id,
        "repo_name": repo_name,
        "file_path": file_path,
        "branch": branch,
    }


# ============================================================
# LFS 专属辅助函数
# ============================================================
def _get_oid_and_size(data):
    """计算 SHA256 (OID) 和文件大小"""
    h = hashlib.sha256()
    size = 0
    if isinstance(data, str) and os.path.isfile(data):
        with open(data, "rb") as f:
            while chunk := f.read(1024 * 1024 * 4):
                h.update(chunk)
                size += len(chunk)
    else:
        h.update(data)
        size = len(data)
    return h.hexdigest(), size


def _make_lfs_pointer(oid, size):
    """生成 Git LFS 的标准指针文件内容"""
    return (f"version https://git-lfs.github.com/spec/v1\n"
            f"oid sha256:{oid}\nsize {size}\n").encode("utf-8")


def _detect_lfs_identity(token, domain, org_id, repo_name):
    """
    解析 LFS Basic Auth 候选账号列表。

    优先级：
      1. !config.json 中配置的 BASIC_AUTH_USERNAME（如有）→ 跳过嗅探，直接用它。
      2. 否则走 OpenAPI 自动嗅探（查询 /platform/user 等接口）。
    """
    cache_key = (token, domain, org_id, repo_name)
    if cache_key in _lfs_identity_cache:
        return _lfs_identity_cache[cache_key]

    candidate_usernames = []

    # ---------- 分支 1: config 提供了 Basic Auth 账号 ----------
    if BASIC_AUTH_USERNAME:
        print(f"  [√] 使用配置中的 Basic Auth 账号: {BASIC_AUTH_USERNAME}"
              f"（跳过自动嗅探）")
        candidate_usernames.append(BASIC_AUTH_USERNAME)
    else:
        # ---------- 分支 2: 自动嗅探 ----------
        oapi_headers = {
            "x-yunxiao-token": token,
            "Private-Token": token,
            "Authorization": f"Bearer {token}",
        }
        user_endpoints = [
            f"https://{domain}/oapi/v1/platform/user",
            f"https://{domain}/oapi/v1/user",
            f"https://{domain}/oapi/v1/codeup/user",
        ]
        for u_url in user_endpoints:
            try:
                res = _req("GET", u_url, headers=oapi_headers, verify=False, timeout=8)
                if res.status_code == 200:
                    data = res.json()
                    user_info = data.get("result") or data
                    if isinstance(user_info, dict):
                        if user_info.get("username"):
                            candidate_usernames.append(str(user_info["username"]))
                        if user_info.get("name"):
                            candidate_usernames.append(str(user_info["name"]))
                        if user_info.get("id"):
                            candidate_usernames.append(str(user_info["id"]))
                        if user_info.get("email"):
                            candidate_usernames.append(str(user_info["email"]).split("@")[0])
                        print(f"  [√] OpenAPI 嗅探真实账号: "
                              f"{user_info.get('username') or user_info.get('name') or user_info.get('id')}")
                        break
            except Exception:
                pass

    # candidate_usernames.extend([org_id, "git"])
    # candidate_usernames = list(dict.fromkeys([u for u in candidate_usernames if u]))
    # print(f"  [*] LFS Basic Auth 账号候选列表: {candidate_usernames}")
    _lfs_identity_cache[cache_key] = candidate_usernames
    return candidate_usernames


def _lfs_batch_request(operation, oid, size, repo_name, token, domain, org_id, branch=None):
    """调用 Git LFS Batch API 获取上传/下载授权链接。"""
    branch = branch or DEFAULT_BRANCH
    candidate_usernames = _detect_lfs_identity(token, domain, org_id, repo_name)

    payload = {
        "operation": operation,
        "transfers": ["basic"],
        "objects": [{"oid": oid, "size": size}],
        "ref": {"name": f"refs/heads/{branch}"},
    }

    # ---- 候选 LFS Batch URL（按经验顺序：标准约定优先，再兜底）----
    target_urls = [
        f"https://{domain}/codeup/{repo_name}.git/info/lfs/objects/batch",
        f"https://{domain}/codeup/{org_id}/{repo_name}.git/info/lfs/objects/batch",
    ]
    target_urls = list(dict.fromkeys(target_urls))

    ua = "git-lfs/3.5.1 (GitHub; windows amd64; go 1.21.0) git/2.44.0.windows.1"

    last_err = None
    for url in target_urls:
        for user in candidate_usernames:
            # 密码恒为 token（已按用户要求去掉独立的 BASIC_AUTH_PASSWORD 配置）
            auth_b64 = base64.b64encode(f"{user}:{token}".encode("utf-8")).decode("utf-8")
            headers = {
                "Accept": "application/vnd.git-lfs+json",
                "Content-Type": "application/vnd.git-lfs+json",
                "User-Agent": ua,
                "Authorization": f"Basic {auth_b64}",
            }
            try:
                r = _req("POST", url, json=payload, headers=headers,
                         verify=False, timeout=30)
                if r.status_code in (200, 202):
                    ct = r.headers.get("Content-Type", "")
                    if "json" in ct.lower() or r.text.strip().startswith("{"):
                        try:
                            return r.json()
                        except Exception:
                            last_err = f"响应非 JSON: {r.text[:200]}"
                            continue
                    else:
                        last_err = "HTTP 200 但返回 HTML 页面，非目标 API 路由"
                        continue
                elif r.status_code == 401:
                    last_err = f"401 (账号 '{user}' 未匹配或密码错误)"
                elif r.status_code == 403:
                    last_err = f"403 -> {r.text[:120]}"
                else:
                    last_err = f"[{r.status_code}] {r.text[:150]}"
            except requests.RequestException as e:
                last_err = f"网络异常: {e}"

    raise CodeupError(
        f"LFS Batch API 鉴权失败 [{operation}]。最后错误: {last_err}\n"
        "[*] 若持续失败，可在 Codeup 控制台 → 个人设置 → HTTPS密码 查看【克隆账号】，\n"
        "    填入 !config.json 的 BASIC_AUTH_USERNAME 后重试。"
    )


# ============================================================
# OpenAPI 普通上传
# ============================================================
def upload_openapi(data, repo_name=DEFAULT_REPO, file_path=None, token=None, domain=None,
                   org_id=DEFAULT_ORG_ID, branch=None, visibility=None,
                   commit_message=None, overwrite=True, timeout=None):
    repo_name = repo_name or DEFAULT_REPO
    branch = branch or DEFAULT_BRANCH
    visibility = visibility or DEFAULT_VISIBILITY
    timeout = timeout or DEFAULT_TIMEOUT

    if isinstance(data, str):
        if not os.path.isfile(data):
            raise CodeupError(f"文件不存在: {data}")
        file_path = file_path or os.path.basename(data)
        size = os.path.getsize(data)
        if size > MAX_OPENAPI_SIZE:
            raise CodeupError(f"文件 {size/1024/1024:.1f}MB 超过 OpenAPI 50MB 上限")
        with open(data, "rb") as f:
            payload = f.read()
    elif isinstance(data, (bytes, bytearray)):
        payload = bytes(data)
        if len(payload) > MAX_OPENAPI_SIZE:
            raise CodeupError(f"bytes {len(payload)/1024/1024:.1f}MB 超过 50MB 上限")
        if not file_path:
            raise CodeupError("bytes 输入必须提供 file_path")
    else:
        raise CodeupError("data 必须是 bytes 或文件路径字符串")

    if not commit_message:
        commit_message = _auto_msg_from_bytes(payload)

    base, headers, dom = _base(token, domain, org_id)
    rid = f'{org_id}%2F{repo_name}'

    content_b64 = base64.b64encode(payload).decode()
    body = {
        "branch": branch,
        "commitMessage": commit_message,
        "content": content_b64,
        "encoding": "base64",
        "filePath": file_path,
    }
    payload_json = json.dumps(body).encode()
    stream = _ProgressStream(payload_json, prefix="↑[OpenAPI上行]")
    h = headers.copy()
    h["Content-Length"] = str(len(payload_json))

    try:
        r = _req("POST", f"{base}/repositories/{rid}/files",
                 data=stream, headers=h, verify=False, timeout=timeout)
    except requests.RequestException as e:
        print()
        raise CodeupError(f"网络上传失败: {e}")

    delay = (time.time() - stream.finish_time) * 1000
    print(f"\n[-] 服务端处理延时: {delay:.2f}ms")

    if r.status_code in (200, 201):
        return _make_url(dom, rid, file_path, branch)

    if r.status_code in (400, 409) and overwrite:
        print("[*] 已存在，覆盖上传...")
        url_put = (f"{base}/repositories/{rid}/files/"
                   f"{requests.utils.quote(file_path, safe='')}")
        stream2 = _ProgressStream(payload_json, prefix="↑[OpenAPI上行]")
        try:
            r2 = _req("PUT", url_put, data=stream2, headers=h,
                      verify=False, timeout=timeout)
        except requests.RequestException as e:
            print()
            raise CodeupError(f"覆盖网络失败: {e}")
        print(f"\n[-] 覆盖服务端延时: {(time.time()-stream2.finish_time)*1000:.2f}ms")
        if r2.status_code in (200, 201):
            return _make_url(dom, rid, file_path, branch)
        raise CodeupError(f"覆盖失败 [{r2.status_code}]: {r2.text[:300]}")

    raise CodeupError(f"上传失败 [{r.status_code}]: {r.text[:300]}")


# ============================================================
# LFS 大文件上传
# ============================================================
def upload_lfs(data, repo_name=DEFAULT_REPO, file_path=None, token=None, domain=None,
               org_id=DEFAULT_ORG_ID, branch=None, visibility=None,
               commit_message=None, overwrite=True, timeout=DEFAULT_TIMEOUT):
    token = token or DEFAULT_TOKEN
    domain = domain or DEFAULT_DOMAIN
    repo_name = repo_name or DEFAULT_REPO
    branch = branch or DEFAULT_BRANCH

    if isinstance(data, str):
        if not os.path.isfile(data):
            raise CodeupError(f"文件不存在: {data}")
        file_path = file_path or os.path.basename(data)
    elif isinstance(data, (bytes, bytearray)):
        if not file_path:
            raise CodeupError("bytes 输入必须提供 file_path")
    else:
        raise CodeupError("data 必须是 bytes 或文件路径字符串")

    # 上传路径需要确保仓库存在（仅首次会打 1~2 个请求，之后走 _repo_cache）
    _ensure_repo(repo_name, token, domain, org_id,return_id=False)

    print(f"[*] 准备 LFS 上传 [{file_path}]，正在计算 SHA256...")
    oid, size = _get_oid_and_size(data)

    batch_res = _lfs_batch_request("upload", oid, size, repo_name, token, domain, org_id, branch)
    obj_info = batch_res["objects"][0]

    if "error" in obj_info:
        raise CodeupError(f"阿里云 LFS 返回错误: {json.dumps(obj_info, ensure_ascii=False)}")

    actions = obj_info.get("actions", {}) or {}
    if "upload" in actions:
        upload_action = actions["upload"]
        url = upload_action["href"]
        headers = dict(upload_action.get("header", {}) or {})
        headers["Content-Length"] = str(size)
        headers.pop("Transfer-Encoding", None)

        print("[*] 执行 LFS 大文件流式传输...")
        stream = _ProgressStream(data, prefix="↑[LFS上行]")
        try:
            r = _req("PUT", url, headers=headers, data=stream,
                     verify=False, timeout=timeout)
        except requests.RequestException as e:
            print()
            raise CodeupError(f"LFS 二进制上传失败: {e}")

        delay = (time.time() - stream.finish_time) * 1000
        print(f"\n[-] LFS 存储服务端延时: {delay:.2f}ms")

        if r.status_code not in (200, 201):
            raise CodeupError(f"LFS 存储响应失败 [{r.status_code}]: {r.text[:300]}")
    else:
        print("[*] LFS 服务器已存在该相同文件(秒传)，跳过二进制传输。")

    pointer_bytes = _make_lfs_pointer(oid, size)
    commit_message = commit_message or _auto_msg_from_bytes(data)
    print("[*] 正在提交 LFS 指针 (Pointer) 到代码库...")

    return upload_openapi(pointer_bytes, repo_name=repo_name, file_path=file_path,
                          token=token, domain=domain, org_id=org_id, branch=branch,
                          visibility=visibility, commit_message=commit_message,
                          overwrite=overwrite, timeout=timeout)


lfs_upload = upload_lfs


# ============================================================
# 对外统一入口：智能 Upload
# ============================================================
def upload(data, repo_name=DEFAULT_REPO, file_path=None, token=None, domain=None,
           org_id=DEFAULT_ORG_ID, branch=None, visibility=None,
           commit_message=None, overwrite=True, timeout=None, print_req=False):
    """
    对外统一的智能上传接口：
      - <= 50MB: OpenAPI 普通上传
      - >  50MB: 自动切换至 Git LFS 上传
    print_req=True 时打印所有 HTTP 请求（method / url / headers / body）
    """
    with _PrintReqCtx(print_req):
        if isinstance(data, str):
            if not os.path.isfile(data):
                raise CodeupError(f"文件不存在: {data}")
            size = os.path.getsize(data)
        elif isinstance(data, (bytes, bytearray)):
            size = len(data)
        else:
            raise CodeupError("data 必须是 bytes 或文件路径字符串")

        if size > MAX_OPENAPI_SIZE:
            print(f"\n[!] 自动判断: 数据大小 {size/1024/1024:.2f}MB > 50MB，自动切入 LFS 模式...")
            return upload_lfs(data, repo_name, file_path, token, domain, org_id, branch,
                              visibility, commit_message, overwrite, timeout)
        else:
            print(f"\n[*] 自动判断: 数据大小 {size/1024/1024:.2f}MB <= 50MB，使用常规 OpenAPI 模式...")
            return upload_openapi(data, repo_name, file_path, token, domain, org_id, branch,
                                  visibility, commit_message, overwrite, timeout)


# ============================================================
# OpenAPI 普通下载（不再调用 _ensure_repo，直接拼 rid）
# ============================================================
def download_openapi(file_path=None, repo_name=None, token=None, domain=None,
                     org_id=None, branch=None, timeout=None, save_to=None,
                     progress=None):
    if not file_path:
        raise CodeupError("必须提供 file_path")
    repo_name = repo_name or DEFAULT_REPO
    branch = branch or DEFAULT_BRANCH
    timeout = timeout or DEFAULT_TIMEOUT
    org_id = org_id or DEFAULT_ORG_ID
    base, headers, dom = _base(token, domain, org_id)
    # 直接用字符串 rid（OpenAPI 同样接受 org%2Frepo 形式），不再探测仓库数字 id
    rid = f"{org_id}%2F{repo_name}"
    url = (f"{base}/repositories/{rid}/files/"
           f"{requests.utils.quote(file_path, safe='')}?ref={branch}")

    print(f"[*] OpenAPI 请求元数据 [{file_path}] ...")
    t0 = time.time()
    try:
        r = _req("GET", url, headers=headers, verify=False, stream=True, timeout=timeout)
    except requests.RequestException as e:
        raise CodeupError(f"请求失败: {e}")

    ttfb = (time.time() - t0) * 1000
    if r.status_code != 200:
        raise CodeupError(f"下载失败 [{r.status_code}]: {r.text[:300]}")
    print(f"[-] Codeup TTFB: {ttfb:.2f}ms")

    total = int(r.headers.get("Content-Length", 0))
    chunks = []
    downloaded = 0
    t0 = time.time()
    for chunk in r.iter_content(131072):
        if chunk:
            chunks.append(chunk)
            downloaded += len(chunk)
            _fire_progress(progress, downloaded, total)
            el = time.time() - t0
            sp = (downloaded / 1024 / 1024) / el if el > 0 else 0
            if total > 50000:
                print(f"\r↓[OpenAPI下行]:{downloaded/1024/1024:.2f}/{total/1024/1024:.2f}MB "
                      f"{sp:.2f}MB/s", end="", flush=True)

    raw = b"".join(chunks)
    obj = json.loads(raw.decode())
    content = obj.get("content", "")
    enc = obj.get("encoding", "base64")
    result = base64.b64decode(content) if enc == "base64" else content.encode("utf-8")

    if save_to:
        os.makedirs(os.path.dirname(os.path.abspath(save_to)), exist_ok=True)
        with open(save_to, "wb") as f:
            f.write(result)
        return save_to
    return result


# ============================================================
# LFS 下载（自动识别 pointer）
# ============================================================
def download_lfs(file_path=None, repo_name=DEFAULT_REPO, token=None, domain=None,
                 org_id=DEFAULT_ORG_ID, branch=None, timeout=DEFAULT_TIMEOUT, save_to=None,
                 progress=None):
    token = token or DEFAULT_TOKEN
    domain = domain or DEFAULT_DOMAIN
    branch = branch or DEFAULT_BRANCH

    # pointer 文件很小（百字节级）。先缓冲 OpenAPI 段的最后一次进度：
    # 若确为 LFS，指针包进度丢弃、由下面真实 LFS 流汇报；若不是 LFS（普通文件），
    # 回放一次完整进度，避免小文件（图片等）完全没有进度回调。
    buffered = {}

    def _openapi_progress(got, total):
        buffered["got"], buffered["total"] = got, total

    raw_data = download_openapi(file_path=file_path, repo_name=repo_name, token=token,
                                domain=domain, org_id=org_id, branch=branch, timeout=timeout,
                                progress=_openapi_progress if progress is not None else None)

    if raw_data.startswith(b"version https://git-lfs.github.com/spec/v1"):
        print(f"\n[*] 识别为 LFS 大文件指针，正在向 LFS 服务器请求真实对象...")
        text = raw_data.decode("utf-8")
        oid = ""
        size = 0
        for line in text.splitlines():
            if line.startswith("oid sha256:"):
                oid = line.split(":", 1)[1].strip()
            elif line.startswith("size "):
                size = int(line.split(" ")[1].strip())

        if not oid:
            raise CodeupError("LFS 指针文件损坏：未找到 oid")

        batch_res = _lfs_batch_request("download", oid, size, repo_name, token,
                                       domain, org_id, branch)
        obj_info = batch_res["objects"][0]
        if "error" in obj_info:
            raise CodeupError(f"LFS 返回错误: {json.dumps(obj_info, ensure_ascii=False)}")
        dl_action = obj_info["actions"]["download"]
        url = dl_action["href"]
        headers = dict(dl_action.get("header", {}) or {})

        print(f"[*] 获取 LFS 二进制流...")
        t0 = time.time()
        r = _req("GET", url, headers=headers, verify=False, stream=True, timeout=timeout)
        ttfb = (time.time() - t0) * 1000
        print(f"[-] LFS 节点 TTFB: {ttfb:.2f}ms")

        if r.status_code != 200:
            raise CodeupError(f"LFS 对象下载失败 [{r.status_code}]: {r.text[:300]}")

        total = size
        chunks = []
        downloaded = 0
        t0 = time.time()

        if save_to:
            os.makedirs(os.path.dirname(os.path.abspath(save_to)), exist_ok=True)
            f_out = open(save_to, "wb")

        for chunk in r.iter_content(131072):
            if chunk:
                if save_to:
                    f_out.write(chunk)
                else:
                    chunks.append(chunk)

                downloaded += len(chunk)
                _fire_progress(progress, downloaded, total)
                el = time.time() - t0
                sp = (downloaded / 1024 / 1024) / el if el > 0 else 0
                print(f"\r↓[LFS下行]:{downloaded/1024/1024:.2f}/{total/1024/1024:.2f}MB "
                      f"{sp:.2f}MB/s", end="", flush=True)

        print()
        if save_to:
            f_out.close()
            print(f"[+] LFS 文件已保存到 {save_to}")
            return save_to
        return b"".join(chunks)

    else:
        print("\n[*] 识别为普通文件，直接返回。")
        # 普通文件：OpenAPI 段已下完，回放最后一次（通常即完成）进度。
        if buffered:
            _fire_progress(progress, buffered.get("got", 0), buffered.get("total", 0))
        if save_to:
            os.makedirs(os.path.dirname(os.path.abspath(save_to)), exist_ok=True)
            with open(save_to, "wb") as f:
                f.write(raw_data)
            return save_to
        return raw_data


lfs_download = download_lfs


# ============================================================
# 对外统一入口：智能 Download
# ============================================================
def download(file_path=None, repo_name=DEFAULT_REPO, token=None, domain=None,
             org_id=DEFAULT_ORG_ID, branch=None, timeout=DEFAULT_TIMEOUT,
             save_to=None, max_show_bytes_size=99, print_req=False, progress=None):
    """
    对外统一的智能下载接口：
      - file_path 支持：纯文件名 / 相对绝对路径 / upload() 返回的完整 Codeup URL
      - 若识别为 LFS Pointer，会自动拉取背后真实的 LFS 大文件
      - 返回 bytes 且长度超过 max_show_bytes_size 时，包装为带自定义 repr 的
        bytes 子类实例（仅影响显示，不截断数据）
    print_req=True 时打印所有 HTTP 请求（method / url / headers / body）
    """
    with _PrintReqCtx(print_req):
        if not file_path:
            raise CodeupError("必须提供 file_path 或 Codeup 文件 URL")

        if isinstance(file_path, str) and file_path.startswith(("http://", "https://")):
            parsed = _parse_codeup_file_url(file_path)
            if parsed is None:
                raise CodeupError(f"无法解析该 Codeup 文件 URL: {file_path}")
            print(f"[*] 从 URL 解析: domain={parsed['domain']} "
                  f"org={parsed['org_id']} repo={parsed['repo_name']} "
                  f"branch={parsed['branch']}")
            print(f"[*] 目标文件: {parsed['file_path']}")
            file_path = parsed["file_path"]
            domain    = parsed["domain"]    or domain
            org_id    = parsed["org_id"]    or org_id
            repo_name = parsed["repo_name"] or repo_name
            branch    = parsed["branch"]    or branch

        b = download_lfs(file_path, repo_name, token, domain, org_id, branch, timeout,
                         save_to, progress=progress)

        if isinstance(b, (bytes, bytearray)) and max_show_bytes_size and len(b) > max_show_bytes_size:
            return object_custom_repr(b, max_show_bytes_size=max_show_bytes_size)
        return b


# ============================================================
# 自测
# ============================================================
if __name__ == "__main__":
    SMALL_FILE = "test_1MB.bin"
    LARGE_FILE = "lfs_test_100MB.bin"

    if not os.path.exists(SMALL_FILE):
        print("生成 1MB 测试小文件...")
        with open(SMALL_FILE, "wb") as f:
            f.write(os.urandom(1 * 1024 * 1024))

    if not os.path.exists(LARGE_FILE):
        print("生成 100MB 测试大文件...")
        with open(LARGE_FILE, "wb") as f:
            f.write(os.urandom(100 * 1024 * 1024))

    print("\n" + "=" * 50)
    print("测试统一 UPLOAD 接口 (传小文件) + print_req")
    print("=" * 50)
    url_small = upload(SMALL_FILE, file_path="test_1MB.bin", print_req=True)
    print(f"[+] 小文件链接: {url_small}")

    print("\n" + "=" * 50)
    print("测试统一 UPLOAD 接口 (传大文件)")
    print("=" * 50)
    url_large = upload(LARGE_FILE, file_path="lfs_test_100MB.bin")
    print(f"[+] 大文件链接: {url_large}")

    print("\n" + "=" * 50)
    print("测试统一 DOWNLOAD 接口")
    print("=" * 50)
    saved_path = download("lfs_test_100MB.bin", save_to="downloaded_100MB.bin")
    print(f"[+] 下载测试成功，文件在: {saved_path}")