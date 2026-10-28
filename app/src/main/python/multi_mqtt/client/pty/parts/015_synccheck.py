# ==================== 版本自检：单文件 vs pty/parts 分片是否同步 ====================
#
# 单文件由 generate_single_file.py 把 parts/ 全部分片按序字节拼接而成，
# 「单文件字节 == 分片拼接」时两侧 sha256 必然相同——不需要在文件里嵌
# 任何版本标记（嵌标记反而破坏逐字节拼接的不变量）。main() 启动时自动
# 比对一次：分片改了没重组（或有人手改了单文件）会打 WARN 并给出重组
# 命令；结果也可随时经 HTTP RPC 查询：
#
#     curl "http://127.0.0.1:1188/r=check_parts_sync()"
#
# 部署机上没有 parts 目录时自动跳过，纯单文件场景不告警。


def check_parts_sync(verbose: bool = True) -> dict:
    """比对运行中的单文件与 client/pty/parts 分片是否逐字节同步。

    返回 {"ok": True/False/None(无法判定), "single", "parts",
    "sha_single", "sha_parts"}；verbose=True 时把结论写本地日志。
    """
    import hashlib

    here = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(here) == "pty":     # 包模式 client/pty/__init__.py
        parts_dir = os.path.join(here, "parts")
        single = os.path.join(os.path.dirname(here), "pty_client_mqtt.py")
    else:                                    # 单文件模式（含 _generated 预览）
        parts_dir = os.path.join(here, "pty", "parts")
        single = os.path.abspath(__file__)
    try:
        names = sorted(n for n in os.listdir(parts_dir) if n.endswith(".py"))
    except OSError:
        names = []
    if not names:
        res = {"ok": None, "reason": "parts 目录不存在（纯部署场景），跳过",
               "single": single, "parts": parts_dir}
        if verbose:
            _info("[pty] 版本自检：%s" % res["reason"])
        return res
    h = hashlib.sha256()
    for n in names:
        with open(os.path.join(parts_dir, n), "rb") as f:
            h.update(f.read())
    sha_parts = h.hexdigest()
    try:
        with open(single, "rb") as f:
            sha_single = hashlib.sha256(f.read()).hexdigest()
    except OSError:
        sha_single = None
    if sha_single is None:
        res = {"ok": False, "single": single, "parts": parts_dir,
               "sha_single": None, "sha_parts": sha_parts[:12]}
        if verbose:
            _emit_local("[pty][WARN] 版本自检：单文件 %s 不存在"
                        "（parts 才是真相源）\n" % single)
        return res
    res = {"ok": sha_single == sha_parts, "single": single, "parts": parts_dir,
           "sha_single": sha_single[:12], "sha_parts": sha_parts[:12]}
    if verbose and res["ok"]:
        _info("[pty] 版本自检：与 pty/parts 分片同步（sha256=%s）" % sha_parts[:12])
    elif verbose:
        _emit_local("[pty][WARN] 版本自检：当前文件与 client/pty/parts 分片不一致"
                    "（文件 %s / 分片 %s）！分片改动后请重组："
                    "python client/pty/generate_single_file.py "
                    "-o client/pty_client_mqtt.py -f\n"
                    % (res["sha_single"], res["sha_parts"]))
    return res


