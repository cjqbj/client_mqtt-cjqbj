#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""remote_cmd —— 与网络层无关的「远端 Shell / 文件编辑」命令封装层。

设计原则
========
1. **本模块只负责把各种操作包装成自包含 Python 代码（str），并解析回包。**
   网络怎么走由注入的 :class:`Transport` 决定（MQTT / HTTP / TCP / WebSocket …），
   换网络层时本文件一行都不用改——只需新写一个实现 ``request(code, timeout)``
   一问一答接口的 Transport。

2. **远端零改动**：远端只需要能执行 Python 字符串并返回
   ``{"ok": bool, "r": str, "stdout": str, "error"?: str}`` 形态的回包。

3. **报文是 JSON 文本协议，不是字节协议**（实测）：
   - 文本（含中文）直接放在 JSON 字符串里，无需任何编码；
   - 任意二进制用 latin-1 映射成 U+0000..U+00FF 字符承载（1 字节 ↔ 1 字符，
     远端 ``.encode('latin-1')`` 无损还原）；
   - 不能往报文里塞 Python bytes（json.dumps 直接 TypeError），也不能发绕过
     JSON 的原始 MQTT 二进制帧（对端 decode('utf-8') 会丢弃）。

4. **单报文 ≤ 1 MiB（MQTT broker 实际上限，实测 1000KiB 可过、1200KiB 被丢，
   且 512KiB 已需 4 秒）**。文件传输默认禁止超过 ``MAX_TRANSFER``（1 MiB），
   大文件必须在远端就地处理（grep/sed/编辑/下载/解压都在远端跑），见
   :class:`RemoteShell` 文档与 mqtt-remote-shell skill。
"""
from __future__ import annotations

import base64
import io
import json
import os
import posixpath
import shlex
import tarfile
import threading
import time
from abc import ABC, abstractmethod

# ---- 传输尺寸约束（均按实测的公共 broker 行为设定） --------------------------
MAX_TRANSFER = 1 << 20          # 1 MiB：单文件经本报文通道传输的默认硬上限
WIRE_BUDGET = 1 << 17          # 128 KiB：单条报文 JSON 转义后在线尺寸目标（实测 0.3s）
# pull_dir 整目录压缩包：硬顶 1 MiB（用户红线，远端同样强制，调更高必被拒）；
# 默认阈值只给 700 KiB——压缩包还要 base64（膨胀 4/3 ≈ 934KiB）外加 JSON/信封，
# 必须稳在 broker 实测可过的 1000 KiB 线以内。
MAX_DIR_ARCHIVE = 1 << 20
DEFAULT_DIR_ARCHIVE_MAX = 700 << 10
INLINE_MAX = 8192              # 命令输出小于该字节数时直接随回包返回
DEFAULT_TIMEOUT = 60           # 单次问答默认等待秒数
DEFAULT_STREAM_TOPIC = "sys/device/stream"  # 远端周期汇报的默认 topic

# ---- PTY 会话约束 ------------------------------------------------------------
PTY_FRAME_MAX = 1 << 14        # 16 KiB：PTY 单帧原始字节上限（最坏 latin-1 转义后 < 96KiB）
DEFAULT_PTY_TTL = 3600*24    # 孤儿 PTY 会话最长存活秒数（默认 12 小时）
MAX_PTY_TTL = 3600*24*30        # TTL 允许设定的上限


# ============================ 网络层抽象（更换协议时实现它） ============================

class Transport(ABC):
    """一问一答的代码执行通道。换网络层只需实现这两个方法。

    输入  : Python 代码字符串、超时秒数
    输出  : 原始回包 dict，至少形如
            ``{"ok": bool, "r": str|None, "stdout": str, "error"?: str}``
           （网络层自己的元信息如 latency/server_from 可以保留，本层不看）
    超时/通道故障：返回 ``None`` 或抛异常均可（本层把 None 映射为 RemoteTimeout）。
    """

    @abstractmethod
    def request(self, code: str, timeout: float = DEFAULT_TIMEOUT) -> dict | None:
        ...

    def close(self):
        """释放底层连接等资源；无资源可省略。"""

    # ---- 可选能力：服务端推送（周期汇报）。不实现则 RemoteShell.stream() 不可用 ----
    # 约定两个方法，签名：
    #   stream_subscribe(topic: str, handler: Callable[[dict], None]) -> None
    #   stream_unsubscribe(topic: str, handler: Callable) -> None
    # handler 收到的是推送消息解出的 dict；RemoteShell 按帧里的 stream/seq 自行
    # 过滤 sid 与多 broker 重复帧，transport 只负责"订阅 + 原样分发"。
    #
    # ---- 可选能力：PTY（RemotePty 使用）。除上面两个方法外另需 ----
    #   publish(topic: str, payload_dict: dict) -> None
    # 即向任意 topic 发一帧（下行输入/控制），与 request 的一问一答无关。

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ============================ 远端执行模板（自包含，禁止改名） ============================
# 远端持久命名空间里只占一个固定函数名 _cmq_dispatch；函数体内不得 print；
# 最后一条裸表达式调用，REPL 语义把返回的 JSON 字符串放进 r。
# 二进制一律 latin-1 字符承载，文本就是文本。
_REMOTE_TEMPLATE = r'''
def _cmq_dispatch():
    import json as _j, os as _o, sys as _s, time as _t, shutil as _sh
    import stat as _st, hashlib as _hl, tempfile as _tf, traceback as _tb
    import subprocess as _sp, errno as _en
    _a = _j.loads(__PAYLOAD__)
    _res = {}

    def _S(_x):
        # bytes -> 可入 JSON 的字符串：latin-1 对 0..255 一一映射，绝不损失
        return _x.decode("latin-1")

    def _B(_s):
        # JSON 字符串 -> bytes
        return _s.encode("latin-1")

    def _xp(_p):
        return _o.path.abspath(_o.path.expanduser(str(_p)))

    def _stash_dir():
        _d = _o.path.join(_tf.gettempdir(), "cmq_stash")
        _o.makedirs(_d, exist_ok=True)
        try:
            _now = _t.time()
            for _f in _o.listdir(_d):
                _fp = _o.path.join(_d, _f)
                try:
                    if _now - _o.path.getmtime(_fp) > 3600:
                        _o.remove(_fp)
                except Exception:
                    pass
        except Exception:
            pass
        return _d

    def _stash(_data):
        _fd, _fp = _tf.mkstemp(prefix="s", dir=_stash_dir())
        with _o.fdopen(_fd, "wb") as _fh:
            _fh.write(_data)
        return _o.path.basename(_fp)

    def _pack(_tag, _data, _thr):
        _res[_tag + "_size"] = len(_data)
        if len(_data) <= _thr:
            _res[_tag] = _S(_data)
        else:
            _res[_tag] = None
            _res[_tag + "_id"] = _stash(_data)

    def _finfo(_p, _st2):
        _m = _st2.st_mode
        _d = {"path": _p, "size": _st2.st_size,
              "mode": oct(_st.S_IMODE(_m)),
              "mtime": _st2.st_mtime, "uid": _st2.st_uid, "gid": _st2.st_gid,
              "isdir": _st.S_ISDIR(_m), "isfile": _st.S_ISREG(_m),
              "islink": _st.S_ISLNK(_m)}
        if _st.S_ISLNK(_m):
            try:
                _d["linkto"] = _o.readlink(_p)
            except Exception:
                _d["linkto"] = None
        return _d

    def _atomic_write(_dst, _data, _mode, _backup, _enc):
        _d2 = _o.path.dirname(_dst) or "."
        _old_mode = None
        if _o.path.exists(_dst):
            try:
                _old_mode = _st.S_IMODE(_o.lstat(_dst).st_mode)
            except Exception:
                pass
            if _backup:
                _sh.copy2(_dst, _dst + ".bak")
        _fd, _tmp = _tf.mkstemp(prefix=".cmq.", suffix=".part", dir=_d2)
        try:
            with _o.fdopen(_fd, "wb") as _fh:
                _fh.write(_data)
                _fh.flush()
                _o.fsync(_fh.fileno())
            try:
                _o.replace(_tmp, _dst)
            except OSError as _e:
                if _e.errno == _en.EXDEV:
                    _sh.move(_tmp, _dst)
                elif _e.errno == getattr(_en, "EBUSY", 16):
                    # Docker 绑定挂载文件（/etc/hostname|hosts|resolv.conf 等）
                    # 禁止 rename 替换：原地截断写、保留 inode（sed -i 同款坑）
                    with open(_dst, "wb") as _fh:
                        _fh.write(_data)
                        _fh.flush()
                        _o.fsync(_fh.fileno())
                    try:
                        _o.remove(_tmp)
                    except OSError:
                        pass
                else:
                    raise
            if _mode is not None:
                _mm = int(_mode, 8) if isinstance(_mode, str) else int(_mode)
                _o.chmod(_dst, _mm)
            elif _old_mode is not None:
                _o.chmod(_dst, _old_mode)
            elif _o.name == "posix":
                _o.chmod(_dst, 0o644)
        except Exception:
            try:
                _o.remove(_tmp)
            except OSError:
                pass
            raise
        _st3 = _o.stat(_dst)
        return _st3

    def _find_mqtt_net():
        # 复用服务端进程里现成的 MQTT 网络层（server_mqtt.py 的 gms.mqtt_net），
        # 不新建任何 broker 连接；找不到则不支持 stream。
        for _v in list(globals().values()):
            _mn = getattr(_v, "mqtt_net", None)
            if _mn is not None and hasattr(_mn, "publish_broadcast"):
                return _mn
        return None

    def _stats_once():
        # 纯标准库读 /proc 采一帧：整机 CPU%/内存/load/top8 进程。
        # 容器内 /proc 常是宿主视图，口径以远端内核为准。每帧 <1KB，适合秒级推送。
        import glob as _gb
        _page = _o.sysconf("SC_PAGE_SIZE") if hasattr(_o, "sysconf") else 4096

        def _cpu_total():
            with open("/proc/stat") as _fh:
                _v = [int(_x) for _x in _fh.readline().split()[1:]]
            return sum(_v), _v[3] + _v[4]

        def _snap():
            _out = []
            for _sd in _gb.glob("/proc/[0-9]*"):
                try:
                    _stt = open(_sd + "/stat", "rb").read().decode("ascii", "replace").split()
                    _pid = int(_stt[0])
                    _comm = _stt[1].strip("()")
                    _jif = int(_stt[13]) + int(_stt[14])
                    _rss = 0
                    try:
                        _rss = int(open(_sd + "/statm").read().split()[1]) * _page
                    except Exception:
                        pass
                    _out.append((_pid, _comm, _jif, _rss))
                except Exception:
                    pass
            return _out

        _t1, _i1 = _cpu_total()
        _p1 = _snap()
        _t.sleep(0.15)
        _t2, _i2 = _cpu_total()
        _p2 = _snap()
        _dt = max(_t2 - _t1, 1)
        _cpu_pct = round(100.0 * (_dt - (_i2 - _i1)) / _dt, 1)
        _mi = {}
        with open("/proc/meminfo") as _fh:
            for _ln in _fh:
                _k, _v = _ln.split(":", 1)
                _mi[_k] = int(_v.split()[0])
        _mt = _mi.get("MemTotal", 0)
        _ma = _mi.get("MemAvailable", 0)
        _load = open("/proc/loadavg").read().split()[:3]
        _old = {_x[0]: _x for _x in _p1}
        _rows = []
        for _pid, _comm, _j2, _rss2 in _p2:
            _q = _old.get(_pid)
            if _q is None:
                continue
            _pc = max(0.0, 100.0 * (_j2 - _q[2]) / _dt)
            _rows.append((round(_pc, 1), _pid, _comm, _rss2))
        _rows.sort(key=lambda _z: _z[0], reverse=True)
        return {"cpu_pct": _cpu_pct,
                "mem_total_kb": _mt, "mem_avail_kb": _ma,
                "mem_used_pct": round(100.0 * (_mt - _ma) / max(_mt, 1), 1),
                "load": [float(_x) for _x in _load],
                "procs": [{"pid": _pid, "comm": _comm, "cpu": _pc, "rss_kb": _rss}
                          for _pc, _pid, _comm, _rss in _rows[:8]]}

    def _stream_worker(_sid, _cfg, _net):
        # 后台守护线程：周期执行命令或采 /proc，每帧直接 publish 到汇报 topic。
        # 帧无 code 字段 → 网络层不签名，也不会被任何服务端当新命令回环执行。
        _ev = globals().get("_cmq_streams", {}).get(_sid, {}).get("event")
        _seq = 0
        _deadline = _t.time() + float(_cfg["ttl"])
        _reason = "count"
        try:
            while _ev is not None and not _ev.is_set():
                if _t.time() > _deadline:
                    _reason = "ttl"
                    break
                if int(_cfg["count"]) and _seq >= int(_cfg["count"]):
                    _reason = "count"
                    break
                _t0 = _t.time()
                _frame = {"stream": _sid, "seq": _seq, "ts": int(_t0 * 1000)}
                try:
                    if _cfg["mode"] == "stats":
                        _frame["stats"] = _stats_once()
                    else:
                        _gp2 = _sp.run(
                            ["/bin/sh", "-c", str(_cfg.get("cmd") or "top -b -n 1")],
                            stdout=_sp.PIPE, stderr=_sp.PIPE,
                            cwd=_xp(_cfg["cwd"]) if _cfg.get("cwd") else None,
                            timeout=float(_cfg["frame_timeout"]))
                        _frame["rc"] = _gp2.returncode
                        _frame["out"] = (_gp2.stdout or b"").decode("latin-1")
                        _frame["err"] = (_gp2.stderr or b"").decode("latin-1")
                except Exception as _e:
                    _frame["frame_error"] = repr(_e)
                try:
                    _net.publish_broadcast(_cfg["topic"], _frame)
                except Exception:
                    pass
                _seq += 1
                _rest = float(_cfg["interval"]) - (_t.time() - _t0)
                if _ev.wait(max(0.0, _rest)):
                    _reason = "stopped"
                    break
        except Exception:
            _reason = "error"
        finally:
            try:
                _net.publish_broadcast(_cfg["topic"],
                                       {"stream": _sid, "end": True,
                                        "reason": _reason, "frames": _seq})
            except Exception:
                pass
            globals().get("_cmq_streams", {}).pop(_sid, None)

    try:
        _op = _a.get("op")

        if _op == "info":
            import platform as _pf, getpass as _gp
            _u = _o.uname() if hasattr(_o, "uname") else None
            _res = {"ok": True,
                    "system": _pf.system(), "node": _pf.node(),
                    "machine": _pf.machine(), "release": _pf.release(),
                    "uname": (" ".join([_u.sysname, _u.nodename, _u.release,
                                        _u.version, _u.machine])) if _u else "",
                    "python": _s.version.split()[0],
                    "cwd": _o.getcwd(), "home": _o.path.expanduser("~"),
                    "uid": _o.getuid() if hasattr(_o, "getuid") else 0,
                    "gid": _o.getegid() if hasattr(_o, "getegid") else 0,
                    "user": _gp.getuser(),
                    "shell": _o.environ.get("SHELL", ""),
                    "env_PATH": _o.environ.get("PATH", "")}

        elif _op == "run":
            _thr = int(_a.get("inline_max", 8192))
            _cwd = _xp(_a["cwd"]) if _a.get("cwd") else None
            _env = None
            if isinstance(_a.get("env"), dict) and _a["env"]:
                _env = dict(_o.environ)
                for _k, _v in _a["env"].items():
                    if _v is None:
                        _env.pop(str(_k), None)
                    else:
                        _env[str(_k)] = str(_v)
            if _a.get("args") is not None:
                _cmd = [str(_x) for _x in _a["args"]]
            else:
                _exe = _a.get("shell") or ("/bin/sh" if _o.name == "posix" else "cmd.exe")
                _flag = "-c" if _o.name == "posix" else "/c"
                _cmd = [_exe, _flag, str(_a.get("cmd", ""))]
            _kw = {"stdout": _sp.PIPE, "stderr": _sp.PIPE, "cwd": _cwd}
            if _env is not None:
                _kw["env"] = _env
            _t0 = _t.time()
            if _o.name == "posix":
                _p = _sp.Popen(_cmd, preexec_fn=_o.setsid, **_kw)
            else:
                _p = _sp.Popen(_cmd, **_kw)
            try:
                _out, _err = _p.communicate(timeout=_a.get("timeout"))
                _res["timed_out"] = False
            except _sp.TimeoutExpired:
                try:
                    if _o.name == "posix":
                        _o.killpg(_p.pid, 9)
                    else:
                        _p.kill()
                except Exception:
                    _p.kill()
                _out, _err = _p.communicate()
                _res["timed_out"] = True
            _res["ok"] = True
            _res["rc"] = _p.returncode
            _res["dur"] = round(_t.time() - _t0, 3)
            _pack("out", _out or b"", _thr)
            _pack("err", _err or b"", _thr)

        elif _op == "read":
            _p = _xp(_a["path"])
            _off = int(_a.get("off", 0))
            _n = int(_a.get("n", 32768))
            with open(_p, "rb") as _fh:
                _fh.seek(0, 2)
                _total = _fh.tell()
                _fh.seek(_off)
                _chunk = _fh.read(_n)
            _res = {"ok": True, "path": _p, "size": _total,
                    "data": _S(_chunk), "n": len(_chunk),
                    "eof": _off + len(_chunk) >= _total}

        elif _op == "hash":
            _p = _xp(_a["path"])
            _algo = str(_a.get("algo", "sha256"))
            _h = _hl.new(_algo)
            with open(_p, "rb") as _fh:
                while True:
                    _c1 = _fh.read(1 << 20)
                    if not _c1:
                        break
                    _h.update(_c1)
            _res = {"ok": True, "path": _p, "algo": _algo,
                    "digest": _h.hexdigest(), "size": _o.path.getsize(_p)}

        elif _op == "push_init":
            _dst = _xp(_a["path"])
            _d2 = _o.path.dirname(_dst)
            if _a.get("parents", True):
                _o.makedirs(_d2 or ".", exist_ok=True)
            _fd, _part = _tf.mkstemp(prefix=".cmq.", suffix=".part", dir=_d2 or ".")
            _o.close(_fd)
            _res = {"ok": True, "part": _part}

        elif _op == "push_data":
            _part = str(_a["part"])
            _off = int(_a.get("off", 0))
            _data = _B(_a.get("data") or "")
            with open(_part, "rb+") as _fh:
                _cur = _fh.seek(0, 2)
                if _cur != _off:
                    raise RuntimeError("bad chunk offset: expect %d, got %d" % (_cur, _off))
                _fh.seek(_off)
                _fh.write(_data)
                _fh.flush()
                _o.fsync(_fh.fileno())
            if not _a.get("last"):
                _res = {"ok": True, "size": _off + len(_data)}
            else:
                _dst = _xp(_a["path"])
                _st3 = _atomic_write(
                    _dst, open(_part, "rb").read(), _a.get("mode"),
                    bool(_a.get("backup")), _a.get("encoding", "utf-8"))
                try:
                    _o.remove(_part)
                except OSError:
                    pass
                _h = _hl.sha256()
                with open(_dst, "rb") as _fh:
                    while True:
                        _c2 = _fh.read(1 << 20)
                        if not _c2:
                            break
                        _h.update(_c2)
                _res = {"ok": True, "path": _dst, "bytes": _st3.st_size,
                        "sha256": _h.hexdigest(), "mode": oct(_st.S_IMODE(_st3.st_mode))}

        elif _op == "push_abort":
            try:
                _o.remove(str(_a["part"]))
            except FileNotFoundError:
                pass
            _res = {"ok": True}

        elif _op == "fetch":
            _sid = _o.path.basename(str(_a["id"]))
            _fp = _o.path.join(_stash_dir(), _sid)
            _off = int(_a.get("off", 0))
            _n = int(_a.get("n", 32768))
            with open(_fp, "rb") as _fh:
                _fh.seek(0, 2)
                _total = _fh.tell()
                _fh.seek(_off)
                _chunk = _fh.read(_n)
            _res = {"ok": True, "data": _S(_chunk), "n": len(_chunk),
                    "size": _total, "eof": _off + len(_chunk) >= _total}

        elif _op == "drop":
            _sid = _o.path.basename(str(_a["id"]))
            try:
                _o.remove(_o.path.join(_stash_dir(), _sid))
            except FileNotFoundError:
                pass
            _res = {"ok": True}

        elif _op == "ls":
            _p = _xp(_a.get("path", "."))
            _items = []
            for _n2 in sorted(_o.listdir(_p)):
                _fp2 = _o.path.join(_p, _n2)
                _it = {"name": _n2}
                _it.update(_finfo(_fp2, _o.lstat(_fp2)))
                _items.append(_it)
            _res = {"ok": True, "path": _p, "items": _items}

        elif _op == "stat":
            _p = _xp(_a["path"])
            _res = {"ok": True}
            _res.update(_finfo(_p, _o.lstat(_p)))

        elif _op == "exists":
            _p = _xp(_a["path"])
            _res = {"ok": True, "path": _p, "exists": _o.path.exists(_p),
                    "lexists": _o.path.lexists(_p)}

        elif _op == "mkdir":
            _p = _xp(_a["path"])
            _o.makedirs(_p, exist_ok=bool(_a.get("exist_ok", True)))
            if _o.name == "posix" and _a.get("mode") is not None:
                _o.chmod(_p, int(_a["mode"], 8) if isinstance(_a["mode"], str)
                         else int(_a["mode"]))
            _res = {"ok": True, "path": _p}

        elif _op == "rm":
            _p = _xp(_a["path"])
            _rec = bool(_a.get("recursive", False))
            if not _o.path.lexists(_p):
                if not _a.get("missing_ok", True):
                    raise FileNotFoundError(_p)
            elif _st.S_ISDIR(_o.lstat(_p).st_mode) and not _st.S_ISLNK(_o.lstat(_p).st_mode):
                if _rec:
                    _sh.rmtree(_p)
                else:
                    raise IsADirectoryError(_p)
            else:
                _o.remove(_p)
            _res = {"ok": True, "path": _p}

        elif _op == "mv":
            _s2, _d3 = _xp(_a["src"]), _xp(_a["dst"])
            _sh.move(_s2, _d3)
            _res = {"ok": True, "src": _s2, "dst": _d3}

        elif _op == "cp":
            _s2, _d3 = _xp(_a["src"]), _xp(_a["dst"])
            if _o.path.isdir(_s2):
                _sh.copytree(_s2, _d3, dirs_exist_ok=True)
            else:
                _sh.copy2(_s2, _d3)
            _res = {"ok": True, "src": _s2, "dst": _d3}

        elif _op == "chmod":
            _p = _xp(_a["path"])
            _mode = int(_a["mode"], 8) if isinstance(_a["mode"], str) else int(_a["mode"])
            if _a.get("recursive") and _o.path.isdir(_p):
                for _root, _dirs, _files in _o.walk(_p):
                    try:
                        _o.chmod(_root, _mode)
                    except Exception:
                        pass
                    for _f3 in _files:
                        try:
                            _o.chmod(_o.path.join(_root, _f3), _mode)
                        except Exception:
                            pass
            else:
                _o.chmod(_p, _mode)
            _res = {"ok": True, "path": _p, "mode": oct(_st.S_IMODE(_o.stat(_p).st_mode))}

        elif _op == "grep":
            # 结构化远端 grep：-nH 输出 path:lineno:text，由客户端解析；
            # 搜索全程在远端完成，不回传文件内容。
            _gx = _sh.which("grep") or "grep"
            _ga = [_gx, "-nH", "--binary-files=without-match",
                   "-F" if _a.get("fixed") else "-E"]
            if _a.get("ignore_case"):
                _ga.append("-i")
            if _a.get("recursive", True):
                _ga.append("-r")
            for _inc in _a.get("include") or []:
                _ga += ["--include", str(_inc)]
            for _exc in _a.get("exclude") or []:
                _ga += ["--exclude", str(_exc)]
            _ga += ["-e", str(_a["pattern"])]
            _ga += [str(x) for x in (_a.get("paths") or ["."])]
            _gp = _sp.run(_ga, stdout=_sp.PIPE, stderr=_sp.PIPE,
                          cwd=_xp(_a["cwd"]) if _a.get("cwd") else None)
            _res = {"ok": True, "rc": _gp.returncode,
                    "out": _S(_gp.stdout or b""),
                    "err": _S(_gp.stderr or b"")}

        elif _op == "edit":
            # 远端就地文本编辑（精确字符串替换/追加），文件内容不离开远端。
            _p = _xp(_a["path"])
            _enc = str(_a.get("encoding", "utf-8"))
            _old = str(_a.get("old", ""))
            _new = str(_a.get("new", ""))
            with open(_p, "rb") as _fh:
                _raw = _fh.read()
            _txt = _raw.decode(_enc)
            if _a.get("append"):
                _txt2, _hit, _cnt = _txt + _new, 0, 0
            else:
                _cnt = _txt.count(_old)
                if _cnt == 0:
                    raise RuntimeError("pattern not found in file: %s" % _p)
                _lim0 = int(_a.get("count", 0))
                _lim = _lim0 if _lim0 > 0 else _cnt
                _txt2 = _txt.replace(_old, _new, _lim)
                _hit = min(_lim, _cnt)
            _data = _txt2.encode(_enc)
            _st3 = _atomic_write(_p, _data, _a.get("mode"),
                                 bool(_a.get("backup")), _enc)
            _res = {"ok": True, "path": _p, "matches": _cnt if not _a.get("append") else 0,
                    "replaced": _hit, "bytes": _st3.st_size}

        elif _op == "stream_start":
            # 远端后台周期汇报：daemon 线程复用 gms.mqtt_net 推到独立 topic。
            import threading as _th
            _ss2 = globals().setdefault("_cmq_streams", {})
            _net = _find_mqtt_net()
            if _net is None:
                raise RuntimeError(
                    "remote has no reusable MQTT manager (expected gms.mqtt_net)")
            _sid = str(_a.get("sid") or ("cmq-%d" % int(_t.time() * 1000)))
            if _sid in _ss2:
                raise RuntimeError("stream sid already active: %s" % _sid)
            _cfg = {"topic": str(_a.get("topic") or "sys/device/stream"),
                    "interval": max(0.2, float(_a.get("interval", 1.0))),
                    "count": max(0, int(_a.get("count", 0))),
                    "ttl": min(max(5.0, float(_a.get("ttl", 60.0))), 1800.0),
                    "mode": "stats" if _a.get("mode") == "stats" else "cmd",
                    "cmd": _a.get("cmd"),
                    "cwd": _a.get("cwd"),
                    "frame_timeout": float(_a.get("frame_timeout", 20.0))}
            _ev = _th.Event()
            _ss2[_sid] = {"event": _ev, "cfg": _cfg, "started": _t.time()}
            _th.Thread(target=_stream_worker, args=(_sid, _cfg, _net),
                       daemon=True).start()
            _res = {"ok": True, "sid": _sid, "topic": _cfg["topic"],
                    "interval": _cfg["interval"], "ttl": _cfg["ttl"],
                    "mode": _cfg["mode"]}

        elif _op == "stream_stop":
            _sid = str(_a["sid"])
            _ctx = globals().get("_cmq_streams", {}).get(_sid)
            if _ctx is not None:
                _ctx["event"].set()
            _res = {"ok": True, "sid": _sid, "was_active": _ctx is not None}

        elif _op == "stream_list":
            _ss2 = globals().get("_cmq_streams", {})
            _res = {"ok": True, "streams": [
                {"sid": _k, "mode": _v["cfg"]["mode"], "topic": _v["cfg"]["topic"],
                 "interval": _v["cfg"]["interval"],
                 "age": round(_t.time() - _v["started"], 1)}
                for _k, _v in list(_ss2.items())]}

        elif _op == "pull_dir":
            # 整目录打包回传（替代 shell 里的 tar|base64|split 手工流）：
            # 纯标准库 os.walk + tarfile + gzip（mtime=0 确定性输出），
            # base64 承载——gzip 随机字节经 latin-1 映射会被 \uXXXX 膨胀 6 倍，
            # base64 是纯 ASCII 1:1。压缩包硬顶 1MiB：超过立即中止打包并返回
            # 结构化诊断（最大文件清单/原始字节数），绝不靠分块硬闯 broker
            # （公共 broker 实测 1000KiB 可过、1200KiB 丢包）。
            import tarfile as _tf3, gzip as _gz3, base64 as _b63
            import fnmatch as _fn3, io as _io3
            _HARD3 = 1 << 20
            _root = _o.path.normpath(_xp(_a["path"]))
            if not _o.path.isdir(_root0 := _root):
                raise NotADirectoryError(_root0)
            _pats = [str(_x) for _x in (_a.get("excludes") or []) if str(_x)]
            _topn = min(max(1, int(_a.get("top_n", 15))), 100)
            _mb = int(_a.get("max_bytes") or 0)
            if _mb <= 0 or _mb > _HARD3:
                _mb = _HARD3

            def _excluded3(_relp):
                _cs = [_c for _c in _relp.split("/") if _c not in ("", ".")]
                for _pat in _pats:
                    if _fn3.fnmatchcase(_relp, _pat):
                        return True
                    for _c in _cs:
                        if _fn3.fnmatchcase(_c, _pat):
                            return True
                return False

            class _PDIO(_io3.BytesIO):
                # 边压缩边计数，超过上限即停，避免大目录在内存里构建整个包。
                def __init__(self, limit):
                    super().__init__()
                    self.written = 0
                    self.limit = limit

                def write(self, b):
                    z = super().write(b)
                    self.written += z
                    return z

            class _PDStop(Exception):
                pass

            _entries = []
            _nfiles = _ndirs = _nlink = _nspec = _nexcl = 0
            _raw = 0
            _big = []
            for _dp, _dn3, _fns in _o.walk(_root, topdown=True, followlinks=False):
                _rb = _o.path.relpath(_dp, _root)
                _rb = "" if _rb == "." else _rb.replace(_o.sep, "/")
                _keep = []
                for _d in sorted(_dn3):
                    if _o.path.islink(_o.path.join(_dp, _d)):
                        _nlink += 1
                        continue
                    _relp = (_rb + "/" + _d) if _rb else _d
                    if _pats and _excluded3(_relp):
                        _nexcl += 1
                        continue
                    _keep.append(_d)
                _dn3[:] = _keep  # 被排除/链接目录就地剪枝，walk 不再下钻
                _entries.append((_dp, "." if not _rb else _rb, True))
                _ndirs += 1
                for _f in sorted(_fns):
                    _ap = _o.path.join(_dp, _f)
                    _relp = (_rb + "/" + _f) if _rb else _f
                    if _pats and _excluded3(_relp):
                        _nexcl += 1
                        continue
                    _m = _o.lstat(_ap).st_mode
                    if _st.S_ISLNK(_m):
                        _nlink += 1
                        continue
                    if not _st.S_ISREG(_m):
                        _nspec += 1
                        continue
                    _sz = _o.path.getsize(_ap)
                    _nfiles += 1
                    _raw += _sz
                    _big.append((_sz, _relp))
                    _entries.append((_ap, _relp, False))

            def _too_large3(_reached):
                _big.sort(key=lambda _z: _z[0], reverse=True)
                _top = _big[:_topn]
                _lines = "\n".join("    %10d  ./%s" % (_sz, _rp)
                                   for _sz, _rp in _top)
                _msg = (
                    "远程目录打包被拒绝: %r 压缩后已达 %d 字节, 超过上限 %d 字节"
                    "（压缩包硬顶 1MiB，不允许调更高）。\n"
                    "- 原始文件合计 %d 字节；文件 %d 个/目录 %d 个；排除命中 %d 项；"
                    "跳过符号链接 %d 个/特殊文件 %d 个。\n"
                    "- 本次 excludes: %s\n"
                    "- 最大的 %d 个文件（优先考虑排除它们）:\n%s\n"
                    "- 处理建议:\n"
                    "  1) 带 --exclude 重拉，例如 --exclude build --exclude out "
                    "--exclude .git --exclude __pycache__ --exclude '*.pyc'；\n"
                    "  2) MQTT 单报文实测 1000KiB 可过、1200KiB 丢包，base64 还会"
                    "膨胀 4/3，所以默认阈值只有 700KiB；大文件/大目录必须在远端"
                    "就地处理（grep/sed/tar 分流/curl），不要经报文通道回传；\n"
                    "  3) apk/图片/so/pyc 等二进制压缩率极低，应优先排除。"
                ) % (_root, _reached, _mb, _raw, _nfiles, _ndirs, _nexcl,
                     _nlink, _nspec,
                     ("[" + ", ".join(_pats) + "]") if _pats else "(无)",
                     len(_top), _lines or "    (无)")
                return {"ok": False, "reason": "too_large", "error": _msg,
                        "path": _root, "reached_bytes": _reached, "max_bytes": _mb,
                        "raw_bytes": _raw, "files": _nfiles, "dirs": _ndirs,
                        "excluded": _nexcl, "skipped_links": _nlink,
                        "skipped_special": _nspec, "excludes": _pats,
                        "largest": [{"path": _rp, "size": _sz}
                                    for _sz, _rp in _top]}

            class _GuardedReader:
                # tarfile.addfile 以 16KiB 块调 read()：每块前看压缩输出计数，
                # 越过上限即中止——即使是单个 2GB 不可压缩文件，内存也只多一块。
                def __init__(self, fh, counter):
                    self._fh = fh
                    self._c = counter

                def read(self, n=-1):
                    if self._c.written > self._c.limit:
                        raise _PDStop()
                    return self._fh.read(n)

            _io3b = _PDIO(_mb)
            _aborted = False
            try:
                with _gz3.GzipFile(filename="", mode="wb", fileobj=_io3b,
                                   compresslevel=9, mtime=0) as _gzf:
                    with _tf3.open(fileobj=_gzf, mode="w") as _tar:
                        for _ap, _arc, _isd in _entries:
                            _ti = _tar.gettarinfo(_ap, arcname=_arc)
                            if _isd:
                                _tar.addfile(_ti)
                            else:
                                with open(_ap, "rb") as _fh2:
                                    _tar.addfile(_ti, _GuardedReader(_fh2, _io3b))
                            if _io3b.written > _mb:
                                raise _PDStop()
            except _PDStop:
                _aborted = True
            if _aborted:
                _res = _too_large3(_io3b.written)
            else:
                _blob = _io3b.getvalue()
                if len(_blob) > _mb:
                    _res = _too_large3(len(_blob))
                else:
                    _res = {"ok": True, "path": _root,
                            "b64": _b63.b64encode(_blob).decode("ascii"),
                            "md5": _hl.md5(_blob).hexdigest(),
                            "arc_bytes": len(_blob), "max_bytes": _mb,
                            "raw_bytes": _raw, "files": _nfiles, "dirs": _ndirs,
                            "skipped_links": _nlink, "skipped_special": _nspec,
                            "excluded": _nexcl, "excludes": _pats}

        else:
            _res = {"ok": False, "error": "unknown op: %r" % (_op,)}

    except Exception:
        _res = {"ok": False, "error": _tb.format_exc()}
    return _j.dumps(_res, ensure_ascii=False)
_cmq_dispatch()
'''


def build_code(payload: dict) -> str:
    """把操作信封编译成远端可直接执行的 Python 代码。"""
    # json.dumps(str) 产出合法 Python 字面量；ensure_ascii=True 保证代码本体 ASCII。
    lit = json.dumps(json.dumps(payload, ensure_ascii=False))
    return _REMOTE_TEMPLATE.replace("__PAYLOAD__", lit)


# ============================ PTY 启动模板（client 下发，禁止改名） ============================
# 仿 ssh 的 PTY：服务端只 fork 一次常驻 shell（用户登录 shell），之后所有按键写入
# 同一个 PTY master，绝不在每条命令上重启 bash。uplink 帧经服务端 mqtt_net 的回调
# 路由表直达 PTY（不经过 gms.handle_message，避免 "missing code" 噪声）；downlink
# 由读线程直接 publish 到协商好的 out topic。flush_interval=0 为实时回传。
_PTY_START_TEMPLATE = r'''
def _cmq_pty_start():
    import json as _j
    _a = _j.loads(__PAYLOAD__)
    _mfd = _sfd = None
    _res = {}
    try:
        import os as _o, time as _t, threading as _th, traceback as _tb
        if _o.name != "posix":
            raise RuntimeError("PTY 只能在 POSIX 服务端建立，当前系统: %s" % _o.name)
        import pty as _pty, fcntl as _fc, termios as _te, struct as _st
        import select as _sel, subprocess as _sp, signal as _sg, queue as _qe

        # 本服务端进程的稳定身份（executor 全局命名空间跨握手持久，同一进程
        # 每次握手拿到同一个 uid）。多个持相同 key 的 server_mqtt 进程（旧
        # 机器/旧容器没关、同机起了两个）会同时应答同一个握手、各自开一个
        # PTY 订阅同一 in topic、往同一 out topic 各自从 seq=0 推流；客户端
        # 只认首个应答者，并通过上行帧里的 owner 让其他"影子 PTY"自行了断。
        _g = globals()
        _uid = _g.get("_cmq_server_uid")
        if not _uid:
            _uid = _o.urandom(6).hex()
            _g["_cmq_server_uid"] = _uid
        try:
            _host = _o.uname().nodename
        except Exception:
            _host = "?"

        _sid = str(_a["sid"])
        _in_topic = str(_a["in_topic"])
        _out_topic = str(_a["out_topic"])

        # 同进程内同 sid 幂等：网络层 req_id 已保证同一条握手代码只执行一次，
        # 这里是兜底，防止任何重复执行再开出一个往同一 out topic 推流的 PTY
        # （两条近同流会在客户端按 seq 拼出 1 列偏移的错位重影）。
        _sess_lock = _g.get("_cmq_pty_sess_lock")
        if _sess_lock is None:
            _sess_lock = _th.Lock()
            _g["_cmq_pty_sess_lock"] = _sess_lock
        _sessions = _g.setdefault("_cmq_pty_sessions", {})
        with _sess_lock:
            _cached_env = _sessions.get(_sid)
        if _cached_env is not None:
            return _cached_env

        def _find_net():
            # 复用服务端进程里现成的 MQTT 网络层（gms.mqtt_net），不新建连接。
            # 同时返回服务端实例的 handle_message —— 它是消息回调链唯一可信的
            # "链底"，用于把已被旧版本 _router 自裹成闭环的回调链一步接回真底。
            for _v in list(globals().values()):
                _mn = getattr(_v, "mqtt_net", None)
                if _mn is not None and hasattr(_mn, "publish_broadcast"):
                    _cb = getattr(_v, "handle_message", None)
                    return _mn, (_cb if callable(_cb) else None)
            return None, None

        _net, _srv_cb = _find_net()
        if _net is None:
            raise RuntimeError("服务端没有可复用的 MQTT 管理器（期望 gms.mqtt_net）")

        _rows = max(1, int(_a.get("rows", 24)))
        _cols = max(1, int(_a.get("cols", 80)))
        _frame_max = max(512, int(_a.get("frame_max", 16384)))
        _interval = max(0.0, float(_a.get("flush_interval", 0.0)))
        _ttl = min(max(60.0, float(_a.get("ttl", 43200.0))), 86400.0)
        _login = bool(_a.get("login", True))
        # 会话期内可热调的参数（客户端本地命令栏经 set 帧修改，无需重连/重开
        # PTY）。放在 dict 里供 in/out/heartbeat 三个线程共享读取。
        _live = {
            "interval": _interval,
            "ttl": _ttl,
            "heartbeat": max(0.0, float(_a.get("heartbeat", 0.0))),
        }

        _shell = _a.get("shell")
        if not _shell:
            try:
                import pwd as _pw
                _shell = _o.environ.get("SHELL") or _pw.getpwuid(_o.getuid()).pw_shell
            except Exception:
                _shell = None
            if not _shell or not _o.path.isfile(_shell):
                _shell = "/bin/bash" if _o.path.isfile("/bin/bash") else "/bin/sh"
        _term = str(_a.get("term") or _o.environ.get("TERM") or "xterm-256color")
        _cwd_warn = None
        _cwd = _a.get("cwd") or _o.path.expanduser("~")
        # cwd 不存在只是客户端给错了参数，不该让整个 PTY 起不来（更不该把
        # client 打退出）：逐级回退 HOME → /，并把实情通过回包告知客户端。
        if not _o.path.isdir(_cwd):
            _cwd_warn = "请求的 cwd 不存在或不是目录: %s" % _cwd
            _cwd = _o.path.expanduser("~")
            if not _o.path.isdir(_cwd):
                _cwd = "/"

        _mfd, _sfd = _pty.openpty()
        _fc.ioctl(_sfd, _te.TIOCSWINSZ, _st.pack("HHHH", _rows, _cols, 0, 0))
        _envv = dict(_o.environ)
        _envv["TERM"] = _term
        _argv0 = ("-" + _o.path.basename(_shell)) if _login else _shell

        # 子进程开新会话（setsid，成为会话/进程组首进程）。刻意不用
        # preexec_fn：它会强制 subprocess 走 fork+exec 老路——在大 Python
        # 进程 + CPU 限流容器上实测裸 fork ~0.4s、整次 spawn ~0.9s，是 PTY
        # 握手首包最大的一笔固定开销；start_new_session 走 posix_spawn 快
        # 路径只要 ~1ms（glibc>=2.26 由 POSIX_SPAWN_SETSID 在 exec 前完成
        # setsid；旧系统自动回退 fork+exec，语义不变只是慢些）。控制终端
        # 不再显式 TIOCSCTTY：会话首进程以 tty 为 0/1/2 启动后，交互 shell
        # （bash/dash，登录/非登录均实测）会自行抢占 slave 为控制终端，
        # tty 与 Ctrl-C 作业控制（SIGINT->cat->130）和旧路径完全一致。
        _proc = _sp.Popen(
            [_argv0], executable=_shell,
            stdin=_sfd, stdout=_sfd, stderr=_sfd,
            cwd=_cwd, env=_envv, close_fds=True,
            start_new_session=True)
        try:
            _o.close(_sfd)
        except OSError:
            pass
        _sfd = None

        _st0 = {"reason": "exit"}
        _end = _th.Event()

        def _kill():
            if _proc.poll() is not None:
                _end.set()
                return
            try:
                _o.killpg(_o.getpgid(_proc.pid), _sg.SIGHUP)
            except Exception:
                try:
                    _proc.terminate()
                except Exception:
                    pass
            _t.sleep(0.5)
            if _proc.poll() is None:
                try:
                    _o.killpg(_o.getpgid(_proc.pid), _sg.SIGKILL)
                except Exception:
                    try:
                        _proc.kill()
                    except Exception:
                        pass
            _end.set()

        def _flush(_seq, _data):
            try:
                _net.publish_broadcast(
                    _out_topic,
                    {"pty": _sid, "seq": _seq, "d": _data.decode("latin-1"),
                     "owner": _uid})
            except Exception:
                pass

        # 递归防线：正常链路深度恒为 1（router → 链底）。阈值兜底是为了防止
        # 历史坏版本留下的自裹回调链再次把分发线程炸穿——宁可丢一帧也不递归。
        _rlocal = _th.local()

        def _router(_topic, _data, _broker):
            # PTY 专用 topic：入队后不再下传给原 RPC 回调；其余消息原样透传。
            if getattr(_rlocal, "depth", 0) >= 8:
                return None
            _rlocal.depth = getattr(_rlocal, "depth", 0) + 1
            try:
                _q = _net._cmq_pty_router.get(_topic)
                if _q is not None and isinstance(_data, dict):
                    _q.put(_data)
                    return None
                return _net._cmq_pty_orig(_topic, _data, _broker)
            finally:
                _rlocal.depth -= 1

        # 安装必须幂等。旧版用"router dict 是否为空"判断是否已安装，但会话
        # 结束只 pop topic、空 dict 仍残留（falsy），每次重连都把旧 router
        # 当成"原回调"再裹一层；旧 router 又动态回读 _net._cmq_pty_orig，
        # 而该属性此刻指向它自己 → 自调用闭环，任何非 PTY topic（含
        # sys/device/request）都会 RecursionError，整个服务端卡死。
        # 用独立布尔标记保证全程只安装一次，dict 与链底永不被覆盖。
        if not getattr(_net, "_cmq_pty_installed", False):
            # 链底优先用服务端实例自己的 handle_message：即使本进程已被旧版
            # 代码裹坏（_cmq_pty_orig 指向旧 router 闭环），新安装也能一步
            # 跳到真正的链底，顺手完成自愈。
            _base = _srv_cb if _srv_cb is not None else _net.message_callback
            _net._cmq_pty_orig = _base
            _net._cmq_pty_router = {}
            _net._cmq_pty_installed = True
            _net.set_on_message(_router)

        _inq = _qe.Queue()
        _net._cmq_pty_router[_in_topic] = _inq
        _net.subscribe(_in_topic)

        def _in_loop():
            # 上行帧单调闸门：同一帧会被每个 broker 各投递一次（实测 11 个
            # 连接能到 ~15 份），而不同 broker 的路径延迟抖动还会让帧乱序
            # 到达。普通 RPC 帧靠网络层 req_id 的 TTLCache 去重，PTY 帧没有
            # req_id，必须靠客户端打的 iseq 做单调检查：iseq 已放行就丢
            # ——既去掉多 broker 重复副本，也挡下乱序迟到的旧帧。后者对
            # winsz 尤其致命：拖动窗口时一串尺寸帧广播出去，慢 broker 把
            # 早先更大的尺寸晚送到，若覆盖当前尺寸，远端 PTY 会一直比本地
            # 窗口宽，pip/gradle 的 \r 进度条从此全线错位。无 iseq 的旧版
            # 客户端帧照旧放行（向后兼容）。
            _iseq_last = None
            while not _end.is_set():
                try:
                    _fr = _inq.get(timeout=0.5)
                except _qe.Empty:
                    continue
                try:
                    if not isinstance(_fr, dict) or _fr.get("pty") != _sid:
                        continue
                    _ow = _fr.get("owner")
                    if _ow is not None and _ow != _uid:
                        # 归属仲裁：客户端只认首个握手应答者，上行帧的 owner
                        # 是赢家 uid。本进程收到不匹配的帧说明自己是同时应答
                        # 的影子 PTY，立即结束会话并停止推流——否则它的输出流
                        # 与赢家那条在客户端按 seq 交错，渲染出整屏错位重影。
                        # 必须在 iseq 闸门之前判断：重复/乱序的他方帧也要杀。
                        _st0["reason"] = "claim_lost"
                        _kill()
                        return
                    _iq = _fr.get("iseq")
                    if _iq is not None:
                        _iq = int(_iq)
                        if _iseq_last is not None and _iq <= _iseq_last:
                            continue  # 重复副本或乱序迟到的旧帧（按键/旧尺寸）
                        _iseq_last = _iq
                    if _fr.get("stop") or _fr.get("end"):
                        _st0["reason"] = "stopped"
                        _kill()
                        return
                    _ss = _fr.get("set")
                    if isinstance(_ss, dict):
                        # 会话内热调时间参数（客户端本地命令栏）：只允许
                        # interval/heartbeat/ttl，且各自夹在安全区间。
                        if _ss.get("redraw"):
                            # 强制前台进程组重绘：内核只在 winsz 真正变化时
                            # 才发 SIGWINCH，同尺寸 resize 唤不醒 tmux/vim；
                            # 客户端本地菜单关闭后用它让全屏程序整屏重画，
                            # 抹掉菜单可能造成的滚屏残留。
                            try:
                                _o.killpg(_o.getpgid(_proc.pid), _sg.SIGWINCH)
                            except Exception:
                                pass
                        try:
                            if "interval" in _ss:
                                _live["interval"] = min(
                                    max(0.0, float(_ss["interval"])), 60.0)
                            if "heartbeat" in _ss:
                                _live["heartbeat"] = min(
                                    max(0.0, float(_ss["heartbeat"])), 3600.0)
                            if "ttl" in _ss:
                                _live["ttl"] = min(
                                    max(60.0, float(_ss["ttl"])), 86400.0)
                        except (TypeError, ValueError):
                            pass
                    _wz = _fr.get("winsz")
                    if _wz:
                        _r2 = max(1, int(_wz[0]))
                        _c2 = max(1, int(_wz[1]))
                        _fc.ioctl(_mfd, _te.TIOCSWINSZ,
                                  _st.pack("HHHH", _r2, _c2, 0, 0))
                    _kk = _fr.get("k")
                    if _kk:
                        _b = _kk.encode("latin-1")
                        _off = 0
                        while _off < len(_b):
                            _off += _o.write(_mfd, _b[_off:])
                except Exception:
                    pass

        def _out_loop():
            _seq = 0
            _buf = b""
            _mark = _t.time()
            _start = _t.time()
            _poll = _sel.poll()
            _poll.register(_mfd, _sel.POLLIN | _sel.POLLHUP | _sel.POLLERR)
            _reason = None
            try:
                while True:
                    _now = _t.time()
                    _ttl2 = float(_live["ttl"])
                    if _now - _start >= _ttl2:
                        _st0["reason"] = "ttl"
                        _reason = "ttl"
                        break
                    _left = max(0.05, _start + _ttl2 - _now)
                    _iv = float(_live["interval"])
                    if _iv <= 0:
                        _wait_ms = min(1000.0, _left * 1000)
                    elif _buf:
                        _wait_ms = min(
                            500.0,
                            max(10.0, (_iv - (_now - _mark)) * 1000),
                            _left * 1000)
                    else:
                        _wait_ms = min(500.0, _left * 1000)
                    _evs = _poll.poll(_wait_ms)
                    _hup = False
                    if _evs:
                        try:
                            _chunk = _o.read(_mfd, _frame_max)
                        except OSError:
                            _chunk = b""
                        if _chunk:
                            if not _buf:
                                _mark = _t.time()
                            _buf += _chunk
                        if _evs[0][1] & (_sel.POLLHUP | _sel.POLLERR):
                            _hup = True
                    if _hup:
                        while True:
                            try:
                                _c2 = _o.read(_mfd, _frame_max)
                            except OSError:
                                break
                            if not _c2:
                                break
                            _buf += _c2
                        _reason = _reason or _st0["reason"]
                        break
                    if _buf and (_iv <= 0
                                 or (_t.time() - _mark) >= _iv
                                 or len(_buf) >= _frame_max):
                        _flush(_seq, _buf)
                        _seq += 1
                        _buf = b""
                        _mark = _t.time()

                if _buf:
                    _flush(_seq, _buf)
                if _proc.poll() is None:
                    try:
                        _rc = _proc.wait(timeout=3)
                    except _sp.TimeoutExpired:
                        _kill()
                        try:
                            _rc = _proc.wait(timeout=3)
                        except Exception:
                            _rc = -1
                else:
                    _rc = _proc.returncode
                _end.set()
                try:
                    _net._cmq_pty_router.pop(_in_topic, None)
                except Exception:
                    pass
                try:
                    with _sess_lock:
                        _sessions.pop(_sid, None)
                except Exception:
                    pass
                try:
                    _net.publish_broadcast(
                        _out_topic,
                        {"pty": _sid, "end": True,
                         "reason": _reason, "rc": _rc, "owner": _uid})
                except Exception:
                    pass
            finally:
                try:
                    _o.close(_mfd)
                except OSError:
                    pass

        _th.Thread(target=_in_loop, name="pty-in", daemon=True).start()
        _th.Thread(target=_out_loop, name="pty-out", daemon=True).start()

        # 心跳：服务端进程活着就周期发一帧。客户端靠它区分"shell 没输出"
        # 和"服务器已死"——否则 broker 仍在线、服务端进程被杀时，end 帧永远
        # 发不出来，客户端会无限干等（按键全进黑洞）。间隔可经 set 帧热调，
        # 所以用 0.5s 粒度的调度循环而不是一次性 wait(heartbeat)。
        def _hb_loop():
            _next = _t.time()
            while not _end.wait(0.5):
                _h = float(_live.get("heartbeat", 0.0))
                if _h <= 0.0:
                    continue
                _now = _t.time()
                if _now < _next:
                    continue
                _next = _now + _h
                try:
                    _net.publish_broadcast(
                        _out_topic,
                        {"pty": _sid, "hb": int(_t.time() * 1000),
                         "owner": _uid})
                except Exception:
                    pass

        # 线程恒启动：heartbeat=0 时空转，set 帧热调成 >0 后立刻能发心跳
        _th.Thread(target=_hb_loop, name="pty-hb", daemon=True).start()

        _res = {"ok": True, "sid": _sid, "owner": _uid, "host": _host,
                "heartbeat": float(_live["heartbeat"]),
                "in_topic": _in_topic, "out_topic": _out_topic,
                "shell": _shell, "pid": _proc.pid, "term": _term,
                "rows": _rows, "cols": _cols, "cwd": _cwd,
                "cwd_warning": _cwd_warn,
                "flush_interval": _interval, "ttl": _ttl, "login": _login}
        _env_json = _j.dumps(_res, ensure_ascii=False)
        with _sess_lock:
            _sessions[_sid] = _env_json
    except Exception:
        for _fd in (_mfd, _sfd):
            try:
                if _fd is not None:
                    _o.close(_fd)
            except Exception:
                pass
        _res = {"ok": False, "error": _tb.format_exc()}
        _env_json = _j.dumps(_res, ensure_ascii=False)
    return _env_json
_cmq_pty_start()
'''


def build_pty_start_code(payload: dict) -> str:
    """把 PTY 启动信封编译成远端可直接执行的自包含 Python 代码。"""
    lit = json.dumps(json.dumps(payload, ensure_ascii=False))
    return _PTY_START_TEMPLATE.replace("__PAYLOAD__", lit)


def bytes_to_wire(b: bytes) -> str:
    """客户端字节 -> 报文字符串（latin-1 无损映射）。"""
    return b.decode("latin-1")


def wire_to_bytes(s) -> bytes:
    return (s or "").encode("latin-1")


def _wire_len(s: str) -> int:
    """预估该字符串经网络层 json.dumps(ensure_ascii=True) 转义后的在线字节数。

    可打印 ASCII（除引号/反斜杠）1 字节；控制字符与非 ASCII 都是 \\uXXXX 6 字节，
    引号/反斜杠转义后 2 字节。
    """
    total = 0
    for ch in s:
        o = ord(ch)
        if ch == '"' or ch == "\\":
            total += 2
        elif 0x20 <= o <= 0x7E:
            total += 1
        else:
            total += 6
    return total


def _iter_wire_chunks(s: str, budget: int = WIRE_BUDGET):
    """按在线尺寸预算切字符串，产出 (字节偏移, 字节偏移, 子串)。

    在线长度随 j 单调，用二分找最大可行 j（O(log n) 次长度计算）；
    绝不能逐字符递减全量重算——那是 O(n^2)，200KB 随机二进制会卡数十分钟。
    对 latin-1 映射串字符偏移 == 字节偏移（1:1）。
    """
    n = len(s)
    i = 0
    while i < n:
        lo, hi, best = i + 1, min(i + budget, n), i
        while lo <= hi:
            mid = (lo + hi) // 2
            if _wire_len(s[i:mid]) <= budget:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        yield i, best, s[i:best]
        i = best


# ============================ pull_dir 本地解压 ============================

def _is_within(base: str, target: str) -> bool:
    base = os.path.abspath(base)
    target = os.path.abspath(target)
    return base == target or target.startswith(base.rstrip(os.sep) + os.sep)


def _validate_tar_member(member: tarfile.TarInfo) -> str:
    """拒绝一切逃逸/非普通条目：绝对路径、盘符、.. 穿越、硬/软链接、设备文件。

    返回校验过的 posix 风格成员名。pull_dir 远端只写目录/普通文件，这里是
    防"回包在传输途中被替换"的纵深防线。
    """
    name = member.name.replace("\\", "/")
    if not name or name.startswith("/") or posixpath.isabs(name):
        raise RemoteError(f"非法 tar 条目（绝对路径）: {member.name!r}")
    if ":" in name.split("/")[0]:  # Windows 盘符 C:
        raise RemoteError(f"非法 tar 条目（盘符）: {member.name!r}")
    parts = [p for p in name.split("/") if p]
    if any(p == ".." for p in parts):
        raise RemoteError(f"非法 tar 条目（.. 穿越）: {member.name!r}")
    if member.issym() or member.islnk():
        raise RemoteError(f"非法 tar 条目（链接）: {member.name!r}")
    if not (member.isdir() or member.isfile()):
        raise RemoteError(f"非法 tar 条目（特殊文件）: {member.name!r}")
    return "/".join(parts) + ("/" if member.isdir() else "")


def safe_extract_tar(archive: bytes, dest: str) -> int:
    """把 pull_dir 回来的 tar.gz 字节安全解压到 dest，返回条目数。"""
    dest = os.path.abspath(dest)
    os.makedirs(dest, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tf:
        members = []
        for m in tf.getmembers():
            _validate_tar_member(m)
            target = os.path.join(dest, *m.name.split("/"))
            if not _is_within(dest, target):
                raise RemoteError(f"非法 tar 条目（越界）: {m.name!r}")
            members.append(m)
        # 3.12+ 官方 data filter 再兜一层（uid/setuid/特殊设备等）；
        # 旧版解释器没有 filter 参数，条目已在上面手工校验过。
        try:
            tf.extractall(dest, members=members, filter="data")
        except TypeError:
            tf.extractall(dest, members=members)
    return len(members)


# ============================ 异常 / 结果对象 ============================

class RemoteError(RuntimeError):
    """所有远端客户端异常的基类。"""


class RemoteTimeout(RemoteError):
    """请求超时（通道无回包）。"""


class RemoteRpcError(RemoteError):
    """远端 Python 执行失败（语法/未捕获异常）。"""

    def __init__(self, message, response=None):
        super().__init__(message)
        self.response = response


class RemoteOpError(RemoteError):
    """远端 op 逻辑返回失败。"""

    def __init__(self, message, env=None, response=None):
        super().__init__(message)
        self.env = env
        self.response = response


class TransferTooLarge(RemoteError):
    """试图经报文通道传输超过 MAX_TRANSFER 的文件/数据。"""


class DirArchiveTooLarge(TransferTooLarge):
    """pull_dir 压缩包超过允许大小（远端强制 1 MiB 硬顶）。

    ``env`` 是远端返回的结构化诊断：raw_bytes/files/dirs/excluded/
    skipped_links/skipped_special/excludes/largest[{path,size}]，
    ``str(exc)`` 已是面向调用方 AI 的完整中文说明与建议。
    """

    def __init__(self, message, env=None):
        super().__init__(message)
        self.env = env or {}


class CmdResult:
    """shell 命令结果。stdout/stderr 始终是 bytes。"""

    __slots__ = ("rc", "stdout", "stderr", "duration", "timed_out")

    def __init__(self, rc, stdout, stderr, duration, timed_out):
        self.rc = rc
        self.stdout = stdout
        self.stderr = stderr
        self.duration = duration
        self.timed_out = timed_out

    @property
    def ok(self) -> bool:
        return self.rc == 0 and not self.timed_out

    @property
    def text(self) -> str:
        return self.stdout.decode("utf-8", "replace")

    @property
    def err_text(self) -> str:
        return self.stderr.decode("utf-8", "replace")

    def check(self) -> "CmdResult":
        if not self.ok:
            raise RemoteError(
                f"remote command failed (rc={self.rc}, timed_out={self.timed_out}):\n"
                f"{self.err_text}")
        return self

    def __repr__(self):
        return (f"CmdResult(rc={self.rc}, {len(self.stdout)}B out, "
                f"{len(self.stderr)}B err, {self.duration}s, timed_out={self.timed_out})")


# ============================ 与传输无关的远端操作封装 ============================

class RemoteShell:
    """远端 Shell / 文件操作客户端。网络层由 ``transport`` 注入。

    Parameters
    ----------
    transport : Transport
        一问一答代码通道（MQTT 版见 cmd_client_mqtt.MqttTransport）。
    timeout : 默认问答等待秒数
    max_transfer : 经报文通道传输单文件/数据的硬上限（默认 1 MiB，实测 broker 上限）
    wire_budget : 单条报文在线尺寸目标（默认 128 KiB）

    大文件原则
    ----------
    超过 ``max_transfer`` 的读写/上传下载默认直接抛 :class:`TransferTooLarge`。
    请改为在远端就地处理：:meth:`grep` / :meth:`edit_replace` /
    ``run("sed -i ...")`` / ``run("curl -O ..." )`` / ``run("tar ...")`` /
    :meth:`apt_install` / :meth:`pip_install`，或让远端之间直接传文件。
    """

    def __init__(self, transport: Transport, timeout: float = DEFAULT_TIMEOUT,
                 max_transfer: int = MAX_TRANSFER, wire_budget: int = WIRE_BUDGET):
        if not isinstance(transport, Transport):
            raise TypeError("transport 必须是 remote_cmd.Transport 实例")
        self.tr = transport
        self.timeout = timeout
        self.max_transfer = max_transfer
        self.wire_budget = wire_budget
        self._info = None
        self._cwd = None
        self._lock = threading.RLock()

    # ---------- 底层问答 ----------

    def _call(self, op, req_timeout=None, **args) -> dict:
        payload = {"op": op}
        for k, v in args.items():
            if v is not None:
                payload[k] = v
        resp = self.tr.request(build_code(payload), req_timeout or self.timeout)
        if not resp:
            raise RemoteTimeout(f"请求超时无回包: op={op}")
        if not resp.get("ok"):
            raise RemoteRpcError(resp.get("error", "remote rpc failed"), resp)
        try:
            env = json.loads(resp.get("r") or "")
        except (ValueError, TypeError) as e:
            raise RemoteRpcError(f"无法解析远端回包 r={resp.get('r')!r}: {e}", resp)
        if not env.get("ok"):
            raise RemoteOpError(env.get("error", "remote op failed"), env, resp)
        return env

    def _guard(self, name, size, allow_large):
        if size > self.max_transfer and not allow_large:
            raise TransferTooLarge(
                f"{name} 大小 {size} 字节超过报文通道上限 {self.max_transfer} 字节 "
                f"(1MiB)：MQTT broker 实测 1.2MiB 即丢包。请改为远端就地处理——"
                f"grep/edit_replace/sed/curl/tar/apt 等都在远端执行；"
                f"确需经本通道传输时显式传 allow_large=True 自行承担风险。")

    def close(self):
        self.tr.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ---------- 环境 ----------

    def info(self, refresh=False) -> dict:
        if self._info is None or refresh:
            with self._lock:
                self._info = self._call("info")
                self._cwd = self._info["cwd"]
        return self._info

    @property
    def cwd(self) -> str:
        if self._cwd is None:
            self.info()
        return self._cwd

    def cd(self, path) -> str:
        st = self._call("stat", path=str(path))
        if not st.get("isdir"):
            raise RemoteError(f"not a directory: {st.get('path')}")
        self._cwd = st["path"]
        return self._cwd

    # ---------- 命令（像 SSH 一样用） ----------

    def run(self, cmd, cwd=None, timeout=None, env=None, shell=True, check=False) -> CmdResult:
        """执行远端命令（默认 /bin/sh -c）。

        timeout 为远端命令超时秒数，到时 kill 整个进程组；本地问答等待自动 +15s 余量。
        """
        args = {"cwd": cwd or self._cwd, "env": env, "inline_max": INLINE_MAX}
        req_to = self.timeout
        if timeout is not None:
            args["timeout"] = timeout
            req_to = max(self.timeout, float(timeout) + 15)
        if shell:
            args["cmd"] = str(cmd)
        else:
            args["args"] = list(cmd)
        env_resp = self._call("run", req_to, **args)
        result = CmdResult(
            rc=env_resp.get("rc", -1),
            stdout=self._collect(env_resp, "out"),
            stderr=self._collect(env_resp, "err"),
            duration=env_resp.get("dur", 0.0),
            timed_out=env_resp.get("timed_out", False),
        )
        if check:
            result.check()
        return result

    def _collect(self, env, tag) -> bytes:
        sid = env.get(tag + "_id")
        if sid:
            return self._fetch_stash(sid)
        return wire_to_bytes(env.get(tag))

    def _fetch_stash(self, sid) -> bytes:
        parts = []
        off = 0
        try:
            while True:
                r = self._call("fetch", id=sid, off=off, n=self.wire_budget)
                parts.append(wire_to_bytes(r["data"]))
                off += r["n"]
                if r["eof"]:
                    break
                if r["n"] == 0:
                    # n=0 却不宣告 eof（远端暂存被截断/实现异常）：继续拉
                    # 只会无限重放同一帧，必须显式失败让 finally 去 drop。
                    raise RemoteError(
                        f"远端暂存读取停滞: id={sid} off={off} 返回 n=0 且未 eof")
        finally:
            try:
                self._call("drop", id=sid)
            except Exception:
                pass
        return b"".join(parts)

    def py(self, code, timeout=None) -> dict:
        """逃生舱：直接发任意 Python 代码，返回原始回包 dict。"""
        resp = self.tr.request(code, timeout or self.timeout)
        if not resp:
            raise RemoteTimeout("python 请求超时无回包")
        return resp

    # ---------- 远端就地操作（首选，文件内容不离开远端） ----------

    def grep(self, pattern, paths=(".",), recursive=True, ignore_case=False,
             fixed=False, include=None, exclude=None, cwd=None,
             max_matches=0) -> list[dict]:
        """在远端 grep，返回 ``[{path, lineno, text}, ...]``，不拉文件。

        默认 ERE；fixed=True 按字面量（grep -F）。rc=1（无匹配）正常返回空列表。
        max_matches>0 时只保留前 N 条（客户端截断）。
        """
        r = self._call("grep", pattern=str(pattern),
                       paths=[str(p) for p in (paths if isinstance(paths, (list, tuple))
                                               else (paths,))],
                       recursive=recursive, ignore_case=ignore_case, fixed=fixed,
                       include=include, exclude=exclude, cwd=cwd or self._cwd)
        if r.get("rc", 0) > 1 and r.get("err"):
            raise RemoteOpError(r["err"], r)
        matches = []
        for line in wire_to_bytes(r.get("out")).decode("utf-8", "replace").splitlines():
            # -nH 形如 path:lineno:text（文件名含冒号是已知边界情况）
            first = line.find(":")
            second = line.find(":", first + 1) if first >= 0 else -1
            if first < 0 or second < 0:
                continue
            path = line[:first]
            num_txt = line[first + 1:second]
            if not num_txt.isdigit():
                continue
            matches.append({"path": path, "lineno": int(num_txt), "text": line[second + 1:]})
            if max_matches and len(matches) >= max_matches:
                break
        return matches

    def edit_replace(self, path, old, new, count=1, backup=True,
                     encoding="utf-8", cwd=None) -> dict:
        """远端就地精确字符串替换（sed 的安全替代，无需引号地狱）。

        old 必须存在，否则报错；count=0 替换全部；backup=True 先留 .bak。
        返回 {path, matches, replaced, bytes}。
        """
        return self._call("edit", path=str(path), old=old, new=new, count=count,
                          backup=backup, encoding=encoding, append=False, cwd=cwd)

    def append_text(self, path, text, backup=False, encoding="utf-8") -> dict:
        """远端就地追加文本（文件内容不回传）。"""
        return self._call("edit", path=str(path), old="", new=text,
                          backup=backup, encoding=encoding, append=True)

    def install_packages(self, packages, manager=None, update=None,
                         timeout=900) -> CmdResult:
        """在**服务端 shell 环境**里用本机包管理器安装（客户端不硬编码任何发行版）。

        - ``manager=None``：在服务端按 apt-get → apk → dnf → yum → microdnf
          → zypper → pacman → brew → pkg 的顺序探测可用者；显式指定则强制使用。
        - ``update=None``：apt/dnf/yum/pkg 先刷新索引；传 ``False`` 跳过。
        """
        if isinstance(packages, str):
            packages = [packages]
        pkgs = " ".join(shlex.quote(str(p)) for p in packages)
        script = f"""set -e
PKS={shlex.quote(pkgs)}
FORCE_MGR={shlex.quote(str(manager or ""))}
DO_UPDATE={shlex.quote("" if update is None or update is True else "0")}
if [ -n "$FORCE_MGR" ]; then
  M="$FORCE_MGR"
  command -v "$M" >/dev/null 2>&1 || {{ echo "package manager not found: $M" >&2; exit 127; }}
else
  M=""
  for c in apt-get apk dnf yum microdnf zypper pacman brew pkg; do
    if command -v "$c" >/dev/null 2>&1; then M="$c"; break; fi
  done
fi
[ -n "$M" ] || {{ echo "no supported package manager on remote" >&2; exit 127; }}
case "$M" in
  apt-get)
    export DEBIAN_FRONTEND=noninteractive
    [ "$DO_UPDATE" = "0" ] || apt-get update -y
    exec apt-get install -y --no-install-recommends $PKS ;;
  apk) exec apk add $PKS ;;
  dnf|yum)
    [ "$DO_UPDATE" = "0" ] || $M makecache fast >/dev/null 2>&1 || true
    exec $M install -y $PKS ;;
  microdnf) exec microdnf install -y $PKS ;;
  zypper) exec zypper --non-interactive install $PKS ;;
  pacman) exec pacman -S --noconfirm --needed $PKS ;;
  brew) exec brew install $PKS ;;
  pkg)
    export ASSUME_ALWAYS_YES=yes
    [ "$DO_UPDATE" = "0" ] || pkg update
    exec pkg install -y $PKS ;;
  *) echo "unsupported package manager: $M" >&2; exit 2 ;;
esac
"""
        return self.run(script, timeout=timeout, check=True)

    def apt_install(self, packages, update=True, timeout=600) -> CmdResult:
        """兼容旧接口：显式走服务端 apt-get。通用安装请直接用 install_packages。"""
        return self.install_packages(packages, manager="apt-get",
                                     update=update, timeout=timeout)

    def pip_install(self, packages, args=(), timeout=600, python=None) -> CmdResult:
        """在**服务端 shell 环境**里 pip 安装；不假设解释器名，也不盲目加
        --break-system-packages。

        - 服务端自行探测 python3/python（或用 ``python=`` 指定）；
        - 检测 PEP 668 EXTERNALLY-MANAGED 标记，仅在存在时加
          ``--break-system-packages``；若仍因 externally-managed 失败再自动重试一次；
        - ``args`` 额外透传（旧调用里的 --break-system-packages 不会有害）。
        """
        if isinstance(packages, str):
            packages = [packages]
        pkgs = " ".join(shlex.quote(str(p)) for p in packages)
        extra = " ".join(shlex.quote(str(a)) for a in (args or ()))
        script = f"""set -e
PKS={shlex.quote(pkgs)}
EXTRA={shlex.quote(extra)}
PY={shlex.quote(str(python or ""))}
if [ -z "$PY" ]; then
  for c in python3 python; do
    if command -v "$c" >/dev/null 2>&1; then PY="$c"; break; fi
  done
fi
[ -n "$PY" ] || {{ echo "no python interpreter on remote" >&2; exit 127; }}
BRK=""
if "$PY" -c 'import glob, os, sysconfig; p = sysconfig.get_path("stdlib"); sys.exit(0 if glob.glob(os.path.join(p, "EXTERNALLY-MANAGED")) else 1)' 2>/dev/null; then
  BRK="--break-system-packages"
fi
if ! "$PY" -m pip install --no-cache-dir $BRK $EXTRA $PKS; then
  if [ "$BRK" = "--break-system-packages" ]; then exit 1; fi
  exec "$PY" -m pip install --no-cache-dir --break-system-packages $EXTRA $PKS
fi
"""
        return self.run(script, timeout=timeout, check=True)

    def which(self, name) -> str | None:
        r = self.run("command -v -- %s || true" % shlex.quote(str(name)))
        p = r.text.strip()
        return p or None

    def tmux_capture_pane(self, session=0, max_lines=9999, reverse=False,
                     socket="", capture_args="-J", timeout=None) -> str:
        """抓取远端 tmux 窗格内容（capture-pane + show-buffer），返回文本。

        参数语义与 qgb.U.tmux_capture_pane 对齐：
        - ``session=0``：目标窗格；``0``/``None``/``""`` 表示 tmux 默认目标；
          支持 ``'sess'`` / ``'sess:win'`` / ``'sess:win.pane'``（含空格也可，
          自动 shell 引号）。
        - ``max_lines=9999``：向上回溯行数（capture-pane ``-S -N``），负值夹到 0。
        - ``reverse=True``：行序倒转（最新一行在最上，本地做，不发额外请求）。
        - ``socket``：tmux ``-S`` 套接字路径（多 server / 非默认 socket 时用）。
        - ``capture_args='-J'``：原样透传给 capture-pane 的额外参数
          （``-J`` 合并折行；要保留 ANSI 颜色可加 ``-e``）。
        远端先 ``unset TMUX``，在 tmux 会话内调用也不会被拒。
        """
        sock = ("-S " + shlex.quote(str(socket).strip())) if socket else ""
        sess = "" if session in (None, "") else str(session).strip()
        tgt = ("-t " + shlex.quote(sess)) if sess else ""
        n = max(0, int(max_lines))
        script = ("unset TMUX; "
                  f"tmux {sock} capture-pane -S -{n} {tgt} {capture_args} "
                  f"&& tmux {sock} show-buffer")
        out = self.run(script, timeout=timeout, check=True).text
        if reverse:
            lines = out.split("\n")
            lines.reverse()
            out = "\n".join(lines)
        return out

    # ---------- 周期汇报 / 持续输出（像 ssh 里跑 top，但是订阅推送模型） ----------

    def stream(self, cmd=None, interval=1.0, count=0,
               topic=DEFAULT_STREAM_TOPIC, ttl=60.0, stats=False,
               cwd=None, frame_timeout=20.0, on_frame=None,
               stop_event=None, req_timeout=30.0) -> int:
        """让远端后台线程周期执行/采集并推送到 topic，本地订阅实时收帧。

        - ``stats=True``：远端纯标准库读 /proc 采 CPU%/内存/load/top8 进程，
          每帧约 1KB，**容器里没装 top 也能用**；payload 为 stats dict。
        - ``stats=False``：周期 shell 执行 ``cmd``（如 ``'top -b -n 1'``），
          payload 为 ``{"rc", "out": bytes, "err": bytes, "frame_error"}``。
        - ``count=0`` 持续到 ``ttl`` 或 KeyboardInterrupt；退出时自动 stream_stop。
        - ``on_frame(seq:int, payload, raw:dict)``：每帧（已按 sid/seq 去重）。
        - ``stop_event``：外部 threading.Event，set 即优雅停止。
        返回实际收到的帧数。要求 transport 实现 stream_subscribe/unsubscribe。
        """
        if not callable(getattr(self.tr, "stream_subscribe", None)):
            raise RemoteError("当前 transport 不支持推送订阅（缺 stream_subscribe）")
        sid = "cmq-%d-%s" % (int(time.time() * 1000), os.urandom(2).hex())
        state = {"n": 0, "end": None}
        seen = set()
        wake = threading.Event()
        lock = threading.Lock()

        def _handler(data):
            if not isinstance(data, dict) or data.get("stream") != sid:
                return
            if data.get("end"):
                state["end"] = data.get("reason")
                wake.set()
                return
            key = ("f", data.get("seq", 0))
            with lock:
                if key in seen:
                    return  # 同一帧被多个 broker 重复送达
                seen.add(key)
                state["n"] += 1
                n = state["n"]
            if stats or "stats" in data:
                payload = data.get("stats") or {}
            else:
                payload = {"rc": data.get("rc"),
                           "out": wire_to_bytes(data.get("out")),
                           "err": wire_to_bytes(data.get("err")),
                           "frame_error": data.get("frame_error")}
            if on_frame is not None:
                try:
                    on_frame(n, payload, data)
                except Exception:
                    pass
            wake.set()

        self.tr.stream_subscribe(topic, _handler)
        try:
            r = self._call("stream_start", req_timeout=req_timeout, sid=sid,
                           topic=topic, interval=interval, count=count, ttl=ttl,
                           mode=("stats" if stats else "cmd"), cmd=cmd,
                           cwd=cwd or self._cwd, frame_timeout=frame_timeout)
            deadline = time.time() + min(float(r.get("ttl", ttl)) + 15.0, 1830.0)
            while True:
                if state["end"] is not None or (count and state["n"] >= count):
                    break
                if stop_event is not None and stop_event.is_set():
                    break
                if time.time() > deadline:
                    break
                wake.wait(0.5)
                wake.clear()
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self._call("stream_stop", req_timeout=10.0, sid=sid)
            except Exception:
                pass
            self.tr.stream_unsubscribe(topic, _handler)
        return state["n"]

    def monitor(self, on_frame, interval=1.0, count=0, ttl=60.0, **ka) -> int:
        """stats 模式便捷封装：``on_frame(seq, stats:dict, raw)``，返回帧数。"""
        return self.stream(stats=True, interval=interval, count=count,
                           ttl=ttl, on_frame=on_frame, **ka)

    def stream_list(self, req_timeout=20.0) -> list:
        """列出远端仍在运行的汇报流（诊断用）。"""
        return self._call("stream_list", req_timeout=req_timeout).get("streams", [])

    # ---------- 文件传输（仅用于 ≤1MiB 的小文件！） ----------

    def read(self, path, allow_large=False) -> bytes:
        """读远端文件全文（自动分块）。超过 1MiB 默认拒绝——大文件请在远端 grep/tail。"""
        parts = []
        total = None
        off = 0
        while True:
            r = self._call("read", path=str(path), off=off, n=self.wire_budget)
            total = r["size"]
            if off == 0:
                self._guard(f"读取 {path}", total, allow_large)
            parts.append(wire_to_bytes(r["data"]))
            off += r["n"]
            if r["eof"]:
                break
            if r["n"] == 0:
                # 文件可能在读取间隔被截断到当前 offset，或服务端异常：
                # n=0 且不 eof 时继续循环永不退出，显式失败。
                raise RemoteError(
                    f"远端读取停滞: {path} off={off} 返回 n=0 且未 eof")
        return b"".join(parts)

    def read_text(self, path, encoding="utf-8", allow_large=False) -> str:
        return self.read(path, allow_large=allow_large).decode(encoding)

    def write(self, path, data, mode=None, backup=False, allow_large=False) -> dict:
        """把 bytes/str 原子写入远端文件（临时文件 + replace；默认保留原权限）。"""
        if isinstance(data, str):
            data = data.encode("utf-8")
        data = bytes(data)
        self._guard(f"写入 {path}", len(data), allow_large)
        part = self._call("push_init", path=str(path))["part"]
        try:
            r = None
            if data:
                mapped = bytes_to_wire(data)
                for off, _j, chunk in _iter_wire_chunks(mapped, self.wire_budget):
                    last = off + len(chunk) >= len(mapped)
                    ka = {"part": part, "off": off, "data": chunk, "last": last}
                    if last:
                        ka.update(path=str(path), mode=mode, backup=backup)
                    r = self._call("push_data", **ka)
            else:
                r = self._call("push_data", part=part, off=0, data="",
                               last=True, path=str(path), mode=mode, backup=backup)
            return r
        except Exception:
            try:
                self._call("push_abort", part=part)
            except Exception:
                pass
            raise

    def write_text(self, path, text, encoding="utf-8", mode=None, backup=False,
                   allow_large=False) -> dict:
        return self.write(path, text.encode(encoding), mode=mode, backup=backup,
                          allow_large=allow_large)

    def download(self, remote, local, verify=True, allow_large=False, progress=None) -> dict:
        """远端小文件下载到本地（分块 + 临时文件替换 + sha256 校验）。"""
        remote = str(remote)
        local = os.path.abspath(str(local))
        if os.path.isdir(local):
            local = os.path.join(local, posixpath.basename(remote.rstrip("/")) or "download")
        st = self._call("stat", path=remote)
        self._guard(f"下载 {remote}", st["size"], allow_large)
        os.makedirs(os.path.dirname(local) or ".", exist_ok=True)
        tmp = local + ".cmqpart"
        got = 0
        with open(tmp, "wb") as fh:
            off = 0
            while True:
                r = self._call("read", path=remote, off=off, n=self.wire_budget)
                fh.write(wire_to_bytes(r["data"]))
                off += r["n"]
                got = off
                if progress:
                    progress(got, r["size"])
                if r["eof"]:
                    total = r["size"]
                    break
                if r["n"] == 0:
                    # 与 read/_fetch_stash 同款停滞保护：n=0 且不 eof 时
                    # 无限循环会把本地 .cmqpart 写个不停，必须立即失败。
                    raise RemoteError(
                        f"远端下载停滞: {remote} off={off} 返回 n=0 且未 eof")
        os.replace(tmp, local)
        digest = None
        if verify:
            import hashlib
            digest = self._call("hash", path=remote)["digest"]
            hh = hashlib.sha256()
            with open(local, "rb") as fh:
                for blk in iter(lambda: fh.read(1 << 20), b""):
                    hh.update(blk)
            if hh.hexdigest() != digest:
                raise RemoteError(f"下载校验失败: {remote} sha256 不一致")
        return {"local": local, "bytes": got, "remote_size": total, "sha256": digest}

    def upload(self, local, remote, mode=None, backup=False, allow_large=False,
               progress=None) -> dict:
        """本地小文件分块上传到远端（sha256 校验）。"""
        local = os.path.abspath(str(local))
        remote = str(remote)
        size = os.path.getsize(local)
        self._guard(f"上传 {local}", size, allow_large)
        part = self._call("push_init", path=remote)["part"]
        r = None
        try:
            with open(local, "rb") as fh:
                data = fh.read()
            if not data:
                r = self._call("push_data", part=part, off=0, data="",
                               last=True, path=remote, mode=mode, backup=backup)
            else:
                mapped = bytes_to_wire(data)
                off = 0
                for off, _j, chunk in _iter_wire_chunks(mapped, self.wire_budget):
                    last = off + len(chunk) >= len(mapped)
                    ka = {"part": part, "off": off, "data": chunk, "last": last}
                    if last:
                        ka.update(path=remote, mode=mode, backup=backup)
                    r = self._call("push_data", **ka)
                    if progress:
                        progress(min(off + len(chunk), size), size)
        except Exception:
            try:
                self._call("push_abort", part=part)
            except Exception:
                pass
            raise
        if r.get("bytes") != size:
            raise RemoteError(f"上传大小不一致: local={size} remote={r.get('bytes')}")
        return r

    # ---------- 整目录打包拉取（压缩包硬顶 1 MiB） ----------

    @staticmethod
    def _normalize_dir_max(max_bytes) -> int:
        if max_bytes is None:
            return DEFAULT_DIR_ARCHIVE_MAX
        mb = int(max_bytes)
        if mb <= 0:
            raise ValueError("max_bytes 必须是正整数（字节）")
        if mb > MAX_DIR_ARCHIVE:
            raise DirArchiveTooLarge(
                f"请求的压缩包上限 {mb} 字节超过硬顶 {MAX_DIR_ARCHIVE} 字节（1MiB）："
                "公共 broker 单报文实测 1000KiB 可过、1200KiB 丢包，压缩包还要 "
                "base64 膨胀 4/3，默认阈值仅 700KiB。请改用 --exclude 排除构建产物"
                "/依赖目录，或在远端就地处理大文件，不要把超 1MiB 的包经 MQTT 回传。")
        return mb

    def pull_dir_bytes(self, remote, excludes=(), max_bytes=None) -> "tuple[bytes, dict]":
        """远端整目录打包（纯标准库 tar.gz），返回 ``(压缩包 bytes, 元信息)``。

        - ``excludes``：fnmatch 模式，对**任意层级的目录/文件名组件**与相对路径
          匹配（如 ``"build"``、``"__pycache__"``、``"*.pyc"``），目录命中即剪枝；
        - 压缩包大小在远端强制：``max_bytes`` 默认 700 KiB，硬顶 1 MiB，超限抛
          :class:`DirArchiveTooLarge`，异常消息自带最大文件清单与排除建议；
        - 回包经 md5 + arc_bytes 双校验；符号链接/特殊文件远端一律跳过。
        """
        import hashlib
        mb = self._normalize_dir_max(max_bytes)
        try:
            env = self._call(
                "pull_dir", path=str(remote),
                excludes=[str(x) for x in excludes], max_bytes=mb)
        except RemoteOpError as e:
            if (e.env or {}).get("reason") == "too_large":
                raise DirArchiveTooLarge(str(e), e.env) from e
            raise
        try:
            blob = base64.b64decode(env["b64"], validate=True)
        except Exception as e:
            raise RemoteError(f"pull_dir 回包 base64 解码失败: {e}") from e
        if len(blob) != int(env.get("arc_bytes", -1)):
            raise RemoteError(
                f"pull_dir 大小不一致: 本地解码 {len(blob)} != 远端 arc_bytes "
                f"{env.get('arc_bytes')}（可能报文被截断/污染，重试或缩小范围）")
        if hashlib.md5(blob).hexdigest() != env.get("md5"):
            raise RemoteError(
                "pull_dir 校验失败: 压缩包 md5 与远端不一致（报文可能损坏，重试）")
        return blob, env

    def pull_dir(self, remote, local, excludes=(), max_bytes=None) -> dict:
        """整目录拉取并安全解压到 ``local``（不存在自动建）。

        返回元信息：local/files/dirs/arc_bytes/raw_bytes/md5/excluded/
        skipped_links/skipped_special/excludes。压缩包超限时抛
        :class:`DirArchiveTooLarge`（内含面向 AI 的完整诊断与建议）。
        """
        blob, env = self.pull_dir_bytes(remote, excludes=excludes, max_bytes=max_bytes)
        local_abs = os.path.abspath(str(local))
        n = safe_extract_tar(blob, local_abs)
        env["local"] = local_abs
        env["extracted"] = n
        return env

    # ---------- 目录 / 元信息 ----------

    def ls(self, path=".") -> list:
        return self._call("ls", path=str(path))["items"]

    def stat(self, path) -> dict:
        return self._call("stat", path=str(path))

    def exists(self, path) -> bool:
        return self._call("exists", path=str(path))["exists"]

    def mkdir(self, path, parents=True, mode=None) -> dict:
        return self._call("mkdir", path=str(path), parents=parents, mode=mode)

    def rm(self, path, recursive=False, missing_ok=True) -> dict:
        return self._call("rm", path=str(path), recursive=recursive, missing_ok=missing_ok)

    def mv(self, src, dst) -> dict:
        return self._call("mv", src=str(src), dst=str(dst))

    def cp(self, src, dst) -> dict:
        return self._call("cp", src=str(src), dst=str(dst))

    def chmod(self, path, mode, recursive=False) -> dict:
        return self._call("chmod", path=str(path), mode=mode, recursive=recursive)

    def hash(self, path, algo="sha256") -> dict:
        return self._call("hash", path=str(path), algo=algo)


# ============================ 传输无关的远端 PTY 会话 ============================

class _PtyReorderBuffer:
    """下行帧按 ``seq`` 重排，吸收多 broker 路径的乱序与重复副本。

    同一帧会经每个 broker 各投递一次，不同 broker 的路径延迟抖动还会让
    后发的帧先到（输出洪峰时几乎必然发生）。终端 CSI 转义序列经常被拆在
    两个 chunk 里，一旦乱序渲染，本地终端的状态机就会被打坏（进度条碎片
    堆叠、光标错位）。这里像 TCP 一样按 seq 连续放行；中间缺帧只短暂
    等待 :data:`GAP_TIMEOUT`，超时即认定真丢帧并跳号，把已在途的 chunk
    按序全部放行，避免某一帧被 broker 丢掉后输出永久卡死。
    """

    GAP_TIMEOUT = 0.75

    def __init__(self, on_chunks):
        self._on_chunks = on_chunks   # callable(list[bytes])，在锁外回调
        self._lock = threading.Lock()
        self._next = None
        self._pending = {}            # seq -> chunk
        self._timer = None

    def add(self, seq: int, chunk: bytes) -> None:
        out = []
        with self._lock:
            seq = int(seq)
            if self._next is not None and seq < self._next:
                return  # 迟到的重复副本，内容已渲染过
            self._pending[seq] = chunk
            if self._next is None and 0 in self._pending:
                # 服务端 seq 从 0 开始：收到 0 立即锚定，正常路径零等待。
                # 首帧若是 seq>0（0 经慢 broker 晚点），先暂存等前序帧。
                self._next = 0
            if self._next is not None:
                out = self._drain_locked()
            if not self._pending and self._timer is not None:
                t, self._timer = self._timer, None
                t.cancel()
            elif self._pending and self._timer is None:
                self._timer = threading.Timer(
                    self.GAP_TIMEOUT, self._gap_timeout)
                self._timer.daemon = True
                self._timer.start()
        if out:
            self._on_chunks(out)

    def _drain_locked(self) -> list:
        out = []
        while self._next in self._pending:
            out.append(self._pending.pop(self._next))
            self._next += 1
        return out

    def _gap_timeout(self):
        out = []
        with self._lock:
            self._timer = None
            if not self._pending:
                return
            # 缺口等了 GAP_TIMEOUT 仍没补上（或首帧 seq>0 且 0 一直没到）：
            # 按丢帧处理，锚到最小在途 seq，连续放行后若还有缺口再等一轮
            self._next = min(self._pending)
            out = self._drain_locked()
            if self._pending:
                self._timer = threading.Timer(
                    self.GAP_TIMEOUT, self._gap_timeout)
                self._timer.daemon = True
                self._timer.start()
        if out:
            self._on_chunks(out)

    def close(self):
        with self._lock:
            self._pending.clear()
            t, self._timer = self._timer, None
        if t is not None:
            t.cancel()


def _parse_pty_responders(resp, extras):
    """从握手首包 + 迟到回包里解析全部应答服务端，首包标记 winner。

    返回 ``[{"owner","host","pid","winner"}]``，按 owner/host 去重；
    回包不是 PTY 启动响应（无法解析 r）时跳过。
    """
    out = []
    seen = set()
    for i, r in enumerate([resp] + list(extras or [])):
        if not isinstance(r, dict):
            continue
        try:
            env = json.loads(r.get("r") or "")
        except (ValueError, TypeError):
            continue
        if not isinstance(env, dict) or not env.get("ok"):
            continue
        key = env.get("owner") or ("%s:%s" % (env.get("host"), env.get("pid")))
        if key in seen:
            continue
        seen.add(key)
        out.append({"owner": env.get("owner"), "host": env.get("host"),
                    "pid": env.get("pid"), "winner": i == 0})
    return out


class RemotePty:
    """远端交互式 PTY 客户端（ssh 模型），网络层由注入的 transport 决定。

    生命周期：:meth:`open` → :meth:`send` / :meth:`resize` → 远端退出或
    :meth:`detach` → :meth:`close`。

    协商
    ----
    :meth:`open` 生成 sid 并提出 in/out 两个 topic（可覆盖）；本地先订阅 out
    topic，再把 :data:`_PTY_START_TEMPLATE` 整段自包含代码经一问一答下发执行。
    服务端只运行通用 server_mqtt.py，无需为 PTY 做任何改动；服务端常驻 shell
    只启动一次，之后每个按键都写入同一个 PTY。

    transport 需具备可选能力：``publish`` / ``stream_subscribe`` /
    ``stream_unsubscribe``（MQTT 版见 cmd_client_mqtt.MqttTransport）。

    帧约定
    ------
    - 上行（client→server，in topic）：每帧带单调递增的 ``iseq``，服务端
      按它做单调闸门（同一帧会被每个 broker 各投递一次，旧 iseq 帧乱序
      迟到也一律丢弃，避免旧 winsz 覆盖新尺寸）；帧还带 ``owner``（首个
      握手应答者的进程 uid），其他同时应答的影子 PTY 收到不匹配帧立即
      自杀：
      ``{"pty": sid, "iseq": n, "owner": uid, "k": latin-1 按键}`` /
      ``{"pty": sid, "iseq": n, "owner": uid, "winsz": [rows, cols]}`` /
      ``{"pty": sid, "iseq": n, "owner": uid, "stop": true}`` /
      ``{"pty": sid, "iseq": n, "owner": uid, "claim": true}`` /
      ``{"pty": sid, "iseq": n, "owner": uid,
      "set": {"interval": s, "heartbeat": s, "ttl": s}}``（只允许时间参数）
    - 下行（server→client，out topic）：帧带同一 ``owner``，非赢家帧一律
      丢弃（含影子 PTY 的 end 帧，不能让它终止本视图）：
      ``{"pty": sid, "seq": n, "owner": uid, "d": latin-1 输出}`` /
      ``{"pty": sid, "owner": uid, "hb": ts_ms}``（周期心跳）/
      ``{"pty": sid, "owner": uid, "end": true, "reason": ..., "rc": ...}``
    """

    def __init__(self, transport: Transport, timeout: float = DEFAULT_TIMEOUT):
        if not isinstance(transport, Transport):
            raise TypeError("transport 必须是 remote_cmd.Transport 实例")
        self.tr = transport
        self.timeout = timeout
        self.sid: str | None = None
        self.in_topic: str | None = None
        self.out_topic: str | None = None
        self.server_info: dict | None = None
        self.end_reason: str | None = None
        # 会话归属：首个握手应答服务端的进程 uid；同时应答的其他进程是影子
        self.owner: str | None = None
        # 握手收集到的全部应答者：[{"owner","host","pid","winner"}]
        self.responders: list = []
        self.foreign_frames = 0   # 被丢弃的影子 PTY 帧数（诊断用）
        self._owner_ready = False
        self._pending_frames: list = []
        self._end_event = threading.Event()
        self._handler = None
        self._reorder = None
        self._frames = 0
        self._lock = threading.RLock()
        # 上行帧序号：所有按键/控制帧走 _publish_input 统一打号，
        # 服务端 _in_loop 据此丢弃多 broker 重复帧
        self._iseq = 0
        self._ilock = threading.Lock()

    def _check_caps(self):
        missing = [n for n in ("publish", "stream_subscribe", "stream_unsubscribe")
                   if not callable(getattr(self.tr, n, None))]
        if missing:
            raise RemoteError("当前 transport 不支持 PTY（缺少: %s）"
                              % ", ".join(missing))

    def open(self, rows, cols, *, shell=None, term=None, cwd=None,
             login=True, flush_interval=0.0, ttl=DEFAULT_PTY_TTL,
             frame_max=PTY_FRAME_MAX, sid=None, in_topic=None,
             out_topic=None, on_data=None, heartbeat=0.0,
             on_heartbeat=None, req_timeout=None,
             owner_gather=0.8, on_responders=None) -> dict:
        """协商并启动远端 PTY，返回服务端确认信息（含实际 topic/shell/pid）。

        heartbeat>0 时要求服务端按该间隔（秒）周期发心跳帧，每收到一帧
        （输出或心跳）回调一次 on_heartbeat，客户端据此做存活检测。

        owner_gather：首包回收后再多等几秒收集其他持相同 key 的服务端的
        迟到握手回包（多应答者检测/告警）；transport 不支持 request_many
        时自动退化为只收首包。

        on_responders：给定回调时启用**后台收集**——首包一定主就立即返回
        （不阻塞 owner_gather 秒，首屏零延迟），迟到应答者到齐后回调
        ``on_responders(responders)``（仅当应答者多于 1 个）；为 None 时
        维持同步 gather 的旧行为。
        """
        self._check_caps()
        sid = sid or ("pty-%d-%s" % (int(time.time() * 1000),
                                      os.urandom(2).hex()))
        in_topic = in_topic or ("pty/%s/in" % sid)
        out_topic = out_topic or ("pty/%s/out" % sid)
        req_timeout = req_timeout or max(float(self.timeout), 30.0)

        def _emit(chunks):
            # 重排缓冲（或无 seq 兜底）放行一批按序 chunk
            for chunk in chunks:
                if on_data is not None:
                    try:
                        on_data(chunk)
                    except Exception:
                        pass
            if chunks:
                with self._lock:
                    self._frames += len(chunks)

        reorder = _PtyReorderBuffer(_emit)
        self._reorder = reorder

        def handle_one(data):
            # 非赢家服务端的帧（含 end）一律丢弃：影子 PTY 退出不能终止
            # 本视图，它的输出流更不能进重排缓冲制造缺口/错位。
            _ow = data.get("owner")
            if _ow is not None and self.owner and _ow != self.owner:
                self.foreign_frames += 1
                return
            if data.get("end"):
                self.end_reason = data.get("reason") or "end"
                self._end_event.set()
                return
            if data.get("hb") is not None:
                # 心跳帧：无输出也能证明服务端进程还活着
                if on_heartbeat is not None:
                    try:
                        on_heartbeat(data.get("hb"))
                    except Exception:
                        pass
                return
            seq = data.get("seq")
            chunk = wire_to_bytes(data.get("d"))
            if not chunk:
                return
            if seq is not None:
                # 按 seq 重排后再渲染：跨 broker 乱序的 CSI 分片不能直接写终端
                reorder.add(seq, chunk)
            else:
                _emit([chunk])

        def handler(data):
            if not isinstance(data, dict) or data.get("pty") != sid:
                return
            if not self._owner_ready:
                # 握手首包未到、属主未定：多个应答者的 shell 启动输出可能
                # 已在飞，先缓存，定主后按 owner 过滤再重放，保证首屏干净。
                self._pending_frames.append(data)
                if len(self._pending_frames) > 4096:
                    del self._pending_frames[:-2048]
                return
            handle_one(data)

        self._handler = handler
        # 先订阅再启动，避免丢失最早的输出
        self.tr.stream_subscribe(out_topic, handler)
        payload = {"sid": sid, "in_topic": in_topic, "out_topic": out_topic,
                   "rows": int(rows), "cols": int(cols),
                   "shell": shell, "term": term, "cwd": cwd,
                   "login": bool(login),
                   "flush_interval": float(flush_interval),
                   "ttl": min(max(60.0, float(ttl)), MAX_PTY_TTL),
                   "frame_max": int(frame_max),
                   "heartbeat": max(0.0, float(heartbeat))}
        code = build_pty_start_code(payload)
        try:
            request_many = getattr(self.tr, "request_many", None)
            bg_on_extras = None
            if callable(request_many) and owner_gather > 0 \
                    and on_responders is not None:
                import inspect as _inspect
                try:
                    _supports_bg = len(_inspect.signature(
                        request_many).parameters) >= 4
                except (TypeError, ValueError):
                    _supports_bg = False
                if _supports_bg:
                    # holder 让后台 gather 线程读到首包；收集窗 >=0.8s，
                    # 首包返回后立即写入，竞态可忽略。
                    holder = {"resp": None}

                    def bg_on_extras(extras):
                        # 迟到应答者到齐：多于 1 个才回调（影子进程告警）。
                        full = _parse_pty_responders(holder["resp"], extras)
                        self.responders = full
                        if len(full) > 1:
                            try:
                                on_responders(full)
                            except Exception:
                                pass

                    resp, extras = request_many(
                        code, req_timeout, owner_gather,
                        on_extras=bg_on_extras)
                    if not resp:
                        raise RemoteTimeout("PTY 启动请求超时无回包")
                    holder["resp"] = resp
                else:
                    resp, extras = request_many(code, req_timeout, owner_gather)
            elif callable(request_many) and owner_gather > 0:
                resp, extras = request_many(code, req_timeout, owner_gather)
            else:
                resp, extras = self.tr.request(code, req_timeout), []
            if not resp:
                raise RemoteTimeout("PTY 启动请求超时无回包")
            try:
                env = json.loads(resp.get("r") or "")
            except (ValueError, TypeError) as e:
                raise RemoteRpcError("无法解析 PTY 启动回包: %s" % e, resp)
            if not env.get("ok"):
                raise RemoteOpError(env.get("error", "pty start failed"),
                                    env, resp)
        except Exception:
            try:
                self.tr.stream_unsubscribe(out_topic, handler)
            except Exception:
                pass
            raise
        self.sid = sid
        self.in_topic = in_topic
        self.out_topic = out_topic
        self.server_info = env
        self.owner = env.get("owner")
        self.responders = _parse_pty_responders(resp, extras)
        # 定主：放行为后续帧，并重放定主前缓存（影子 PTY 的早期帧在此被滤掉）
        self._owner_ready = True
        pending = self._pending_frames
        self._pending_frames = []
        for fr in pending:
            handle_one(fr)
        if self.owner:
            # 立即发一帧 claim：影子服务端在下一次按键/resize 之前就自杀，
            # 不用等 0.25s 的 resize_watch 兜底
            try:
                self._publish_input({"pty": sid, "claim": True})
            except Exception:
                pass
        return env

    def _publish_input(self, payload: dict) -> None:
        """上行帧唯一出口：打单调 ``iseq``、盖 ``owner`` 后发出。

        PTY 帧没有 req_id，不经过网络层的首帧去重，而 publish 会广播到
        所有 broker，同一帧必然被服务端收多份；序号让服务端只放行首帧。
        owner 让同时应答的影子 PTY 收到第一帧即自杀。

        注意：打号与 publish 必须在同一把锁内原子完成。否则两个线程
        （键盘线程发 k、resize 线程发 winsz）可能 A 拿到 iseq=1、B 拿到
        iseq=2 后 B 先 publish、A 后 publish；服务端 _in_loop 的单调闸门
        会把晚到的 iseq=1 当旧帧永久丢弃，表现为按键偶发丢失。publish
        对本地网络层只是把一帧推进 broker 队列，持锁代价可接受。
        """
        with self._ilock:
            if self.owner:
                payload["owner"] = self.owner
            payload["iseq"] = self._iseq
            self._iseq += 1
            self.tr.publish(self.in_topic, payload)

    def send(self, data) -> None:
        """把本地按键字节写入远端 PTY（latin-1 承载，可逐键也可合并）。"""
        if self.sid is None:
            raise RemoteError("PTY 尚未 open")
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._publish_input(
            {"pty": self.sid, "k": bytes(data).decode("latin-1")})

    def resize(self, rows, cols) -> None:
        """通知服务端调整 PTY 窗口（内核会向 shell 转发 SIGWINCH）。"""
        if self.sid is None:
            return
        self._publish_input(
            {"pty": self.sid, "winsz": [int(rows), int(cols)]})

    def configure(self, interval=None, heartbeat=None, ttl=None,
                  redraw=False) -> dict:
        """会话内热调时间参数，无需重连/重开 PTY。

        - interval：服务端输出攒批间隔秒，0=实时，上限 60；
        - heartbeat：心跳间隔秒，0=关闭，上限 3600；
        - ttl：孤儿会话存活秒，夹在 60~86400；
        - redraw：给远端前台进程组发 SIGWINCH 强制整屏重绘（同尺寸 resize
          内核不发信号，唤不醒已附着的 tmux/vim；本地菜单关闭后用它收尾）。
        返回实际下发（已夹取）的值，供调用方回显/记录。
        """
        if self.sid is None:
            raise RemoteError("PTY 尚未 open")
        settings = {}
        if interval is not None:
            settings["interval"] = min(max(0.0, float(interval)), 60.0)
        if heartbeat is not None:
            settings["heartbeat"] = min(max(0.0, float(heartbeat)), 3600.0)
        if ttl is not None:
            settings["ttl"] = min(max(60.0, float(ttl)), 86400.0)
        if redraw:
            settings["redraw"] = True
        if settings:
            self._publish_input({"pty": self.sid, "set": settings})
        return settings

    def detach(self) -> None:
        """请求服务端结束会话（kill 常驻 shell 及其进程组）。"""
        if self.sid is None:
            return
        try:
            self._publish_input({"pty": self.sid, "stop": True})
        except Exception:
            pass

    def wait_end(self, timeout=None) -> bool:
        """阻塞到远端会话结束（或超时）。返回是否已结束。"""
        return self._end_event.wait(timeout)

    def close(self) -> None:
        """断开：通知服务端停止、取消本地订阅；不停底层 transport/node。"""
        if self.sid is not None:
            self.detach()
            try:
                self._end_event.wait(1.5)
            except Exception:
                pass
        if self._handler is not None and self.out_topic is not None:
            try:
                self.tr.stream_unsubscribe(self.out_topic, self._handler)
            except Exception:
                pass
        if self._reorder is not None:
            self._reorder.close()
            self._reorder = None
        self.sid = None
        self.owner = None
        self._owner_ready = False
        self._pending_frames = []
        self.foreign_frames = 0
