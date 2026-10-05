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

import json
import os
import posixpath
import shlex
import threading
import time
from abc import ABC, abstractmethod
from collections import deque

# ---- 传输尺寸约束（均按实测的公共 broker 行为设定） --------------------------
MAX_TRANSFER = 1 << 20          # 1 MiB：单文件经本报文通道传输的默认硬上限
WIRE_BUDGET = 1 << 17          # 128 KiB：单条报文 JSON 转义后在线尺寸目标（实测 0.3s）
INLINE_MAX = 8192              # 命令输出小于该字节数时直接随回包返回
DEFAULT_TIMEOUT = 60           # 单次问答默认等待秒数
DEFAULT_STREAM_TOPIC = "sys/device/stream"  # 远端周期汇报的默认 topic

# ---- PTY 会话约束 ------------------------------------------------------------
PTY_FRAME_MAX = 1 << 14        # 16 KiB：PTY 单帧原始字节上限（最坏 latin-1 转义后 < 96KiB）
DEFAULT_PTY_TTL = 12 * 3600    # 孤儿 PTY 会话最长存活秒数（默认 12 小时）
MAX_PTY_TTL = 24 * 3600        # TTL 允许设定的上限


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
        from collections import deque as _dq

        _sid = str(_a["sid"])
        _in_topic = str(_a["in_topic"])
        _out_topic = str(_a["out_topic"])

        def _find_net():
            # 复用服务端进程里现成的 MQTT 网络层（gms.mqtt_net），不新建连接。
            for _v in list(globals().values()):
                _mn = getattr(_v, "mqtt_net", None)
                if _mn is not None and hasattr(_mn, "publish_broadcast"):
                    return _mn
            return None

        _net = _find_net()
        if _net is None:
            raise RuntimeError("服务端没有可复用的 MQTT 管理器（期望 gms.mqtt_net）")

        _rows = max(1, int(_a.get("rows", 24)))
        _cols = max(1, int(_a.get("cols", 80)))
        _frame_max = max(512, int(_a.get("frame_max", 16384)))
        _interval = max(0.0, float(_a.get("flush_interval", 0.0)))
        _ttl = min(max(60.0, float(_a.get("ttl", 43200.0))), 86400.0)
        _login = bool(_a.get("login", True))

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
        _cwd = _a.get("cwd") or _o.path.expanduser("~")

        _mfd, _sfd = _pty.openpty()
        _fc.ioctl(_sfd, _te.TIOCSWINSZ, _st.pack("HHHH", _rows, _cols, 0, 0))
        _envv = dict(_o.environ)
        _envv["TERM"] = _term
        _argv0 = ("-" + _o.path.basename(_shell)) if _login else _shell

        def _become():
            # 子进程：新会话 + 把 slave 设为控制终端（ssh 同款）。
            _o.setsid()
            _fc.ioctl(_sfd, _te.TIOCSCTTY, 0)

        _proc = _sp.Popen(
            [_argv0], executable=_shell,
            stdin=_sfd, stdout=_sfd, stderr=_sfd,
            cwd=_cwd, env=_envv, close_fds=True, preexec_fn=_become)
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
                    {"pty": _sid, "seq": _seq, "d": _data.decode("latin-1")})
            except Exception:
                pass

        def _router(_topic, _data, _broker):
            # PTY 专用 topic：入队后不再下传给原 RPC 回调；其余消息原样透传。
            _q = _net._cmq_pty_router.get(_topic)
            if _q is not None and isinstance(_data, dict):
                _q.put(_data)
                return
            return _net._cmq_pty_orig(_topic, _data, _broker)

        if not getattr(_net, "_cmq_pty_router", None):
            _net._cmq_pty_orig = _net.message_callback
            _net._cmq_pty_router = {}
            _net.set_on_message(_router)

        _inq = _qe.Queue()
        _net._cmq_pty_router[_in_topic] = _inq
        _net.subscribe(_in_topic)

        def _in_loop():
            # 上行首帧去重：同一帧会被每个 broker 各投递一次（实测 11 个连接
            # 能到 ~15 份），普通 RPC 帧靠网络层 req_id 的 TTLCache 去重，PTY
            # 帧没有 req_id，必须靠客户端打的 iseq 自己去重，否则一次回车会
            # 被写进 PTY 十几遍（命令跑一遍 + 一串空命令提示符）。
            _iseq_recent = _dq(maxlen=256)
            while not _end.is_set():
                try:
                    _fr = _inq.get(timeout=0.5)
                except _qe.Empty:
                    continue
                try:
                    if not isinstance(_fr, dict) or _fr.get("pty") != _sid:
                        continue
                    _iq = _fr.get("iseq")
                    if _iq is not None:
                        if _iq in _iseq_recent:
                            continue  # 同一帧被多个 broker 重复送达，只留首帧
                        _iseq_recent.append(_iq)
                    if _fr.get("stop") or _fr.get("end"):
                        _st0["reason"] = "stopped"
                        _kill()
                        return
                    _wz = _fr.get("winsz")
                    if _wz:
                        _r2, _c2 = int(_wz[0]), int(_wz[1])
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
            _deadline = _t.time() + _ttl
            _poll = _sel.poll()
            _poll.register(_mfd, _sel.POLLIN | _sel.POLLHUP | _sel.POLLERR)
            _reason = None
            try:
                while True:
                    _now = _t.time()
                    if _now >= _deadline:
                        _st0["reason"] = "ttl"
                        _reason = "ttl"
                        break
                    _left = max(0.05, _deadline - _now)
                    if _interval <= 0:
                        _wait_ms = min(1000.0, _left * 1000)
                    elif _buf:
                        _wait_ms = min(
                            500.0,
                            max(10.0, (_interval - (_now - _mark)) * 1000),
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
                    if _buf and (_interval <= 0
                                 or (_t.time() - _mark) >= _interval
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
                    _net.publish_broadcast(
                        _out_topic,
                        {"pty": _sid, "end": True,
                         "reason": _reason, "rc": _rc})
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
        # 发不出来，客户端会无限干等（按键全进黑洞）。
        _hb = max(0.0, float(_a.get("heartbeat", 0.0)))

        def _hb_loop():
            while _hb > 0.0 and not _end.wait(_hb):
                try:
                    _net.publish_broadcast(
                        _out_topic,
                        {"pty": _sid, "hb": int(_t.time() * 1000)})
                except Exception:
                    pass

        if _hb > 0.0:
            _th.Thread(target=_hb_loop, name="pty-hb", daemon=True).start()

        _res = {"ok": True, "sid": _sid, "heartbeat": _hb,
                "in_topic": _in_topic, "out_topic": _out_topic,
                "shell": _shell, "pid": _proc.pid, "term": _term,
                "rows": _rows, "cols": _cols, "cwd": _cwd,
                "flush_interval": _interval, "ttl": _ttl, "login": _login}
    except Exception:
        for _fd in (_mfd, _sfd):
            try:
                if _fd is not None:
                    _o.close(_fd)
            except Exception:
                pass
        _res = {"ok": False, "error": _tb.format_exc()}
    return _j.dumps(_res, ensure_ascii=False)
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
DO_UPDATE={shlex.quote("" if update else "0")}
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
      按它做多 broker 首帧去重（同一帧会被每个 broker 各投递一次）：
      ``{"pty": sid, "iseq": n, "k": latin-1 按键}`` /
      ``{"pty": sid, "iseq": n, "winsz": [rows, cols]}`` /
      ``{"pty": sid, "iseq": n, "stop": true}``
    - 下行（server→client，out topic）：
      ``{"pty": sid, "seq": n, "d": latin-1 输出}`` /
      ``{"pty": sid, "hb": ts_ms}``（周期心跳，证明服务端存活）/
      ``{"pty": sid, "end": true, "reason": ..., "rc": ...}``
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
        self._end_event = threading.Event()
        self._handler = None
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
             on_heartbeat=None, req_timeout=None) -> dict:
        """协商并启动远端 PTY，返回服务端确认信息（含实际 topic/shell/pid）。

        heartbeat>0 时要求服务端按该间隔（秒）周期发心跳帧，每收到一帧
        （输出或心跳）回调一次 on_heartbeat，客户端据此做存活检测。
        """
        self._check_caps()
        sid = sid or ("pty-%d-%s" % (int(time.time() * 1000),
                                      os.urandom(2).hex()))
        in_topic = in_topic or ("pty/%s/in" % sid)
        out_topic = out_topic or ("pty/%s/out" % sid)
        req_timeout = req_timeout or max(float(self.timeout), 30.0)
        recent = deque(maxlen=256)

        def handler(data):
            if not isinstance(data, dict) or data.get("pty") != sid:
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
            if seq is not None:
                if seq in recent:
                    return  # 多 broker 重复送达
                recent.append(seq)
            chunk = wire_to_bytes(data.get("d"))
            if chunk:
                with self._lock:
                    self._frames += 1
                if on_data is not None:
                    try:
                        on_data(chunk)
                    except Exception:
                        pass

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
            resp = self.tr.request(code, req_timeout)
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
        return env

    def _publish_input(self, payload: dict) -> None:
        """上行帧唯一出口：打单调 ``iseq`` 后发出。

        PTY 帧没有 req_id，不经过网络层的首帧去重，而 publish 会广播到
        所有 broker，同一帧必然被服务端收多份；序号让服务端只放行首帧。
        """
        with self._ilock:
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
        self.sid = None
