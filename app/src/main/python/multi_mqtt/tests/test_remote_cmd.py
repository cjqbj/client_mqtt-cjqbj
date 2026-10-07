#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""remote_cmd 模板与客户端封装的测试（重点：pull_dir 整目录拉取）。

不连任何 broker：
- LocalExecTransport 在本进程真实执行 build_code 生成的自包含远端模板
  （等价于 server_mqtt.py executor 跑下发代码），操作临时目录；
- 覆盖 pull_dir：exclude 组件剪枝、md5/长度校验、压缩包硬顶与结构化拒绝、
  确定性 gzip、回包篡改检测、tar 解压防穿越。

运行：
    cd multi_mqtt && python -m unittest tests.test_remote_cmd -v
"""
import io
import json
import os
import re
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.remote_cmd import (  # noqa: E402
    Transport, RemoteShell, RemotePty, RemoteError, DirArchiveTooLarge,
    build_code, safe_extract_tar,
    MAX_DIR_ARCHIVE, DEFAULT_DIR_ARCHIVE_MAX,
)

IS_POSIX = os.name == "posix"


# ============================ 本进程执行下发代码（伪服务端） ============================

class LocalExecTransport(Transport):
    """把 build_code 的代码在本进程跑一遍，回包形态与 server_mqtt 一致。"""

    def __init__(self):
        self.codes = []

    def request(self, code, timeout=60):
        self.codes.append(code)
        ns = {}
        exec(compile(code, "<remote_template>", "exec"), ns)
        r = ns["_cmq_dispatch"]()  # 同一 payload 再跑一次并捕获 r
        return {"ok": True, "r": r, "stdout": "", "error": None}


def run_op(payload):
    resp = LocalExecTransport().request(build_code(payload))
    return json.loads(resp["r"])


def _write(path, data=b"x"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)


def _make_tree(root):
    _write(os.path.join(root, "a.txt"), b"hello a\n")
    _write(os.path.join(root, "sub", "b.txt"), b"hello b\n")
    _write(os.path.join(root, "sub", "secret.txt"), b"top secret\n")
    _write(os.path.join(root, "root_cached.pyc"), b"\x00pyc")
    _write(os.path.join(root, "sub", "__pycache__", "m.pyc"), b"cache bytecode")
    _write(os.path.join(root, "build", "out.bin"), b"build artifact")
    if IS_POSIX:
        os.symlink("a.txt", os.path.join(root, "link_to_a"))


# ============================ 远端模板 pull_dir 直接测试 ============================

class PullDirTemplateTests(unittest.TestCase):

    def setUp(self):
        self.td = tempfile.mkdtemp()
        self.src = os.path.join(self.td, "src")
        os.makedirs(self.src)
        _make_tree(self.src)

    def test_full_tree_stats(self):
        env = run_op({"op": "pull_dir", "path": self.src, "max_bytes": 1 << 20})
        self.assertTrue(env["ok"], env.get("error"))
        self.assertEqual(env["files"], 6)
        self.assertGreaterEqual(env["dirs"], 4)  # . sub build sub/__pycache__
        self.assertEqual(env["skipped_links"], 1 if IS_POSIX else 0)
        self.assertEqual(env["skipped_special"], 0)
        self.assertEqual(env["excluded"], 0)
        self.assertEqual(env["arc_bytes"],
                         len(__import__("base64").b64decode(env["b64"])))
        self.assertRegex(env["md5"], r"^[0-9a-f]{32}$")

    def test_excludes_prune_and_filter(self):
        env = run_op({
            "op": "pull_dir", "path": self.src,
            "excludes": ["__pycache__", "*.pyc", "build"],
            "max_bytes": 1 << 20,
        })
        self.assertTrue(env["ok"], env.get("error"))
        # build 目录、sub/__pycache__ 目录各剪枝一次；root_cached.pyc 文件名命中
        self.assertEqual(env["excluded"], 3)
        self.assertEqual(env["files"], 3)  # a.txt sub/b.txt sub/secret.txt
        names = set()
        with tarfile.open(fileobj=io.BytesIO(
                __import__("base64").b64decode(env["b64"])), mode="r:gz") as tf:
            names = {m.name for m in tf.getmembers()}
        self.assertIn("a.txt", names)
        self.assertIn("sub/b.txt", names)
        self.assertIn("sub/secret.txt", names)
        self.assertFalse(any("build" in n or "pycache" in n or
                             n.endswith(".pyc") for n in names), names)

    def test_exclude_relative_path_pattern(self):
        env = run_op({
            "op": "pull_dir", "path": self.src,
            "excludes": ["sub/secret.txt"], "max_bytes": 1 << 20,
        })
        self.assertTrue(env["ok"], env.get("error"))
        self.assertEqual(env["excluded"], 1)
        blob = __import__("base64").b64decode(env["b64"])
        with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
            names = {m.name for m in tf.getmembers()}
        self.assertNotIn("sub/secret.txt", names)
        self.assertIn("sub/b.txt", names)

    def test_deterministic_archive(self):
        e1 = run_op({"op": "pull_dir", "path": self.src, "max_bytes": 1 << 20})
        e2 = run_op({"op": "pull_dir", "path": self.src, "max_bytes": 1 << 20})
        self.assertEqual(e1["b64"], e2["b64"])  # gzip mtime=0 + 排序 + 同名目不变

    def test_too_large_refused_with_diagnostics(self):
        big = os.path.join(self.td, "big.bin")
        _write(big, os.urandom(16 * 1024))
        env = run_op({"op": "pull_dir", "path": self.td, "max_bytes": 4096})
        self.assertFalse(env["ok"])
        self.assertEqual(env["reason"], "too_large")
        self.assertIn("big.bin", env["error"])
        self.assertIn("处理建议", env["error"])
        self.assertIn("excludes", env["error"])
        self.assertGreaterEqual(env["raw_bytes"], 16 * 1024)
        self.assertEqual(env["largest"][0]["path"], "big.bin")
        self.assertEqual(env["max_bytes"], 4096)

    def test_server_side_hard_clamp(self):
        big = os.path.join(self.td, "big2.bin")
        _write(big, os.urandom(int(1.2 * (1 << 20))))
        # 客户端请求 2MiB，远端必须夹回 1MiB 硬顶并拒绝
        env = run_op({"op": "pull_dir", "path": self.td, "max_bytes": 2 << 20})
        self.assertFalse(env["ok"])
        self.assertEqual(env["reason"], "too_large")
        self.assertEqual(env["max_bytes"], MAX_DIR_ARCHIVE)

    def test_missing_dir_raises(self):
        env = run_op({"op": "pull_dir", "path": os.path.join(self.td, "nope")})
        self.assertFalse(env["ok"])
        self.assertIn("NotADirectoryError", env["error"])

    def test_empty_dir(self):
        empty = os.path.join(self.td, "empty")
        os.makedirs(empty)
        env = run_op({"op": "pull_dir", "path": empty})
        self.assertTrue(env["ok"], env.get("error"))
        self.assertEqual(env["files"], 0)
        self.assertGreater(env["arc_bytes"], 0)


# ============================ RemoteShell 客户端封装 ============================

class PullDirClientTests(unittest.TestCase):

    def setUp(self):
        self.td = tempfile.mkdtemp()
        self.src = os.path.join(self.td, "src")
        os.makedirs(self.src)
        _make_tree(self.src)
        self.dest = os.path.join(self.td, "dest")

    def test_pull_dir_extracts_and_reports(self):
        sh = RemoteShell(LocalExecTransport())
        meta = sh.pull_dir(self.src, self.dest,
                           excludes=["__pycache__", "*.pyc", "build"])
        self.assertTrue(os.path.isfile(os.path.join(self.dest, "a.txt")))
        self.assertTrue(os.path.isfile(os.path.join(self.dest, "sub", "b.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.dest, "build")))
        self.assertFalse(os.path.exists(
            os.path.join(self.dest, "sub", "__pycache__")))
        self.assertEqual(meta["local"], os.path.abspath(self.dest))
        self.assertGreater(meta["extracted"], 0)
        self.assertRegex(meta["md5"], r"^[0-9a-f]{32}$")
        if IS_POSIX:
            self.assertFalse(os.path.exists(os.path.join(self.dest, "link_to_a")))

    def test_pull_dir_bytes_roundtrip(self):
        sh = RemoteShell(LocalExecTransport())
        blob, env = sh.pull_dir_bytes(self.src)
        self.assertEqual(len(blob), env["arc_bytes"])
        n = safe_extract_tar(blob, self.dest)
        self.assertGreater(n, 0)
        with open(os.path.join(self.dest, "a.txt"), "rb") as fh:
            self.assertEqual(fh.read(), b"hello a\n")

    def test_default_max_is_wire_safe(self):
        self.assertLessEqual(DEFAULT_DIR_ARCHIVE_MAX, MAX_DIR_ARCHIVE)
        # base64 膨胀后必须稳在 1000KiB 实测可过线以下
        self.assertLess(DEFAULT_DIR_ARCHIVE_MAX * 4 // 3, 1000 << 10)

    def test_client_rejects_over_hard_top(self):
        sh = RemoteShell(LocalExecTransport())
        with self.assertRaises(DirArchiveTooLarge):
            sh.pull_dir_bytes(self.src, max_bytes=MAX_DIR_ARCHIVE + 1)
        with self.assertRaises(ValueError):
            sh.pull_dir_bytes(self.src, max_bytes=0)

    def test_too_large_maps_to_exception(self):
        big = os.path.join(self.td, "big.bin")
        _write(big, os.urandom(16 * 1024))
        sh = RemoteShell(LocalExecTransport())
        with self.assertRaises(DirArchiveTooLarge) as cm:
            sh.pull_dir_bytes(self.td, max_bytes=4096)
        self.assertEqual(cm.exception.env["reason"], "too_large")
        self.assertEqual(cm.exception.env["largest"][0]["path"], "big.bin")
        self.assertIn("处理建议", str(cm.exception))

    def test_arc_size_mismatch_detected(self):
        real = LocalExecTransport()

        class TamperTransport(Transport):
            def request(self, code, timeout=60):
                resp = real.request(code, timeout)
                if '"pull_dir"' in code:
                    env = json.loads(resp["r"])
                    if env.get("ok"):
                        env["arc_bytes"] += 1
                        resp["r"] = json.dumps(env)
                return resp

        sh = RemoteShell(TamperTransport())
        with self.assertRaisesRegex(RemoteError, "大小不一致"):
            sh.pull_dir_bytes(self.src)

    def test_md5_mismatch_detected(self):
        real = LocalExecTransport()

        class TamperTransport(Transport):
            def request(self, code, timeout=60):
                resp = real.request(code, timeout)
                if '"pull_dir"' in code:
                    env = json.loads(resp["r"])
                    if env.get("ok"):
                        env["md5"] = "0" * 32
                        resp["r"] = json.dumps(env)
                return resp

        sh = RemoteShell(TamperTransport())
        with self.assertRaisesRegex(RemoteError, "md5"):
            sh.pull_dir_bytes(self.src)


# ============================ tar 解压安全 ============================

class SafeExtractTests(unittest.TestCase):

    def setUp(self):
        self.td = tempfile.mkdtemp()

    def _arc(self, members):
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for ti, data in members:
                tf.addfile(ti, io.BytesIO(data) if data is not None else None)
        return buf.getvalue()

    def test_reject_parent_traversal(self):
        ti = tarfile.TarInfo("../evil.py")
        ti.size, ti.mode = 5, 0o644
        blob = self._arc([(ti, b"evil!")])
        with self.assertRaises(RemoteError):
            safe_extract_tar(blob, os.path.join(self.td, "d"))

    def test_reject_absolute_path(self):
        ti = tarfile.TarInfo("/tmp/evil.py")
        ti.size, ti.mode = 5, 0o644
        blob = self._arc([(ti, b"evil!")])
        with self.assertRaises(RemoteError):
            safe_extract_tar(blob, os.path.join(self.td, "d"))

    def test_reject_symlink_member(self):
        ti = tarfile.TarInfo("link")
        ti.type = tarfile.SYMTYPE
        ti.linkname = "/etc/passwd"
        ti.mode = 0o777
        blob = self._arc([(ti, None)])
        with self.assertRaises(RemoteError):
            safe_extract_tar(blob, os.path.join(self.td, "d"))


# ==================== install_packages update 语义 ====================

class InstallPackagesUpdateSemanticsTests(unittest.TestCase):
    """文档约定：update=None/True 刷新索引，仅 update=False 跳过。
    apt_install 默认 update=True，两者默认行为必须一致。"""

    def _do_update_line(self, update, method="install_packages"):
        sh = RemoteShell(LocalExecTransport())
        captured = {}

        def fake_run(script, timeout=None, check=False, **_kw):
            captured["script"] = script

        sh.run = fake_run
        if method == "install_packages":
            sh.install_packages(["sl"], update=update)
        else:
            sh.apt_install(["sl"], update=update)
        m = re.search(r"^DO_UPDATE=(.*)$", captured["script"], re.M)
        return m.group(1)

    def test_none_and_true_refresh_false_skips(self):
        # shlex.quote("") -> "''"；shlex.quote("0") 是安全词 -> "0"
        self.assertEqual(self._do_update_line(None), "''")
        self.assertEqual(self._do_update_line(True), "''")
        self.assertEqual(self._do_update_line(False), "0")

    def test_apt_install_matches_install_packages_defaults(self):
        self.assertEqual(self._do_update_line(True, "apt_install"), "''")
        self.assertEqual(self._do_update_line(False, "apt_install"), "0")


# ==================== 分块拉取停滞保护 ====================

class TransferStallTests(unittest.TestCase):
    """n=0 且不 eof 必须显式报错，不得无限重放同一帧。"""

    def setUp(self):
        self.td = tempfile.mkdtemp()
        self.sh = RemoteShell(LocalExecTransport())

    def tearDown(self):
        shutil.rmtree(self.td, ignore_errors=True)

    def test_read_raises_on_zero_progress_without_eof(self):
        def fake_call(op, **_kw):
            return {"n": 0, "eof": False, "data": "", "size": 0}

        self.sh._call = fake_call
        with self.assertRaisesRegex(RemoteError, "停滞"):
            self.sh.read("/x")

    def test_read_empty_file_with_eof_succeeds(self):
        # 空文件合法地以 n=0 + eof=True 结束，不能误报停滞
        def fake_call(op, **_kw):
            return {"n": 0, "eof": True, "data": "", "size": 0}

        self.sh._call = fake_call
        self.assertEqual(self.sh.read("/x"), b"")

    def test_fetch_stash_raises_and_still_drops(self):
        dropped = []

        def fake_call(op, **kw):
            if op == "fetch":
                return {"n": 0, "eof": False, "data": ""}
            if op == "drop":
                dropped.append(kw.get("id"))
            return {}

        self.sh._call = fake_call
        with self.assertRaisesRegex(RemoteError, "停滞"):
            self.sh._fetch_stash("sid-1")
        self.assertEqual(dropped, ["sid-1"])

    def test_download_raises_without_replacing_local(self):
        target = os.path.join(self.td, "out.bin")

        def fake_call(op, **_kw):
            if op == "stat":
                return {"size": 10}
            return {"n": 0, "eof": False, "data": "", "size": 10}

        self.sh._call = fake_call
        with self.assertRaisesRegex(RemoteError, "停滞"):
            self.sh.download("/remote/x", target)
        self.assertFalse(os.path.exists(target), "停滞失败不得 replace 出本地文件")


# ==================== PTY 上行帧 iseq 顺序 ====================

class _RecordingPtyTransport(Transport):
    """记录 publish 看到的 iseq，并验证调用发生在序号锁内。"""

    def __init__(self):
        self.seen = []
        self.pty = None
        self._rec_lock = threading.Lock()

    def request(self, code, timeout=60):
        return {"ok": True, "r": "{}"}

    def publish(self, topic, payload):
        # 结构性断言：publish 必须在 _ilock 临界区内（非重入锁，同线程
        # 再次获取必然失败）；锁外发送即旧 bug 复现。
        assert self.pty._ilock.acquire(blocking=False) is False, \
            "publish 必须在 iseq 锁内调用"
        time.sleep(0.001)
        with self._rec_lock:
            self.seen.append(payload["iseq"])

    def stream_subscribe(self, topic, handler):
        pass

    def stream_unsubscribe(self, topic, handler):
        pass


class PtyIseqOrderingTests(unittest.TestCase):
    def test_concurrent_send_resize_publishes_in_iseq_order(self):
        tr = _RecordingPtyTransport()
        pty = RemotePty(tr)
        pty.sid = "pty-test"
        pty.in_topic = "pty/test/in"
        tr.pty = pty

        n_threads = 32
        barrier = threading.Barrier(n_threads)

        def worker(i):
            barrier.wait()
            if i % 2:
                pty.send("k")          # 模拟键盘线程
            else:
                pty.resize(24, 80)     # 模拟 resize 线程

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(sorted(tr.seen), list(range(n_threads)),
                         "iseq 不得重复")
        self.assertEqual(tr.seen, list(range(n_threads)),
                         "实际 publish 顺序必须与序号一致，旧帧不能晚发被服务端丢弃")


# ==================== tmux_capture_pane ====================

class TmuxCaptureTests(unittest.TestCase):
    """验证生成的远端脚本与 reverse 行为（不真连 tmux，截获 run）。"""

    def _shell(self, text="l1\nl2\nl3\n"):
        sh = RemoteShell(LocalExecTransport())
        captured = {}

        class _R:
            pass

        def fake_run(script, timeout=None, check=False, **_kw):
            captured["script"] = script
            r = _R()
            r.text = text
            return r

        sh.run = fake_run
        return sh, captured

    def test_default_script_matches_qgb_semantics(self):
        sh, cap = self._shell()
        out = sh.tmux_capture_pane()
        s = cap["script"]
        self.assertIn("unset TMUX", s)
        self.assertIn("capture-pane -S -9999 -t 0 -J", s)
        self.assertIn("show-buffer", s)
        self.assertNotIn("-S ", s.split("capture-pane")[0].replace("unset TMUX; tmux ", ""))
        self.assertEqual(out, "l1\nl2\nl3\n")

    def test_session_window_pane_and_socket_quoted(self):
        sh, cap = self._shell()
        sh.tmux_capture_pane("my sess:2.1", max_lines=50,
                        socket="/tmp/tmux-1000/default", capture_args="-J -e")
        s = cap["script"]
        self.assertIn("-S /tmp/tmux-1000/default", s)
        self.assertIn("-t 'my sess:2.1'", s)
        self.assertIn("-S -50", s)
        self.assertIn("-J -e", s)

    def test_none_session_omits_target(self):
        sh, cap = self._shell()
        sh.tmux_capture_pane(None)
        self.assertNotIn(" -t ", cap["script"])

    def test_negative_max_lines_clamped_to_zero(self):
        sh, cap = self._shell()
        sh.tmux_capture_pane(0, max_lines=-5)
        self.assertIn("capture-pane -S -0 ", cap["script"])

    def test_reverse_flips_lines(self):
        sh, _cap = self._shell("a\nb\nc")
        self.assertEqual(sh.tmux_capture_pane(reverse=True), "c\nb\na")

    def test_no_reverse_keeps_order(self):
        sh, _cap = self._shell("a\nb\nc")
        self.assertEqual(sh.tmux_capture_pane(), "a\nb\nc")


if __name__ == "__main__":
    unittest.main()
