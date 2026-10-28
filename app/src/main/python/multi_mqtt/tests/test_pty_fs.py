#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MCP 文件工具（client/pty/parts/098_mcp_files.py）的单元测试。

覆盖读写/编辑/查找/文件管理、只读开关、二进制与 CRLF 处理，以及
「工具层 → transport.request → 远端 exec 源码包」的整条远端链路。
运行：
    cd multi_mqtt && python -m unittest tests.test_pty_fs -v
"""
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import pty_client_mqtt as pcm  # noqa: E402


class PtyFsTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="pty_fs_test_")
        pcm._fs_configure(False)

    def tearDown(self):
        pcm._fs_configure(False)
        shutil.rmtree(self.root, ignore_errors=True)

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    # ---------------- 读写 ----------------

    def test_write_read_roundtrip(self):
        target = self.path("a.txt")
        res = pcm._fs_op_write(target, "one\ntwo\nthree\n")
        self.assertTrue(res["ok"], res)
        self.assertTrue(res["created"])
        self.assertEqual(res["total_lines"], 3)
        read = pcm._fs_op_read(target)
        self.assertTrue(read["ok"], read)
        self.assertEqual(read["total_lines"], 3)
        self.assertIn("     1→one", read["content"])
        self.assertIn("     3→three", read["content"])
        page = pcm._fs_op_read(target, offset=1, limit=1)
        self.assertEqual(page["returned_lines"], 1)
        self.assertIn("     2→two", page["content"])
        self.assertTrue(page["truncated"])

    def test_read_requires_regular_utf8_file(self):
        self.assertFalse(pcm._fs_op_read(self.path("missing.txt"))["ok"])
        binary = self.path("b.bin")
        pcm._fs_op_write(binary, "x")
        with open(binary, "wb") as f:
            f.write(b"abc\x00def")
        self.assertFalse(pcm._fs_op_read(binary)["ok"])

    def test_write_outside_cwd_allowed(self):
        outside = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
        outside.close()
        try:
            self.assertTrue(pcm._fs_op_write(outside.name, "ok")["ok"])
            with open(outside.name, encoding="utf-8") as f:
                self.assertEqual(f.read(), "ok")
            self.assertTrue(pcm._fs_op_read(outside.name)["ok"])
        finally:
            os.unlink(outside.name)

    def test_read_only_mode_blocks_write_family(self):
        target = self.path("a.txt")
        pcm._fs_op_write(target, "abc")
        pcm._fs_configure(True)
        self.assertFalse(pcm._fs_op_write(target, "x")["ok"])
        self.assertFalse(pcm._fs_op_edit(target, "abc", "x")["ok"])
        self.assertFalse(pcm._fs_op_mkdir(self.path("d"))["ok"])
        self.assertFalse(pcm._fs_op_move(target, self.path("b.txt"))["ok"])
        self.assertFalse(pcm._fs_op_delete(target)["ok"])
        self.assertTrue(pcm._fs_op_read(target)["ok"])

    def test_write_missing_parent_rejected(self):
        self.assertFalse(pcm._fs_op_write(self.path("no", "a.txt"), "x")["ok"])

    # ---------------- 编辑 ----------------

    def test_edit_unique_multiple_and_replace_all(self):
        target = self.path("e.txt")
        pcm._fs_op_write(target, "alpha\nbeta\nalpha\n")
        dup = pcm._fs_op_edit(target, "alpha", "A")
        self.assertFalse(dup["ok"])
        self.assertIn("2", dup["error"])
        one = pcm._fs_op_edit(target, "beta", "B")
        self.assertTrue(one["ok"], one)
        self.assertEqual(one["replacements"], 1)
        self.assertEqual(one["first_line"], 2)
        allres = pcm._fs_op_edit(target, "alpha", "A", replace_all=True)
        self.assertTrue(allres["ok"], allres)
        self.assertEqual(allres["replacements"], 2)
        with open(target, encoding="utf-8") as f:
            self.assertEqual(f.read(), "A\nB\nA\n")

    def test_edit_crlf_preserved_with_lf_old_string(self):
        target = self.path("crlf.txt")
        with open(target, "wb") as f:
            f.write(b"line1\r\nline2\r\n")
        res = pcm._fs_op_edit(target, "line2\n", "LINE2\n")
        self.assertTrue(res["ok"], res)
        with open(target, "rb") as f:
            self.assertEqual(f.read(), b"line1\r\nLINE2\r\n")

    def test_edit_empty_old_string_rejected(self):
        target = self.path("e.txt")
        pcm._fs_op_write(target, "abc")
        self.assertFalse(pcm._fs_op_edit(target, "", "x")["ok"])

    # ---------------- 查找/目录 ----------------

    def test_ls_glob_grep(self):
        pcm._fs_op_mkdir(self.path("sub", "deep"))
        pcm._fs_op_write(self.path("sub", "one.py"), "import os\nprint('hit')\n")
        pcm._fs_op_write(self.path("sub", "deep", "two.py"), "HIT again\n")
        pcm._fs_op_write(self.path("sub", "note.md"), "hit text\n")
        ls = pcm._fs_op_ls(self.root)
        self.assertTrue(ls["ok"], ls)
        self.assertEqual([e["name"] for e in ls["entries"]][0], "sub")
        glob = pcm._fs_op_glob("**/*.py", self.root)
        self.assertTrue(glob["ok"], glob)
        self.assertEqual(glob["total"], 2)
        grep = pcm._fs_op_grep("hit", self.root, "*.py", ignore_case=True)
        self.assertTrue(grep["ok"], grep)
        self.assertEqual(grep["matched_files"], 2)
        self.assertIn(":2:print('hit')", grep["text"])
        count = pcm._fs_op_grep("hit", self.root, "*.py", ignore_case=True,
                            output_mode="count")
        self.assertIn(":1", count["text"])
        files = pcm._fs_op_grep("hit", self.root, output_mode="files_with_matches",
                            ignore_case=True)
        self.assertIn("note.md", files["text"])

    def test_grep_skips_git_and_binary(self):
        pcm._fs_op_mkdir(self.path(".git"))
        pcm._fs_op_write(self.path(".git", "config"), "needle\n")
        pcm._fs_op_write(self.path("bin.dat"), "needle\n")
        with open(self.path("bin.dat"), "wb") as f:
            f.write(b"needle\x00\n")
        res = pcm._fs_op_grep("needle", self.root)
        self.assertTrue(res["ok"], res)
        self.assertEqual(res["matched_files"], 0)

    # ---------------- 文件管理 ----------------

    def test_mkdir_move_delete(self):
        d = self.path("x", "y")
        self.assertTrue(pcm._fs_op_mkdir(d)["ok"])
        src = self.path("x", "y", "a.txt")
        pcm._fs_op_write(src, "a")
        dst = self.path("x", "b.txt")
        self.assertTrue(pcm._fs_op_move(src, dst)["ok"])
        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(dst))
        self.assertFalse(pcm._fs_op_delete(self.path("x"), recursive=False)["ok"])
        self.assertTrue(pcm._fs_op_delete(self.path("x"), recursive=True)["ok"])
        self.assertFalse(os.path.exists(self.path("x")))

    def test_move_outside_cwd_allowed(self):
        src = self.path("a.txt")
        pcm._fs_op_write(src, "a")
        outside = tempfile.mkdtemp(prefix="pty_fs_out_")
        try:
            dst = os.path.join(outside, "a.txt")
            self.assertTrue(pcm._fs_op_move(src, dst)["ok"])
            self.assertTrue(os.path.exists(dst))
            self.assertFalse(os.path.exists(src))
        finally:
            shutil.rmtree(outside, ignore_errors=True)


class _FakeTransport:
    """模拟远端 server_mqtt.py：在本进程 exec 客户端发来的源码并回传 r。"""

    def __init__(self):
        self.ns = {}
        self.calls = 0

    def request(self, code, timeout=None):
        self.calls += 1
        self.ns.setdefault("__builtins__", __builtins__)
        exec(compile(code, "<remote>", "exec"), self.ns)
        return {"ok": True, "r": self.ns.get("r")}


class PtyFsRemoteTests(unittest.TestCase):
    """工具层 → transport.request → 远端 exec 的整条链路（用假 transport 离线跑）。"""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="pty_fs_remote_")
        pcm._fs_configure(False)
        self.prev = getattr(pcm.ai_bridge, "transport", None)
        self.fake = _FakeTransport()
        pcm.ai_bridge.transport = self.fake

    def tearDown(self):
        pcm.ai_bridge.transport = self.prev
        pcm._fs_configure(False)
        shutil.rmtree(self.root, ignore_errors=True)

    def test_roundtrip_through_packed_source(self):
        target = os.path.join(self.root, "r.txt")
        wrote = pcm.fs_write(target, "a\nb\n")
        self.assertTrue(wrote["ok"], wrote)
        read = pcm.fs_read(target)
        self.assertTrue(read["ok"], read)
        self.assertIn("     2→b", read["content"])
        edited = pcm.fs_edit(target, "a\n", "A\n")
        self.assertTrue(edited["ok"], edited)
        with open(target, encoding="utf-8") as f:
            self.assertEqual(f.read(), "A\nb\n")
        ls = pcm.fs_ls(self.root)
        self.assertTrue(ls["ok"], ls)
        self.assertEqual([e["name"] for e in ls["entries"]], ["r.txt"])

    def test_read_only_blocks_before_remote(self):
        pcm._fs_configure(True)
        res = pcm.fs_write(os.path.join(self.root, "x.txt"), "x")
        self.assertFalse(res["ok"])
        self.assertIn("只读", res["error"])
        self.assertEqual(self.fake.calls, 0)

    def test_without_transport_reports_error(self):
        pcm.ai_bridge.transport = None
        res = pcm.fs_read(os.path.join(self.root, "x.txt"))
        self.assertFalse(res["ok"])
        self.assertIn("未建立", res["error"])


if __name__ == "__main__":
    unittest.main()
