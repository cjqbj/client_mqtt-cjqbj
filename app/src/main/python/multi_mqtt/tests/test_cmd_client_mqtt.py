# -*- coding: utf-8 -*-
"""cmd_client_mqtt REPL 解析函数测试。

覆盖两个审计修复：
- _parse_watch：`watch df -h` 旧实现丢首词 "df"（split(None,2) 只取
  toks[2]），实际周期执行 "-h"；
- _parse_grep_opts：`%grep --include`（缺模式值）旧实现对空列表 pop
  抛 IndexError 穿透 REPL 主循环直接退出。
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from client import cmd_client_mqtt as ccm


class ParseWatchTests(unittest.TestCase):
    def test_single_word_command(self):
        self.assertEqual(ccm._parse_watch("watch df"), (1.0, "df"))

    def test_multi_word_command_keeps_first_word(self):
        # 回归：旧实现返回 (1.0, "-h")，"df" 被整个丢掉
        self.assertEqual(ccm._parse_watch("watch df -h"), (1.0, "df -h"))

    def test_long_command_preserves_tail(self):
        self.assertEqual(ccm._parse_watch("watch df -h extra"),
                         (1.0, "df -h extra"))

    def test_interval_flag(self):
        self.assertEqual(ccm._parse_watch("watch -n 2 df -h"),
                         (2.0, "df -h"))

    def test_interval_flag_float(self):
        self.assertEqual(ccm._parse_watch("watch -n 0.5 uptime"),
                         (0.5, "uptime"))

    def test_interval_without_command(self):
        self.assertIsNone(ccm._parse_watch("watch -n"))

    def test_interval_bad_number(self):
        self.assertIsNone(ccm._parse_watch("watch -n abc df"))

    def test_bare_watch(self):
        self.assertIsNone(ccm._parse_watch("watch"))
        self.assertIsNone(ccm._parse_watch("  watch  "))


class ParseGrepOptsTests(unittest.TestCase):
    def test_defaults(self):
        toks = ["PAT"]
        opts, err = ccm._parse_grep_opts(toks)
        self.assertIsNone(err)
        self.assertEqual(opts, {"ignore_case": False, "fixed": False,
                                "recursive": True, "include": None})
        self.assertEqual(toks, ["PAT"])

    def test_flags_consumed(self):
        toks = ["-i", "-F", "-r", "PAT", "path"]
        opts, err = ccm._parse_grep_opts(toks)
        self.assertIsNone(err)
        self.assertTrue(opts["ignore_case"])
        self.assertTrue(opts["fixed"])
        self.assertTrue(opts["recursive"])
        self.assertEqual(toks, ["PAT", "path"])

    def test_include_takes_value(self):
        toks = ["--include", "*.py", "PAT"]
        opts, err = ccm._parse_grep_opts(toks)
        self.assertIsNone(err)
        self.assertEqual(opts["include"], "*.py")
        self.assertEqual(toks, ["PAT"])

    def test_include_without_value_is_error(self):
        # 回归：旧实现 toks.pop(0) 对空列表抛 IndexError，REPL 整个退出
        toks = ["--include"]
        opts, err = ccm._parse_grep_opts(toks)
        self.assertIsNone(opts)
        self.assertIn("--include", err)

    def test_dash_alone_stops_option_parsing(self):
        toks = ["-", "PAT"]
        opts, err = ccm._parse_grep_opts(toks)
        self.assertIsNone(err)
        self.assertEqual(toks, ["-", "PAT"])


if __name__ == "__main__":
    unittest.main()
