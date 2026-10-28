"""rpc_executor code alias 功能的独立测试。

运行方式（任选其一）:
    python tests/test_rpc_alias.py
    python -m unittest tests.test_rpc_alias -v
    pytest tests/test_rpc_alias.py -v

覆盖: #alias 增删查改、#unalias 删除、关键字 list 自定义（如 #a / #una）、
名字与语法校验、整段精确匹配替换、定义即执行、请求级 globals 透传与还原、
以及 server_http HTTP 通道端到端（含 %23 编码与 execution['r'] 回退）。
"""
import os
import sys
import unittest
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rpc_executor
from rpc_executor import ALIAS_KEYWORDS, UNALIAS_KEYWORDS, PythonExecutor


class AliasCrudTests(unittest.TestCase):
    def setUp(self):
        self.ex = PythonExecutor()

    # ---------------- 增 / 用 ---------------- #

    def test_define_and_use(self):
        res = self.ex.execute("#alias abc\n1+1")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)                      # 定义即执行
        self.assertEqual(res["alias"]["action"], "saved")
        self.assertEqual(res["alias"]["name"], "abc")

        used = self.ex.execute("abc")                      # 整段精确匹配 → 替换执行
        self.assertTrue(used["ok"])
        self.assertEqual(used["r"], 2)
        self.assertEqual(self.ex.aliases["abc"]["hits"], 1)

    def test_define_multiline_body(self):
        res = self.ex.execute("#alias sum3\nx=1\ny=2\nr=x+y")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 3)
        self.assertEqual(self.ex.execute("sum3")["r"], 3)

    def test_use_priority_over_same_name_variable(self):
        self.ex.execute("zz=40")
        self.ex.execute("#alias zz\nr=7")
        self.assertEqual(self.ex.execute("zz")["r"], 7)    # 精确匹配 → 别名优先
        self.assertEqual(self.ex.execute("zz+2")["r"], 42) # 更长表达式 → 命名空间变量

    def test_exact_match_only(self):
        self.ex.execute("#alias abc\nr=1+1")
        longer = self.ex.execute("abc+1")                  # 不是精确匹配 → 不展开
        self.assertFalse(longer["ok"])
        self.assertIn("NameError", longer["error"])

    def test_use_surrounding_whitespace(self):
        self.ex.execute("#alias w1\nr='ok'")
        self.assertEqual(self.ex.execute("  w1 \n")["r"], "ok")

    def test_await_body(self):
        res = self.ex.execute(
            "#alias slp\nimport asyncio\nr=await asyncio.sleep(0, 'done')"
        )
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], "done")
        self.assertEqual(self.ex.execute("slp")["r"], "done")

    def test_request_globals_passthrough_and_restore(self):
        # 定义即执行的那一次运行里，请求级注入变量可见；执行后不泄漏
        res = self.ex.execute("#alias greet\nr='hi '+name", globals={"name": "bob"})
        self.assertEqual(res["r"], "hi bob")
        self.assertNotIn("name", self.ex.globals_dict)
        # 事后单独使用时 name 不在 → 报 NameError 而不是悄悄换结果
        again = self.ex.execute("greet")
        self.assertFalse(again["ok"])

    # ---------------- 查 ---------------- #

    def test_show_single(self):
        self.ex.execute("#alias abc\n1+1")
        res = self.ex.execute("#alias abc")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"]["name"], "abc")
        self.assertEqual(res["r"]["code"], "1+1")
        self.assertEqual(res["r"]["hits"], 0)

    def test_show_missing(self):
        res = self.ex.execute("#alias nope")
        self.assertFalse(res["ok"])
        self.assertIn("not found", res["error"])

    def test_list_all(self):
        self.ex.execute("#alias b\n2")
        self.ex.execute("#alias a\n1")
        res = self.ex.execute("#alias")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], {"a": "1", "b": "2"})   # 排序后的 name -> code
        self.assertEqual(res["alias"]["count"], 2)

    # ---------------- 改 ---------------- #

    def test_update_overwrite(self):
        self.ex.execute("#alias abc\n1+1")
        self.ex.execute("abc")
        res = self.ex.execute("#alias abc\nr=1+2")
        self.assertEqual(res["alias"]["action"], "updated")
        self.assertEqual(self.ex.execute("abc")["r"], 3)
        info = self.ex.aliases["abc"]
        self.assertEqual(info["hits"], 2)                  # 更新保留使用计数
        self.assertLessEqual(info["created"], info["updated"])

    # ---------------- 删 ---------------- #

    def test_unalias_single_and_multi(self):
        self.ex.execute("#alias a\n1")
        self.ex.execute("#alias b\n2")
        res = self.ex.execute("#unalias a b")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"]["deleted"], ["a", "b"])
        self.assertEqual(res["r"]["missing"], [])
        self.assertNotIn("a", self.ex.aliases)
        self.assertFalse(self.ex.execute("a")["ok"])       # 删除后恢复 NameError

    def test_unalias_missing(self):
        res = self.ex.execute("#unalias nope")
        self.assertFalse(res["ok"])
        self.assertIn("not found", res["error"])

    def test_unalias_no_args(self):
        res = self.ex.execute("#unalias")
        self.assertFalse(res["ok"])
        self.assertIn("usage", res["error"])

    # ---------------- 关键字 list ---------------- #

    def test_keywords_are_mutable_lists(self):
        # 类属性与模块级 list 是同一对象：运行期改模块 list 立即生效
        self.assertIs(PythonExecutor.alias_keywords, ALIAS_KEYWORDS)
        self.assertIs(PythonExecutor.unalias_keywords, UNALIAS_KEYWORDS)

        ALIAS_KEYWORDS.append("a")
        UNALIAS_KEYWORDS.append("una")
        try:
            res = self.ex.execute("#a t1\n2+2")            # [alias, a] → #a 也生效
            self.assertTrue(res["ok"])
            self.assertEqual(res["r"], 4)
            self.assertEqual(self.ex.execute("t1")["r"], 4)

            self.assertTrue(self.ex.execute("#una t1")["ok"])  # unalias 列表同理
            self.assertNotIn("t1", self.ex.aliases)

            self.assertTrue(self.ex.execute("#alias t1\n1")["ok"])  # 原关键字仍可用
            self.ex.execute("#unalias t1")
        finally:
            ALIAS_KEYWORDS.remove("a")
            UNALIAS_KEYWORDS.remove("una")

    # ---------------- 校验 ---------------- #

    def test_invalid_name_rejected(self):
        for bad in ("1abc", "a-b", "a b"):
            res = self.ex.execute(f"#alias {bad}\n1")
            self.assertFalse(res["ok"], bad)
        self.assertEqual(self.ex.aliases, {})

    def test_python_keyword_name_rejected(self):
        res = self.ex.execute("#alias for\n1")
        self.assertFalse(res["ok"])
        self.assertNotIn("for", self.ex.aliases)

    def test_invalid_body_not_saved(self):
        res = self.ex.execute("#alias bad\n1+")
        self.assertFalse(res["ok"])
        self.assertNotIn("bad", self.ex.aliases)

    def test_too_many_names_on_directive_line(self):
        res = self.ex.execute("#alias a b\n1")
        self.assertFalse(res["ok"])

    # ---------------- alias 开关 ---------------- #

    def test_disabled_on_instance(self):
        ex = PythonExecutor(alias=False)
        res = ex.execute("#alias abc\n1+1")      # 指令行退化为普通注释
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)
        self.assertEqual(ex.aliases, {})          # 未注册
        self.assertFalse(ex.execute("abc")["ok"]) # 也不做精确匹配替换

    def test_per_call_disable(self):
        ex = PythonExecutor()                     # 默认启用
        res = ex.execute("#alias tmp\n5+5", alias=False)
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 10)
        self.assertEqual(ex.aliases, {})
        ex.execute("#alias u\nr=8")
        self.assertFalse(ex.execute("u", alias=False)["ok"])  # 关闭时不替换
        self.assertEqual(ex.execute("u")["r"], 8)             # 默认仍启用

    def test_per_call_enable_on_disabled_instance(self):
        ex = PythonExecutor(alias=False)
        res = ex.execute("#alias t\nr=3", alias=True)
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 3)
        self.assertIn("t", ex.aliases)
        self.assertEqual(ex.execute("t", alias=True)["r"], 3)
        self.assertFalse(ex.execute("t")["ok"])  # 实例级默认仍关闭

    def test_execute_default_follows_instance(self):
        self.assertTrue(PythonExecutor().alias)
        self.assertFalse(PythonExecutor(alias=False).alias)

    def test_directive_must_be_first_nonempty_line(self):
        # 指令不在首行 → 只是普通代码里的注释，不注册任何别名
        res = self.ex.execute("x=1\n#alias y\nr=x+1")
        self.assertTrue(res["ok"])
        self.assertEqual(res["r"], 2)
        self.assertNotIn("y", self.ex.aliases)

    def test_leading_blank_lines_still_directive(self):
        self.ex.execute("\n\n#alias late\nr=9")
        self.assertEqual(self.ex.execute("late")["r"], 9)

    def test_space_after_hash_tolerated(self):
        self.ex.execute("# alias sp\nr=5")
        self.assertEqual(self.ex.execute("sp")["r"], 5)


class AliasHttpChannelTests(unittest.TestCase):
    """端到端：真实起一个 server_http，走 URL-as-code 通道。"""

    @classmethod
    def setUpClass(cls):
        import server_http
        cls.server_http = server_http
        # port=0 让系统分配空闲端口；log_sink 吞掉请求日志保持输出干净
        cls.server = server_http.start_rpc_server(
            port=0, key='', globals={}, daemon=True,
            log_sink=lambda line: None,
        )
        cls.port = cls.server.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    @classmethod
    def call(cls, code):
        url = f"http://127.0.0.1:{cls.port}/" + urllib.parse.quote(code, safe='')
        return urllib.request.urlopen(url, timeout=10).read().decode('utf-8')

    def test_roundtrip(self):
        # execution['r'] 回退修复：尾表达式直接返回，与 MQTT 通道对齐
        self.assertEqual(self.call("1+1").strip(), "2")

        # %23 / %0A 编码端到端：定义即执行
        self.assertEqual(self.call("#alias sum2\n1+1").strip(), "2")
        # 使用
        self.assertEqual(self.call("sum2").strip(), "2")
        # 查询：pformat 后能回显 body
        self.assertIn("1+1", self.call("#alias sum2"))

        # 删除后再用 → 500 NameError
        self.assertIn("deleted", self.call("#unalias sum2"))
        with self.assertRaises(urllib.error.HTTPError):
            self.call("sum2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
