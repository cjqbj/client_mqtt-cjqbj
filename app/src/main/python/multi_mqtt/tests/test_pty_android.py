#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Android / Chaquopy 设备的 PTY 适配测试。

背景
----
Android 的 Python 运行时没有 ``os.openpty``（也没有 ``pty.forkpty``）。标准库
``pty.py`` 在缺失 ``os.openpty`` 时会退到 ``_open_terminal()``，去扫描
``/dev/pty??`` 这种 BSD 老接口；Android 只有 Unix98 的 ``/dev/ptmx``，于是必然
抛 ``OSError('out of pty devices')``。``_PTY_START_TEMPLATE`` 里的
``_pty_openpty()`` 因此按 ``os.openpty -> libc openpty -> /dev/ptmx`` 分级兜底。

测试分两部分：
1. ``PortableOpenptyTests`` —— 全平台可跑。从 build_pty_start_code() 生成的
   远端代码里抽出 ``_pty_openpty()`` 函数源码，用假模块注入，覆盖：
   - 平台原生 ``os.openpty`` 可用时直接复用
   - Android 缺 ``os.openpty`` 时走 libc ``openpty``
   - libc 也不可用时走 Unix98 ``/dev/ptmx`` + 数值 ioctl
   - Unix98 打开 slave 失败时关闭已打开的 master fd，不泄漏
2. ``AndroidDevicePtyTests`` —— 真机集成测试，默认跳过。需要设置：
   ``PTY_ANDROID_TOPIC``（设备 request topic）、``PTY_ANDROID_KEY``（私钥或
   整数表达式）。设备标识与凭据只从环境变量读，不写进文件。

运行：
    cd multi_mqtt && python -m unittest tests.test_pty_android -v

真机：
    PTY_ANDROID_TOPIC=<topic> PTY_ANDROID_KEY=<key> \\
        python -m unittest tests.test_pty_android -v
"""
import ast
import os
import struct
import sys
import textwrap
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.remote_cmd import build_pty_start_code  # noqa: E402

# 任意占位 payload 即可：这里只关心模板里的 _pty_openpty() 源码
_START_CODE = build_pty_start_code(
    {"sid": "pty-android-test", "in_topic": "pty/t/in", "out_topic": "pty/t/out"})

TIOCSPTLCK = 0x40045431   # unlockpt
TIOCGPTN = 0x80045430     # 取 slave 编号


def _extract_openpty_src() -> str:
    """从生成的远端 PTY 代码里抽出 _pty_openpty 的源码。"""
    tree = ast.parse(_START_CODE)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_pty_openpty":
            segment = ast.get_source_segment(_START_CODE, node)
            if segment:
                return textwrap.dedent(segment)
    raise AssertionError("模板里找不到 _pty_openpty()")


class _FakeOS:
    """模拟 os 模块；不传 openpty 参数即模拟 Android 缺 os.openpty。"""

    O_RDWR = os.O_RDWR
    O_NOCTTY = getattr(os, "O_NOCTTY", 0)

    def __init__(self, openpty_result=None, opens=()):
        if openpty_result is not None:
            if isinstance(openpty_result, Exception):
                self.openpty = mock.Mock(side_effect=openpty_result)
            else:
                self.openpty = mock.Mock(return_value=openpty_result)
        self._opens = list(opens)
        self.opened = []
        self.closed = []

    def open(self, path, flags):
        self.opened.append((path, flags))
        if not self._opens:
            raise OSError(6, "No such device or address")
        item = self._opens.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self, fd):
        self.closed.append(fd)


class _FakeFC:
    def __init__(self, pts_no=7):
        self.pts_no = pts_no
        self.calls = []

    def ioctl(self, fd, request, arg):
        self.calls.append((fd, request))
        if request == TIOCGPTN:
            return struct.pack("I", self.pts_no)
        return None


class _FakeLibc:
    def __init__(self, master_fd=21, slave_fd=22, rc=0):
        self.master_fd = master_fd
        self.slave_fd = slave_fd
        self.rc = rc

    def openpty(self, pm, ps, *rest):
        if self.rc == 0:
            pm._obj.value = self.master_fd
            ps._obj.value = self.slave_fd
        return self.rc


class PortableOpenptyTests(unittest.TestCase):
    """_pty_openpty() 的分级兜底逻辑（不依赖真实设备）。"""

    def setUp(self):
        self.src = _extract_openpty_src()

    def _load(self, fake_os, fake_fc=None):
        ns = {
            "_o": fake_os,
            "_fc": fake_fc if fake_fc is not None else _FakeFC(),
            "_st": struct,
        }
        exec(compile(self.src, "<pty_openpty>", "exec"), ns)
        return ns["_pty_openpty"]

    def test_prefers_native_os_openpty(self):
        fake_os = _FakeOS(openpty_result=(11, 12))
        self.assertEqual(self._load(fake_os)(), (11, 12))
        self.assertEqual(fake_os.opened, [])

    def test_native_os_openpty_oserror_falls_back(self):
        fake_os = _FakeOS(openpty_result=OSError(6, "nope"), opens=[31, 32])
        with mock.patch("ctypes.CDLL", side_effect=OSError("no libc")):
            self.assertEqual(self._load(fake_os)(), (31, 32))

    def test_android_without_os_openpty_uses_libc(self):
        fake_os = _FakeOS()
        with mock.patch("ctypes.CDLL", return_value=_FakeLibc()):
            self.assertEqual(self._load(fake_os)(), (21, 22))
        self.assertEqual(fake_os.opened, [])

    def test_unix98_fallback_when_libc_unavailable(self):
        fake_os = _FakeOS(opens=[41, 42])
        fake_fc = _FakeFC(pts_no=9)
        with mock.patch("ctypes.CDLL", side_effect=OSError("no libc")):
            self.assertEqual(self._load(fake_os, fake_fc)(), (41, 42))
        self.assertEqual(fake_os.opened,
                         [("/dev/ptmx", os.O_RDWR | getattr(os, "O_NOCTTY", 0)),
                          ("/dev/pts/9", os.O_RDWR | getattr(os, "O_NOCTTY", 0))])
        self.assertEqual([c[1] for c in fake_fc.calls], [TIOCSPTLCK, TIOCGPTN])

    def test_unix98_closes_master_when_slave_open_fails(self):
        fake_os = _FakeOS(opens=[51, OSError(6, "no pts node")])
        with mock.patch("ctypes.CDLL", side_effect=OSError("no libc")):
            with self.assertRaises(OSError):
                self._load(fake_os)()
        self.assertEqual(fake_os.closed, [51])

    def test_template_calls_portable_helper_only(self):
        self.assertIn("_mfd, _sfd = _pty_openpty()", _START_CODE)
        self.assertNotIn("_pty.openpty()", _START_CODE)
        self.assertIn("0x40045431", _START_CODE)
        self.assertIn("0x80045430", _START_CODE)


_ANDROID_TOPIC = os.environ.get("PTY_ANDROID_TOPIC", "").strip()
_ANDROID_KEY = os.environ.get("PTY_ANDROID_KEY", "").strip()


@unittest.skipUnless(_ANDROID_TOPIC and _ANDROID_KEY,
                     "未设置 PTY_ANDROID_TOPIC / PTY_ANDROID_KEY，跳过真机测试")
class AndroidDevicePtyTests(unittest.TestCase):
    """真机端到端：Android 设备上开 PTY、执行命令、正常退出。"""

    def test_open_echo_exit(self):
        from client.cmd_client_mqtt import MqttTransport
        from client.remote_cmd import RemotePty

        out = bytearray()
        tr = MqttTransport(request_topic=_ANDROID_TOPIC,
                           private_key=_ANDROID_KEY, allow_no_pub=True)
        pty = RemotePty(tr, timeout=20)
        try:
            env = pty.open(24, 80, on_data=out.extend, heartbeat=2)
            self.assertTrue(env.get("ok"), env)

            pty.send(b"printf 'RESULT_%s\\n' 424242\n")
            deadline = time.time() + 8
            while time.time() < deadline and b"RESULT_424242" not in out:
                time.sleep(0.1)
            self.assertIn(b"RESULT_424242", out,
                          "命令没有回显，PTY 输出链路异常")

            pty.send(b"exit\n")
            self.assertTrue(pty.wait_end(10), "远端 shell 未正常结束")
            self.assertEqual(pty.end_reason, "exit")
        finally:
            pty.close()
            tr.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
