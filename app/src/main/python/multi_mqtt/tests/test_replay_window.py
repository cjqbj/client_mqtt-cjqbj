# -*- coding: utf-8 -*-
"""[S2 重放窗口] MultiMQTTManager._on_message 验签通过后的 timestamp 新鲜度校验测试。

背景：公共 broker 允许 ``#`` 通配订阅，合法签名帧可被被动嗅探；原先唯一
重放防线是 30s 的 dedup TTLCache，TTL 过期后同一签名帧可原样重发并通过
验签。现在验签通过后额外校验 ``|now - timestamp| <= timestamp_window``
（默认 400s，构造参数可调，<=0 关闭）。

另一条同源防线：dedup TTL 必须覆盖整个重放窗口（``window + 30s`` 余量）。
公共 broker 会把同一帧投递多份且可能迟到 1~4 分钟；若去重仍固定 30s，
30s~window 之间的迟到副本会躲过去重、又通过新鲜度检查，导致同一 code
被执行两次（2026-10-30 真机日志实测）。

全部用本机内存对象直接调用 _on_message，不连任何 broker。
"""
import hashlib
import json
import queue
import sys
import time
import types
import unittest
import unittest.mock as mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ecdsa
import multi_mqtt
from multi_mqtt import MultiMQTTManager


def _make_manager(**ka):
    """生成一对临时 ECDSA 密钥并构造离线 manager（不 start、不联网）。"""
    sk = ecdsa.SigningKey.generate(curve=ecdsa.NIST256p)
    m = MultiMQTTManager(
        brokers=[],
        server_public_key_bytes=sk.get_verifying_key().to_pem(),
        client_private_key_bytes=sk.to_pem(),
        enable_stats=False,
        log_connection=False,
        recovery_enabled=False,
        **ka,
    )
    m.set_on_message(lambda *_a: None)   # 入队前提：上层回调已注册
    return m, sk


def _signed_frame(sk, req_id, code, ts_ms):
    """按 publish_broadcast 的签名协议构造一条已签名请求帧。"""
    ts_str = str(int(float(ts_ms)))   # 与 _on_message 拼签名串时的口径一致
    sign_msg = f"{req_id}|{code}|{ts_str}".encode("utf-8")
    sig = sk.sign(sign_msg, hashfunc=hashlib.sha256)
    return {
        "req_id": f"{req_id}|{sig.hex()}",
        "code": code,
        "timestamp": ts_ms,
        "reply_topic": "test/reply",
    }


def _deliver(m, data):
    msg = types.SimpleNamespace(topic="test/req",
                                payload=json.dumps(data).encode("utf-8"))
    m._on_message(None, "fake-host", msg)


def _drained(m):
    out = []
    while True:
        try:
            out.append(m._msg_queue.get_nowait())
        except queue.Empty:
            return out


class ReplayWindowTests(unittest.TestCase):
    def test_fresh_timestamp_passes(self):
        m, sk = _make_manager()
        _deliver(m, _signed_frame(sk, "fresh1", "1+1", time.time() * 1000))
        got = _drained(m)
        self.assertEqual(len(got), 1, "新鲜 timestamp 的合法签名帧应被放行")
        self.assertEqual(got[0][1]["code"], "1+1")
        self.assertNotIn("_cmq_timestamp_reject", got[0][1],
                         "放行帧不得携带拒收标记")

    def test_stale_timestamp_rejected_with_marker(self):
        """过期 timestamp 不再静默丢弃：带 _cmq_timestamp_reject 标记继续投递，
        由 server_mqtt.handle_message 按标准回包格式向 reply_topic 报错。"""
        m, sk = _make_manager()
        old = (time.time() - 600) * 1000   # 10 分钟前，超出默认 400s 窗口
        _deliver(m, _signed_frame(sk, "stale1", "1+1", old))
        got = _drained(m)
        self.assertEqual(len(got), 1, "拒收帧必须投递给上层（带标记），不能静默吞掉")
        reject = got[0][1].get("_cmq_timestamp_reject")
        self.assertIsNotNone(reject)
        self.assertEqual(reject["reason"], "timestamp_window")
        self.assertEqual(reject["window"], 400.0)
        self.assertIn("replay window", reject["error"])
        self.assertIn("server_now_ms", reject)

    def test_future_timestamp_rejected_with_marker(self):
        m, sk = _make_manager()
        future = (time.time() + 600) * 1000
        _deliver(m, _signed_frame(sk, "fut1", "1+1", future))
        got = _drained(m)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0][1]["_cmq_timestamp_reject"]["reason"],
                         "timestamp_window")

    def test_wider_window_accepts_older_frame(self):
        m, sk = _make_manager(timestamp_window=3600.0)
        old = (time.time() - 1800) * 1000   # 30 分钟前，在 1h 窗口内
        _deliver(m, _signed_frame(sk, "wide1", "1+1", old))
        got = _drained(m)
        self.assertEqual(len(got), 1, "窗口参数应被尊重")
        self.assertNotIn("_cmq_timestamp_reject", got[0][1])

    def test_window_disabled_accepts_old_frame(self):
        m, sk = _make_manager(timestamp_window=0)
        old = (time.time() - 3600) * 1000
        _deliver(m, _signed_frame(sk, "off1", "1+1", old))
        got = _drained(m)
        self.assertEqual(len(got), 1, "timestamp_window<=0 应关闭校验（兼容场景）")
        self.assertNotIn("_cmq_timestamp_reject", got[0][1])

    def test_missing_timestamp_rejected_when_window_on(self):
        m, sk = _make_manager()
        frame = _signed_frame(sk, "nots1", "1+1", time.time() * 1000)
        # 签名串里的 ts 与帧内一致；这里再把帧内 timestamp 剔除，模拟缺字段
        frame.pop("timestamp")
        _deliver(m, frame)
        self.assertEqual(
            _drained(m), [],
            "窗口开启时缺失/非法 timestamp 的已验签帧应被拒绝")

    def test_unsigned_mode_not_affected(self):
        """未配置公钥（不验签）的 manager 不受窗口影响——窗口只保护已验签路径。"""
        m = MultiMQTTManager(brokers=[], enable_stats=False,
                             log_connection=False, recovery_enabled=False)
        m.set_on_message(lambda *_a: None)
        old = (time.time() - 3600) * 1000
        _deliver(m, {"req_id": "unsigned1", "code": "1+1", "timestamp": old})
        self.assertEqual(len(_drained(m)), 1)

    def test_bad_signature_still_rejected(self):
        m, sk = _make_manager()
        frame = _signed_frame(sk, "bad1", "1+1", time.time() * 1000)
        frame["code"] = "2+2"   # 篡改 code，签名不再匹配
        _deliver(m, frame)
        self.assertEqual(_drained(m), [], "签名校验本身仍必须生效")

    def test_default_window_is_400s(self):
        # 公网 broker 实测同一帧迟到可达 230s（2026-10-30），120s 会把正常的
        # 迟到副本当重放拒收；默认放宽到 400s 留出余量。
        m, _ = _make_manager()
        self.assertEqual(m.timestamp_window, 400.0)


def _patch_clock(clock):
    """把 multi_mqtt 模块内的 time 引用换成可手动推进的假时钟。

    只替换模块属性，不动全局 time 模块，logging 等其它组件仍用真实时间。
    """
    return mock.patch.object(
        multi_mqtt, "time", types.SimpleNamespace(time=lambda: clock[0]))


class DedupWindowCoverageTests(unittest.TestCase):
    """去重 TTL 必须覆盖重放窗口。

    公共 broker 的同一帧副本可能迟到 1~4 分钟。TTL 若仍为 30s，落在
    30s~window 之间的副本会躲过去重、又通过新鲜度检查 → 同一 code 二次执行。
    """

    def test_default_ttl_covers_replay_window(self):
        m, _ = _make_manager()
        self.assertEqual(m.timestamp_window, 400.0)
        self.assertGreaterEqual(
            m.dedup_cache.ttl, m.timestamp_window,
            "dedup TTL 不得短于重放窗口，否则窗口内的迟到副本会被二次执行")
        self.assertGreaterEqual(m.dedup_cache.ttl, m.timestamp_window + 30.0,
                                "应给窗口留出 30s 余量，默认即 430s")

    def test_ttl_follows_wider_window(self):
        m, _ = _make_manager(timestamp_window=3600.0)
        self.assertGreaterEqual(m.dedup_cache.ttl, 3600.0,
                                "调宽窗口时必须同步放大去重 TTL")

    def test_ttl_stays_default_when_window_disabled(self):
        m, _ = _make_manager(timestamp_window=0)
        self.assertEqual(m.dedup_cache.ttl, 30.0,
                         "关闭重放窗口时保持原有 30s 去重语义")

    def test_late_duplicate_within_window_executes_only_once(self):
        """核心回归：100s 才到的第二份副本（>30s 旧 TTL、<120s 窗口）。"""
        m, sk = _make_manager()
        clock = [1000.0]
        frame = _signed_frame(sk, "late-dup-1", "side_effect()",
                              clock[0] * 1000)
        with _patch_clock(clock):
            _deliver(m, frame)
            self.assertEqual(len(_drained(m)), 1, "首份副本应正常执行")
            clock[0] += 100.0
            _deliver(m, frame)
            self.assertEqual(
                _drained(m), [],
                "迟到 100s 的同一副本必须被去重，不得二次执行同一 code")

    def test_three_broker_copies_within_window_execute_once(self):
        """多 broker 广播的 3 份副本（0s / 5s / 119s）只执行一次。"""
        m, sk = _make_manager()
        clock = [1000.0]
        frame = _signed_frame(sk, "late-dup-3", "side_effect()",
                              clock[0] * 1000)
        with _patch_clock(clock):
            _deliver(m, frame)
            clock[0] += 5.0
            _deliver(m, frame)
            clock[0] += 114.0        # 距首份 119s：仍在新窗口内
            _deliver(m, frame)
            self.assertEqual(len(_drained(m)), 1,
                             "窗口内所有重复副本都应被静默去重")

    def test_wide_window_late_duplicate_executes_only_once(self):
        """窗口调到 1h 时，1800s 的迟到副本同样只能执行一次。"""
        m, sk = _make_manager(timestamp_window=3600.0)
        clock = [1000.0]
        frame = _signed_frame(sk, "late-dup-wide", "side_effect()",
                              clock[0] * 1000)
        with _patch_clock(clock):
            _deliver(m, frame)
            self.assertEqual(len(_drained(m)), 1)
            clock[0] += 1800.0       # 在 3600s 窗口内，远超旧的 30s 去重 TTL
            _deliver(m, frame)
            self.assertEqual(_drained(m), [],
                             "宽窗口下迟到副本仍必须被去重")

    def test_duplicate_beyond_window_still_gets_explicit_reject(self):
        """超出窗口的迟到副本仍要明确拒收（带标记），不能被静默吞掉。

        显式把窗口收敛到 120s（默认已放宽到 400s），200s 副本既超窗、
        也超出 150s 去重 TTL，才能走到拒收分支。
        """
        m, sk = _make_manager(timestamp_window=120.0)
        clock = [1000.0]
        frame = _signed_frame(sk, "late-dup-2", "side_effect()",
                              clock[0] * 1000)
        with _patch_clock(clock):
            _deliver(m, frame)
            self.assertEqual(len(_drained(m)), 1)
            clock[0] += 200.0        # 超出 120s 窗口，且超过 window+30s 的 TTL
            _deliver(m, frame)
            got = _drained(m)
            self.assertEqual(
                len(got), 1,
                "超窗副本必须投递给上层（带拒收标记），保留不静默的诊断语义")
            self.assertEqual(
                got[0][1]["_cmq_timestamp_reject"]["reason"], "timestamp_window")

    def test_different_requests_in_same_window_not_blocked(self):
        """TTL 放大不得误杀不同 req_id 的正常请求。"""
        m, sk = _make_manager()
        clock = [1000.0]
        with _patch_clock(clock):
            _deliver(m, _signed_frame(sk, "req-a", "a()", clock[0] * 1000))
            clock[0] += 1.0
            _deliver(m, _signed_frame(sk, "req-b", "b()", clock[0] * 1000))
            got = _drained(m)
            self.assertEqual(len(got), 2, "不同 req_id 的合法请求不得被去重误杀")
            self.assertEqual([item[1]["code"] for item in got], ["a()", "b()"])


if __name__ == "__main__":
    unittest.main()
