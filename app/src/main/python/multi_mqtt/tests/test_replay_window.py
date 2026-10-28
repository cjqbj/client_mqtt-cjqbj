# -*- coding: utf-8 -*-
"""[S2 重放窗口] MultiMQTTManager._on_message 验签通过后的 timestamp 新鲜度校验测试。

背景：公共 broker 允许 ``#`` 通配订阅，合法签名帧可被被动嗅探；原先唯一
重放防线是 30s 的 dedup TTLCache，TTL 过期后同一签名帧可原样重发并通过
验签。现在验签通过后额外校验 ``|now - timestamp| <= timestamp_window``
（默认 120s，构造参数可调，<=0 关闭）。

全部用本机内存对象直接调用 _on_message，不连任何 broker。
"""
import hashlib
import json
import queue
import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ecdsa
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
        old = (time.time() - 600) * 1000   # 10 分钟前，超出默认 120s 窗口
        _deliver(m, _signed_frame(sk, "stale1", "1+1", old))
        got = _drained(m)
        self.assertEqual(len(got), 1, "拒收帧必须投递给上层（带标记），不能静默吞掉")
        reject = got[0][1].get("_cmq_timestamp_reject")
        self.assertIsNotNone(reject)
        self.assertEqual(reject["reason"], "timestamp_window")
        self.assertEqual(reject["window"], 120.0)
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

    def test_default_window_is_120s(self):
        m, _ = _make_manager()
        self.assertEqual(m.timestamp_window, 120.0)


if __name__ == "__main__":
    unittest.main()
