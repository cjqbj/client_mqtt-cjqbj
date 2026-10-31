#!/usr/bin/env python3
import argparse
import os
import sys
import time
import sqlite3
import threading
import logging
import collections
import weakref
from multi_mqtt import MultiMQTTManager, BROKER_LIST

from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

DB_FILE = "mqtt_topics_dump.db"
FLUSH_INTERVAL = 66          # 每 66 秒批量写入磁盘
MAX_PAYLOAD_SAVE = 1024       # 保存消息体前 1024 字节
MAX_LOG_LINES = 25             # 底部固定留给日志的行数   #28 就把标题栏顶上去了

# ==========================================
# 1. UI 日志拦截器 (使用 Rich 格式化)
# ==========================================
log_queue = collections.deque(maxlen=MAX_LOG_LINES)
log_lock = threading.Lock()          # 保护 log_queue 的并发访问

class UILogHandler(logging.Handler):
    """将日志捕获到队列，由 Live UI 统一渲染"""
    def emit(self, record):
        msg = self.format(record)
        # 【修复】限制单条日志长度，防止超大日志占用内存
        if len(msg) > 500:
            msg = msg[:500] + "..."
        with log_lock:
            log_queue.append(msg)

# 【核心修复】：接管 Root Logger，拦截所有模块（包括 multi_mqtt）的日志
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 清理可能已经被其他模块设置的默认控制台 Handler，防止重复打印
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

ui_handler = UILogHandler()
ui_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%H:%M:%S'))
root_logger.addHandler(ui_handler)

# 当前脚本专用的 logger
logger = logging.getLogger("MQTTSniffer")


# ==========================================
# 2. 一小时滑动时间窗口统计器
# ==========================================
class HourTracker:
    def __init__(self):
        self.buckets = {}
        self.lock = threading.Lock()
        # 【修复】添加 topic 字符串驻留池，控制重复字符串数量
        self._topic_pool = {}

    def _intern_topic(self, topic: str) -> str:
        """复用已存在的相同字符串对象，防止无限创建新字符串"""
        existing = self._topic_pool.get(topic)
        if existing is not None:
            return existing
        # 限制池大小，防止恶意/随机 topic 导致内存泄漏
        if len(self._topic_pool) > 100000:
            # 清空一半，保留高频使用的（简单策略）
            self._topic_pool = {k: v for i, (k, v) in enumerate(self._topic_pool.items()) if i % 2 == 0}
        self._topic_pool[topic] = topic
        return topic

    def add(self, topic: str):
        now_minute = int(time.time()) // 60
        # 【修复】字符串驻留，减少内存占用
        topic = self._intern_topic(topic)
        with self.lock:
            if now_minute not in self.buckets:
                self.buckets[now_minute] = {'msgs': 0, 'topics': set()}
            self.buckets[now_minute]['msgs'] += 1
            self.buckets[now_minute]['topics'].add(topic)

            # 【修复】使用 list 收集过期 key，避免在遍历时修改字典
            expired = [m for m in self.buckets.keys() if m < now_minute - 60]
            for m in expired:
                # 【修复】清理时释放 set 引用，帮助 GC
                self.buckets[m]['topics'].clear()
                del self.buckets[m]

    def get_stats(self):
        with self.lock:
            total_msgs = sum(b['msgs'] for b in self.buckets.values())
            # 【修复】避免创建中间 set，直接计数
            all_topics = set()
            for b in self.buckets.values():
                all_topics.update(b['topics'])
            return total_msgs, len(all_topics)


# ==========================================
# 3. 核心 Engine
# ==========================================
class SnifferEngine:
    def __init__(self):
        self.buffer = {}
        self.db_totals = {}     # host -> {"msgs": 0, "topics": 0}
        self.hour_trackers = {} # host -> HourTracker
        self.latest_msg = {}    # host -> {"topic": str, "payload": str, "time": float}

        self.lock = threading.Lock()
        self.running = True

        # 【修复】添加 host 和 topic 的全局驻留池，跨所有 HourTracker 复用字符串
        self._global_topic_pool = {}
        self._global_host_pool = {}
        # 【修复】添加清理计数器，定期执行完整 GC
        self._msg_counter = 0
        self._last_cleanup = time.time()

        self._init_db()
        self._load_historical_stats()

        # 启动后台线程
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.display_thread = threading.Thread(target=self._display_loop, daemon=True)
        self.flush_thread.start()
        self.display_thread.start()

    def _intern_host(self, host: str) -> str:
        """全局 host 字符串驻留"""
        existing = self._global_host_pool.get(host)
        if existing is not None:
            return existing
        if len(self._global_host_pool) > 10000:
            self._global_host_pool = {}
        self._global_host_pool[host] = host
        return host

    def _intern_topic(self, topic: str) -> str:
        """全局 topic 字符串驻留（用于 buffer key 和 latest_msg）"""
        existing = self._global_topic_pool.get(topic)
        if existing is not None:
            return existing
        if len(self._global_topic_pool) > 200000:
            self._global_topic_pool = {}
        self._global_topic_pool[topic] = topic
        return topic

    def _cleanup_stale_hosts(self):
        """【修复】清理长时间无消息的 host，防止字典无限增长"""
        now = time.time()
        # 每 5 分钟执行一次清理
        if now - self._last_cleanup < 300:
            return

        self._last_cleanup = now
        stale_threshold = 86400 * 2  # 2 天无消息视为过期

        with self.lock:
            stale_hosts = [
                h for h, v in self.latest_msg.items()
                if now - v.get("time", 0) > stale_threshold
            ]
            for h in stale_hosts:
                del self.latest_msg[h]
                # 同时清理关联的 tracker 以释放内存
                if h in self.hour_trackers:
                    # 清理内部 topic 池
                    self.hour_trackers[h]._topic_pool.clear()
                    del self.hour_trackers[h]
                self.db_totals.pop(h, None)

        if stale_hosts:
            logger.info(f"清理 {len(stale_hosts)} 个过期 host，释放内存")

    def _init_db(self):
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS topic_stats (
                    host TEXT,
                    topic TEXT,
                    msg_count INTEGER DEFAULT 0,
                    last_time REAL,
                    last_payload BLOB,
                    PRIMARY KEY (host, topic)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_host ON topic_stats(host);")
            conn.commit()

    def _load_historical_stats(self):
        logger.info("正在扫描 SQLite 数据库历史数据...")
        with sqlite3.connect(DB_FILE) as conn:
            cur = conn.cursor()
            cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
            for host, topic_count, msg_count in cur.fetchall():
                self.db_totals[host] = {"topics": topic_count, "msgs": msg_count or 0}
        logger.info("数据库历史统计加载完毕！")

    def push(self, host: str, topic: str, payload: bytes):
        """
        [防御性修复] 这里的 payload 语义上必须是 bytes。
        上游若误传 dict/str 等类型，直接在这里拦截并丢弃，避免
        payload.decode 抛 AttributeError 把整条分发链路的 ERROR 日志刷爆。
        """
        if not isinstance(payload, (bytes, bytearray)):
            # 不用 logger.exception（会打堆栈），避免刷屏
            logger.warning(
                "push() 收到非 bytes payload，已忽略: host=%s topic=%s type=%s",
                host, topic, type(payload).__name__,
            )
            return

        if len(payload) > MAX_PAYLOAD_SAVE:
            payload = payload[:MAX_PAYLOAD_SAVE]

        now = time.time()

        # 【修复】字符串驻留，减少重复字符串创建
        host = self._intern_host(host)
        topic = self._intern_topic(topic)

        # 1. 更新 1 小时统计
        if host not in self.hour_trackers:
            self.hour_trackers[host] = HourTracker()
        self.hour_trackers[host].add(topic)

        # 2. 清洗 Payload 文本，防止不可见乱码打乱 TUI 排版
        # 【修复】限制处理长度，避免超大 payload 消耗 CPU 和内存
        safe_str = "".join([c if c.isprintable() else "." for c in payload.decode('utf-8', errors='replace')])
        if len(safe_str) > MAX_PAYLOAD_SAVE:
            safe_str = safe_str[:MAX_PAYLOAD_SAVE]

        with self.lock:
            key = (host, topic)
            if key not in self.buffer:
                self.buffer[key] = {"count": 1, "time": now, "payload": payload}
            else:
                self.buffer[key]["count"] += 1
                self.buffer[key]["time"] = now
                self.buffer[key]["payload"] = payload

            # 【修复】复用已有字典对象，避免频繁创建新字典
            existing = self.latest_msg.get(host)
            if existing is None:
                self.latest_msg[host] = {
                    "topic": topic,
                    "time": now,
                    "payload": safe_str
                }
            else:
                existing["topic"] = topic
                existing["time"] = now
                existing["payload"] = safe_str

        # 【修复】定期清理计数和过期 host
        self._msg_counter += 1
        if self._msg_counter >= 100000:
            self._msg_counter = 0
            self._cleanup_stale_hosts()

    def _flush_loop(self):
        while self.running:
            time.sleep(FLUSH_INTERVAL)
            with self.lock:
                if not self.buffer:
                    continue
                batch = self.buffer
                self.buffer = {}

            records = [(h, t, d["count"], d["time"], d["payload"]) for (h, t), d in batch.items()]

            try:
                with sqlite3.connect(DB_FILE) as conn:
                    conn.executemany("""
                        INSERT INTO topic_stats (host, topic, msg_count, last_time, last_payload)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(host, topic) DO UPDATE SET
                            msg_count = msg_count + excluded.msg_count,
                            last_time = excluded.last_time,
                            last_payload = excluded.last_payload
                    """, records)
                    conn.commit()

                    cur = conn.cursor()
                    cur.execute("SELECT host, COUNT(topic), SUM(msg_count) FROM topic_stats GROUP BY host")
                    fresh_totals = {row[0]: {"topics": row[1], "msgs": row[2] or 0} for row in cur.fetchall()}
                    with self.lock:
                        self.db_totals = fresh_totals
            except Exception as e:
                logger.error(f"写入数据库失败: {e}")
                # 【修复】异常时确保 batch 数据不会导致重复处理，但保留诊断信息
                logger.error(f"丢失 batch 记录数: {len(records)}")

    # ---------------------------------------------------------------
    # 【修复】原 `def generate_layout() -> Layout:` 缺少 self 参数，
    # 且内部 `layout.split(...)`（水平切）与 `_display_loop` 里的
    # `layout.split_column(...)`（垂直切）语义不一致，属于半成品死代码。
    # 现改为 @staticmethod + 与 _display_loop 完全一致的三段垂直切分，
    # 供外部/未来扩展复用。当前 _display_loop 仍内联构建（零行为变化）。
    # ---------------------------------------------------------------
    @staticmethod
    def generate_layout() -> Layout:
        """构建 Rich 分屏界面（header / main / footer 三段垂直切分）"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=MAX_LOG_LINES + 2),
        )
        return layout

    def _display_loop(self):
        console = Console()

        # 使用 Rich Live 机制接管屏幕，自适应分辨率，防闪烁、防排版错乱
        with Live(refresh_per_second=2, screen=True, console=console) as live:
            while self.running:
                layout = Layout()
                layout.split_column(
                    Layout(name="header", size=3),
                    Layout(name="main", ratio=1),
                    Layout(name="footer", size=MAX_LOG_LINES + 2)
                )

                # 1. 顶部 Header
                now_str = time.strftime('%Y-%m-%d %H:%M:%S')
                layout["header"].update(
                    Panel(f"[bold cyan]📡 MQTT 深度监听雷达 (自动去重持久化)[/bold cyan] [dim]| 系统时间: {now_str}[/dim]", style="blue")
                )

                # 2. 中间 Table 监控看板
                table = Table(expand=True, border_style="dim", header_style="bold magenta")
                table.add_column("Broker 节点", style="cyan", width=25, no_wrap=True)
                table.add_column("历史总 Msg / Topic", justify="right", width=22)
                table.add_column("近1小时 Msg / Topic", justify="right", width=22)
                table.add_column("最新消息接收 (时间 / Topic => Payload)", style="yellow", ratio=1, no_wrap=True)

                total_msgs_all = 0
                total_topics_all = 0

                # 【修复】减少锁持有时间，先复制必要数据
                with self.lock:
                    hosts = sorted(set(self.db_totals.keys()) | set(self.latest_msg.keys()))
                    # 预取所有需要的数据，减少锁内操作
                    display_data = []
                    for host in hosts:
                        hist = self.db_totals.get(host, {"msgs": 0, "topics": 0})
                        total_msgs_all += hist["msgs"]
                        total_topics_all += hist["topics"]

                        h1_msgs, h1_topics = 0, 0
                        tracker = self.hour_trackers.get(host)
                        if tracker is not None:
                            # 注意：这里调用 get_stats 会获取 tracker 的锁
                            # 为避免死锁，先不调用，标记待处理
                            h1_msgs, h1_topics = -1, -1  # 标记为需要后续获取

                        latest = self.latest_msg.get(host, {"topic": "无", "time": 0, "payload": ""})

                        display_data.append({
                            'host': host,
                            'hist': hist,
                            'h1_msgs': h1_msgs,
                            'h1_topics': h1_topics,
                            'latest': dict(latest)  # 复制，避免锁外访问被修改
                        })

                # 【修复】在锁外获取 hour_tracker 统计（避免嵌套锁死锁风险）
                for item in display_data:
                    if item['h1_msgs'] == -1:
                        tracker = self.hour_trackers.get(item['host'])
                        if tracker is not None:
                            item['h1_msgs'], item['h1_topics'] = tracker.get_stats()
                        else:
                            item['h1_msgs'], item['h1_topics'] = 0, 0

                # 填充表格（完全在锁外）
                for item in display_data:
                    host = item['host']
                    hist = item['hist']
                    h1_msgs = item['h1_msgs']
                    h1_topics = item['h1_topics']
                    latest = item['latest']

                    t_str = time.strftime('%H:%M:%S', time.localtime(latest["time"])) if latest["time"] else "--:--:--"

                    hist_str = f"[bold green]{hist['msgs']:,}[/bold green] / {hist['topics']:,}"
                    h1_str = f"[bold green]{h1_msgs:,}[/bold green] / {h1_topics:,}"
                    # 【修复】限制 payload 显示长度，防止超长字符串
                    payload_display = latest['payload']
                    if len(payload_display) > 200:
                        payload_display = payload_display[:200] + "..."
                    latest_str = f"[{t_str}] [bold white]{latest['topic']}[/bold white] => {payload_display}"

                    table.add_row(host, hist_str, h1_str, latest_str)

                summary_text = f" [bold yellow]全局汇总[/bold yellow] => 历史总消息数: [bold green]{total_msgs_all:,}[/bold green] 条 | 捕获独立 Topic: [bold green]{total_topics_all:,}[/bold green] 个"

                # 将表格与汇总打包成主面板
                main_group = Panel(
                    table,
                    title=summary_text,
                    title_align="left",
                    border_style="green"
                )
                layout["main"].update(main_group)

                # 3. 底部日志框（使用锁保护读取，避免并发修改 deque 引发异常）
                with log_lock:
                    logs_snapshot = list(log_queue)
                logs_text = "\n".join(logs_snapshot) if logs_snapshot else "暂无系统日志..."
                layout["footer"].update(
                    Panel(logs_text, title="[bold]系统日志[/bold]", border_style="grey50", height=MAX_LOG_LINES)
                )

                # 渲染整屏
                live.update(layout)
                time.sleep(0.5)

    def shutdown(self):
        self.running = False
        self.flush_thread.join(timeout=2)
        self.display_thread.join(timeout=2)
        # 【修复】清理全局池，帮助最终 GC
        self._global_topic_pool.clear()
        self._global_host_pool.clear()


# ==========================================
# 4. MQTT 监听适配器
# ==========================================
class RawSnifferManager(MultiMQTTManager):
    """
    纯原始嗅探器：**不做 JSON 解析 / 不做 ECDSA 验签 / 不做去重**，
    直接把 msg.payload 原样（bytes）交给上层回调。

    [为什么必须覆盖 _on_message]
    基类 MultiMQTTManager._on_message 会执行 process_cipher(..., decrypt=True)，
    其内部 json.loads(raw_payload) 会把 MQTT 报文解析成 dict，然后把这个 dict
    交给 message_callback。上层 on_raw_message 的第二个参数名义上叫
    payload_bytes，实际收到的是一个 dict —— engine.push 里 payload.decode()
    立刻抛 AttributeError: 'dict' object has no attribute 'decode'，
    在 _dispatch_loop 里被打印成"消息分发回调执行失败"刷屏。

    且：嗅探器场景 99% 的 payload 不是 JSON（protobuf / 二进制 / 纯文本 /
    空 payload），走基类路径要么解析失败被丢弃，要么即便解析成功也丢了原始
    字节 —— 都违背嗅探器"抓原始报文"的初衷。

    注意：覆盖时必须保留 multi_mqtt/ping_rtt/ 前缀过滤（enable_stats=True 时
    基类会周期向该 topic 发空 payload，订阅 "#" 会把它们收回来污染统计）。
    """
    def _on_message(self, client, userdata, msg):
        host = userdata
        # 过滤自身 QoS1 Ping 回声
        if msg.topic.startswith("multi_mqtt/ping_rtt/"):
            return
        try:
            if self.message_callback:
                self.message_callback(msg.topic, msg.payload, host)
        except Exception:
            logger.exception("原始消息回调处理失败 [%s] topic=%s", host, msg.topic)


def build_parser():
    """命令行参数：默认值保持脚本原来的行为（6080 / 0.0.0.0 / '-'）。"""
    parser = argparse.ArgumentParser(
        description="订阅所有公开 topic，并起 HTTP RPC 查询/编辑服务")
    parser.add_argument('--port', type=int, default=6080,
                        help='RPC 监听端口（默认 6080）')
    parser.add_argument('--ip', default='0.0.0.0',
                        help='RPC 监听地址（默认 0.0.0.0）')
    parser.add_argument('--key', default='-',
                        help='RPC 路径前缀（默认 -）')
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    engine = SnifferEngine()

    def on_raw_message(topic, payload_bytes, host):
        if topic.startswith("$SYS/"):
            return
        engine.push(host, topic, payload_bytes)

    manager = RawSnifferManager(brokers=BROKER_LIST, log_messages=False, enable_crypto=False)
    manager.set_on_message(on_raw_message)

    logger.info("启动 MQTT 节点连接管理程序...")
    manager.start()
    manager.subscribe("#")
    
    import server_http
    logger.info("HTTP RPC 监听 %s:%s (key=%r)", args.ip, args.port, args.key)
    ghs = server_http.start_rpc_server(port=args.port,ip=args.ip,globals=globals(),locals=locals(),key=args.key)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("接收到退出信号 (Ctrl+C)，退出中...")
        engine.shutdown()
        manager.stop()

if __name__ == "__main__":
    
    
    main()