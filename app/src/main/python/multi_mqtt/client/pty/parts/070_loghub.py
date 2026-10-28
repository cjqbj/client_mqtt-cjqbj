# ==================== 浏览器实时日志台（WebSocket，仿 realtime_editor） ====================

# 每个日志台连接的内存积压上限：浏览器卡住/慢消费时丢旧行，绝不反压业务线程
_LOG_WS_QUEUE = 1000
# 增量帧的攒批窗口：把 0.15s 内的行合并成一帧，日志风暴时不刷爆浏览器
_LOG_WS_BATCH = 0.15


class LogHub:
    """环形日志到浏览器 WebSocket 的广播枢纽。

    每个连接：先原子取当前快照（snapshot 帧），之后新行经有界队列 + 独立
    写线程增量推送（lines 帧，攒批合帧）；队列满丢最旧并在恢复时插一行
    丢弃提示。慢/死连接只影响自己，绝不拖慢写日志的 broker/渲染线程。
    """

    def __init__(self, ring: "_LogRing"):
        self._ring = ring

    def _frame(self, msg_type: str, text: str, **extra) -> str:
        payload = {"type": msg_type, "text": text,
                   "server_time": time.time(), **extra}
        return json.dumps(payload, ensure_ascii=False)

    def serve(self, websocket, _request) -> None:
        q: "queue.Queue" = queue.Queue(maxsize=_LOG_WS_QUEUE)
        state = {"dropped": 0}

        def push(seq, line):
            try:
                q.put_nowait((seq, line))
            except queue.Full:
                try:
                    q.get_nowait()
                    q.put_nowait((seq, line))
                    state["dropped"] += 1
                except (queue.Empty, queue.Full):
                    state["dropped"] += 1

        snapshot, unsubscribe = self._ring.subscribe(push)
        stop_ev = threading.Event()

        def writer_loop():
            # 先发快照（订阅在发快照之前完成，期间新行已在队列，顺序天然正确）
            snap_text = "\n".join(line for _s, line in snapshot)
            try:
                websocket.send(self._frame(
                    "snapshot", snap_text, lines=len(snapshot),
                    buffered=len(self._ring)))
            except OSError:
                stop_ev.set()
                return
            while not stop_ev.is_set():
                try:
                    first = q.get(timeout=_LOG_WS_BATCH)
                except queue.Empty:
                    continue
                batch = [first[1]]
                # 顺带抽干队列里已到的行合成一帧
                while len(batch) < 500:
                    try:
                        batch.append(q.get_nowait()[1])
                    except queue.Empty:
                        break
                dropped = state["dropped"]
                if dropped:
                    state["dropped"] = 0
                    batch.insert(0, "…日志台积压，已丢弃 %d 旧行…" % dropped)
                try:
                    websocket.send(self._frame("lines", "\n".join(batch)))
                except OSError:
                    stop_ev.set()
                    return

        writer = threading.Thread(target=writer_loop, name="log-ws-writer",
                                  daemon=True)
        writer.start()
        try:
            while not stop_ev.is_set():
                try:
                    raw = websocket.receive()
                except (ConnectionError, OSError, ValueError):
                    break
                if raw is None:
                    break
                # 页面只发心跳；其他内容忽略（日志台对本机进程只读）
                if raw.strip() == "ping":
                    try:
                        websocket.send(json.dumps({"type": "pong",
                                                   "server_time": time.time()}))
                    except OSError:
                        break
        finally:
            stop_ev.set()
            unsubscribe()
            try:
                websocket.close()
            except Exception:
                pass


# 模块级单例：必须在 main() 调 start_rpc_server 之前存在（handler 引用它）
_LOG_HUB = globals().setdefault('_LOG_HUB', LogHub(_LOCAL_LOG))  # setdefault 防 reload 日志分脑：120_cli 已把 .serve 按值捕获进服务器配置、浏览器订阅全挂在旧 hub 上，reload 普通赋值会重绑到新 hub——新代码往新 hub 发、旧 WS 订阅者收不到，快照/增量各走一半；LogHub 构造只存 ring 引用无副作用，reload 时重复求值仅多一个即弃对象


