# multi_mqtt.py
import importlib.util,os,subprocess,sys
def ensure_dependencies():
    packages = {
        "paho": "paho-mqtt",
        "ecdsa": "ecdsa",  # [新增] 仅追加了 ecdsa 依赖，以支持私钥签名
    }
    missing = [package for module, package in packages.items()
               if importlib.util.find_spec(module) is None]
    if not missing:
        return
    if 'PYTHONANYWHERE_DOMAIN' in os.environ:
        index_url='https://pypi.org/simple'
    else:    
        index_url = "https://pypi.tuna.tsinghua.edu.cn/simple"
    print(f"[+] 正在使用清华源安装依赖: {', '.join(missing)}")
    command = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "-i",
        index_url,
        "--trusted-host",
        "pypi.tuna.tsinghua.edu.cn",
        *missing,
    ]
    try:
        subprocess.check_call(command)
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"[!] 依赖安装失败: {error}", file=sys.stderr)
        sys.exit(1)
ensure_dependencies()

import ast,json,time,os,uuid,hashlib,logging,base64,struct,threading,queue,socket
import concurrent.futures
import ecdsa  # [新增]
from collections import OrderedDict
from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

# 组合方（如 pty_client_mqtt）需要把全部日志导去自己的环形缓冲/Web 控制台，
# 而不是让本模块在 import 时往 root 挂一个写 stderr 的 StreamHandler（会插进
# PTY 远端画面）。约定：import 本模块前设置环境变量 CMQ_NO_STDERR_LOG=1，
# 这里只挂 NullHandler 占位，root level 仍由组合方按需配置（日志记录照常
# 向 root 传播，组合方挂自己的 handler 即可收到）。
if os.environ.get("CMQ_NO_STDERR_LOG"):
    logging.basicConfig(level=logging.INFO, handlers=[logging.NullHandler()])
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MultiMQTT")

# =========================================================================
# 预设公共 MQTT Broker 列表
# -------------------------------------------------------------------------
# 条目格式（两种，自动识别，向后兼容）：
#   - (host, port)                       → 匿名连接（原逻辑）
#   - (host, port, [username, password]) → 使用用户名/密码连接
#     密码可为空字符串 "" （例如 demo.tbmq.io 允许空密码）
#
# 示例：
#   BROKER_LIST = [
#       ("broker.mqtt-dashboard.com", 1883),
#       ("demo.tbmq.io", 1883, ["demo", ""]),           # 用户名 demo，密码为空
#   ]
# =========================================================================
BROKER_LIST = [
    ("mqtt.iotbhai.io", 1883),                  # RTT: 429.3 ms | 建连:  597.7 ms (个人站点？实际上不能用。当作测试也挺好)
    ("broker.mqtt-dashboard.com", 1883),        # RTT: 307.1 ms | 建连:  465.6 ms (HiveMQ Dashboard，综合体验佳)
    ("mqtt.touchsocket.net", 1883),             # RTT:  29.5 ms | 建连:  293.4 ms (b 视频)
    ("broker.codenow.cn", 1883),                # RTT:  31.4 ms | 建连:  110.1 ms (CodeNow 国内公共MQTT)
    ("broker.emqx.io", 1883),                   # RTT: 282.9 ms | 建连:  620.4 ms (EMQX 国际)
    ("mqtt.loralab.org", 1883),                 # RTT: 349.0 ms | 建连:  524.4 ms (LoRaLab)
    ("test.mosquitto.org", 1883),               # RTT: 398.4 ms | 建连:  586.0 ms (Mosquitto 官方)
    ("broker-cn.emqx.io", 1883),                # RTT: 407.3 ms | 建连:  751.3 ms (EMQX 中国)
    ("broker.hivemq.com", 1883),                # RTT: 301.0 ms | 建连:12830.1 ms (HiveMQ 官方，建连极慢，收发快)
    ("broker.mqtt.cool", 1883),                 # RTT: 425.4 ms | 建连:  594.8 ms (MQTT.Cool)
    ("mqtt.tyckr.io", 1883),                    # RTT: 436.0 ms | 建连:  596.2 ms (Tyckr)
    ("public-mqtt-broker.bevywise.com", 1883),  # RTT: 451.3 ms | 建连:  575.9 ms (Bevywise)
    ("demo.tbmq.io", 1883, ["demo", ""]),       # ThingsBoard TBMQ可以，下面4个全部不能用
    # ("public.mqtt.pro", 1883, ["ajbkvbp/demo", "OCDWjjOSlSexcWRG"]),        # MQTT.pro 公共沙箱，凭据定期轮换[reference:1]
    # ("mqtt.flespi.io", 1883, ["stPwSVV73Eqw5LSv0iMXbc4EguS7JyuZR9lxU5uLxI5tiNM8ToTVqNpu85pFtJv9", ""]),                     # Flespi，将 YOUR_FLESPI_TOKEN 替换为注册后获取的 Token[reference:2]
    # ("io.adafruit.com", 1883, ["nagecubic","aio_"+"EiZX7901W4K4INtv13RSH6jqkEKl"]),        # Adafruit IO，需注册获取 #改成"aio_"+" 不能直接push error: GH013: Repository rule violations found for refs/heads/master.  GITHUB PUSH PROTECTION
    # ("mqtt.ably.io", 1883, ['yMJ3VQ.PxwimQ','Vw4oM1CCMxx0tm8xTIZtda72vNj3SmNkLbVPipSt5Ek']), # Ably，API Key 按 username:password 拆分填入[reference:4]

]

'''
这5个允许订阅 #  。泄漏所有消息
 Broker 节点                ┃     历史总 Msg / Topic 
━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━
 broker.mqtt.cool          │       18,183,031 / 976 
 mqtt.loralab.org          │             40,734 / 3 
 mqtt.tyckr.io             │        1,694,738 / 436 
 public-mqtt-broker.bevyw… │           200,928 / 69 
 test.mosquitto.org        │   188,030,853 / 77,948 
───────────────────────────┴────────────────────────

'''
class _NotFoundSentinel:
    __slots__=('msg',)
    def __init__(self,msg='Not found matched kargs'):self.msg=msg
    def __repr__(self):return f'No({self.msg!r})'
    def __bool__(self):return False
DEFAULT_get_duplicated_kargs=GET_DUPLICATED_KARGS_DEFAULT=_NotFoundSentinel()
def get_duplicated_kargs(ka,*keys,default=GET_DUPLICATED_KARGS_DEFAULT,no_pop=False):
    '''从 dict `ka` 按 keys 顺序取第一个存在的键: 找到一个返回其值; 多个且值(真值或去重后)不一致则 raise; 都没找到返回 default. 默认 pop 命中的 key, no_pop=True 则只读取.'''
    if not ka:return default
    if not isinstance(ka,dict):raise TypeError(f'ka should be a dict, but got {type(ka).__name__}: {ka!r}')
    r=[]
    for i in keys:
        if not isinstance(i,str):raise TypeError(f'keys should be a list of str, but got {type(i).__name__}: {i!r}')
        if i in ka:r.append(ka[i] if no_pop else ka.pop(i))
    if not r:return default
    if len(r)>1:
        r=[x for x in r if x] or list(set(r))
        if len(r)>1:raise ValueError('kargs 存在多个重复的 key',ka,keys)
    if len(r)==1:return r[0]
    raise ValueError('kargs matched keys len <> 1',ka,keys)
# get_ka=get_multi_ka=getDuplicatedKargs=getKargsDuplicated=getKArgsDuplicated=get_kargs_duplicated=get_duplicated_kargs

def stime(ms=0, format='%Y-%m-%d__%H.%M.%S', ms_splitor='__.'):
    """可读毫秒级时间戳。ms 传整数毫秒；不传则取当前 UTC 毫秒。"""
    if not ms:
        ms = utc_ms()
    elif isinstance(ms, float) and ms < 1e11:
        # 看起来是 time.time() 的秒级浮点，转成整数毫秒
        ms = round(ms * 1000)
    else:
        ms = int(ms)
    sec, milli = divmod(ms, 1000)
    
    if ms_splitor:ms_splitor=ms_splitor + f"{milli:03d}"
    return time.strftime(format, time.localtime(sec)) + ms_splitor

def utc_ms():
    """Return the current UTC Unix timestamp in integer milliseconds."""
    return time.time_ns() // 1_000_000

def get_req_id(ms=0):
    """ get_req_id 的返回值只精确到了毫秒，如果单片机在同一毫秒内连发两条不同状态，后一条会被 TTLCache 误杀丢弃。
这是设计好行为。我不需要1毫秒发两个请求
    """
    # import random hash_str = hashlib.md5(f"{time.time()}_{random.random()}".encode()).hexdigest()[:6]
    return f"{stime(ms=ms,format='%Y%m%d_%H%M%S',ms_splitor='.')}"

AES_KEY = b"12345678901234567890123456789012"
def process_cipher(data, decrypt=False, enabled=False, key=AES_KEY):
    """加密 解密 浓缩函数：默认关闭 (enabled=False)。开启时使用 AES-GCM，关闭时仅转换 JSON"""
    if not enabled:
        return json.loads(data) if decrypt else json.dumps(data)

    # 动态导入，关闭时无需依赖 cryptography 库
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aesgcm = AESGCM(key)
    if decrypt:
        raw = base64.b64decode(data.encode('utf-8'))
        return json.loads(aesgcm.decrypt(raw[:12], raw[12:], None).decode('utf-8'))
    else:
        nonce = os.urandom(12)
        plaintext = json.dumps(data).encode('utf-8')
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return base64.b64encode(nonce + ciphertext).decode('utf-8')

class TTLCache:
    """轻量级内存去重缓存（[修复-容量上限] 新增 max_size，超限按 FIFO 淘汰）"""
    # [修复] 默认 max_size=50000。原先无上限：30 秒 TTL 内的瞬时峰值可能被恶意
    # 灌入海量不同 req_id（即使每条只有 512 字节上限），连发百万级也能顶爆内存。
    # 加上限后，超过时按 OrderedDict 插入顺序淘汰最老条目，保证内存有界。
    def __init__(self, ttl_seconds=30, max_size=50000):
        self.ttl = ttl_seconds
        self.max_size = max_size
        self.cache = OrderedDict()
        self.lock = threading.Lock()

    def add_if_not_exists(self, key: str) -> bool:
        now = time.time()
        with self.lock:
            # 1. 先按 TTL 滚出过期条目（从队首开始）
            while self.cache and next(iter(self.cache.values())) < now - self.ttl:
                self.cache.popitem(last=False)
            # 2. 命中：直接返回 False，不更新位置（去重语义）
            if key in self.cache:
                return False
            # 3. [修复-容量上限] 容量已满：FIFO 淘汰最老条目，保证内存有界
            if len(self.cache) >= self.max_size:
                self.cache.popitem(last=False)
            self.cache[key] = now
            return True

def _eval_safe_int_expression(expr):
    """Safely evaluate a small Python integer expression used as a secret exponent."""
    if expr is None:
        return None
    if isinstance(expr, int):
        return expr
    if not isinstance(expr, str):
        raise ValueError("private key expression must be a string or int")

    expr = expr.strip()
    if not expr:
        return None

    try:
        node = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"无法解析私钥表达式: {expr}") from exc

    allowed_nodes = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Constant,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.FloorDiv,
        ast.Mod,
        ast.Pow,
        ast.UAdd,
        ast.USub,
    )

    def _eval(node):
        if not isinstance(node, allowed_nodes):
            raise ValueError(f"不允许的私钥表达式节点: {type(node).__name__}")
        if isinstance(node, ast.Constant):
            if isinstance(node.value, int) and not isinstance(node.value, bool):
                return node.value
            raise ValueError("私钥表达式必须计算出整数")
        if isinstance(node, ast.UnaryOp):
            operand = _eval(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
            raise ValueError("不支持的一元运算")
        if isinstance(node, ast.BinOp):
            left = _eval(node.left)
            right = _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left // right
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            if isinstance(node.op, ast.Mod):
                return left % right
            if isinstance(node.op, ast.Pow):
                return left ** right
            raise ValueError("不支持的二元运算")
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        raise ValueError("无法计算私钥表达式")

    value = _eval(node)
    if not isinstance(value, int):
        raise ValueError("私钥表达式必须结果为整数")
    return value


def get_standard_pem_bytes(key_input) -> bytes:
    """
    [新增组件] 统一私钥解析：支持 整数secexp、Python 整数表达式、文件路径、OpenSSH格式、标准PEM格式(bytes/str)
    最终统一返回 ecdsa 库直接兼容的标准 PEM (SEC1) 字节流。
    """
    if not key_input:
        return None

    # 1. 整数或安全 Python 整数表达式当作 secexp 处理
    if isinstance(key_input, int):
        secexp = key_input
        return ecdsa.SigningKey.from_secret_exponent(secexp=secexp, curve=ecdsa.NIST256p).to_pem()
    if isinstance(key_input, str):
        text = key_input.strip()
        if text.isdigit() or text.startswith(("0x", "0X")) or any(ch in text for ch in "+-*/%**() "):
            try:
                secexp = _eval_safe_int_expression(text)
                return ecdsa.SigningKey.from_secret_exponent(secexp=secexp, curve=ecdsa.NIST256p).to_pem()
            except ValueError:
                pass

    # 2. 如果是文件路径，读取内容；否则转为 bytes
    raw_bytes = b""
    if isinstance(key_input, str):
        if not key_input.startswith("-----") and len(key_input) < 255 and os.path.isfile(key_input):
            with open(key_input, "rb") as f:
                raw_bytes = f.read()
        else:
            raw_bytes = key_input.encode('utf-8')
    elif isinstance(key_input, bytes):
        raw_bytes = key_input

    if not raw_bytes:
        raise ValueError("无法解析传入的 client_private_key_bytes")

    # 3. 检查是否已经是原生 ecdsa 支持的格式
    if b"-----BEGIN EC PRIVATE KEY-----" in raw_bytes or b"-----BEGIN PRIVATE KEY-----" in raw_bytes:
        return raw_bytes

    # 4. 如果是 OpenSSH 格式，动态借助 cryptography 转换为 ecdsa 库兼容的标准格式
    if b"-----BEGIN OPENSSH PRIVATE KEY-----" in raw_bytes:
        try:
            from cryptography.hazmat.primitives import serialization
        except ImportError:
            raise ImportError("解析 OpenSSH 格式私钥需要 cryptography 库，请先安装。")

        try:
            priv_key = serialization.load_ssh_private_key(raw_bytes, password=None)
        except ValueError:
            priv_key = serialization.load_pem_private_key(raw_bytes, password=None)

        return priv_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )

    return raw_bytes # 默认兜底返回

def _describe_public_key(value: bytes | str | None,short_text=False) -> str:
    """返回可直接写入日志的公钥摘要与详细值。"""
    if not value:
        return "未配置"
    try:
        data = value if isinstance(value, (bytes, bytearray)) else str(value).encode('utf-8')
        if not data:
            return "空值"
        text = data.decode('utf-8', 'replace').replace('\n',' ').strip()
        if b"BEGIN PUBLIC KEY" in data or b"BEGIN EC PUBLIC KEY" in data:
            if short_text:text=text[27:40]+'...'
            return f"已配置(type=PEM, len={len(data)}, value={text})"
        if b"ecdsa-sha2-nistp256" in data:
            return f"已配置(type=OpenSSH, len={len(data)}, value={text})"
        return f"已配置(type=unknown, len={len(data)}, value={text})"
    except Exception:
        return f"已配置(len={len(str(value))}, value={str(value)})"


def get_standard_public_pem_bytes(key_input) -> bytes:
    """
    统一公钥解析：支持 OpenSSH 公钥、PEM 公钥、文件路径、bytes/str。
    最终返回 ecdsa 库兼容的标准 PEM 公钥字节流。
    """
    if not key_input:
        return None

    raw_bytes = b""
    if isinstance(key_input, str):
        if not key_input.startswith("-----") and len(key_input) < 255 and os.path.isfile(key_input):
            with open(key_input, "rb") as f:
                raw_bytes = f.read()
        else:
            raw_bytes = key_input.encode('utf-8')
    elif isinstance(key_input, bytes):
        raw_bytes = key_input

    if not raw_bytes:
        raise ValueError("无法解析传入的公钥")

    # 已经是 PEM 格式
    if b"-----BEGIN PUBLIC KEY-----" in raw_bytes:
        return raw_bytes

    # 尝试作为 OpenSSH 公钥解析。cryptography 是可选依赖，因此这里保留
    # 一个仅依赖 ecdsa 的解析路径。
    if raw_bytes.startswith(b"ecdsa-sha2-nistp256 "):
        try:
            encoded_key = raw_bytes.split(None, 2)[1]
            key_blob = base64.b64decode(encoded_key, validate=True)
            offset = 0

            def read_ssh_field():
                nonlocal offset
                if offset + 4 > len(key_blob):
                    raise ValueError("OpenSSH 公钥字段长度无效")
                field_length = struct.unpack(">I", key_blob[offset:offset + 4])[0]
                offset += 4
                field = key_blob[offset:offset + field_length]
                if len(field) != field_length:
                    raise ValueError("OpenSSH 公钥字段被截断")
                offset += field_length
                return field

            key_type = read_ssh_field()
            curve_name = read_ssh_field()
            point = read_ssh_field()
            if key_type != b"ecdsa-sha2-nistp256" or curve_name != b"nistp256":
                raise ValueError("仅支持 ecdsa-sha2-nistp256 公钥")
            return ecdsa.VerifyingKey.from_string(
                point, curve=ecdsa.NIST256p
            ).to_pem()
        except (ValueError, IndexError, TypeError, base64.binascii.Error) as error:
            raise ValueError(f"无法解析 OpenSSH 公钥: {error}") from error

    # 尝试借助 cryptography 解析其它 OpenSSH 公钥格式
    try:
        from cryptography.hazmat.primitives import serialization
        public_key = serialization.load_ssh_public_key(raw_bytes)
        pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem
    except Exception:
        # 如果解析失败，可能已经是其他格式，直接返回
        return raw_bytes

# =========================================================================
# [新增解耦功能] 网络质量统计模型与管理模块（默认不占额外内存，极其轻量）
class BrokerStat:
    """单一节点的数据模型 保证O(1)极低内存开销）"""
    # [轻量化] __slots__ 避免每个实例再挂一份 __dict__，配合"极其轻量"的设计目标
    __slots__ = (
        "disconnect_count", "last_disconnect_time", "last_connect_time",
        "max_offline_time", "total_online_time", "total_offline_time",
        "is_connected", "latency_min", "latency_max", "latency_sum",
        "latency_count", "pending_pings", "created_at",
    )

    def __init__(self):
        self.disconnect_count = 0
        self.last_disconnect_time = 0.0
        self.last_connect_time = 0.0
        self.max_offline_time = 0.0
        self.total_online_time = 0.0
        self.total_offline_time = 0.0
        self.is_connected = False
        # 延迟指标滚动记录
        self.latency_min = float('inf')
        self.latency_max = 0.0
        self.latency_sum = 0.0
        self.latency_count = 0
        # [修复-M4] 发送 QoS1 Ping 时的上下文记录。
        # 用 dict[mid] = send_time 保存"所有未确认的 ping"，
        # 而不是单一 mid/time 字段——避免周期重叠时相互覆盖。
        self.pending_pings = {}
        self.created_at = time.time()

    def update_latency(self, latency_ms):
        if latency_ms < self.latency_min: self.latency_min = latency_ms
        if latency_ms > self.latency_max: self.latency_max = latency_ms
        self.latency_sum += latency_ms
        self.latency_count += 1

    @property
    def avg_latency(self):
        return self.latency_sum / self.latency_count if self.latency_count > 0 else 0.0

    @property
    def reliability(self):
        """
        可靠性(%)：统一口径为「1 - 累计离线时长 / 自创建以来总时长」。
        本属性是唯一口径，get_report 直接复用，避免双份实现漂移。

        [修复-⑤] 若从未成功连接过（last_connect_time==0 且当前未连接），
        直接返回 0.0，避免启动初期因 total_offline_time 尚未滚动累加，
        短暂误报为 100% 的极端盲区。
        """
        now = time.time()
        total = now - self.created_at
        if total <= 0:
            # 极端情况（时钟回拨 / 刚创建）：退化为二值判断
            return 100.0 if self.is_connected else 0.0
        # 从未成功连上过 → 可靠性就是 0%
        if self.last_connect_time == 0 and not self.is_connected:
            return 0.0
        offline = self.total_offline_time
        # 当前仍处于离线：把"正在发生的离线"也计入
        if not self.is_connected and self.last_disconnect_time > 0:
            offline += (now - self.last_disconnect_time)
        rel = 100.0 * (1.0 - (offline / total))
        return max(0.0, min(100.0, rel))


class ConnectionQualityStats:
    """网络连接质量统筹管理器，可方便开启与关闭"""
    # [可配置] Ping 周期（秒）。原代码硬编码在 _monitor_loop 里，
    # 现抽出为类属性：保留默认 600，也不影响 __init__ 签名（便于 diff）。
    PING_INTERVAL = 600
    # [修复-M4] 单条 ping 记录的最长存活时间：超过该时长仍未收到 ACK，
    # 认为该 ping 已经彻底丢失（QoS1 重传也救不回来），从待确认表中清除，
    # 防止 pending_pings 因极端网络状况无限膨胀。
    PING_TTL = 120

    # [修复-N1] __init__ 新增 lock 参数：
    #   - 传 None（默认）→ 本类自建一把锁，用于独立使用场景；
    #   - 由 MultiMQTTManager 传入 → 与 manager 共享同一把锁，
    #     保证 self.clients_ref（就是 manager.clients 同一个 dict）
    #     的读取与 manager 的 clients 写入使用同一把锁，彻底消除跨锁竞态。
    def __init__(self, enabled=True, print_interval=3600, lock=None):
        self.enabled = enabled
        self.stats = {}
        self.lock = lock if lock is not None else threading.Lock()
        self.print_interval = print_interval
        self.running = False
        # [合并线程] 不再维护独立监控线程，相关属性已删除
        self.clients_ref = {}

    def start(self, clients_ref):
        """[合并线程] 仅初始化，不启动独立线程。由 MultiMQTTManager 的分发线程统一驱动。"""
        if not self.enabled: return
        with self.lock:
            if self.running:
                return  # 幂等保护：防止重复 start
            self.clients_ref = clients_ref
            for host in self.clients_ref.keys():
                # [修复-S2] 用 setdefault 而不是直接赋值。
                # MultiMQTTManager.start() 里 client.loop_start() 是非阻塞的，
                # 极快的 broker（如 broker.codenow.cn 建连仅 110ms）可能在
                # stats.start() 之前就触发 _on_connect 创建好 BrokerStat，
                # 直接赋值会把这份"已连接"状态用全新的未连接对象覆盖，
                # 导致初次连接的状态被静默丢弃。
                self.stats.setdefault(host, BrokerStat())
            self.running = True

    def stop(self):
        """[合并线程] 仅修改标志位，无需等待线程退出。"""
        self.running = False

    def on_connect(self, host):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            # [修复首连算入离线时长的Bug]: 仅在有过真实掉线记录(last_disconnect_time > 0)时才结算离线时长
            if not stat.is_connected:
                if stat.last_disconnect_time > 0:
                    offline_duration = now - stat.last_disconnect_time
                    stat.total_offline_time += offline_duration
                    if offline_duration > stat.max_offline_time:
                        stat.max_offline_time = offline_duration
            stat.is_connected = True
            stat.last_connect_time = now

    def on_disconnect(self, host):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            if stat.is_connected:
                # 正常从在线变为离线：结算在线时间，必然增加1次掉线
                if stat.last_connect_time > 0:
                    stat.total_online_time += (now - stat.last_connect_time)
                stat.is_connected = False
                stat.disconnect_count += 1
            else:
                # 初始连接失败或持续重连失败期间，防抖机制记录掉线
                if stat.last_disconnect_time == 0 or (now - stat.last_disconnect_time) > 1.0:
                    stat.disconnect_count += 1
            stat.last_disconnect_time = now

    def finalize_for_shutdown(self, host):
        """
        [修复-④] 服务终止时为指定 host 结算"最后一段在线时长"。
        Manager.stop() 会触发 paho 的 on_disconnect，但那属于"主动停止"：
        - 结算 total_online_time，避免丢失最后一段真实在线时长
        - 不增加 disconnect_count（主动停止 ≠ 掉线）
        """
        if not self.enabled: return
        with self.lock:
            stat = self.stats.get(host)
            if stat and stat.is_connected:
                now = time.time()
                if stat.last_connect_time > 0:
                    stat.total_online_time += (now - stat.last_connect_time)
                stat.is_connected = False
                stat.last_disconnect_time = now

    def record_ping_send(self, host, mid):
        if not self.enabled: return
        with self.lock:
            stat = self.stats.setdefault(host, BrokerStat())
            now = time.time()
            # [修复-M4] 按 mid 写入待确认表，多个 in-flight ping 互不干扰。
            # 之前的单一字段在周期重叠时会相互覆盖，导致先发 ping 的延迟样本丢失。
            stat.pending_pings[mid] = now
            # 顺手清理超时未确认的旧条目，防止极端网络下 dict 无限增长
            if len(stat.pending_pings) > 1:
                expired = [m for m, t in stat.pending_pings.items() if now - t > self.PING_TTL]
                for m in expired:
                    stat.pending_pings.pop(m, None)

    def on_publish_ack(self, host, mid):
        """挂钩到底层 on_publish 回调计算 QoS 1 的精准 RTT 延迟"""
        if not self.enabled: return
        with self.lock:
            stat = self.stats.get(host)
            if not stat: return
            # [修复-M4] 按 mid 精确匹配并原子弹出：
            # - 命中：计算本次延迟后即刻删除，避免同一 mid 被重复计入
            # - 未命中：说明该 ACK 不是我们发的 ping（例如业务消息的 QoS1 ACK），直接忽略
            sent_at = stat.pending_pings.pop(mid, None)
            if sent_at is None:
                return
            latency_ms = (time.time() - sent_at) * 1000.0
            if 0.0 <= latency_ms < 10000.0:  # 剔除由于断线堆积重发导致的超长异常延迟(>10s)
                stat.update_latency(latency_ms)
#

    def get_report(self, probe=True, sort="min", reverse=False, probe_timeout=3.0, is_windows_cmd=False):
        """
        获取网络连接质量统计报告

        统一指标名（排序键 / 数据键 / 表头 完全一致，全部小写）：
            broker    - Broker 地址
            rel       - 可靠性(%)
            avg       - 平均延迟(ms)
            min       - 最小延迟(ms)
            max       - 最大延迟(ms)
            drops     - 掉线次数
            max_off   - 最大离线时长(s)
            last_drop - 最后一次掉线时间

        :param sort: 排序指标，取值 broker / rel / avg / min / max / drops / max_off
        :param reverse: 是否降序排列（默认 False）
        :param probe: [新增] 为 True 时，先向所有已连接 Broker 发一轮 QoS1 Ping
                      并同步等待 ACK（或超时），把最新一轮 RTT 纳入统计后再渲染。
        :param probe_timeout: [新增] 单轮实时探测的最长同步等待秒数（仅在 probe=True 生效）。
        :param is_windows_cmd: [新增] 是否为 Windows CMD 环境，若为 True 则使用 '+' / '-' 替代 Emoji，完美对齐 CMD。
        """
        # ------------------------------------------------------------------
        # 显示宽度辅助函数（定义在方法内部，避免污染模块命名空间）
        # ------------------------------------------------------------------
        import unicodedata

        def _dwidth(s):
            """字符串在终端里的显示宽度（emoji/CJK/全角 = 2，组合字符 = 0，其他 = 1）。"""
            w = 0
            for ch in str(s):
                if unicodedata.combining(ch):
                    continue
                if unicodedata.east_asian_width(ch) in ('W', 'F'):
                    w += 2
                else:
                    w += 1
            return w

        def _dfit(s, width, ellipsis=''):
            """按显示宽度把 s 截断 / 补齐到 width（截断时在末尾加省略号）。"""
            s = str(s)
            cur = _dwidth(s)
            if cur <= width:
                return s + ' ' * (width - cur)
            ell_w = _dwidth(ellipsis)
            out, w = [], 0
            for ch in s:
                cw = _dwidth(ch)
                if w + cw + ell_w > width:
                    break
                out.append(ch)
                w += cw
            out.append(ellipsis)
            w += ell_w
            return ''.join(out) + ' ' * max(0, width - w)

        # [新增-实时探测]
        # 必须在进入 self.lock 之前完成探测：
        #   - _send_pings 内部也要获取 self.lock 做 clients 快照与 record_ping_send；
        #   - 若在这里先持锁再调 _send_pings，会造成自死锁。
        # 探测失败（异常/无连接/超时）只记日志，不阻断报告生成。
        if probe:
            try:
                pinged = self._send_pings(wait_timeout=probe_timeout)
                logger.debug(
                    "实时探测完成: probe_timeout=%.2fs, pinged_hosts=%s",
                    probe_timeout, pinged,
                )
            except Exception:
                logger.exception("实时探测执行失败，将基于历史统计生成报告")

        columns = ["broker", "rel", "avg", "min", "max", "drops", "max_off", "last_drop"]

        # 根据 is_windows_cmd 参数设定不同模式下的状态符号和字符宽度
        if is_windows_cmd:
            conn_marker = "+"
            disc_marker = "-"
            MARK_W = 1      # '+' / '-' 显示宽度为 1
        else:
            conn_marker = "🟢"
            disc_marker = "🔴"
            MARK_W = 2      # Emoji 显示宽度为 2

        LEAD = MARK_W + 1   # 符号 + 后面 1 个空格
        BROKER_W = 23       # broker 列统一显示宽度，不再使用硬编码微调

        lines = ["\n" + "=" * 90]
        lines.append(
            f"{' ' * LEAD}"
            f"{_dfit('broker', BROKER_W-1 if is_windows_cmd else BROKER_W)} | {'rel':>5} | {'avg':>6} | {'min':>5} | "
            # 只有这样表头对齐 
            f"{'max':>6} | {'drops':>5} | {'max_off':>9} | {'last_drop':>10}"
        )
        lines.append("-" * 90)

        with self.lock:
            display_stats = []
            # 1. 数据预处理
            for host, stat in self.stats.items():
                is_conn = stat.is_connected
                rel = stat.reliability  # [统一口径] 复用 BrokerStat 的属性
                max_off = stat.max_offline_time
                
                # [修复Bug 4]: 安全获取 last_disconnect_time，防御 None 值
                last_drop = getattr(stat, 'last_disconnect_time', 0.0) or 0.0

                if not is_conn and last_drop > 0:
                    max_off = max(max_off, time.time() - last_drop)

                display_stats.append({
                    "broker":    host,
                    "rel":       rel,
                    "avg":       stat.avg_latency if stat.latency_count > 0 else -1.0,
                    "min":       stat.latency_min if stat.latency_count > 0 else -1.0,
                    "max":       stat.latency_max if stat.latency_count > 0 else -1.0,
                    "drops":     stat.disconnect_count,
                    "max_off":   max_off,
                    "last_drop": last_drop,
                    "is_conn":   is_conn,
                })

            # 2. [修复Bug 2]: 修复二级排序逻辑与外层 reverse 抵消的问题
            def sort_key(item):
                k = sort.lower()
                if k not in columns:
                    k = "rel"
                val = item[k]

                # 主指标无数据处理：无论正序反序，无数据项均沉底
                if k in ("avg", "min", "max") and val < 0:
                    primary_val = float("-inf") if reverse else float("inf")
                elif k == "broker":
                    primary_val = str(val)
                else:
                    primary_val = val

                # 二级指标 avg 延迟处理（无数据时始终沉底）
                avg_val = item["avg"]
                if avg_val < 0:
                    secondary_val = float("-inf") if reverse else float("inf")
                else:
                    secondary_val = avg_val  # 直接保持原值，由外层 reverse 统一决定方向

                # 末级：已连接优先（根据 reverse 翻转权重）
                conn_rank = (1 if item["is_conn"] else 0) if reverse else (0 if item["is_conn"] else 1)

                return (primary_val, secondary_val, conn_rank, item["broker"])

            sorted_stats = sorted(display_stats, key=sort_key, reverse=reverse)

            # 3. 渲染输出
            for item in sorted_stats:
                rel_str = f"{item['rel']:.1f}"
                avg_str = f"{item['avg']:.1f}" if item["avg"] >= 0 else "-"
                min_str = f"{item['min']:.1f}" if item["min"] >= 0 else "-"
                max_str = f"{item['max']:.1f}" if item["max"] >= 0 else "-"
                drops_str = str(item["drops"])
                max_off_str = f"{item['max_off']:.1f}"

                # [修复Bug 4]: 安全格式化时间，规避空值和非正值
                if item["last_drop"] and item["last_drop"] > 0:
                    last_drop_str = time.strftime("%H:%M:%S", time.localtime(item["last_drop"]))
                else:
                    last_drop_str = "-"

                status_marker = conn_marker if item["is_conn"] else disc_marker
                bs = _dfit(item['broker'], BROKER_W)

                row = (
                    f"{status_marker} {bs}| "
                    f"{rel_str:>5} | {avg_str:>6} | {min_str:>5} | {max_str:>6} | "
                    f"{drops_str:>5} | {max_off_str:>9} | {last_drop_str:>10}"
                )
                lines.append(row)

        lines.append("=" * 90)
        return "\n".join(lines)+'\n'
    # [合并线程] _monitor_loop 已彻底删除，其逻辑合并到 MultiMQTTManager._dispatch_loop 中

    def _send_pings(self, wait_timeout=0.0):
        """
        [原逻辑抽离] 遍历已连接的 broker 发送 QoS1 Ping，单点异常不互相影响。

        [新增-实时探测] 参数说明：
            wait_timeout <= 0 → 原异步行为：发完即走（_dispatch_loop 的周期 Ping 走这条路）。
            wait_timeout >  0 → 同步等待模式：发完后轮询 pending_pings，
                                直到所有 sent_mids 的 ACK 都回来或到 wait_timeout 截止，
                                专供 get_report(probe=True) 用。
                                轮询以 20ms 为粒度，避免忙等。
        返回值：
            本次实际发出 Ping 的 host 列表（异步调用方可忽略；探测路径可用于日志）。
        """
        # [修复-5] 若 stats 已被 stop() 关闭，立即退出，避免在关闭过程中继续发 Ping。
        if not self.running:
            return []
        # [修复-N1] 使用 self.lock（在 MultiMQTTManager 场景下就是 manager.lock），
        # 与 manager.stop() 里 `with self.lock: self.clients.clear()` 使用同一把锁。
        # 两边互斥，不会再出现"另一个线程正在 clear 时这边 list() 迭代"的 RuntimeError。
        with self.lock:
            clients_snapshot = list(self.clients_ref.items())

        pinged_hosts = []
        # [新增-实时探测] host -> mid 映射，同步等待 ACK 时按 mid 精确配对
        sent_mids = {}

        for host, client in clients_snapshot:
            # [修复-5] 每个 host 之间也检查一次，中途被打断立即返回
            if not self.running:
                return pinged_hosts
            stat = self.stats.get(host)
            if not stat or not stat.is_connected:
                continue
            try:
                if not client.is_connected():
                    continue
                msg_info = client.publish(f"multi_mqtt/ping_rtt/{host}", b"", qos=1)
                if msg_info is not None and msg_info.mid is not None:
                    self.record_ping_send(host, msg_info.mid)
                    pinged_hosts.append(host)
                    sent_mids[host] = msg_info.mid
            except Exception:
                # 单点异常不影响其他 broker
                logger.debug("Ping 发送失败 [%s]", host, exc_info=True)

        # [新增-实时探测] 同步等待模式：仅 probe 路径会走到这里
        if wait_timeout > 0 and sent_mids:
            deadline = time.time() + wait_timeout
            # 轮询粒度 20ms：兼顾响应速度与 CPU 占用（探测是低频操作，不构成热点）
            while time.time() < deadline:
                if not self.running:
                    # 探测途中被 stop()：立即放弃等待，别阻塞关闭流程
                    break
                with self.lock:
                    # 只要 sent_mids 里的 mid 仍留在 pending_pings 中，就说明 ACK 还没回来。
                    # 注意：host 一定在 self.stats 里（发送时已确保），无需再判存在。
                    remaining = sum(
                        1 for host, mid in sent_mids.items()
                        if mid in self.stats[host].pending_pings
                    )
                if remaining == 0:
                    break
                time.sleep(0.02)

        return pinged_hosts


# =========================================================================
class MultiMQTTManager:
    # [修复-③] 消息分发队列上限：满则丢弃并告警，绝不阻塞 paho 网络线程
    MSG_QUEUE_MAXSIZE = 10000

    # [修复-N4] 允许的最大 req_id 长度：超过直接拒绝。
    MAX_REQ_ID_LEN = 512

    # [修复-N3] stop() 中等待分发线程退出的超时（秒）。
    DISPATCH_JOIN_TIMEOUT = 5.0

    def __init__(self, brokers=BROKER_LIST, log_messages=False, enable_crypto=False, server_public_key_bytes=None, client_private_key_bytes=None, enable_stats=True, log_connection=None, keepalive=60*5, max_reconnect_delay=3600, recovery_enabled=True, recovery_interval=15.0, recovery_probe_timeout=3.0, recovery_force_gap=30.0, recovery_start_grace=90.0):
        self.brokers = brokers
        self.keepalive = keepalive
        self.max_reconnect_delay = max_reconnect_delay
        # ---- [网络恢复看门狗] ----
        # 背景：手机进入 Doze（尤其华为等 ROM 的 light-idle）后全部 socket 被
        # 切断，paho 指数退避很快顶到 max_reconnect_delay（默认 3600s）；网络
        # 恢复后旧网络线程仍在最长 1 小时的 sleep 中，导致所有 broker 红灯数
        # 小时。看门狗只在【全部节点都掉线】时介入：周期裸 TCP 探测，网络一
        # 恢复就重建所有卡在长退避睡眠里的 client。只要还有任一节点在线，单个
        # 坏节点（TCP 通但 MQTT 不可用）一律不碰，交还 paho 常规退避，避免对
        # 坏节点产生周期性重连与日志刷屏。
        self.recovery_enabled = recovery_enabled
        self.recovery_interval = recovery_interval
        self.recovery_probe_timeout = recovery_probe_timeout
        self.recovery_force_gap = recovery_force_gap
        self.recovery_start_grace = recovery_start_grace
        self._recovery_last_force = {}
        self._recovery_thread = None
        self._started_at = 0.0
        self.clients = {}
        self.log_messages = log_messages
        self.enable_crypto = enable_crypto  # 默认关闭加密
        self.server_public_key_bytes = get_standard_public_pem_bytes(server_public_key_bytes)
        self.client_private_key_bytes = get_standard_pem_bytes(client_private_key_bytes)
        # ------------------------------------------------------------------
        # [修复-⑥ 严重性能瓶颈] ECDSA 密钥对象仅在初始化时解析一次并缓存。
        # ------------------------------------------------------------------
        self._server_vk_invalid = False
        self.server_vk = ecdsa.VerifyingKey.from_pem(self.server_public_key_bytes) if self.server_public_key_bytes else None
        try:
            self.client_sk = (
                ecdsa.SigningKey.from_pem(self.client_private_key_bytes)
                if self.client_private_key_bytes else None
            )
        except Exception:
            logger.exception("解析客户端私钥失败，签名功能将被禁用")
            self.client_sk = None
        # ------------------------------------------------------------------
        self.lock = threading.Lock()
        # --- [统计功能] ---
        self.enable_stats = enable_stats
        self.log_connection = log_connection if log_connection is not None else not enable_stats
        self.stats = ConnectionQualityStats(enabled=self.enable_stats, lock=self.lock)
        # ------------------
        self.dedup_cache = TTLCache(ttl_seconds=30, max_size=50000)
        self.message_callback = None
        self.subscribed_topics = set()
        self._stop_event = threading.Event()
        self._msg_err_log_state = {}
        self._msg_queue = queue.Queue(maxsize=self.MSG_QUEUE_MAXSIZE)
        self._dispatch_thread = None
        self.is_windows_cmd=False

    def set_on_message(self, callback):
        """设置上层回调，签名: fn(topic, data_dict, rx_broker)"""
        self.message_callback = callback

    def _log_msg_error(self, host, exc):
        now = time.time()
        state = self._msg_err_log_state.get(host)
        if state is None:
            self._msg_err_log_state[host] = (now, 0)
            logger.error("处理消息失败 [%s]: %s", host, exc)
            return
        last_time, suppressed = state
        if now - last_time >= 1.0:
            if suppressed > 0:
                logger.error("处理消息失败 [%s]: %s (过去1秒内另有 %d 条同类异常被省略)",
                             host, exc, suppressed)
            else:
                logger.error("处理消息失败 [%s]: %s", host, exc)
            self._msg_err_log_state[host] = (now, 0)
        else:
            self._msg_err_log_state[host] = (last_time, suppressed + 1)

    def _unpack_broker(self, entry):
        """
        [新增-用户名密码支持] 统一解包 broker 配置条目。
        """
        if not isinstance(entry, (tuple, list)):
            logger.error("跳过非法 Broker 配置条目（非 tuple/list）: %r", entry)
            return None
        if len(entry) == 2:
            host, port = entry
            return (host, port, None, None)
        if len(entry) == 3:
            host, port, auth = entry
            if isinstance(auth, (tuple, list)) and len(auth) >= 2:
                username = auth[0]
                password = auth[1]
                if username is None:
                    return (host, port, None, None)
                return (host, port, str(username), "" if password is None else str(password))
            if isinstance(auth, str):
                if ":" in auth:
                    username, password = auth.split(":", 1)
                    return (host, port, username, password)
                return (host, port, auth, "")
            logger.error("跳过非法 Broker 配置条目（auth 字段格式不支持）: %r", entry)
            return None
        logger.error("跳过非法 Broker 配置条目（长度不是 2 或 3）: %r", entry)
        return None

    def start(self):
        """启动与所有 Broker 的连接并启用后台自动断线重连"""
        with self.lock:
            if self.clients:
                logger.warning("MultiMQTTManager.start() 重复调用，已忽略")
                return
            old_thread = self._dispatch_thread
            if old_thread is not None and old_thread.is_alive():
                logger.error(
                    "上一次的分发线程尚未退出（可能有 message_callback 阻塞），"
                    "拒绝启动新的分发线程以免双线程抢队列。请稍后重试或排查阻塞回调。"
                )
                return
            self._stop_event.clear()
            drained = 0
            while True:
                try:
                    self._msg_queue.get_nowait()
                    drained += 1
                except queue.Empty:
                    break
            if drained:
                logger.debug("start() 排空了 %d 条残留消息（含可能的旧毒药丸）", drained)
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_loop, daemon=True, name="MQTTMsgDispatch"
        )
        self._dispatch_thread.start()
        self._started_at = time.time()
        for broker_entry in self.brokers:
            host, port, client = self._build_client(broker_entry)
            if client is None:
                continue
            try:
                client.connect_async(host, port, keepalive=self.keepalive)
                client.loop_start()
                with self.lock:
                    self.clients[host] = client
                if self.log_connection:
                    unpacked = self._unpack_broker(broker_entry)
                    username = unpacked[2] if unpacked else None
                    auth_tag = f" user={username!r}" if username is not None else " (anonymous)"
                    logger.info(f"开启后台连接任务 -> {host}:{port}{auth_tag}")
            except Exception as e:
                try:
                    client.loop_stop()
                except Exception:
                    pass
                logger.error(f"连接初始化失败 [{host}]: {e}")
        if self.enable_stats:
            self.stats.start(self.clients)
        if self.recovery_enabled:
            self._recovery_thread = threading.Thread(
                target=self._recovery_loop, daemon=True, name="MQTTRecovery"
            )
            self._recovery_thread.start()

    def _build_client(self, broker_entry):
        """创建一个 paho client（不连接、不启动 loop）。

        :return: (host, port, client)；非法配置返回 (None, None, None)。
        """
        unpacked = self._unpack_broker(broker_entry)
        if unpacked is None:
            return None, None, None
        host, port, username, password = unpacked
        client_id = f"multi_client_{int(time.time()*1000)}_{uuid.uuid4().hex[:4]}"
        client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt_client.MQTTv311)
        client.user_data_set(host)
        if username is not None:
            try:
                client.username_pw_set(username, password)
            except Exception:
                logger.exception("设置 Broker [%s] 用户名密码失败，仍尝试匿名连接", host)
        client.reconnect_delay_set(min_delay=1, max_delay=self.max_reconnect_delay)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client.on_publish = self._on_publish
        return host, port, client

    def _host_port_map(self):
        """返回当前 broker 配置的 {host: port}（用于恢复时的裸 TCP 探测与重建）。"""
        result = {}
        for broker_entry in self.brokers:
            unpacked = self._unpack_broker(broker_entry)
            if unpacked is not None:
                result[unpacked[0]] = unpacked[1]
        return result

    def _probe_reachable(self, host, port):
        """裸 TCP 探测：网络/对端是否可达。不可达时绝不打扰 paho，避免无谓重建。"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.recovery_probe_timeout)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False
        finally:
            try:
                sock.close()
            except OSError:
                pass

    def _recover_client(self, host):
        """重建一个彻底卡死的 paho client。

        paho 的重连线程陷在最长 3600s 的 ``time.sleep`` 里，外部没有 API 可以
        提前唤醒；直接对其调用 reconnect() 也没人跑 loop 服务新 socket。唯一
        可靠的做法是 terminate 旧线程，用原配置重建一个全新 client。
        """
        with self.lock:
            old = self.clients.get(host)
        if old is not None:
            try:
                old._thread_terminate = True
                try:
                    # 唤醒可能阻塞在 select 上的网络线程
                    try:
                        old._sockpairW.send(b"x")
                    except Exception:
                        pass
                    old.loop_stop()
                finally:
                    try:
                        old._sock_close()
                    except Exception:
                        pass
            except Exception:
                logger.debug("终止旧 client 线程 [%s] 时出现异常", host, exc_info=True)
        entry = None
        for broker_entry in self.brokers:
            unpacked = self._unpack_broker(broker_entry)
            if unpacked is not None and unpacked[0] == host:
                entry = broker_entry
                break
        if entry is None:
            logger.warning("恢复重建时找不到 broker 配置，已跳过: %s", host)
            return False
        new_host, port, client = self._build_client(entry)
        client.connect_async(new_host, port, keepalive=self.keepalive)
        client.loop_start()
        with self.lock:
            # 极端情况下 stop() 已清空 dict：放弃这个新 client
            if self._stop_event.is_set():
                try:
                    client.loop_stop()
                except Exception:
                    pass
                return False
            self.clients[new_host] = client
        logger.info("♻️ [网络恢复] Broker %s:%s 已重建 client（网络可达但旧连接未自愈）", new_host, port)
        return True

    def _recovery_loop(self):
        """网络恢复看门狗主循环（仅在“全集群掉线”时介入）。

        核心策略（真机结论）：
        - 集群里**只要还有任意一个 broker 在线**，个别掉线节点（典型：TCP
          能连但 MQTT 服务不可用的坏节点）一律交给 paho 自身的指数退避
          （封顶 max_reconnect_delay），看门狗**绝不**强制重建——否则会对
          这种坏节点产生周期性重连与日志刷屏，也没有任何收益。
        - 只有**全部节点都掉线**（整机断网 / Doze 冻结后 socket 全断）时，
          才判定为全局网络事件：裸 TCP 并行探活，一旦有 broker 可达即认为
          网络恢复，一次性重建所有“可达但仍掉线”的 client，绕开它们陷在
          paho 长退避睡眠里的旧网络线程。一个都不可达则安静等待下一轮。
        """
        ev = self._stop_event
        # 启动宽限期：让首轮正常连接完成，避免把"还没连上"误判成"全灭"
        if ev.wait(self.recovery_start_grace):
            return
        while not ev.is_set():
            try:
                with self.lock:
                    snapshot = dict(self.clients)
                if snapshot:
                    online, dead_hosts = 0, []
                    for host, client in snapshot.items():
                        try:
                            if client.is_connected():
                                online += 1
                            else:
                                dead_hosts.append(host)
                        except Exception:
                            dead_hosts.append(host)
                    if online == 0 and dead_hosts:
                        self._global_recovery_scan(dead_hosts)
            except Exception:
                logger.exception("网络恢复看门狗一轮扫描异常")
            ev.wait(self.recovery_interval)

    def _global_recovery_scan(self, dead_hosts):
        """全集群掉线时的快速恢复扫描。

        只在所有节点都不在线时调用：TCP 探活确认网络是否回来，回来就一次性
        重建所有可达节点；网络仍断则完全静默（不重建、不刷日志）。
        """
        ev = self._stop_event
        now = time.time()
        due = [
            h for h in dead_hosts
            if now - self._recovery_last_force.get(h, 0.0) >= self.recovery_force_gap
        ]
        if not due:
            return
        host_port = self._host_port_map()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=min(8, len(due))
        ) as pool:
            reachable = dict(zip(
                due,
                pool.map(
                    lambda h: self._probe_reachable(h, host_port.get(h, 1883)),
                    due,
                ),
            ))
        good = [h for h, ok in reachable.items() if ok]
        if not good:
            # 全部 broker 都不可达 = 网络仍未恢复，安静等待，绝不重建
            return
        built = 0
        for host in good:
            if ev.is_set():
                break
            # 重建前再次确认仍未连接（paho 可能恰好在此刻自愈了）
            with self.lock:
                current = self.clients.get(host)
            try:
                if current is not None and current.is_connected():
                    continue
            except Exception:
                pass
            self._recovery_last_force[host] = time.time()
            try:
                if self._recover_client(host):
                    built += 1
            except Exception:
                logger.exception("恢复重建 client 失败 [%s]", host)
        if built:
            logger.warning(
                "🌐 [全局恢复] 检测到全部 %d 个 broker 掉线后网络恢复，"
                "已快速重建 %d 个可达节点（其余单点掉线仍走 paho 常规退避）",
                len(dead_hosts), built,
            )


    def wait_connected(self, min_count=1, timeout=10.0, poll_interval=0.05):
        """阻塞等待，直到至少 ``min_count`` 个 broker 建立连接或 ``timeout`` 到期。

        替代早期调用方写死的 ``time.sleep(2)``：最快的 broker（实测约
        110ms）一连上就立即放行后续订阅/握手，不再无意义干等；全部 broker
        都连不上时最多等到超时，随后台自动重连继续，调用方仍可往下走。

        :return: 放行/超时时刻的在线连接数（超时也不抛异常，由调用方决定）。
        """
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            with self.lock:
                snapshot = list(self.clients.values())
            online = 0
            for c in snapshot:
                try:
                    if c.is_connected():
                        online += 1
                except Exception:
                    pass
            if online >= min_count or time.monotonic() >= deadline:
                return online
            time.sleep(max(0.005, float(poll_interval)))

    # ================= [基于 Userdata 的统一回调] =================
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        host = userdata
        if rc == 0:
            if self.log_connection:
                logger.info(f"✅ [已连接] Broker: {host}")
            self.stats.on_connect(host)
            with self.lock:
                topics_snapshot = list(self.subscribed_topics)
            for topic in topics_snapshot:
                client.subscribe(topic)
        else:
            if self.log_connection:
                logger.warning(f"❌ [连接失败] Broker: {host}, rc={rc}")

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        host = userdata
        if self._stop_event.is_set():
            self.stats.finalize_for_shutdown(host)
            return
        self.stats.on_disconnect(host)
        if rc != 0:
            if self.log_connection:
                logger.warning(f"⚠️ [意外断开] Broker: {host} (rc={rc})，自动尝试重连...")

    def _on_publish(self, client, userdata, mid, *args, **kwargs):
        host = userdata
        if self.enable_stats:
            self.stats.on_publish_ack(host, mid)

    def _on_message(self, client, userdata, msg):
        """
        注意：此回调运行在 paho-mqtt 的后台网络循环线程中。
        因此这里只做轻量的解析 / 验签 / 去重 / 入队，把业务回调丢到独立线程消费。
        """
        host = userdata
        try:
            if msg.topic.startswith("multi_mqtt/ping_rtt/"):
                return
            if self.log_messages:
                logger.info(f"📩 收到消息 [{msg.topic}] 来自 {host}  {msg} {msg.payload}")    
            raw_payload = msg.payload.decode('utf-8')
            data = process_cipher(raw_payload, decrypt=True, enabled=self.enable_crypto)
            if not isinstance(data, dict):
                logger.warning(f"⚠️ [{host}] 收到无效非字典消息格式，忽略处理")
                return
            raw_req_id = data.get("req_id")
            if raw_req_id is None:
                req_id = None
            elif isinstance(raw_req_id, str):
                req_id = raw_req_id
            else:
                req_id = str(raw_req_id)    
            if req_id is not None and len(req_id) > self.MAX_REQ_ID_LEN:
                logger.warning(
                    f"⚠️ [{host}] 拒绝处理: req_id 长度超限 (len={len(req_id)} > {self.MAX_REQ_ID_LEN})"
                )
                return
            if req_id and not self.dedup_cache.add_if_not_exists(req_id):
                return
            
            # --- ECDSA 验证防重放核心逻辑 ---
            if "code" in data:
                if self.server_vk is None:
                    logger.debug(
                        "ℹ️ [%s] 服务器未配置公钥，跳过验签检查。req_id=%s | has_code=%s",
                        host, req_id, ("code" in data),
                    )
                else:
                    if not isinstance(req_id, str) or "|" not in req_id:
                        logger.warning(
                            f"⚠️ [{host}] 拒绝执行: 缺少 ECDSA 签名结构 "
                            f"(req_id 类型={type(raw_req_id).__name__}) | "
                            f"server_pubkey={_describe_public_key(self.server_public_key_bytes,short_text=True)}"
                        )
                        return
                    base_req_id, sig_hex = req_id.rsplit("|", 1)
                    logger.debug(
                        "🔑 [%s] 请求已签名，开始验签: req_id=%s | base_req_id=%s | signature_len=%d | server_pubkey=%s",
                        host, req_id, base_req_id, len(sig_hex),
                        _describe_public_key(self.server_public_key_bytes,short_text=True),
                    )
                    code_str = str(data.get("code", ""))
                    try:
                        ts_str = str(int(float(data.get("timestamp", 0))))
                    except (ValueError, TypeError):
                        ts_str = "0"
                    sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
                    try:
                        self.server_vk.verify(bytes.fromhex(sig_hex), sign_msg, hashfunc=hashlib.sha256)
                        logger.debug("✅ [%s] ECDSA 验签成功，允许执行: req_id=%s", host, base_req_id)
                    except Exception:
                        logger.warning(f"⚠️ [{host}] 拒绝执行: ECDSA 签名无效 , req_id={req_id} , server_pubkey={_describe_public_key(self.server_public_key_bytes,short_text=True)}")
                        return
            
            if self.message_callback:
                try:
                    self._msg_queue.put_nowait((msg.topic, data, host))
                except queue.Full:
                    logger.warning(
                        "⚠️ [%s] 消息分发队列已满(%d)，丢弃消息 topic=%s",
                        host, self.MSG_QUEUE_MAXSIZE, msg.topic,
                    )
        except Exception as e:
            self._log_msg_error(host, e)

    def _dispatch_loop(self):
        """
        [合并线程] 独立的分发线程：串行消费队列，并统一驱动 Stats 的 Ping 与报告打印。
        """
        if self.enable_stats:
            ping_interval = self.stats.PING_INTERVAL
            print_interval = self.stats.print_interval
            next_ping_at = time.time() #首次 Ping 不要等
            next_print_at = (time.time() + print_interval) if print_interval > 0 else float('inf')
        else:
            next_ping_at = next_print_at = float('inf')

        while not self._stop_event.is_set():
            now = time.time()
            
            if self.enable_stats:
                wait_time = min(next_ping_at - now, next_print_at - now)
                wait_time = max(0.1, wait_time)  # 兜底防负数
            else:
                wait_time = None 

            try:
                item = self._msg_queue.get(timeout=wait_time)
                
                if item is None:
                    return
                    
                topic, data, host = item
                try:
                    if self.message_callback:
                        self.message_callback(topic, data, host)
                except Exception:
                    logger.exception("消息分发回调执行失败 [%s] topic=%s", host, topic)
                    
            except queue.Empty:
                pass

            if self._stop_event.is_set():
                return

            if self.enable_stats:
                now = time.time()
                if now >= next_ping_at:
                    # [说明] 周期 Ping 走异步模式（wait_timeout=0），不等 ACK，
                    # 保持分发线程不被探测阻塞。需要实时数据时由 get_report(probe=True) 单独走同步探测。
                    self.stats._send_pings()
                    next_ping_at = now + ping_interval
                
                if now >= next_print_at:
                    try:
                        logger.info("📡 [连接质量统计报告]" + self.stats.get_report(probe=False,is_windows_cmd=self.is_windows_cmd))
                    except Exception:
                        logger.exception("生成连接质量统计报告失败")
                    next_print_at = now + print_interval

    def subscribe(self, topic: str):
        with self.lock:
            self.subscribed_topics.add(topic)
            clients_snapshot = list(self.clients.items())
        for host, c in clients_snapshot:
            if c.is_connected():
                c.subscribe(topic)

    def publish_broadcast(self, topic: str, payload_dict: dict, client_private_key_bytes=None):
        """广播传输消息"""
        out_payload = payload_dict.copy()
        if client_private_key_bytes:
            try:
                sk = ecdsa.SigningKey.from_pem(get_standard_pem_bytes(client_private_key_bytes))
            except Exception:
                logger.exception("解析调用方临时私钥失败，本次广播将不做签名")
                sk = None
        else:
            sk = self.client_sk
        if sk is not None and "code" in out_payload:
            if "req_id" not in out_payload:
                raise ValueError(
                    "publish_broadcast: 报文包含 'code' 且已配置私钥时，必须提供 'req_id' 字段"
                )
            if "timestamp" not in out_payload:
                raise ValueError(
                    "publish_broadcast: 报文包含 'code' 且已配置私钥时，必须提供 'timestamp' 字段"
                )
            base_req_id = str(out_payload["req_id"])
            code_str = str(out_payload.get("code", ""))
            try:
                ts_str = str(int(float(out_payload["timestamp"])))
            except (ValueError, TypeError):
                ts_str = "0"
            sign_msg = f"{base_req_id}|{code_str}|{ts_str}".encode('utf-8')
            signature = sk.sign(sign_msg, hashfunc=hashlib.sha256)
            out_payload["req_id"] = f"{base_req_id}|{signature.hex()}"
        payload_str = process_cipher(out_payload, decrypt=False, enabled=self.enable_crypto)
        with self.lock:
            clients_snapshot = list(self.clients.items())
        for host, c in clients_snapshot:
            if c.is_connected():
                c.publish(topic, payload_str, qos=0)

    def stop(self):
        self._stop_event.set()

        try:
            while True:
                try:
                    self._msg_queue.get_nowait()
                except queue.Empty:
                    break
            try:
                self._msg_queue.put_nowait(None)
            except queue.Full:
                while True:
                    try:
                        self._msg_queue.get_nowait()
                    except queue.Empty:
                        break
                try:
                    self._msg_queue.put_nowait(None)
                except queue.Full:
                    logger.warning(
                        "毒药丸投递失败（队列持续满载），分发线程将依赖 _stop_event 自然退出"
                    )
        except Exception:
            logger.debug("投递毒药丸时出现异常", exc_info=True)

        if self.enable_stats:
            self.stats.stop()
        rt = self._recovery_thread
        if rt is not None and rt.is_alive():
            # 看门狗检查 _stop_event 退出；一轮扫描最坏受探测超时限制，稍作等待
            rt.join(timeout=self.recovery_probe_timeout * 2 + self.recovery_interval + 2.0)
            if rt.is_alive():
                logger.warning("网络恢复看门狗线程未在预期时间内退出")
        self._recovery_thread = None
        with self.lock:
            clients_snapshot = list(self.clients.values())
            self.clients.clear()
        for c in clients_snapshot:
            try:
                c.loop_stop()
                c.disconnect()
            except Exception:
                logger.debug("关闭 MQTT client 时出现异常", exc_info=True)
        t = self._dispatch_thread
        if t and t.is_alive():
            t.join(timeout=self.DISPATCH_JOIN_TIMEOUT)
            if t.is_alive():
                logger.warning(
                    "⚠️ 分发线程未在 %.1f 秒内退出，可能有 message_callback 阻塞；"
                    "下次 start() 前请先确认旧线程已结束，否则会被拒绝启动",
                    self.DISPATCH_JOIN_TIMEOUT,
                )
        if t is not None and not t.is_alive():
            self._dispatch_thread = None
        logger.info("所有 MQTT 连接已安全关闭")