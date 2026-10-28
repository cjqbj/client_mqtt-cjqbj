multi_mqtt 全库审计报告
TL;DR：代码工程质量和测试纪律（190 项测试全绿）远超一般个人项目，但有 3 个已动态实锤的协议级安全漏洞（去重先于验签可被投毒、签名请求 30 秒后可无限重放、硬编码共享信任锚）、1 个死掉的模块（rpc_tcp.py 构造即崩溃）、以及一条贯穿全项目的架构软肋：在公共 broker 上明文跑一条“任意 Python 执行”通道，而本地 HTTP 控制面同样无鉴权暴露在 0.0.0.0。

一、安全类隐藏问题（高危，全部有验证证据）
S1. 去重发生在验签之前 → 未认证的请求压制 DoS（已动态验证）
multi_mqtt.py:1326 的 dedup_cache.add_if_not_exists(req_id) 在 multi_mqtt.py:1357 的 ECDSA 验签之前执行。我用临时脚本验证了三个后果：

任何人向 request topic 发一条带某 req_id 的垃圾消息（无需签名）→ 该 req_id 被“占坑”，随后到达的真正签名请求被 dedup 直接丢弃；
req_id 格式完全可预测（get_req_id = 本地时间到毫秒，multi_mqtt.py:130-135），攻击者可以按当前时间批量预占未来几十毫秒的 req_id，压制受害者请求；
缓存 max_size=50000 FIFO 淘汰——灌 5 万个垃圾 req_id 即可整缸冲掉，或等 30s TTL 过期。
修法方向：把去重移到验签成功之后（对验签失败的 req_id 不应占用去重容量）。

S2. 无 timestamp 新鲜度校验 → 签名请求可无限重放（已动态验证）
签名覆盖 req_id|code|timestamp，但服务端从不检查 timestamp 距当前时间多远（全仓库 grep 确认只在拼签名串时用到它）。重放防线仅有 30 秒 TTLCache——我验证了 TTL 过期后同一“签名帧”再次通过 dedup。考虑到：

你们自己的注释（multi_mqtt.py:96-107）记录了 5 个公共 broker 允许 # 通配订阅，任何人都可被动嗅探到合法签名请求；
嗅探到的帧等 30 秒重发即通过验签。
对一个能执行任意 Python 的通道来说，这是完整的重放 RCE 链。修法方向：验签通过后校验 |now - timestamp| < 窗口（如 ±120s），且去重缓存条目对已验签 req_id 永久保留（或按 timestamp 拒绝过期帧，使重放天然无效）。

S3. 共享信任锚与“验签可一键关闭”
server_mqtt.py:27 硬编码 DEFAULT_PUB_KEY：所有默认部署共用同一把验证公钥，持对应私钥者（密钥生成者）可向任何默认服务器下发命令。我尝试了 1~5000 及常见弱整数暴力匹配，不是弱私钥，但“默认共享信任锚”本身的模型问题不变。
server_mqtt.py:148（pub_key or None）+ 提交 9a8a2e1：rpc.json 写入 pub_key: "" 即视为不验签，此后任何知道 topic 的人可执行任意代码。
发现链是现成的：global_topic_list 报告默认开启并以 retain=True 发布到公共 broker（server_mqtt.py:42,618），内容包含该服务器的 request_topic——等于公开目录服务。
附带：multi_mqtt.py:137 硬编码 AES_KEY=b"12345678901234567890123456789012"，开加密等于没开；socks5/pty 数据帧完全不加密不签名（子代理确认，见 S6）。
S4. 本地 HTTP RPC 控制面：零鉴权 + 默认 0.0.0.0
server_http.py:369 的 key 默认空 = URL 路径直接当 Python 执行（server_http.py:308）。三个入口默认绑所有网卡且注入完整 globals/locals：

客户端 REPL client_mqtt.py:765-786（0.0.0.0:1166，_default_client 含私钥，LAN 内任何人可 /print(_default_client.client_private_key_bytes) 直接偷走签名私钥）；
服务端 CLI server_mqtt.py:802-835（0.0.0.0:1177）；
pty 客户端 pty_client_mqtt.py:2242-2308（0.0.0.0:1188，子代理确认还可经 ai_bridge.run() 横向跳到远端执行 shell）。
对比之下 socks5 客户端反而做对了（127.0.0.1 + 告警）。修法方向：这三个默认值改 127.0.0.1 并默认启用随机 key。

S5. 混淆的第三方凭据进了公开仓库
multi_mqtt.py:88-89：runmqtt.com 的用户名/密码明文；flespi token 用 bytes.decode('cp500') 混淆（旁边的注释还记录了因 GitHub push protection 而绕道的痕迹）。这不是你们自己的服务凭据，是第三方平台的，建议从源码移到本地配置。

S6. socks5/pty 数据面在公共 broker 上完全裸奔（子代理实锤）
会话帧只靠 topic 不可猜（sid 仅 16 bit 熵）+ 明文 owner 字段。任何订阅 s5/+/out 的人可：读全部隧道流量、注入 open 让你的服务器向任意目标发起 TCP（SSRF/内网穿透）、注入数据污染字节流、发 stop 杀会话、伪造大 NACK 列表造成内存放大。握手已有 ECDSA，修法方向：由握手协商会话密钥，对数据帧加 MAC（哪怕不加密）。

二、可靠性隐藏问题（中高危）
R1. 一行 while True: pass 永久瘫痪整个 MQTT 服务端
server_mqtt.py:550 的 executor.execute(code) 在 MQTTMsgDispatch 单分发线程内同步执行，且 executor 的 RLock 覆盖整个执行过程（我验证了并发第二请求被阻塞 1.3s 的串行语义）。死循环代码 = 分发线程永久卡死：不再处理任何消息、ping/统计/周期任务全部停摆，且无超时无看门狗。HTTP 侧同理：server_http.py:304-313 的模块级 _stdout_lock 包住整个执行，一条慢请求冻结所有 HTTP RPC（pty 子代理确认 ai_pty_status/日志台在写线程卡死这类最需要诊断的时刻恰好不可用）。 修法方向：用户代码移入带超时的 worker 池执行；_stdout_lock 只护 stdout 交换那一小段（executor 内部已有 _redirect_stdout，外层这层是重复的）。

R2. socks5 服务端模板 _in_loop 是无保护单点（子代理，高危）
socks5_client_mqtt.py:892（int(_sq)）、:753（int(port)）等无 try：一条畸形帧（如 seq:"x"）即打死会话线程，跳过全部收尾（连接、路由、注册表、心跳照常 → 会话表面健康实则黑洞，_inq 无界堆积直到 24h TTL）。客户端侧同路径早修了（补发改到了专职线程），模板没同步。

R3. rpc_tcp.py 整体烂掉（已动态验证）
TCPServer.__init__ 第一行就 NameError: locals_dict is not defined（rpc_tcp.py:56，参数里根本没有这个变量）；__main__ 传的 globals_dict= 也不是合法 kwarg；_process_request 调 execute(code, locals_dict=...) 而该方法签名只有 globals——9 月 17 日 executor 重构后此文件从未跟上。要么删要么修，留着会误导。


R5. 其他中等问题（子代理实锤，列举）
U.get_machine_id() 每次调用跑 PowerShell/reg，实测 ~1 秒且无缓存（在 MQTTServer.__init__ 路径上）；get_system_id() 返回空的机器（极简容器）会得到 mf_mo_ 这种碰撞 topic（U.py:201-205）。
rpc() 默认 min_broker_count=11（client_mqtt.py:336）：公共 broker 挂到只剩 10 个时所有请求硬失败，脆。
服务端把执行结果广播到攻击者指定的 reply_topic（server_mqtt.py:508,578），且默认 sys/device/response 是共享 topic——任何人订阅公共 broker 上的它就能看到别人命令的全部 stdout 和返回值。


grep/ls/pull_dir 单条回包绕过 1MiB 在线红线（remote_cmd.py:581-585 等），大输出表现为无诊断的 RemoteTimeout。
REPL 的 %watch 丢首词（cmd_client_mqtt.py:535）、%grep --include 缺参崩溃（:661）、pty 魔术栏热调 heartbeat 不联动 dead_timeout 误杀会话、detach 意外杀远端裸 shell（与文档承诺相反）等——细节见各文件。
_on_connect 重连重订阅不带 qos（multi_mqtt.py:1278）而 subscribe(**ka) 接受 qos——目前上层全用 QoS0 所以没炸，属 API 契约陷阱。
每条请求 logger.info（server_mqtt.py:518）与“日志要克制”的注释自相矛盾，公共 topic 上可被刷日志。


三、值得肯定的部分
公平地说，这些做得很好：竞速协议（多 broker 首胜）+ 30s 去重的设计对公共 broker 的不确定性是对的；rpc.json 原子写（tempfile+fsync+os.replace，server_mqtt.py:224-231）；socks5 那批线程生命周期/反压修复（git 历史里 4f87eb1、e823093 等提交质量很高）；build_code 的双重 JSON 编码把注入面收敛得干净（子代理专项确认无逃逸）；别名表统一三处参数名；测试套件 190 项 62 秒全绿（注意：部分测试会真连公共 broker，在 CI 里算隐患）。

四、架构优化建议（按收益排序，均未动手）
安全三件套（最高优先）：去重移到验签后 + timestamp 窗口校验 + 废除 DEFAULT_PUB_KEY（首启生成 per-deployment 密钥对）。这三条改动小、收益最大。
执行隔离：MQTT/HTTP 两通道的用户代码统一进“带超时的 worker 池”，分发线程只做路由；_stdout_lock 缩小到 stdout 交换段。
HTTP 控制面收敛：默认 127.0.0.1 + 随机 key；把 RPCRequestHandler 的类级可变状态（executor/key/websocket 配置）改成实例化 server 对象——现在同进程起两个 server 会静默互相覆盖配置。
socks5/pty 协议单源化：服务端模板与客户端各自维护一套 seq 重组/NACK/常量，已出现 open_timeout、enable_crypto 两处漂移；建议帧格式与重组算法抽成一段共享代码，模板由它生成。pty_client_mqtt.py（2333 行七层职责）同理可拆 console/termwriter/loghub/ai_bridge/session 五块。
清理：删 rpc_tcp.py（或修好）；删 server_http.qpsu() 里的 192.168.1.100 死代码；U.get_machine_id 加 functools.lru_cache；requirements.txt 补 paho-mqtt/ecdsa 并钉版本（_recover_client 依赖 _thread_terminate/_sockpairW 私有 API，paho 升级会静默破坏）；ensure_dependencies 的 import 时 pip install 副作用移到显式入口。
仓库卫生：还剩一个 10 月 5 日的 stash（"WIP on master"）和过期的 origin/master 指针；ReadMe.md 第 98 行起是带 + 前缀的粘贴 diff 残留；test_all_public_topic.py 订阅 # 抓全量公共流量（188M 消息那个统计就是它跑出来的），建议在文件头加显著的使用警告。
需要的话，下一步我可以把第 1 条（安全三件套）做成最小 diff 补丁并配套回归测试。