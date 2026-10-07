# MQTT RPC 请求/响应流程与安全边界
+
+## 1. 真实的消息流
+
+### 1.1 客户端发送请求
+
+客户端在发起 `code` 请求时，如果自己配置了私钥（例如 `--key ...`），会在 `MultiMQTTManager.publish_broadcast()` 中为 `req_id` 追加签名：
+
+- 原始 `req_id`：`20260911_173656.404 8d795f`
+- 签名后：`20260911_173656.404 8d795f|6a0520...`
+
+签名内容是：
+
+- `base_req_id`
+- `code`
+- `timestamp`
+
+也就是：
+
+`sign_msg = f"{base_req_id}|{code}|{timestamp}"`
+
+这一步是“客户端发起命令”时的签名，不是服务端返回结果的签名。
+
+### 1.2 服务端接收请求
+
+服务端收到请求时，`MultiMQTTManager._make_on_message()` 会先检查是否存在 `code` 字段；如果服务端已配置了 `server_public_key_bytes`，那么才会执行验签。
+
+关键点：
+
+- `server_public_key_bytes` 是服务端自己的公钥
+- `client_private_key_bytes` 是客户端自己的私钥
+- 两者是不同侧的密钥材料
+
+因此：
+
+- 客户端启动时是否带私钥，决定它是否签名发请求
+- 服务端启动时是否带公钥，决定它是否验签收到的请求
+
+`server_mqtt.py --pub ...` 只是“服务端启用验签功能”的启动参数，不是客户端默认自动注入的公钥。
+
+### 1.3 服务端返回结果
+
+服务端处理 `code` 后，返回的是普通 JSON：
+
+```python
+{
+    "req_id": "...",
+    "r": ...,
+    "stdout": ...,
+    "ok": True,
+    "server_time": ...,
+    "server_from": ...,
+}
+```
+
+这里没有 `code` 字段，因此它不是“新的命令请求”，而是“对原请求的执行结果”。
+
+也就是说，服务端回包不会再次走“签名发起请求”的逻辑，它只是普通广播回包。
+
+### 1.4 客户端接收回包
+
+客户端收到回包之后，会拿到 `req_id` 去匹配当前 pending request。
+
+如果客户端启用了私钥签名，而当前没有已知服务端公钥，并且没有显式允许 `allow_no_server_pubkey_response=True`，那么它应该拒绝回包，避免“无公钥/无信任锚点的服务端”伪装成可信响应。
+
+这条规则在客户端代码中必须做，且默认值应该是拒绝，而不是“静默放行”。
+
+壳层逻辑是：
+
+```python
+signed_req = bool(client_private_key_bytes)
+no_server_pubkey = not bool(self.mqtt_net.server_public_key_bytes)
+
+if signed_req and no_server_pubkey and not allow_no_server_pubkey_response:
+    # 拦截无公钥服务器返回
+    return
+```
+
+这不是“客户端搞公钥”，而是“在私钥模式下，对未知服务器的返回做安全门槛”。
+
+## 2. 这次错误的根源
+
+问题不是“客户端不该碰公钥”，而是“之前把默认公钥注入到了客户端，导致流程被绕乱了”。
+
+这会导致：
+
+- 一个正常的默认 server 被当成“带公钥的可信服务器”
+- 结果绕过了本来应该出现的拒绝门槛
+- 违背了“在私钥模式下，如果不显式允许，就不接受无公钥服务器返回”的设计
+
+因此，正确修法是：
+
+- 不要写死默认 server 公钥
+- 让用户显式决定是否允许无公钥回包
+- 当私钥模式 + 无已知服务端公钥 + 未允许时，直接丢弃回包
+
+## 3. 代码中真正的分工
+
+- `client/client_mqtt.py`：
+  - 发送签名请求
+  - 接收响应
+  - 在私钥模式下对“无公钥服务器回包”应用安全开关
+
+- `server_mqtt.py`：
+  - 只负责本机作为 server
+  - 通过 `--pub` 启用/配置自己的公钥
+  - 处理请求并返回结果
+
+- `multi_mqtt.py`：
+  - 真正的 MQTT 网络层
+  - 处理签名封装、验签、去重和广播
+
+## 4. 一句话总结
+
+客户端签名发出 `code`，服务端验签执行；服务端返回普通结果；客户端在私钥模式下只在“无已知公钥 + 未显式允许”时拒收回包。
+
+这才是符合当前协议设计的主线流程。
+
+
+如果两个服务端 ./server_mqtt.py  同时运行，  client 只会显示最先到达的那个
+
+
+降低 CPU 与耗电 (Low-Resource & Power Efficiency):

去磁盘 IO：丢弃了频繁写 SQLite 的操作，换用内存 TTLCache 实现 O(1) 时间复杂度的消息查重，显著降低 CPU 负载与磁盘读写耗电。

事件驱动唤醒：客户端使用 threading.Event() 实现零 CPU 占用率的等待机制，只要有一个 Broker 返回消息就立刻唤醒主线程。

多路抗丢包与首胜机制 (Racing Protocol):

客户端发送时：一条请求并发投递给 5 个不同的 MQTT Broker。

服务端收到后：仅处理最先到达的那一条，后续到达的相同 msg_id 被 TTLCache 直接抛弃。

服务端响应时：同样广播至所有 Broker；客户端仅接收最先到达的 Response 并解除阻塞。




mqtt_server.py 收到 code
调用 PythonExecutor.execute(code)
ast.parse() 解析代码
发现最后一条语句是表达式 3
把前面的语句执行完
对最后表达式执行 eval
eval("3") 返回整数 3
服务端 format_result(3) 转成字符串 "3"
返回：


r=3这不是最后表达式，而是赋值语句：
进入普通 exec
执行后 locals 中出现 r: 3
执行器读取 locals["r"]
返回结果 3
server 格式化后返回 "r": "3"

所以 MQTT 支持两种结果来源


#TODO
加入非对称加密与解密支持。