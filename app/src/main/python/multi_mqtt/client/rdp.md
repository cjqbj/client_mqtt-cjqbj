# deepseek 代码审查报告
按「确定 Bug / 安全 / 协议 / 页面逻辑 / 布局性能 / 代码质量 / Win32 细节」分类审查。带“潜在”的表示依赖运行环境或外部 server_http.py，但代码本身存在风险。

## 一、确定的功能性 Bug

0. 【这个不是bug  截图后红点/坐标吸附基本失效】

1. `_vk_of` 中 `user32.VkKeyW` 拼写错误
Win32 API 是 `VkKeyScanW`，不是 `VkKeyW`。
任何单字符键（非数字、非 a-z）走到回退分支都会抛 AttributeError。

另外 `VkKeyScanW` 返回 SHORT，高字节是 shift 状态；代码只取 `& 0xFF`，忽略 shift，导致 !、@、? 等需要 Shift 的字符仍会敲错。

修复：`user32.VkKeyScanW(ord(text))`，并根据高字节决定是否先按 Shift。


2. 非 BMP 字符写剪贴板尺寸计算错误（emoji 等）
`set_clipboard_text`：
```python
size = (len(data) + 1) * ctypes.sizeof(ctypes.c_wchar)
handle = kernel32.GlobalAlloc(0x0002, size)
...
ctypes.create_unicode_buffer(data, len(data) + 1)
```

`len(data)` 是 Python 码点数；非 BMP 字符（如 😀）在 UTF-16 中占 2 个 wchar，`len+1` 不够。

后果：GlobalAlloc 分配字节不足，create_unicode_buffer 可能截断或抛 “string too long”，剪贴板内容错误甚至越界。

修复：按 UTF-16 码元数分配：

```
wchar_count = len(data.encode('utf-16-le')) // 2 + 1
size = wchar_count * ctypes.sizeof(ctypes.c_wchar)
buf = ctypes.create_unicode_buffer(data, wchar_count)
```

3. `type_text` 对非 BMP 字符截断
`item.union.ki.wScan = ord(ch)`，`wScan` 是 WORD；`ord('😀') = 0x1F600` 赋给 WORD 被截成 `0xF600`。

正确做法：对非 BMP 字符拆成 UTF-16 surrogate pair，发送两次 `KEYEVENTF_UNICODE`（每个 surrogate 一次 keydown+keyup）。

4. `rdp.key` 无法表达 `+` 键
press 用 `split('+')` 解析组合键，`press('+')` 得到 `['','']` 过滤后为空，抛 empty key combo；`press('ctrl++')` 也被拆坏。
同理，`+` 作为键本身无法发送。

建议：支持转义（如 plus）或改用其他分隔符。

5. 全角数字被错误映射
`_vk_of` 中 `text.isdigit()` 对 `'１'`（全角）返回 True，`int('１')` 得 1，随后映射为 `0x31`（主键盘 1），但用户可能想发送全角字符或应走 Unicode 回退。

建议：只接受 ASCII `'0'-'9'`，或直接走 `VkKeyScanW/Unicode` 路径。

6. `ctypes.windll` 在非 Windows 上导入即失败
模块顶部：

```
user32 = ctypes.windll.user32
```

在 Linux/macOS 上 `ctypes.windll` 不存在，导入直接 AttributeError。

注释说“允许模块导入成功，调用截图时再给出明确报错”，但只保护了 Pillow，没保护 windll。与注释意图不一致。

7. DPI 缩放可能导致截图坐标与鼠标坐标不一致（潜在）
进程未声明 DPI 感知。高 DPI 下 `GetSystemMetrics`、`SetCursorPos`、`ImageGrab.grab()` 的行为取决于 Pillow 版本和系统 DPI 虚拟化。

多显示器不同 DPI 时更容易错位。

建议：启动时调用 `SetProcessDpiAwareness(PROCESS_PER_MONITOR_DPI_AWARE)` 或 `SetProcessDPIAware()`，并明确截图/坐标都走物理像素。

8. `click_at` 把 middle 当 left
只判断 `str(button).lower() == 'right'`，其他一律走左键。

传 middle 会静默左键点击。

建议：显式校验 left/right/middle，未知值报错。

9. `SendInput` 返回 0 时静默失败
`type_text` 只累加 sent，不检查是否为 0。

当目标窗口权限更高（UIPI）或前台窗口不可输入时，`SendInput` 返回 0，用户以为输入成功。

建议：若 `sent == 0` 或少于预期，抛出/返回明确错误。

## 二、安全问题

1. CSRF：GET 请求直接执行鼠标/键盘操作
RPC 全部是 GET，且无 token/Origin 校验。恶意网页可以用 `<img src="[http://127.0.0.1:1144/import%20rdp;rdp.click(p,100,100)](http://127.0.0.1:1144/import%20rdp;rdp.click(p,100,100))">` 或 `fetch(..., {mode:'no-cors'})` 触发点击、输入、剪贴板写入。

同源策略只阻止读响应，不阻止执行。

建议：至少要求 POST + CSRF token，或校验 Origin/Sec-Fetch-Site；本地服务也要防 DNS rebinding。

2. 无认证，依赖 `server_http.py` 绑定地址
文档写 `127.0.0.1`，但服务端代码未提供。若绑定 `0.0.0.0`，局域网任何设备可完全控制桌面。

建议：在文档和代码中强制只绑定 `127.0.0.1`，或增加简单 token。

3. 敏感文本多处泄露

- rdp.input 文本拼进 URL：进入浏览器历史、代理/服务器访问日志。
- 输入历史明文存 localStorage（`rdp_input_hist`）。
- log 显示 RPC 表达式（截断 200 字符），包含输入文本。
- 剪贴板读取无任何提示，直接回填输入框。

建议：敏感输入走 POST body；历史可关闭或加密；日志脱敏。

4. `server_http.py` 执行 RPC 的方式未知（潜在高危）
如果服务端用 `eval/exec` 直接执行路径中的表达式，且未严格限制，存在注入面。虽然前端 pyQuote 对 Python 单引号字面量做了转义，但服务端解析、URL 解码、长度限制等都可能出问题。

必须审查 `server_http.py` 的表达式白名单/沙箱/长度限制。

## 三、协议与传输

1. 长文本会超过 URL 长度限制
`rdp.input`、`rdp.setclip` 把整段文本放进 URL。浏览器和服务器通常限制 2KB–8KB，稍长文本就会失败。

建议：改为 POST body（如 JSON），或分块发送。

2. 并发 RPC 无序
滚动、点击、拖动都是 fire-and-forget fetch。快速拖动滚轮会并发多个 `rdp.scroll`，HTTP/1.1 多连接时服务端执行顺序不保证，滚轮方向可能乱序。

建议：前端串行队列，或服务端按序号排序。

3. fetch 无超时
截图或 RPC 请求挂起时，页面无反馈。busy 会一直阻塞后续截图。

建议：AbortController + 超时。

4. BASE 硬编码依赖 URL 结构
`location.pathname.split(';')[0] + ';'` 假设访问路径一定是 `/import%20rdp;...`。若通过反向代理、重写规则或其他路径访问，RPC 全挂。

建议：由服务端注入 base 路径，或使用相对当前文档的解析。

5. `info(p)` 未被页面使用
冗余接口，可删除或页面用于显示远端分辨率。

## 四、页面逻辑与边界

1. `btnRefresh` 的兜底吸附只在 `lastX === null` 时生效
pointerdown 中：

```
if (lastX === null && lastCursor) snapDotTo(lastCursor);
```

若上次吸附失败但用户点过截图，lastX 非 null，兜底不会执行。结合 Bug 2，红点可能长期停留在旧位置。

2. `suppressLeftClick / suppressRefreshClick` 400ms 可能吞双击
拖动/滚动后 400ms 内的下一次 click 被吞。快速双击可能只触发一次。

建议：用更精确的 pointerup 后标志，或在 click 事件里判断位移。

3. `move_to` 不夹取坐标，返回值不实
`SetCursorPos` 会把越界坐标夹到屏幕边界，但 `move_to` 返回的是请求坐标。

`click_at/drag_at/scroll_at` 做了夹取，`move_to` 没做，不一致。

4. `screenshot` 未捕获 `ImageGrab.grab()` 异常
锁屏、RDP 断开、权限不足时 ImageGrab 可能抛异常，RPC 返回 500，页面只看到“截图失败”。可给出更明确错误。
5. `PAGE.replace('__BW__', ...)` 只替换第一个
当前模板只有一处 `__BW__`，暂时安全。若以后增加占位会漏替换。建议 `replaceAll` 或明确只一处。
6. log 中 `logEl.lastChild.remove()` 假设最后子节点是元素
当前 prepend 只插 div，没有文本节点，安全。但若以后插入文本节点会报错。可用 `logEl.lastElementChild`。
7. `get_clipboard_text` 无法区分“空剪贴板”和“打开失败”
都返回 `''`。页面显示“远程剪贴板 0 字符: 空”，可能误导。
8. 自动刷新固定 2s，与开关无联动
`setInterval` 一直运行，`autoEl` 只控制是否调用。切换开关后最多等 2s 才生效，可接受，但无“正在自动刷新”的视觉反馈。

## 五、页面布局与性能

1. `slideButtons` 未用 requestAnimationFrame 节流
`window.addEventListener('scroll', slideButtons, true)` 在捕获阶段监听所有滚动，包括 #log 内部滚动；每次滚动都读写 offsetWidth/offsetHeight，强制重排。

建议：rAF 节流，且只监听 window 和 visualViewport，避免捕获 #log。

2. 面板反向缩放的 height 计算仍有底部空白
panel 被 `scale(k)`，内容视觉高度 = inner * k；代码设置 `panel.style.height = inner * k`，但 panel 的视觉高度 = `(inner*k)*k = inner*k²`，文档流高度 inner*k，底部仍空 `inner*k*(1-k)`。

注释说“避免底部空出大段空白”，实际只减小了空白，没有消除。k 越小空白越明显。

更稳妥的做法：用负 margin-bottom 抵消，或把面板改为 `position: sticky/fixed` 并单独处理占位。

3. `translateX(pageLeft) scale(k)` 的变换顺序不精确（潜在）
CSS transform 矩阵为 T * S，先缩放后平移。pageLeft 是布局 CSS px，缩放后平移量是否与视觉视口对齐依赖浏览器实现，复杂且脆弱。

建议：用 `transform: scale(k) translateX(...)` 或在 JS 中换算，并多浏览器验证。

4. 垂直方向未跟随 visualViewport
`slideButtons` 只处理水平 pageLeft。页面放大后垂直滚动时，面板可能移出视口，用户需滚到底部才能操作。

移动端远程桌面常见布局问题。

5. `#bar` 绝对定位按钮与 flex 内容重叠
left_click/right_click/刷新按钮是绝对定位，`#coord/#info` 是 flex 内容。刷新按钮中心对齐 `.cspace` 中心，但按钮宽 48px，右半边可能压到 `#coord` 左端；`#bar` 的 `overflow: hidden` 又可能在极窄屏裁剪按钮。

建议：给 `.cspace` 留足宽度（≥48px），或把刷新按钮也纳入 flex 布局。

6. `#panel { transform-origin: 0 0; }` 重复定义两次
CSS 中 `#panel` 出现两次相同声明，冗余，可清理。
7. `sizeDot` 与截图内圈尺寸不完全一致
`dot.style.width = 14 * ratio`，`borderWidth = max(0.5, ratio)`，视觉总直径 ≈ `14*ratio + 2*borderWidth`；截图内圈基准 14px 含描边。两者可能有 1–2px 偏差。小问题，但注释说“与截图内红圈一致”，实际是近似。
8. 输入历史明文存 localStorage
若用户发送密码、验证码，会明文留存。建议提供“不保存历史”开关或仅存摘要。

## 六、代码质量与可维护性

1. 大量 Win32 调用未声明 argtypes/restype
`user32.mouse_event`、`user32.keybd_event`、`user32.OpenClipboard`、`user32.CloseClipboard`、`user32.EmptyClipboard`、`user32.SetCursorPos`、`user32.GetCursorPos`、`user32.GetSystemMetrics`、`user32.VkKeyW`（应为 VkKeyScanW）等均未声明。

64 位下 HANDLE/指针可能被默认 c_int 截断。剪贴板部分已显式声明，其他部分没有，风格不一致，且 `mouse_event` 的负数 dwData 依赖默认转换，不严谨。

2. `_INPUT` 结构手动定义，依赖 ctypes 默认对齐
当前在 64 位 Windows 上大小应为 40 字节，通常正确；但未用 `_pack_/静态断言校验`，跨平台/编译器变化时可能出错。建议 `assert ctypes.sizeof(_INPUT) == 40`（64 位）。
3. 全局可变 `jpeg_quality / click_delay`
模块级变量可被外部修改，RPC 并发时无保护。可改为函数参数或配置对象。
4. `type_text` 每字符 `sleep(0.005)`
1000 字符需 5 秒，且长文本还会先撞 URL 长度限制。建议批量发送或按段落优化。
5. `PAGE` 内嵌大段 HTML/JS，`__BW__` 占位替换
可维护性差，模板中若出现其他 `__BW__` 会误替换。建议用 `string.Template` 或独立静态文件。
6. `screen` 函数名与 `screenshot` 易混
`screen` 是 RPC 入口，`screenshot` 是内部实现。可接受，但阅读时需注意。
7. `histsel` 每次 `renderHist` 全量重建
历史最多 30 条，开销可忽略；但可只增量更新。
8. 日志 400 条上限硬编码
可配置。

## 七、Win32 / ctypes 细节

1. `mouse_event` 的 wheel dwData 传 Python 负数
未设 argtypes，ctypes 默认按 c_int 传 -120，Windows 收到 `0xFFFFFF88` 按 signed 解释为 -120，通常可用；但若以后补上 `argtypes=[DWORD,...]` 会截断。建议显式用 `ctypes.c_int(-120).value & 0xFFFFFFFF` 或声明 argtypes 并用 DWORD。
2. `keybd_event` 的 bScan 是 BYTE
当前传 0，Unicode 键入不走它，安全；但组合键路径依赖 keybd_event，未声明 argtypes，vk 若超过 255 会截断（VK 都 ≤ 255，暂时安全）。
3. 魔数过多
`0x0002（GMEM_MOVEABLE）`、`13（CF_UNICODETEXT）`、`1（INPUT_KEYBOARD）`、`0x0004（KEYEVENTF_UNICODE）`、`0x0800（MOUSEEVENTF_WHEEL）` 等建议命名常量。
4. `SetClipboardData` 成功返回 HANDLE，`if not ok` 判断正确
但 ok 是 c_void_p 返回值，非零指针为真，NULL 为假，逻辑正确。注意成功后不能 GlobalFree，代码已处理。
5. `create_unicode_buffer(data, len(data)+1)` 的 size 是 wchar 数，不是字节数
与 Bug 3 同源，非 BMP 字符会截断。
6. `GetClipboardData` 返回非文本时返回 `''`
合理，但无法区分“无文本”和“剪贴板忙”。可记录错误。

## 八、无法审查的部分

`server_http.py` 未提供，以下关键点无法确认：

- RPC 表达式如何解析/执行（eval/exec/白名单？）
- 是否限制 URL 长度、请求体大小、并发数
- 是否绑定 `127.0.0.1`
- 是否设置 CORS/CSP/CSRF 防护
- 是否对 `p.set_header/p.set_data` 做错误处理
- 截图返回的 X-Cursor 头是否被代理剥离
- 多线程/异步下 user32 调用是否线程安全（GDI/USER32 通常需要消息泵，但鼠标键盘模拟一般可用）

## 建议修复优先级

| 优先级 | 问题 |
| --- | --- |
| P0 | VkKeyW → VkKeyScanW；截图后 snapDotTo 时机；CSRF/绑定地址；非 BMP 剪贴板尺寸 |
| P1 | 非 BMP type_text；长文本 URL 限制；并发 RPC 无序；DPI 感知；SendInput 返回 0 检查 |
| P2 | + 键无法表达；全角数字；click_at middle；move_to 夹取；fetch 超时 |
| P3 | 面板缩放空白/变换顺序；slideButtons rAF；#bar 遮挡；重复 CSS；魔数命名 |

以上覆盖了代码中能定位的确定 Bug、潜在风险和可维护性问题。

