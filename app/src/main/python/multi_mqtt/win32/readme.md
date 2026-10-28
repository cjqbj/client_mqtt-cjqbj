# win32 端口僵尸 socket 诊断工具

2026-10-25 事故：`192.168.1.12:1122`（server_mqtt 的 HTTP RPC 通道）TCP 能连上、
请求发出后永远无响应，curl 卡在 `Request completely sent off`。

## 事故原因（三个 Windows 机制叠加）

1. **SO_REUSEADDR 在 Windows 上允许静默双绑**：与 Linux 只允许复用 TIME_WAIT
   端口不同，Windows 上两个都设了该选项的 socket 可以同时 bind 同一个正在
   监听的端口，第二次 bind 不报错。旧代码 `allow_reuse_address = True` 是
   Linux 习惯，在 Windows 上让新服务器"成功启动"却收不到任何连接。
   微软官方文档明确警告此行为可被用于端口劫持，服务器应改用
   `SO_EXCLUSIVEADDRUSE`。
   https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse
2. **TCP 握手由内核在 listen backlog 完成**：只要 socket 处于 LISTENING，
   三次握手不需要任何进程调用 accept()。所以"curl 显示 Connected"只证明
   端口有人监听，不证明有活进程在应答。
3. **句柄继承 + 引用计数**：winsock 句柄默认可被子进程继承；进程退出只关闭
   自己那份句柄，socket 对象在最后一个引用释放时才销毁。本次事故中开机
   自启动的 IPython（Run 键 MyScript=ipy.bat，qgb rpcServer 绑 1122）退出后，
   listener 句柄被无关的 `7+ Taskbar Tweaker.exe` 继续持有，形成"僵尸"：
   netstat 显示 LISTENING 但属主 PID 已不存在，taskkill 报"没有找到进程"，
   新连接全部进入死 backlog 无人应答。

## 处置流程（不重启清掉僵尸）

```bat
:: 1. 确认僵尸：LISTENING 的 PID 在 tasklist 里查不到
netstat -ano | findstr :1122
tasklist /fi "PID eq <死PID>"     &rem 没有匹配 = 僵尸

:: 2. 扫描找出真正持有句柄的活进程（见下）

:: 3. 结束持有进程，LISTEN 行立即消失
taskkill /F /PID <持有者PID>

:: 4. 验证：只剩一个 LISTENING 且 PID 存活，curl 秒回
```

## 文件

| 脚本 | 依赖 | 说明 |
|---|---|---|
| `scan_port_holder.py` | 无 | 全量扫描。枚举系统句柄表，按 socket 类型索引过滤，分块探测。带超时看门狗，个别句柄会把探测进程卡死在内核调用里，超时自动二分跳过。全表约 8 万句柄，过滤后约 5 千，跑完约 20 分钟。用法：`python scan_port_holder.py [端口]` |
| `probe_afd.py` | handle64.exe | 快速路径。用 Sysinternals handle.exe 导出全系统 `\Device\Afd` socket 句柄（几百个）再探测，约 1 分钟出结果。handle64.exe 放本目录、%TEMP% 或 PATH。下载：https://live.sysinternals.com/handle64.exe |
| `probe_worker.py` | - | 探测子进程，被上面两个脚本调用，不直接使用。原理：OpenProcess + DuplicateHandle 复制目标进程句柄，包装成 socket 调 getsockname() 读出绑定地址 |

两者输出形如 `PID=8764 (7+ Taskbar Tweaker.exe) handle=0x318 ['0.0.0.0', 1122]`
的持有者清单，同时会列出合法监听者（如本例 PID=4184）以便对照。

## 已做的根治修复（本次提交的代码部分）

- `server_http.py` `ThreadedHTTPServer` 与 `server_http_wsgi.py`
  `_ThreadingWSGIServer`：win32 平台 `allow_reuse_address = False` 并在
  `server_bind()` 里设置 `SO_EXCLUSIVEADDRUSE`。端口被占（含僵尸）时启动
  **立即报 WinError 10013/10048**，快速失败，不再静默双绑挂死。
- POSIX 平台保留 `allow_reuse_address = True` 原语义（仅复用 TIME_WAIT）。

## 技术备忘

- `NtQuerySystemInformation(SystemExtendedHandleInformation)` 返回的句柄表，
  Win10 x64 实测表头 **16 字节**（NumberOfHandles 8 + 保留 8），条目 **40 字节**：
  `Object(8) PID(8) Handle(8) GrantedAccess(4) CreatorBackTraceIndex(2)
  ObjectTypeIndex(2) HandleAttributes(4) Reserved(4)`。
  网上常见资料写 8 字节表头，解析会整体错位。
- netstat 的 PID 是 socket **创建者**，不反映句柄当前持有者；系统无自带工具
  查询持有者，这正是本目录脚本存在的理由。
- 杀掉句柄持有进程的瞬间 LISTEN 行立即消失——对象引用归零即销毁，
  内核行为正确，问题只在"看不见谁持有"。
- 另一种"幽灵占端口"：Hyper-V/WSL 端口保留，症状是 bind 报 10013/10048 但
  netstat **没有** LISTENING 行，用 `netsh int ipv4 show excludedportrange
  protocol=tcp` 确认，`net stop winnat && net start winnat` 修复，与本目录
  解决的问题无关但经常被混淆。
