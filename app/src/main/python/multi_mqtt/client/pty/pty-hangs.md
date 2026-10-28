调试完成，原因已经查到最底层： 这是 CPython 控制台写通道的一个“零进度死循环”，把 pty 共用的那个`BufferedWriter` 的锁永久占住了 ；MCP 本身没问题，只是它的重试逻辑把这个故障放大了。

## 一句话结论
远端输出被按字节切成“半个 UTF-8 汉字”的片段 →`_WindowsConsoleIO.write` 对不完整的多字节序列返回 0 字节且不抛异常 →`BufferedWriter.flush()` 在 CPython 里对这个 0 进度无限重试， 持锁 + 烧掉约 0.7 个核 → 之后所有写线程都等同一把锁 → 控制台画面冻结，而控制台本身、MQTT、shell、RPC、MCP 全部健康。

## 现场证据（1188 RPC 实测）
观测 结果 含义 `writer_diag()` out 积压 206B、stall 7→21s 持续增长，err 恒 0 只有 stdout 通道坏 `threading.enumerate()` `pty-out` 线程 62 个 （几分钟前 58→61→62） 每 60s 多一个永不退出的写线程 py-spy（线程 11860） `active+gil` ，Python 帧停在`_loop` 的`self._stream.flush()` 会话最初那个写线程卡在 flush py-spy（其余 57 个） `PyThread_acquire_lock_timed` →`SleepConditionVariableSRW` 全部在 等同一条流对象的内部锁 `GetProcessTimes` 3 秒采样 2.20s CPU / 3s ≈ 73% 单核 持锁线程在空转，不是在等 I/O `GetConsoleMode` `0x7` （QuickEdit 关 、VT 开） 这次 不是 选择/标记模式 直接`os.write(1/2)` 、`open("CONOUT$","wb",0).write` 全部立即成功 控制台消费正常，没冻 对同一 raw 新建`BufferedWriter` 写 成功 底层`_WindowsConsoleIO` 完全正常 对`_bin_stdout()` 本身写 4s 后仍阻塞 锁被占死，只有这个对象坏 字节矩阵（写 raw） `b"\xe4"` →0、`b"\xe4\xb8"` →0、`b"\xf0\x9f"` →0；`b"abc\xe4\xb8"` → 3（丢尾） ；完整中文/emoji/`\xff` /ESC 序列→正常 不完整 UTF-8 尾部 = 0 进度 ，就是死循环的燃料 `_OUT_WRITER._stream is _bin_stdout()` True；`_bin_stdout().raw` =`_io._WindowsConsoleIO` (fd1) 所有写线程共用同一个缓存流对象

## 全部原因（按因果分层）
A. 触发源（这次的真凶）

1. 服务端 remote_cmd.py 的`_out_loop` 按时间间隔/`_frame_max` 原样切分`_buf` 发帧，切点落在字节中间时，一个汉字(3B)/emoji(4B)会被劈成两帧。
2. 客户端 030_termwrite.py 把每个 chunk 原样`stream.write()` ，没有跨帧拼 UTF-8。
3. CPython`_io._WindowsConsoleIO.write` 对“尾部不完整多字节序列”返回 0 而不报错 （实测），`BufferedWriter.flush()` 的 C 层循环便永不结束——持锁空转。
4. 触发时间点吻合：日志里 12:07:56 一串`/mcp POST` 之后立即出现第一条“终端写卡滞 10s”，此前窗口正在刷中文（`build_xime_home/v2rayNG` 那段）。
B. 设计缺陷（为什么自愈无效、为什么越治越坏） 5. 080_writerdiag.py 的`reset_out_writer()` 取的是`_bin_stdout()` 缓存 ，新旧 writer 是 同一个`BufferedWriter` ——换线程必然还是等同一把锁，等于只增加一个阻塞线程。
6. 110_session.py 60s 自动换线程 + 097_mcp_server.py 的`pty_redraw` （stall>2s 就 reset）+ 命令栏`redraw` （ 100_magic.py ）都在反复调用它 → 已泄漏 62 个死线程，每个还各占一份队列（上限 8MB）。
7.`_TerminalWriter.write` 在队列接近 8MB 时`data = data[:room]` 硬切，也会切出半个汉字，是第二个同源隐患。
8. 诊断口只报`stall_seconds` ，不报“持锁者/阻塞原因”，所以`status` /`writer_diag` 看着像“终端慢”，掩盖了锁死本质。

C. 误诊陷阱（历史结论需要修正的部分） 9. 本次`GetConsoleMode=0x7` ，QuickEdit 是关的、ESC 无从解除；之前的“选择/标记模式”解释对这次不适用。
10. 之前“py-spy 看到`_WindowsConsoleIO.write` 持 GIL 空转、换线程即愈”的方向是对的，但“换线程”能“愈”只是因为那次底层调用最终返回、旧线程按`_abandon` 退出并释放了锁；这次底层永远返回 0，释放不了，所以换线程百分之百无效。
11. 历史记录里的“纯 ASCII 压测 1MB 不冻、中文刷屏就冻”正好吻合：只有跨帧的 半字符 才触发。

D. 与 MCP 的关系（用户侧观感） 12. MCP 只是“输出量放大器”：`pty_run` 跑`cat README.zh-CN.md` 、分析 AGENTS.md 之类中文内容，命中半字符切分的概率大增；MCP 的`pty_redraw` 又不停加阻塞线程。MCP 桥接、ASGI 循环线程、`/mcp` 请求本身全部正常（日志 200，`_mcp_loop` 空闲在 select）。

## 进程现状（未 kill）
PID 1572 存活；12/14 broker、远端 shell（`~/build_xime_home/v2rayNG` ）、MCP 会话 1 个、RPC 全在线；约 62 个`pty-out` 死线程、约 0.7 核空转、最多几百 MB 队列内存；屏幕不再更新。Ctrl-] /`redraw` / ESC 对这个故障都无效。

## 候选修复方向（本轮没动代码，等你指示）
1. 根治 ：客户端渲染前做 UTF-8 重组，未完成的尾字节留到下一 chunk（跨帧 carry），永远不把半字符交给控制台。
2. 绕开锁 ：`_TerminalWriter` 不写`BufferedWriter` ，直接写`_bin_stdout().raw` （实测同一 raw 正常、且 0 进度可自行处理），或每个 writer 自己`io.BufferedWriter(raw)` 。
3. 自愈改造 ：`reset_out_writer` 必须换成 新的流对象 才有意义；否则删掉定时 reset，避免线程泄漏。
4. 兜底 ：`write` 入口做强校验（`data.decode('utf-8')` 失败则截到最后一个完整字符），`_TerminalWriter.write` 的 8MB 截断也按字符边界切。
5. 顺带可让`writer_diag()` 输出“锁持有者/0 进度次数”，以后一眼定位。
