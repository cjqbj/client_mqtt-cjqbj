# 环境与真机调试参考（apk-device-deploy）

本文件是 SKILL.md 的背景资料，只在需要排查环境问题时加载。

## 1. 远端构建机（Hugging Face Space）

- Space：`SPACE_ID=huggingface1Q/q`，HTTPS 域名 `https://huggingface1q-q.hf.space`，hostname 前缀 `r-huggingface1q-q-67f`。
- client 仓库（构建用）：`/root/build_xime_home/client`；multi_mqtt 源：`/root/build_xime_home/multi_mqtt`（曾因目录为空导致测试失败，若测试 import 报错先确认该目录内容存在）。
- 构建：`cd /root/build_xime_home/client && ./debug_build_secexp.sh`，约 30-60s；产物 `out/com.qgb.client-1-arm64-v8a.apk`（约 37,263,007 字节）。关注输出里的 `e: `（Kotlin）/`error:`，`BUILD_RC=0` 才算成功。
- Python 测试（改 Python 时）：`cd /root/build_xime_home/client && python3 -m unittest discover -s tests`。

## 2. MQTT 通道（操作远端的唯一通道）

- 解释器：`C:\QGB\miniforge3\python.exe`；所有命令的工作目录 `c:\test\github\multi_mqtt`。
- PTY API：`from client.pty_client_mqtt import ai_pty_run, ai_pty_status, ai_pty_send`。
  - `ai_pty_run(cmd, timeout=60, acquire_timeout=2)` 返回 `{ok, rc, out, timed_out, raw_len}`；通道串行，重叠调用在 2s 抢锁失败后**立刻**返回 `{ok:False, busy:True}`，不会排队——调用方必须自己 sleep 重试（`remote_build.py` 已封装）。
  - 固定桥接参数：topic `q`，key `2**128`。
  - 输出经终端流（ANSI 转义、tmux 状态栏碎片会混进 `out`），解析时用正则，不要逐字断言。
- 文件上传：`python client/cmd_client_mqtt.py -t q -k "2**128" put <local> <remote>`。成功 OK 行走 **stderr**，形如 `OK <remote> 68140B mode=0o644 sha256=<hex>`。PowerShell 里 `2>&1 | Select-String OK`，或直接用 `mqtt_put.py`。
- **硬限制**：不要经 PTY 拉大文件或长 base64——终端流突发会丢字节，base64 分块 200KB 都出现过损坏。37MB APK 只能走第 3 节的 HTTPS。

## 3. Space HTTPS RPC 下载

- 形式：`GET https://huggingface1q-q.hf.space/rpc/<urlencoded python code>`，RPC key 为空。
- 普通表达式如 `r=1+1` 直接回显结果；流式输出二进制必须：

  ```python
  p.set_header('Content-Type', 'application/octet-stream')
  f = open('/root/.../x.apk', 'rb')
  exec("while True:\n b=f.read(1048576)\n if not b: break\n p.write(b)")
  ```

  - 多行循环必须用 `exec(...)` 包，URL 里保留 `\n`（用 `urllib.parse.quote(code, safe="")`）。
  - **不要**加 `Content-Disposition` 头，实测触发 404。
  - 1MiB 分块是验证过的稳定值。
- URL 很长（包含 open/read/循环代码），属正常；GET 无 body，urllib 直接打开即可。

## 4. 本地仓库

- `C:\test\github\client_mqtt-cjqbj`（无 git CLI；提交需用户明确要求）。
- **禁改** `app/src/main/python/multi_mqtt/`：构建时 rsync 权威覆盖；同理不要手改远端该目录。
- 改 Kotlin 后必须：上传 `MainActivity.kt`（或对应文件）→ 远端构建 → 下载 → `adb install -r`。
- Python 改动可只跑远端单测（39 项基线），不一定要出 APK。

## 5. 真机（Nexus 6P / angler / Android 8.1 SDK27 / 1440x2560）

- adb：`C:\Android\SDK\platform-tools\adb.exe`；连接 `adb connect 192.168.1.111:5555`（`adb devices` 确认 device）。
- 安装：`adb install -r <apk>`，输出 `Success`。包名 `com.qgb.client`。
- 启动：`adb shell monkey -p com.qgb.client -c android.intent.category.LAUNCHER 1`；冷启动：先 `adb shell am force-stop com.qgb.client`。
- 截图：**必须设备端落盘再 pull**：
  `adb shell screencap -p /sdcard/x.png` → `adb pull /sdcard/x.png <local>`。
  `adb exec-out screencap -p > x.png` 在本机得到无法解码的文件。本地截图放临时目录，用完删设备端和本地副本。
- 定位元素：`adb shell uiautomator dump /sdcard/ui.xml` → pull → PowerShell：
  `[xml]$x = Get-Content ui.xml -Raw; $x.SelectNodes('//node') | ? {$_.text -match '...'} | % {"$($_.text) -> $($_.bounds)"}`。
- 已知坐标（1440x2560，仅本设备，换设备先重新 dump）：
  - 底部 tab 图标中心 y≈2245：Camera x≈220 / Files x≈720 / Wi-Fi x≈1205。
  - 顶栏 y≈180：菜单 ≈1030、目标设置 ≈1170、设置齿轮 ≈1330。
- 长按：
  - `input swipe x x 1500` 在该机不触发 Compose 长按（有抖动/被识别为滑动）。
  - 该 ROM 的 `input` 没有 `motionevent` 子命令。
  - 可靠方式是 sendevent。触摸屏 `/dev/input/event0`（name `synaptics_dsx`），范围 ABS_MT_POSITION_X 0-1439 / Y 0-2559：

    ```sh
    sendevent /dev/input/event0 3 57 1
    sendevent /dev/input/event0 3 53 <X>
    sendevent /dev/input/event0 3 54 <Y>
    sendevent /dev/input/event0 1 330 1
    sendevent /dev/input/event0 0 0 0
    sleep 1.2
    sendevent /dev/input/event0 3 57 -1
    sendevent /dev/input/event0 1 330 0
    sendevent /dev/input/event0 0 0 0
    ```

  - 换设备先 `getevent -pl | grep -B25 ABS_MT_POSITION_X` 找节点和坐标范围。
- 帧率：`adb shell dumpsys gfxinfo com.qgb.client reset` → 操作 → `dumpsys gfxinfo com.qgb.client | Select-String "Total frames|Janky|90th|95th|99th|Slow UI"`。第一轮含首次组合/JIT 开销，连切两轮看稳态；99th 150ms 量级属正常。
- 日志：`adb logcat -d -t 400 | Select-String "AndroidRuntime|qgb|compose|Gesture"`。
- 编辑文本注意：`input text` 不支持空格用 `%s`、不支持很多标点；`keyevent 67` 退格，`keyevent 4` 是 BACK（会关页面/弹窗，不是收键盘），`keyevent 123` 移光标到末尾。复杂文本编辑优先考虑直接改 `/sdcard/apm/client_mqtt/client_mqtt.json`（先 force-stop App，改完冷启动）。
- App 配置文件：`/sdcard/apm/client_mqtt/client_mqtt.json`（外部脚本模式）。

## 6. PowerShell / 本机陷阱

- PS5 不支持 `&&`；用 `;` 或分开调用。
- 远端命令字符串里的 `$?`、`$变量` 会被 PowerShell 本地展开（`$?` 变 `True`）。复杂远端命令一律写临时 .py（放系统临时目录）调 `ai_pty_run`。
- 设备端 sed/python 内联脚本经 PowerShell 层层引号极易坏；超过一行的设备端脚本 `adb push` 成文件再 `adb shell sh /data/local/tmp/x.sh`，用完删除。
- Shell 工具工作目录不跨调用持久化；脚本均设计为可从任意 cwd 运行。
