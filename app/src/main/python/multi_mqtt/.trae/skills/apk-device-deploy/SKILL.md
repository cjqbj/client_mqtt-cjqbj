---
name: apk-device-deploy
description: qgb client APK 的真机交付循环，MQTT PTY 远端构建 → Space HTTPS 流式下载到临时目录（含连接详情、实时速度、SHA 校验）→ adb 安装 → 真机截图/点击验证。用户要求重新构建、下载、安装 APK 或做真机回归验证时使用；纯 Python 改动且不需要出新 APK 时不要用。
---

# APK 远端构建 → 下载 → 安装 → 真机验证

qgb client（`C:\test\github\client_mqtt-cjqbj`）没有本地 Android 工具链，构建发生在 Hugging Face Space 远端；37MB 的 APK 不能走 MQTT 终端通道（会损坏），必须走 Space 公网 HTTPS RPC 流式下载。本 skill 把这条经过多次真机验证的循环固化为 4 个脚本和一套验证手法。

## 适用 / 不适用

- 适用：改了 Kotlin/资源/打包进 APK 的 Python，需要出新包并装到真机；真机 UI 回归。
- 不适用：只改仓库 Python 做逻辑验证（直接远端 `python3 -m unittest discover -s tests`，用 mqtt-remote-shell skill）；与 client APK 无关的设备操作。

## 环境速览（完整背景见 references/environment.md）

| 项 | 值 |
|---|---|
| adb | `C:\Android\SDK\platform-tools\adb.exe`，真机 `192.168.1.111:5555` |
| 远端构建目录 | `/root/build_xime_home/client`，构建脚本 `./debug_build_secexp.sh`（约 30-60s） |
| 产物 | `out/com.qgb.client-1-arm64-v8a.apk`（约 37MB） |
| PTY 参数 | topic `q`，key `2**128`，Python `C:\QGB\miniforge3\python.exe` |
| 下载入口 | `GET https://huggingface1q-q.hf.space/rpc/<urlencoded code>`，RPC key 为空 |
| 本地 client 仓库 | `C:\test\github\client_mqtt-cjqbj`（无 git CLI） |

**禁改** `app/src/main/python/multi_mqtt/`（构建时 rsync 权威覆盖）；不要手改远端该目录。

## 标准循环（5 步）

脚本在 `scripts/`，全部可从任意 cwd 运行（自行定位仓库根），下载物一律进系统临时目录，**不要**在工作区留 `_tmp_*.py`、`_client.apk`、`_*.png`。

### 1. 上传改动到远端

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-device-deploy\scripts\mqtt_put.py <local文件> <remote绝对路径>
```

默认 topic `q` / key `2**128`。输出含 `OK <remote> <size>B ... sha256=<hash>` 才算成功（OK 行走 stderr，脚本已合并）。改了 Python 时可先让 mqtt-remote-shell 跑一遍远端单测。

### 2. 远端构建

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-device-deploy\scripts\remote_build.py
```

串行复用 PTY（busy 就等它跑完，不要并发第二条 PTY 命令）。输出 `BUILD_RC=0`、产物字节数、sha256；非 0 时打印 `e:`/`error:` 编译错误前 20 行并以退出码 1 失败。**记下 sha256**，下一步校验用。

### 3. HTTPS 流式下载 APK 到临时目录

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-device-deploy\scripts\pull_apk.py --sha256 <第2步的sha>
```

默认保存 `%TEMP%\qgb_client_latest.apk`（`--out` 可改，但不要放工作区）。脚本会打印：连接详情（Host/远端路径/URL 长度/期望 SHA）、HTTP 响应头详情（状态码、Server、Content-Type、Content-Length、Transfer-Encoding）、带 ETA 的进度条、瞬时/平均速度、最终 SHA256 比对。SHA 不一致退出码 2，**禁止安装**，直接重跑本步。

下载失败时不要盲目重试：先分层定位是 DNS、TLS 握手（RST/超时）还是 HTTP 错误，脚本本身会打印 `HTTPError` 状态码+响应体或 `URLError` 原因。

### 4. 安装

```powershell
& "C:\Android\SDK\platform-tools\adb.exe" install -r "$env:TEMP\qgb_client_latest.apk"
```

看到 `Success`。设备掉线先 `adb connect 192.168.1.111:5555`。

### 5. 真机验证

全部 adb 命令用全路径。常用手法（坐标/事件节点随设备不同，先 `wm size`、`getevent -pl` 核实）：

- 启动：`adb shell monkey -p com.qgb.client -c android.intent.category.LAUNCHER 1`；冷启动场景先 `adb shell am force-stop com.qgb.client`。
- 点击：`adb shell input tap <x> <y>`。
- **截图必须设备端落盘再 pull**：`adb shell screencap -p /sdcard/x.png` 然后 `adb pull`（`exec-out` 重定向在本机会得到无法解码的文件）。验证完删掉 `/sdcard/x.png` 和本地截图（本地截图也放临时目录）。
- UI 树定位：`adb shell uiautomator dump /sdcard/ui.xml` → pull → PowerShell `[xml]` 解析节点 text/bounds，比盲猜坐标可靠。
- **长按**：`input swipe x x 1500` 在部分系统不触发长按、SDK27 没有 `input motionevent`。可靠做法是 sendevent（Nexus 6P 触摸屏 `/dev/input/event0`，X 0-1439 / Y 0-2559）：
  `sendevent /dev/input/event0 3 57 1; sendevent ... 3 53 X; 3 54 Y; 1 330 1; 0 0 0; sleep 1.2; 3 57 -1; 1 330 0; 0 0 0`
- 帧率：`adb shell dumpsys gfxinfo com.qgb.client reset` → 操作 → 再 dump，看 Total frames / Janky frames / 90th/95th/99th。第一轮含首次组合开销，连切两轮看稳态。
- 查崩溃：`adb logcat -d -t 400 | Select-String "AndroidRuntime|qgb|compose"`。

验证完成后：临时下载物留在 `%TEMP%` 即可（同名覆盖），工作区必须干净；若验证了行为变更，追加 `PROGRESS.md` 并用第 1 步上传。

## 易错点

- PowerShell 5 不支持 `&&`；远端命令里的 `$?`、`$变量` 会被本地展开——复杂远端命令写成临时 .py 调 `ai_pty_run`，脚本一律放临时目录。
- 不要用 PTY 通道拉大文件或长 base64（终端流突发丢字节）；只走 Space HTTPS。
- RPC 流式代码里读文件循环是多行语句，URL 里必须用 `exec("while True:\n ...")` 包裹；加 `Content-Disposition` 头会 404，只留 Content-Type。
- `adb shell input keyevent 4` 是 BACK，会关闭上层页面（设置页/弹窗），不要把它只当"收键盘"用。
