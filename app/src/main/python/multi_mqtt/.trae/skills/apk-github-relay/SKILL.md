---
name: apk-github-relay
description: APK 经 GitHub 临时仓周转的国外到国内传输，远端 git push 秒级上传后国内用 ghfast.top 代理下载，输出 DNS/TCP/TLS/TTFB/缓存命中与实时速度并校验 SHA。用户要求 GitHub 周转、ghfast/ghproxy 加速下载 APK、需要连接详情和速度报告时使用；MQTT PTY 直拉、Codeup 或 HF Space 通道场景不要用。
---

# APK GitHub 周转传输（国外远端 → 国内本地）

远端构建机在国外，Codeup 国外侧很慢；GitHub 从远端推送极快，国内再经
`ghfast.top` 代理拉 raw，比 HF Space 直拉快约一倍且地址稳定可复用。
本 skill 固化三段：建周转仓 → 远端 push → 国内代理下载（含速度报告）。

## 实测基线（2026-10-06，62MB xime APK）

| 段 | 通道 | 结果 |
|---|---|---|
| 远端 → GitHub | git smart push | **5.4s，11.49 MB/s** |
| 本地下载（冷缓存 x-cache MISS） | ghfast.top | 165s，均速 0.38 MB/s，峰值 3.80 |
| 本地下载（热缓存 x-cache HIT） | ghfast.top | **117s，均速 0.53 MB/s**，峰值 3.20 |
| 对比：GitHub 直连 | 无代理 | 183s，0.34 MB/s |
| 对比：HF Space pull_apk.py | 直拉 | 约 240s，0.27 MB/s |

注意：ghfast 节点偶发在 1MB 处断流，下载脚本自带 Range 续传重试，属正常。

## 环境

- 本地 Python `C:\QGB\miniforge3\python.exe`，脚本纯标准库。
- 远端 git 二进制可用；远端 token 临时放 `/tmp/relay/.tok`（chmod 600），用完即删。
- 周转仓固定 `eightobox/xime-relay-test`，**public**（私有仓代理 404）。
- 本地 token：文件 `%USERPROFILE%\.config\apk-relay\github.token`，或环境变量
  `RELAY_GH_TOKEN`。token 永远不进 skill 目录、不进任何 git 仓库。
- 大文件上传仍走 mqtt_put 传脚本（参考 apk-device-deploy skill）。

## 标准流程

### 1. 确保周转仓存在（本地，一次性）

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-github-relay\scripts\relay_repo.py ensure
```

自动建仓并写 README 初始化 main（空仓库会让 Git Data API 409、push 无默认分支）。
彻底不用时 `... relay_repo.py delete` 删仓。

### 2. 远端把 APK push 上去

先把脚本和 token 放到远端（token 文件不要在命令行里展开）：

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-device-deploy\scripts\mqtt_put.py `
  .trae\skills\apk-github-relay\scripts\relay_push.py /tmp/relay/relay_push.py
C:\QGB\miniforge3\python.exe <本地token文件> /tmp/relay/.tok
```

经 PTY 后台执行（长任务一律 setsid 后台跑再轮询日志，见 mqtt-remote-shell skill）：

```
cd /tmp/relay && chmod 600 .tok &&
(setsid python3 relay_push.py --apk <远端APK绝对路径> > /tmp/relay/push.log 2>&1 < /dev/null &)
```

输出 `PUSH_SECONDS`、`PUSH_SPEED_MBPS`、`RAW_URL`、`GHFAST_URL`。
token 经 `http.extraHeader` 注入，不落 `.git/config`；同文件重复推送是新增 commit，
raw 路径不变。

### 3. 国内下载并出速度报告

```powershell
C:\QGB\miniforge3\python.exe .trae\skills\apk-github-relay\scripts\relay_pull.py `
  --owner eightobox --repo xime-relay-test --path out/<apk文件名> --sha256 <sha>
```

也可直接 `--url <任意 github raw/blob 链接>`（自动剥离已带的代理前缀）。
默认存 `%TEMP%\<文件名>`。输出：解析 IP、DNS/TCP/TLS 分段耗时、HEAD 状态与
x-cache 命中（HIT 热缓存明显更快）、百分比进度+本轮速率+ETA、尝试次数（断点续传）、
总耗时、平均速度、SHA256 比对。退出码 0 成功 / 1 连接失败 / 2 SHA 不一致（禁止安装）。
排查通道时 `--proxy ""` 走 GitHub 直连对比，`--proxy https://ghproxy.net` 换备用代理。

### 4. 安装与真机验证

接 apk-device-deploy skill 的第 4-5 步（adb install -r、截图/点击验证）。

### 5. 收尾

- 远端：`rm -rf /tmp/relay/.tok /tmp/relay/work`（脚本与日志可留）。
- 本地：`%TEMP%\*.part` 残留可删；APK 同名覆盖即可。
- 周转仓是公开仓，**禁止推任何带密钥/隐私的文件**；长期不用执行 delete。

## 易错点

- 空仓库直接 push/调 Git API 会 409/无默认分支——先 ensure（README 初始化）。
- 不要用 Contents API（1MB 上限）或 Blob API（87MB base64 JSON 实测 422）传 APK；
  62MB 级文件只走 git pack 协议。
- 仓库必须 public；ghfast URL 形态固定为
  `https://ghfast.top/https://github.com/<owner>/<repo>/raw/refs/heads/<branch>/<path>`。
- 代理断流是常态不是故障：脚本 Range 续传；若连续 4 次失败再换代理或走直连。
- Windows 控制台非 UTF-8 时脚本已 reconfigure stdout；PowerShell 5 用反引号续行。
