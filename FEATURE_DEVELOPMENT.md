# Feature 开发约定（给人和 AI）

## 1. 文件与命名

- 一个 feature = 一个 `feature_<name>.py`，`<name>` 必须是合法标识符。
- 内置：`app/src/main/python/feature_<name>.py`，名字注册进 [bootstrap.py](app/src/main/python/bootstrap.py) 的 `BUILTIN_FEATURES`。
- 运行时：`<script-root>/py_updates/feature_<name>.py`，同名遮蔽内置。script-root 默认应用内存储；授权并开启外置后为 `/sdcard/apm/client_mqtt/`。
- 下载有原子写、compile 校验、2MB 上限；只接受 `feature_*.py`。
- **残留的旧 py_updates 文件会遮蔽内置新代码**，排查"代码没生效"先查这里（含 `__pycache__`）。

## 2. FEATURE 清单（模块顶层）

```python
FEATURE = {
    "name": "demo",          # 必填，与文件名一致
    "title": "Demo",         # 底栏/侧栏显示名，缺省用 name
    "version": 1,
    "actions": ["run"],      # ui=compose 时生成动作按钮；ui=python 可留空
    "icon": "terminal",      # 可选
    "ui": "python",          # 缺省 "compose"
}
```

## 3. 两种 UI 模式

**compose（默认）**：顶层函数即动作，参数只接受位置参数，返回 JSON 字符串：

```python
import json, client_service

def run():
    return json.dumps({"ok": True}, ensure_ascii=False)
```

**python（自绘）**：必须实现 `build_view(context)`，在主线程同步返回一个 Android `View`。网络/RPC 一律用 `pyui_kit.run_async(...)`，禁止阻塞主线程。参考 feature_wifi / feature_camera / feature_files。

## 4. pyui_kit 铁律（违反必崩）

Chaquopy 对同一个 Java 接口，全进程只能建**一个** `dynamic_proxy` 代理类；重复建类第二次调用必崩 `_chaquopyGetType is abstract`。

- 回调**只许**用 pyui_kit 现成封装：`pyui_kit.click(view, fn)`、`pyui_kit.run_async(...)`、`pyui_kit.watch_target(anchor, on_change)`、`pyui_kit._post(fn)`。
- **禁止**在 feature 里自己 `dynamic_proxy(View.OnClickListener)` 之类重复建类；同类回调多实例共享单例类。
- 组件只用：`Page`、`make_text/make_button/make_edit/make_hrow`、`copy_text`、`render_result/pretty`。
- 跨实例状态（相机回调对象等）挂 `sys` 全局缓存，仿 feature_camera 的 `sys._qgb_photo_cb_cls`。

### 4.1 错误隔离铁律：feature 错误不允许崩 client

设计原则（按层兜底，每层独立）：

- **服务器/topic 错误不影响 feature**：目标端报错、RPC 超时、返回 `ok:false`，feature 一律转成页面上的结构化错误文本（`on_error`/状态行），不弹崩溃框。
- **feature 错误不影响 client 主程序**：任何 JVM→Python 回调里未捕获的异常都会变成 Android 未捕获异常直接杀进程。pyui_kit 的所有代理边界（点击、`_post` Runnable、attach/detach、watch tick、`run_async` 的 `on_ok/on_error`/按钮恢复）已统一吞掉异常并写 logcat（tag `qgb-pyui`）。feature 要做到：
  - `run_async(work, on_ok, on_error, buttons)`：work 线程只管抛，错误经 `on_error` 回主线程；**不要**自己在 except 里定义引用异常变量的闭包再 `_post`（CPython 退出 except 会 `del error`，延迟回调必 NameError 崩进程——已踩过）。需要绑定时用默认参数，或直接交给 `run_async`。
  - 回调里拿不准的 JVM 调用（View 已 detach、bitmap 为 None 后 setImageBitmap 等）自己也 try 一下，失败显示错误文本；即使漏了，边界护栏也只吞不崩，但页面会停在旧状态。
  - `call_feature` 把 feature 动作异常（含 SystemExit）转成 `{ok:false, error}` JSON；Kotlin 建 View 异常兜成错误页。native/JVM 硬崩溃不在保护范围。

### 4.2 Python 调用原生控件规范（原生优先，Python 兜底）

复杂手势/高性能控件用 Kotlin 写好放在 `com.qgb.clientmqtt.*`，Python 按"尝试原生 → 失败回退纯 Python 静态版本"的固定模式调用，**任何一步失败页面也要完整可用**：

- 图片缩放/拖动：`image = pyui_kit.make_zoom_image(context, height_dp=300)`，内部优先实例化 `com.qgb.clientmqtt.ZoomableImageView`（双指缩放、拖动、双击还原，边界约束在原生侧）；拿不到就回退静态 `ImageView`（不能动但能看）。返回值都是 `ImageView` 子类，直接 `setImageBitmap` / `setVisibility`。
- 新增同类控件照此办理：Kotlin 控件放在 app 包内并保证 `@JvmOverloads` 构造可单 `context` 实例化；pyui_kit 提供 `make_xxx()` 包装，`try: from com.qgb.clientmqtt import Xxx` 失败时 `report_feature_error(...)` 并构造 Python 兜底 View；**禁止 feature 直接 `from com.qgb... import`**（否则旧 APK 上直接 ImportError 白屏）。
- 固定高度控件经 `Page.add(view)` 识别 `qgb-zoom-image` tag 沿用预置高度，或显式 `page.add(view, height_dp=300)`。
- 未放大时控件必须把滑动还给 ScrollView/Pager（原生侧 `requestDisallowInterceptTouchEvent` 已处理），feature 不要在容器层再包一层拦截手势的 View。

### 4.3 feature 互操作规范（A feature 调 B feature）

- **唯一通道**：`client_service.call_feature("b_name", "action", *args)`（feature 内部也可 `bootstrap.call_feature`）。它永远返回 JSON 字符串，被调方抛错会被结构化成 `{"ok": false, "error": ...}`，调用方必须判 `ok`，失败显示错误文本，不允许崩。
- 参数和返回只许 JSON 友好类型（str/int/float/bool/None/list/dict）；动作名必须在被调方 `FEATURE["actions"]` 声明；B feature 的 action 要能接受可选参数（如 `audio.play(path)`，不给走默认值）。
- **禁止** `import feature_b`、共享模块级可变状态、直接调对方私有函数；跨 feature 数据不落盘、不经过 MQTT 额外通道。
- 示例（Files → Audio）：文件列表里音频扩展名（`.mp3/.wav/.aac/.m4a/.ogg/.flac/.opus/.amr`，白名单写在 feature_files.AUDIO_EXTENSIONS）显示 `▶ Play` 标签，点击 `call_feature("audio", "play", remote_path)` 在目标端播放，结果写 Files 自己的状态行。

### 4.4 长任务进度汇报规范（隔几秒，不要求实时）

- 本机 HTTP 下载：下载函数接受 `progress=None` 回调（`download_remote_to_file` / `download_transfer_base64`，经 `client_service.make_progress(fn, interval=2.5)` 节流，首包和完成包必达），回调签名 `fn(got_bytes, total_bytes)`，在**工作线程**触发；更新 UI 必须包 `pyui_kit.on_main(lambda: set_status(...))`。
- 目标端上传（RPC 阻塞、拿不到字节进度）：用 `stop = pyui_kit.repeat_every(3, fn)` 在主线程每 3s 汇报已等待秒数，`finally: stop()` 必停。
- 汇报内容统一写页面副标题下的状态行（Files 的 "Browse and download files on the target" 页内 status），格式示例：`↓ Downloading x.apk: 12.30/87.19MB · 14% · 182 KB/s · 69s`；完成行带阶段耗时 `Saved to downloads/ (↑8s ↓412s total 420s)`。
- 回调只做显示，不许抛错（节流器已吞）；禁止在回调里再发 RPC 或做重计算。

## 5. import 规则

- `android.*`、`java.*` 及任何 Chaquopy/JVM 相关 import **只许写在函数体内**，禁止模块顶层 import（桌面单测与非安卓环境要能 import 模块）。
- 顶层只 import `json`、`client_service`、标准库。
- 禁改 `app/src/main/python/multi_mqtt/`；feature 不直接依赖另一个 feature 的代码生成器。

## 6. RPC 与超时

```python
client_service.rpc(code, device=None, timeout=None)
```

- `timeout=None`：用目标设备配置的 timeout（默认 10s），**它只是默认值**。
- feature 按操作显式传秒数覆盖，范围裁剪到 [1, 600]：如 wifi=10、scan=20、拍照=45、上传=120。新照此约定。
- 需要更底层控制（如 probe 的 2s 自定义）可直调 `client_service._mqtt_client_module().rpc(...)`，自带 `request_topic/timeout/client_private_key_bytes` 参数。
- 大数据走目标端 Aliyun 上传/下载，MQTT 只传短元数据，禁止传大字节。
- 返回给 UI 必须是 JSON 字符串（`json.dumps(..., ensure_ascii=False)`），不能返回 dict（执行器会 repr 成单引号）。
- 未捕获异常（含 SystemExit）由 `call_feature` 转成 `{ok:false, error}` JSON，崩不了 Activity；但 native 崩溃兜不住。

## 7. 下载分发与 URL 根

- 设置页可配置 feature 下载根目录，持久化在 `client_mqtt.json` 的 `feature_url_root`。
- 默认根（ghfast 代理 GitHub main，国内直连慢才加的代理）：

```text
https://ghfast.top/https://raw.githubusercontent.com/cjqbj/client_mqtt-cjqbj/refs/heads/main/app/src/main/python/
```

- 下载 URL = `<root>feature_<name>.py`（根自动补尾斜杠，只接受 http/https）。
- 内置 feature 首选失败再回退 raw.githubusercontent.com → github.com；**自定义新 feature 不做 GitHub 回退**（必然 404，无意义重试）。
- Python API：
  - `feature_download_settings()` / `update_feature_download_settings({"feature_url_root": ...})`
  - `install_named_feature(script_root, "demo", retries=4, timeout=20)`：接受裸名或全名，下到 py_updates 并立刻弹模块缓存+清 pyc。
  - `install_builtin_features(script_root, 4, 15)`（补缺失）/ `reinstall_builtin_features(...)`（force 全重下）/ `install_builtin_feature(script_root, filename, 4, 15)`。
- **发现机制**：catalog 合并 `BUILTIN_FEATURES` + 扫描两个目录，每 3 秒自动轮询；设置页"Refresh feature list"调 `rescan_features()` 立即重扫。手动放进 py_updates 的文件刷新后即出现，下次动作按需 import；删除文件重扫后运行时模块清出 sys.modules。
- 已加载模块常驻缓存：线上改文件后让用户长按 feature → reload（`reload_feature(name)`）；唯一自动重载是 py_updates 文件首次遮蔽内置模块。

## 8. 每目标持久化（不要自己读写 client_mqtt.json）

- `client_service.feature_settings(name)` / `update_feature_settings(name, values)`：按当前选中目标存 feature 自己的扁平 JSON（浅合并，别放秘钥/大 blob）。
- 当前选中目标 tab 由外壳自动记忆（`selected_feature`），feature 不用管。
- 当前目标配置：`client_service.selected_config()`（pyui_kit 内同名便捷封装）。
- **每目标 feature 生效列表**（`enabled_features`）：
  - 缺省/`null` = **全部 feature 对该目标生效（默认全选）**；显式列表 = 白名单，只有名单内 feature 在该目标的底栏/侧栏/pager 出现。全部重新勾齐回归 null，新装 feature 自动可见。
  - 由目标设置页勾选，外壳负责过滤与持久化，feature 自身无感知。确需在代码里判断时用 `client_service.is_feature_enabled("camera", device_ref=None)` / `enabled_features(device_ref)`；写入只走 `update_device_settings`（自动归一化：去 `feature_` 前缀、去重、丢弃非法标识）。
  - 字段随目标记录持久化，契约贯穿 `client_mqtt.json` → `device_catalog/device_settings` JSON → Kotlin `TargetDescriptor.enabledFeatures` → 过滤 UI，任何一端新增消费方都要按 null=全选兜底。

## 9. 自检

- 新 feature 至少能被桌面 import 并走通 `bootstrap.describe_features()`；测试放仓库 `tests/`，本地：

```powershell
$env:PYTHONPATH="."; python -m unittest discover -s tests   # cwd=app/src/main/python
```
