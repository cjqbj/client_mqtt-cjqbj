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

## 9. 自检

- 新 feature 至少能被桌面 import 并走通 `bootstrap.describe_features()`；测试放仓库 `tests/`，本地：

```powershell
$env:PYTHONPATH="."; python -m unittest discover -s tests   # cwd=app/src/main/python
```
