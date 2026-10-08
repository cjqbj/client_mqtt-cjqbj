# Client MQTT 项目协作指南

## 工作范围
- 只修改当前 Client MQTT 项目；不要修改工作区级的 `../multi_mqtt/` 或 `../Xime_rpc/`。
- `app/src/main/python/multi_mqtt/` 是由 `debug_build_secexp.sh` 从 `.gitmodules` 指定的源码同步生成的副本，不要手工编辑。默认源码位于相邻的 `../multi_mqtt/`。
- MQTT 依赖的平铺模块会被放入同名目录；客户端通过 `client_service.py` 兼容加载。修复应放在项目自己的桥接层，不要改生成副本。
- Chaquopy APK 不保证保留 `.py` 源文件；三个内置 feature 必须由 `bootstrap.py` 静态登记，不能只靠 `os.listdir` 扫描。

## feature脚本边界
- MQTT 只承载 RPC 控制代码和少量 JSON 元数据。
- 远程浏览通过目标设备上的 RPC 执行 `os.walk` 和 `os.stat`。
- 文件和照片字节通过目标端 `aliyun_git` 上传，再由客户端通过 HTTP 下载。
- 拍照数据必须保留在内存中，不得在目标设备写入照片文件。

- 每个 `request_topic` 对应带稳定 ID 的独立目标配置；topic、远程根目录、私钥、超时、验签回退选项和 feature 生效列表（`enabled_features`，缺省/null=全选）保存在脚本根目录的 `client_mqtt.json`。Aliyun 是公共配置，保存在同一文件的顶层，不得放进目标记录。目标页和公共 Aliyun 设置页修改后自动保存，并轮询感知外部文件变更。
- 错误隔离设计原则，任何改动不得破坏：
  - 服务器/topic 错误不影响 feature：目标端报错、RPC 超时/失败必须结构化为页面错误文本，不弹崩溃框。
  - feature 错误不影响 client 主程序：JVM→Python 回调边界（pyui_kit 的 Runnable/OnClick/attach/watch/run_async 回调）统一吞异常并写 logcat（tag `qgb-pyui`），禁止 feature 自建 dynamic_proxy 绕过护栏；异常变量不得在 except 闭包里延迟引用。
  - 顶栏名称只是别名，request topic 必须同屏显示；老内置名 "Target" 在显示层和下次保存时回退 topic 末段。
- `enabled_features` 契约：每个目标可独立勾选生效 feature，默认全选（存 null，不存名单，新装 feature 自动可见）；字段贯穿持久化 → `device_catalog/device_settings` JSON → Kotlin `TargetDescriptor.enabledFeatures` → 底栏/侧栏/pager 过滤，所有消费端按 null=全选兜底；被过滤掉的 feature 不参与 tab 索引与恢复。
- Python 调用原生控件：复杂手势控件用 Kotlin 写在 `com.qgb.clientmqtt.*`（如 ZoomableImageView：双指缩放/拖动/双击，未放大时放行父容器滚动手势），Python 只通过 pyui_kit 的 `make_xxx()` 包装调用（如 `make_zoom_image`），拿不到原生控件必须自动回退 Python 静态 View；禁止 feature 直接 import app 包类。
- feature 互操作：跨 feature 只允许 `client_service.call_feature("b", "action", *json_args)`（JSON 入参/返回，异常结构化 ok:false），禁止 import 对方模块/共享状态；action 必须在 `FEATURE["actions"]` 声明且可选参数。已落地：Files 音频行 ▶Play 调 `audio.play(path)`。
- 长任务进度：下载链路支持 `progress(got,total)` 回调并经 `client_service.make_progress` 节流到 ~2.5s（首包/完成必达），UI 更新走 `pyui_kit.on_main`；阻塞式 RPC 上传用 `pyui_kit.repeat_every(3, fn)` 心跳汇报秒数并 finally 停止；进度统一写页面状态行，回调不得抛错/做重活。
- 拍照目标端代码和 `capture` 入口集中在 `feature_camera.py`；Compose 页面只负责选择相机并调用 feature。
- 首次使用外部脚本目录时，可通过设置页的 Python 下载器安装缺失的 files、camera、wifi 脚本。下载要有超时、备用地址和重试，并在界面日志区显示逐步结果。

## 开发与文档维护
- 保持 Compose UI、Python 桥接、RPC 代码生成和传输适配器职责清晰。
- 新增运行时脚本遵循 `FEATURE_DEVELOPMENT.md`；不要把新的脚本专属 Compose 页面继续堆入主 Activity。
- 每个 feature 自己负责领域代码生成和 RPC 动作；`client_service` 只提供通用 RPC、JSON、配置和传输桥接，不新增 `build_wifi_code`、`scan_remote` 这类 feature 专属 helper。
- Feature 返回结构化 JSON；普通信息由 Compose 展示，照片等媒体通过 feature 返回的短 URL/元数据交给 Compose 下载和预览，不能把媒体字节经 MQTT 返回。
- `client_service.call_feature` 必须捕获 feature 的 Python 异常并返回结构化错误，不能让第三方脚本异常越过 Chaquopy 边界关闭 Activity；原生/JVM 崩溃不属于此保护范围。
- RPC 必须写入 App 内可查看且可复制的有界诊断日志，包含关联 ID、topic、reply topic、完整请求代码、MQTT envelope/raw response、目标 error/r、key 配置状态及类型/标准化长度、阶段、耗时和 broker 状态。
- 通用设置必须提供在线探测开关和间隔；任一目标 topic 的成功 RPC 都更新其健康时间，并推迟该目标的下一次探测，避免重复 ping。
- RPC 结果使用明确 JSON，不要为新协议依赖 Python 的 repr 输出。
- 递归扫描必须限制分页大小并返回 `has_more`、`next_offset`；拒绝路径穿越、符号链接逃逸和无界递归。
- files feature 的默认浏览页只扫描当前目录，结果同时包含 directory/file 项；Compose 负责进入子目录、返回上级和下载文件。分页 offset 必须基于已遍历条数。
- RPC 的 `r` 为 null 或目标返回 `ok: false` 时，必须保留目标错误详情并作为结构化结果展示，不能转成裸 JSON null。
- 扩大改动前先添加或更新针对性测试。
- 每完成一个可验证里程碑，更新 `PROGRESS.md`；RPC 请求或响应契约变化时更新 `PROTOCOL.md`。
- 架构、构建流程、feature API 或剩余工作发生变化时，分别同步 `ARCHITECTURE.md`、`BUILD.md`、`FEATURE_DEVELOPMENT.md` 或 `TODO.md`；不要把未验证事项写成已完成。

## 验证顺序
1. `bash -n debug_build_secexp.sh`
2. `python3 -m unittest discover -s tests`
3. `git diff --check`
4. 构建后按需比较 `.gitmodules` 指定源码与 `app/src/main/python/multi_mqtt/`，不要手改生成副本。
5. 运行 `./debug_build_secexp.sh`，并确认 APK 签名和包信息。
6. 目标设备在线时再运行设备 RPC 验证；未执行的相机、传输、权限等设备测试必须记录在 `PROGRESS.md`，不能声称已通过。
