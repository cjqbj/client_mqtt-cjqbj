"""Remote audio playback + volume control feature."""

import json

import client_service

FEATURE = {
    "name": "audio",
    "title": "Audio",
    "version": 1,
    "actions": ["play", "get_volume", "set_volume"],
    "ui": "python",
    "icon": "audio",
}

AUDIO_PATH = "/sdcard/apm/mm.mp3"
PLAY_TIMEOUT = 20
VOLUME_TIMEOUT = 10

# 支持的音频流类型（按 UI 顺序）：
#   3=媒体, 2=铃声, 4=闹钟, 5=通知
STREAM_OPTIONS = [
    (3, "Media"),
    (2, "Ring"),
    (4, "Alarm"),
    (5, "Notify"),
]
STREAM_NAMES = dict(STREAM_OPTIONS)
DEFAULT_STREAM = 3
DEFAULT_VOLUME_MAX = 15  # 只是 SeekBar 初始上限，真实值会从设备端拉取


# ---------------------------------------------------------------------------
# 目标设备执行代码（字符串形式，由 client_service.rpc 下发）
# ---------------------------------------------------------------------------

def build_play_code(path=AUDIO_PATH):
    path_value = json.dumps(str(path), ensure_ascii=False)
    return f'''import json, os, sys, time, types
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
MediaPlayer = jclass("android.media.MediaPlayer")
AudioManager = jclass("android.media.AudioManager")
Context = jclass("android.content.Context")
AUDIO_PATH = {path_value}
ctx = ActivityThread.currentApplication()
am = ctx.getSystemService(Context.AUDIO_SERVICE)
r = {{
    "ok": False,
    "path": AUDIO_PATH,
    "file_exists": False,
    "file_size": 0,
    "stream": "STREAM_MUSIC",
    "vol_before": None,
    "vol_after": None,
    "vol_max": None,
    "duration_ms": None,
    "playing": False,
    "stopped_previous": False,
    "steps": [],
    "errors": [],
}}
try:
    r["file_exists"] = os.path.isfile(AUDIO_PATH)
    if r["file_exists"]:
        r["file_size"] = os.path.getsize(AUDIO_PATH)
        r["steps"].append("file_size=%d" % r["file_size"])
except Exception as exc:
    r["errors"].append("stat: " + " ".join(str(exc).split())[:150])

if not r["file_exists"]:
    r["errors"].append("file not found")
else:
    try:
        if "_mp_holder" not in sys.modules:
            sys.modules["_mp_holder"] = types.ModuleType("_mp_holder")
        holder = sys.modules["_mp_holder"]
        old = getattr(holder, "mp", None)
        if old is not None:
            try:
                old.stop()
            except Exception:
                pass
            try:
                old.release()
            except Exception:
                pass
            r["stopped_previous"] = True
            r["steps"].append("stopped previous MediaPlayer")
        holder.mp = None
    except Exception as exc:
        r["errors"].append("stop_prev: " + " ".join(str(exc).split())[:150])

    try:
        max_vol = int(am.getStreamMaxVolume(AudioManager.STREAM_MUSIC))
        cur_vol = int(am.getStreamVolume(AudioManager.STREAM_MUSIC))
        r["vol_max"] = max_vol
        r["vol_before"] = cur_vol
        am.setStreamVolume(AudioManager.STREAM_MUSIC, max_vol, 0)
        r["vol_after"] = int(am.getStreamVolume(AudioManager.STREAM_MUSIC))
        r["steps"].append("volume %d -> %d (max=%d)" %
                          (cur_vol, r["vol_after"], max_vol))
    except Exception as exc:
        r["errors"].append("volume: " + " ".join(str(exc).split())[:150])

    mp = None
    try:
        mp = MediaPlayer()
        mp.setAudioStreamType(AudioManager.STREAM_MUSIC)
        mp.setDataSource(AUDIO_PATH)
        mp.setVolume(1.0, 1.0)
        mp.prepare()
        r["duration_ms"] = int(mp.getDuration())
        mp.start()
        r["playing"] = bool(mp.isPlaying())
        r["steps"].append("started, duration=%dms" % r["duration_ms"])
        sys.modules["_mp_holder"].mp = mp
        r["ok"] = True
    except Exception as exc:
        r["errors"].append("play: " + " ".join(str(exc).split())[:250])
        if mp is not None:
            try:
                mp.release()
            except Exception:
                pass

order = ["ok", "path", "file_exists", "file_size", "stream",
         "vol_before", "vol_after", "vol_max", "duration_ms", "playing",
         "stopped_previous", "steps", "errors"]
# 关键：r 必须是 JSON 字符串（RPC 执行器对非字符串会走 repr，
# 客户端 json.loads 会因单引号报错）。与 feature_camera 一致。
r = json.dumps({{key: r[key] for key in order if key in r}}, ensure_ascii=False)
'''


def build_volume_code(action="set", level=15, stream=DEFAULT_STREAM):
    """查询 / 设置某个音频流的音量。

    - action="get" 只读取，返回 max_volume / after_volume；
    - action="set" 先钳位、写入，再回读 after_volume（真实生效值）。
    """
    action_literal = json.dumps(str(action))
    level_value = int(level)
    stream_value = int(stream)
    return f'''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
Context = jclass("android.content.Context")
AudioManager = jclass("android.media.AudioManager")

ACTION = {action_literal}
LEVEL = {level_value}
STREAM = {stream_value}

ctx = ActivityThread.currentApplication()
am = ctx.getSystemService(Context.AUDIO_SERVICE)

r = {{"ok": False, "action": ACTION, "stream": STREAM}}

try:
    max_vol = int(am.getStreamMaxVolume(STREAM))
    current_vol = int(am.getStreamVolume(STREAM))
    r["max_volume"] = max_vol
    r["before_volume"] = current_vol

    if ACTION == "set":
        # 确保不越界
        target = max(0, min(LEVEL, max_vol))
        am.setStreamVolume(STREAM, target, 0)  # 0 = 不弹系统 UI
        r["after_volume"] = int(am.getStreamVolume(STREAM))
        r["target"] = target
    else:
        r["after_volume"] = current_vol
        r["target"] = current_vol

    r["ok"] = True
except Exception as e:
    r["error"] = " ".join(str(e).split())[:200]

r = json.dumps(r, ensure_ascii=False)
'''


# ---------------------------------------------------------------------------
# 远端动作入口（保持 compose 风格：返回 JSON 字符串）
# ---------------------------------------------------------------------------

def play(path=AUDIO_PATH):
    # 起播 + 状态采集，20s 上限，不依赖设备默认 timeout。
    result = client_service.rpc(build_play_code(path), timeout=PLAY_TIMEOUT)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def get_volume(stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_volume_code("get", 0, stream), timeout=VOLUME_TIMEOUT
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def set_volume(level, stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_volume_code("set", int(level), stream), timeout=VOLUME_TIMEOUT
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Chaquopy 自绘 UI
# ---------------------------------------------------------------------------

def build_view(context):
    """播放结果区 + 流类型选择 + 长期有效的音量滑动条 + 底部 Play。"""
    import bootstrap
    import pyui_kit
    from java import dynamic_proxy, jclass
    from android.view import View
    from android.widget import LinearLayout, SeekBar

    page = pyui_kit.Page(
        context, "Audio",
        "Play %s on the target speaker; drag the slider to set volume."
        % AUDIO_PATH,
    )

    # 全局 UI 状态（跨目标切换共享 stream 选择；level/max 是当前流的值）
    ui_state = {
        "key": None,
        "stream": DEFAULT_STREAM,
        "level": 0,
        "max": DEFAULT_VOLUME_MAX,
    }

    status = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(status)

    # ---- 流类型选择行（4 个等宽按钮） ----
    stream_row = LinearLayout(context)
    stream_row.setOrientation(LinearLayout.HORIZONTAL)
    page.add(stream_row, top=8)

    stream_buttons = []  # [(stream, button), ...]

    for s in (3, 2, 4, 5):
        btn = pyui_kit.make_button(context, STREAM_NAMES[s], lambda: None)
        lp = LinearLayout.LayoutParams(0, pyui_kit.wrap(), 1.0)
        stream_row.addView(btn, lp)
        stream_buttons.append((s, btn))

    # ---- 音量标签 + 滑动条 ----
    volume_label = pyui_kit.make_text(context, "Volume: -", selectable=True)
    page.add(volume_label, top=8)

    seekbar = SeekBar(context)
    try:
        seekbar.setMax(DEFAULT_VOLUME_MAX)
    except Exception:
        pass
    page.add(seekbar, top=4)

    # ---- 底部 Play ----
    play_btn = pyui_kit.make_button(context, "Play", lambda: None)
    page.bottom_add(play_btn, weight=1.0)

    # ------------------------------------------------------------------
    # 兼容解析：bootstrap.call_feature 可能返回 dict 或 str
    # ------------------------------------------------------------------
    def parse_result(raw):
        if isinstance(raw, dict):
            if "r" in raw:
                r_val = raw["r"]
                if isinstance(r_val, str):
                    try:
                        return json.loads(r_val)
                    except Exception:
                        return {}
                return r_val if isinstance(r_val, dict) else {}
            return raw
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
        return {}

    # ------------------------------------------------------------------
    # 显示刷新
    # ------------------------------------------------------------------
    def update_volume_label():
        name = STREAM_NAMES.get(ui_state["stream"], "Stream %d" % ui_state["stream"])
        volume_label.setText(
            "%s volume: %d / %d" % (name, ui_state["level"], ui_state["max"])
        )

    def update_stream_buttons():
        for s, btn in stream_buttons:
            marker = "● " if s == ui_state["stream"] else "○ "
            btn.setText(marker + STREAM_NAMES[s])

    def sync_seekbar(level, max_v):
        """把设备端的真实音量/上限同步回 UI；setProgress 触发 fromUser=False，
        会被下面的监听器忽略，不会回写设备。"""
        ui_state["level"] = int(level)
        ui_state["max"] = int(max_v)
        try:
            # 先 setMax 再 setProgress（顺序反了 progress 可能被旧 max 钳位）
            seekbar.setMax(ui_state["max"])
            seekbar.setProgress(ui_state["level"])
        except Exception:
            pass
        update_volume_label()

    # ------------------------------------------------------------------
    # 异步：查询当前流音量
    # ------------------------------------------------------------------
    def refresh_volume_async():
        key = ui_state["key"]
        stream = ui_state["stream"]
        if not key:
            return

        def work():
            return bootstrap.call_feature("audio", "get_volume", stream)

        def apply_ok(raw):
            data = parse_result(raw)
            if not data.get("ok"):
                return
            sync_seekbar(
                int(data.get("after_volume", 0)),
                int(data.get("max_volume", DEFAULT_VOLUME_MAX)),
            )

        def apply_error(error):
            # 静默失败，避免把正常状态覆盖成错误提示
            pass

        pyui_kit.run_async(work, apply_ok, apply_error)

    # ------------------------------------------------------------------
    # 异步：写入音量（松手才调用，避免拖动过程狂发 RPC）
    # ------------------------------------------------------------------
    def apply_volume_async(level):
        key = ui_state["key"]
        stream = ui_state["stream"]
        if not key:
            # 还没选目标：只更新本地显示
            update_volume_label()
            return

        def work():
            return bootstrap.call_feature("audio", "set_volume", level, stream)

        def apply_ok(raw):
            data = parse_result(raw)
            if not data.get("ok"):
                status.setText("Set volume failed: %s" % data.get("error", "?"))
                return
            # 以设备端回读的 after_volume 为准（可能被钳制）
            sync_seekbar(
                int(data.get("after_volume", level)),
                int(data.get("max_volume", ui_state["max"])),
            )

        def apply_error(error):
            status.setText("Set volume failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    # ------------------------------------------------------------------
    # 切换流类型
    # ------------------------------------------------------------------
    def select_stream(stream):
        if ui_state["stream"] == stream:
            return
        ui_state["stream"] = stream
        update_stream_buttons()
        refresh_volume_async()

    # ------------------------------------------------------------------
    # SeekBar 监听（Chaquopy dynamic_proxy 实现 Java 接口）
    # 拖动：只更新本地标签；松手：发一次 set_volume。
    # ------------------------------------------------------------------
    ListenerCls = jclass("android.widget.SeekBar$OnSeekBarChangeListener")

    class _SeekListener(dynamic_proxy(ListenerCls)):
        def onProgressChanged(self, sb, progress, fromUser):
            if not fromUser:
                return
            ui_state["level"] = int(progress)
            update_volume_label()

        def onStartTrackingTouch(self, sb):
            pass

        def onStopTrackingTouch(self, sb):
            apply_volume_async(int(sb.getProgress()))

    seek_listener = _SeekListener()  # 持有引用，防 GC 后回调失效
    try:
        seekbar.setOnSeekBarChangeListener(seek_listener)
    except Exception as exc:
        status.setText("SeekBar listener failed: %r" % (exc,))

    # ------------------------------------------------------------------
    # Play
    # ------------------------------------------------------------------
    def on_play():
        status.setText("Playing ...")

        def work():
            return bootstrap.call_feature("audio", "play")

        def apply_ok(raw):
            status.setText(pyui_kit.render_result(raw))

        def apply_error(error):
            status.setText("Play failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(play_btn,))

    pyui_kit.click(play_btn, on_play)
    for s, btn in stream_buttons:
        pyui_kit.click(btn, lambda ss=s: select_stream(ss))

    update_stream_buttons()
    update_volume_label()

    # ------------------------------------------------------------------
    # 目标切换
    # ------------------------------------------------------------------
    def on_target(key, topic, _cfg):
        ui_state["key"] = key
        page.set_target("target topic: " + str(topic))
        # 拉取该目标上当前流的音量，同步滑块
        refresh_volume_async()

    pyui_kit.watch_target(page.root, on_target)
    return page.root



