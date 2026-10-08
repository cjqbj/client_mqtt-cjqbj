"""Remote audio playback feature."""

import json

import client_service

FEATURE = {
    "name": "audio",
    "title": "Audio",
    "version": 1,
    "actions": ["play"],
    "ui": "python",
    "icon": "audio",
}

AUDIO_PATH = "/sdcard/apm/mm.mp3"


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
# 关键：r 必须是 JSON 字符串，不能直接放 dict。RPC 执行器对非字符串结果会走
# pformat/repr（单引号 Python 字面量），客户端 json.loads 会报
# "Expecting property name enclosed in double quotes"。与 feature_camera 一致。
r = json.dumps({{key: r[key] for key in order if key in r}}, ensure_ascii=False)
'''


def play(path=AUDIO_PATH):
    # 起播 + 状态采集，20s 上限，不依赖设备默认 timeout。
    result = client_service.rpc(build_play_code(path), timeout=20)
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def build_view(context):
    """Chaquopy 自绘：播放结果区 + 底部 Play（目标机扬声器播放 /sdcard/apm/mm.mp3）。"""
    import bootstrap
    import pyui_kit

    page = pyui_kit.Page(
        context, "Audio", "Play %s on the target phone speaker" % AUDIO_PATH
    )
    result = pyui_kit.make_text(context, "Ready", selectable=True)
    page.add(result)
    play = pyui_kit.make_button(context, "Play", lambda: None)

    def on_play():
        result.setText("Playing ...")

        def work():
            return bootstrap.call_feature("audio", "play")

        def apply_ok(raw):
            result.setText(pyui_kit.render_result(raw))

        def apply_error(error):
            result.setText("Play failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(play,))

    pyui_kit.click(play, on_play)
    page.bottom_add(play, weight=1.0)
    pyui_kit.watch_target(
        page.root,
        lambda key, topic, cfg: page.set_target("target topic: " + topic),
    )
    return page.root
