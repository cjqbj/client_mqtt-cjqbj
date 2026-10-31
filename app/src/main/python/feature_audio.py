"""Remote audio playback + volume control + edge-tts speech feature.

动作：
- play(path)      播放目标上已存在的音频文件（默认 /sdcard/apm/mm.mp3）；
- speak(text)     目标端用 edge_tts 生成语音（默认缓存 /sdcard/apm），落盘后
                  立即用 MediaPlayer 播放；
- status()        读取当前播放进度、时长、文件与设备音量（长音频轮询用）；
- stop()          停止并释放 MediaPlayer；
- get/set_volume  查询/设置某个音频流的音量。

关键设计（为什么 tts 定义只发一次）：
目标端的 PythonExecutor 有持久命名空间，所以 tts() 的整套定义（含 helper、
缓存、edge_tts 调用）只在首次连接该目标时随一段 bootstrap 代码下发一次，
之后每次 speak 只发一句 ``await tts(...)`` 的短代码，省流量也省时间。
客户端用 ``_TTS_READY`` 按 request_topic 记住“已下发”；若目标 executor
重启导致命名空间丢失（speak 报 NameError），会自动补发一次定义并重试一次。

长语音不会卡住界面：RPC 在 mp.start() 后立刻返回，播放继续在目标上进行；
页面用 repeat_every 轮询 status，实时显示 position / duration / 音量。
"""
import json

import client_service

FEATURE = {
    "name": "audio",
    "title": "Audio",
    "version": 4,
    "actions": ["play", "speak", "status", "stop", "get_volume", "set_volume"],
    "ui": "python",
    "icon": "audio",
    # 可调参数（长按 feature → Setting 的独立设置页，不占主界面）。
    "settings": [
        {"key": "timeout", "label": "RPC 超时（秒）", "type": "int",
         "default": None, "min": 1, "max": 600, "scope": "topic",
         "help": "留空 = 播放 20s / 状态查询 10s；填了则这几个动作统一用该值。"},
        {"key": "tts_timeout", "label": "TTS 生成超时（秒）", "type": "int",
         "default": None, "min": 1, "max": 600, "scope": "topic",
         "help": "留空 = 90s。只约束目标端 edge_tts 合成（长文本可能几十秒）；播放本身是异步的，不受该值限制。"},
        {"key": "tts_proxy", "label": "TTS 代理（http://host:port）", "type": "text",
         "default": "", "scope": "topic",
         "help": "留空 = 直连。目标网络访问不了 speech.platform.bing.com（被 reset）时填 http 代理，"
                   "如 http://192.168.1.2:7890；edge_tts 只支持 http/https 代理，其它写法按留空处理。"},
    ],
}

AUDIO_PATH = "/sdcard/apm/mm.mp3"
PLAY_TIMEOUT = 20
VOLUME_TIMEOUT = 10

# TTS：默认缓存目录与发音参数（与用户给的 tts() 默认值一致）。
TTS_SAVE_DIR = "/sdcard/apm"
TTS_VOICE = "zh-CN-YunxiaNeural"
TTS_FORMAT = "mp3"
TTS_RATE = "-40%"
TTS_PITCH = "-20Hz"
TTS_TIMEOUT = 90
TTS_TEXT_LIMIT = 2000
# bootstrap 含「缺依赖时 runtime_pip 安装 edge_tts」的兜底路径，给足下载时间；
# 正常情况下仍是毫秒级返回。
BOOTSTRAP_TIMEOUT = 180

DEFAULT_TEXT = "你好，这是一次 QGB 远程 TTS 播放测试。"

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
#
# 模板统一用 @TOKEN@ 占位（replace 注入），避免 f-string 里成对的 {{ }} 转义。
# ---------------------------------------------------------------------------

PLAY_CODE = '''import json, os, sys, time, types
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
MediaPlayer = jclass("android.media.MediaPlayer")
AudioManager = jclass("android.media.AudioManager")
Context = jclass("android.content.Context")
AUDIO_PATH = @PATH@
ctx = ActivityThread.currentApplication()
am = ctx.getSystemService(Context.AUDIO_SERVICE)
r = {
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
}
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
        holder.path = ""
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
        sys.modules["_mp_holder"].path = AUDIO_PATH
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
r = json.dumps({key: r[key] for key in order if key in r}, ensure_ascii=False)
'''

VOLUME_CODE = '''import json
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
Context = jclass("android.content.Context")
AudioManager = jclass("android.media.AudioManager")

ACTION = @ACTION@
LEVEL = @LEVEL@
STREAM = @STREAM@

ctx = ActivityThread.currentApplication()
am = ctx.getSystemService(Context.AUDIO_SERVICE)

r = {"ok": False, "action": ACTION, "stream": STREAM}

try:
    max_vol = int(am.getStreamMaxVolume(STREAM))
    current_vol = int(am.getStreamVolume(STREAM))
    r["max_volume"] = max_vol
    r["before_volume"] = current_vol

    if ACTION == "set":
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

# edge_tts 定义（用户提供版本的完整搬运）+ 一次健康检查。
# 这段代码只在【首次连接该目标】时发一次；执行后 tts() 常驻目标命名空间。
TTS_RUNTIME = r'''import os, re
_FMT_MIME = {'mp3': 'audio/mpeg', 'wav': 'audio/wav', 'ogg': 'audio/ogg', 'webm': 'audio/webm'}
_BOUNDARIES = {'WordBoundary', 'SentenceBoundary'}
_tts_cache = {}
# 微软 TTS 节点在这条网络上会间歇性 reset TLS 握手（k12 实测：同一组参数
# 第一次直接失败、紧接着重试就成功）。最多试 3 次，只影响合成这一环，
# 全失败仍按错误返回（边缘功能只降级）。
TTS_RETRIES = 3

def _normalize_percent(v, kind="rate"):
    if v is None:
        return '+0%'
    if isinstance(v, (int, float)):
        return f'{v:+d}%'
    s = str(v).strip()
    if not s:
        return '+0%'
    if not s.endswith('%'):
        s += '%'
    return s if s[0] in '+-' else '+' + s

def _normalize_pitch(v):
    if v is None:
        return '+0Hz'
    if isinstance(v, (int, float)):
        return f'{v:+d}Hz'
    s = str(v).strip()
    if not s:
        return '+0Hz'
    if not s.lower().endswith('hz'):
        s += 'Hz'
    return s if s[0] in '+-' else '+' + s

def _tts_cache_get(key):
    return _tts_cache.get(key)

def _tts_cache_put(key, data):
    _tts_cache[key] = data

def _err(e, where, **ctx):
    import traceback
    kv = ' '.join(f'{k}={v!r}' for k, v in ctx.items())
    return (f"[tts failed @ {where}] {type(e).__name__}: {e} | {kv} | "
            f"{traceback.format_exc()}")

def _ipv4_connector():
    """强制 IPv4 的 aiohttp connector（拿不到就返回 None，退回默认）。

    k12 实测：getaddrinfo 同时给出 IPv6（2620:1ec:33::10）和 IPv4，
    aiohttp 默认顺序常先连 IPv6，而这条链路的 IPv6 会间歇性被 reset；
    同一参数强制 IPv4 后 4/4 成功。创建失败只降级，不影响合成。
    """
    try:
        import socket
        import aiohttp
        return aiohttp.TCPConnector(family=socket.AF_INET, force_close=True)
    except Exception:
        return None

async def _sleep_quiet(seconds):
    """重试退避：任何异常都吞掉，不让边缘功能把外层流程弄断。"""
    try:
        import asyncio
        await asyncio.sleep(max(0.0, float(seconds)))
    except Exception:
        pass

async def tts(text: str, voice: str = 'zh-CN-YunxiaNeural', fmt="mp3", response=None,
              save_dir: str = @SAVE_DIR@, rate: str = '-40%', pitch: str = '-20Hz', **ka) -> bytes:
    try:
        import edge_tts
    except Exception as e:
        return _err(e, "import edge_tts")
    try:
        options = {}
        if rate is not None:
            options["rate"] = _normalize_percent(rate, "rate")
        if "volume" in ka:
            options["volume"] = _normalize_percent(ka.pop("volume"), "volume")
        if pitch is not None:
            options["pitch"] = _normalize_pitch(pitch)
        if "boundary" in ka:
            boundary = ka.pop("boundary")
            if boundary not in _BOUNDARIES:
                raise ValueError(f"boundary 必须是 {_BOUNDARIES} 之一，收到 {boundary!r}")
            options["boundary"] = boundary
        for key in ("proxy", "connector", "connect_timeout", "receive_timeout"):
            if key in ka:
                options[key] = ka.pop(key)
        if ka:
            raise TypeError(f"tts 不支持的参数：{sorted(ka)}")
    except Exception as e:
        return _err(e, "normalize/params", text=text, voice=voice, fmt=fmt, rate=rate, pitch=pitch)
    content_type = _FMT_MIME.get(fmt, "audio/mpeg")
    cache_key = (fmt, text, voice, options.get("rate", "+0%"),
                 options.get("volume", "+0%"), options.get("pitch", "+0Hz"))
    def _save(data):
        if not save_dir:
            return data
        try:
            os.makedirs(save_dir, exist_ok=True)
            name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', text or '').strip()[:20] or 'tts'
            path = os.path.join(save_dir, f'{name}.{fmt}')
            i = 1
            while os.path.exists(path):
                path = os.path.join(save_dir, f'{name}_{i}.{fmt}')
                i += 1
            with open(path, 'wb') as f:
                f.write(data)
            return path
        except Exception as e:
            return _err(e, "save", save_dir=save_dir, fmt=fmt, text=text)
    try:
        cached = _tts_cache_get(cache_key)
    except Exception as e:
        return _err(e, "cache_get", cache_key=cache_key)
    if cached is not None:
        try:
            if response is not None:
                response.set_header("Content-Type", content_type)
                response.write(cached)
        except Exception as e:
            return _err(e, "response.write(cached)", text=text, voice=voice, fmt=fmt)
        return _save(cached)
    # k12 现场：默认走 IPv6 会间歇性被 reset，强制 IPv4 才稳定；拿不到
    # aiohttp 或创建失败就退回默认行为（只降级，不影响合成主路径）。
    own_connector = None
    if "connector" not in options:
        own_connector = _ipv4_connector()
        if own_connector is not None:
            options["connector"] = own_connector
    try:
        try:
            comm = edge_tts.Communicate(text, voice, **options)
        except Exception as e:
            return _err(e, "Communicate()", text=text, voice=voice, options=options)
        buf = b""
        try:
            if response is not None:
                response.set_header("Content-Type", content_type)
        except Exception as e:
            return _err(e, "response.set_header", text=text, voice=voice, fmt=fmt)
        # 重试：连接阶段的 reset 偶发，卡一下换个新会话重试一般都成。
        # 整段音频没拿全就丢弃重来；只有 HTTP 直出（response 非空，
        # 字节已经写给客户端）才不重试，避免出现重复音频。
        attempts = 1 if response is not None else max(1, int(TTS_RETRIES))
        for attempt in range(attempts):
            buf = b""
            try:
                if attempt:
                    # 每次重试都用全新的 Communicate（新连接/新会话）。
                    await _sleep_quiet(0.3 * attempt)
                    comm = edge_tts.Communicate(text, voice, **options)
                async for chunk in comm.stream():
                    if chunk["type"] != "audio":
                        continue
                    data = chunk["data"]
                    buf += data
                    if response is not None:
                        response.write(data)
            except Exception as e:
                if attempt + 1 >= attempts:
                    return _err(e, "comm.stream", text=text, voice=voice,
                                options=options, got_bytes=len(buf),
                                attempts=attempts)
                continue
            break
        if not buf:
            return _err(RuntimeError("edge-tts 未返回任何音频（参数或发音人不被接受）"),
                        "empty_audio", text=text, voice=voice, options=options)
    finally:
        if own_connector is not None:
            try:
                await own_connector.close()
            except Exception:
                pass
    try:
        _tts_cache_put(cache_key, buf)
    except Exception as e:
        return _err(e, "cache_put", cache_key=cache_key, size=len(buf))
    try:
        if response is not None and not getattr(response, "_streamed", False):
            response.set_data(buf)
    except Exception as e:
        return _err(e, "response.set_data", text=text, voice=voice, fmt=fmt)
    return _save(buf)

# ---- 健康检查 + 依赖自愈（失败只降级，不抛异常） ----
# k12 实测两类坏法：
#   1) edge_tts 已装，但依赖的**模块文件**缺失（有 typing_extensions 的 dist-info
#      却没有 typing_extensions.py）→ import edge_tts 报 ModuleNotFoundError。
#      runtime_pip 按 metadata 判定“依赖已满足”，重装 edge_tts 并不会补这个文件，
#      所以这里按错误里点名的模块名单独补装，再重试 import。
#   2) edge_tts 本身不在 → 直接装 edge_tts。
# runtime_pip.install 失败只回 dict、从不抛异常；最多补装 3 轮就放弃，保持降级。
_qgb_tts_ready = True
_qgb_edge_tts = None
_qgb_edge_tts_error = ''
_qgb_install = {"attempted": False, "ok": None, "method": "", "version": "",
                "error": "", "log_tail": "", "steps": []}


def _qgb_missing_module(exc):
    """从 import 错误里取缺失的顶层模块名；拿不到就返回空串。"""
    name = str(getattr(exc, "name", "") or "").split(".")[0].strip()
    return name if name.isidentifier() else ""


try:
    import edge_tts as _qgb_edge_tts
except Exception as _qgb_exc:
    _qgb_edge_tts = None
    _qgb_edge_tts_error = '%s: %s' % (type(_qgb_exc).__name__, _qgb_exc)
    _qgb_install["attempted"] = True
    try:
        import runtime_pip as _qgb_rpip
    except Exception as _qgb_exc2:
        _qgb_rpip = None
        _qgb_install["error"] = 'import runtime_pip: %s: %s' % (
            type(_qgb_exc2).__name__, _qgb_exc2)
    _qgb_pkgs = [_qgb_missing_module(_qgb_exc) or "edge_tts"]
    _qgb_tried = []
    for _qgb_round in range(3):
        if _qgb_rpip is None or _qgb_edge_tts is not None or not _qgb_pkgs:
            break
        _qgb_pkg = _qgb_pkgs.pop(0)
        _qgb_tried.append(_qgb_pkg)
        try:
            _qgb_r = _qgb_rpip.install(_qgb_pkg)
            _qgb_step = {"pkg": _qgb_pkg, "ok": bool(_qgb_r.get("ok")),
                         "method": str(_qgb_r.get("method") or ""),
                         "version": str(_qgb_r.get("version") or ""),
                         "error": str(_qgb_r.get("error") or "")}
            _qgb_install["steps"].append(_qgb_step)
            _qgb_install["method"] = _qgb_step["method"] or _qgb_install["method"]
            _qgb_install["version"] = _qgb_step["version"] or _qgb_install["version"]
            if _qgb_step["error"]:
                _qgb_install["error"] = _qgb_step["error"]
            _qgb_install["log_tail"] = str(_qgb_r.get("log") or "")[-600:]
        except Exception as _qgb_exc3:
            _qgb_install["error"] = '%s: %s' % (type(_qgb_exc3).__name__, _qgb_exc3)
            _qgb_install["steps"].append({"pkg": _qgb_pkg, "ok": False,
                                          "method": "", "version": "",
                                          "error": _qgb_install["error"]})
        # 装完立刻重试 import；还缺模块就把它插到队首，下一轮补装。
        try:
            import edge_tts as _qgb_edge_tts
            _qgb_edge_tts_error = ''
            break
        except Exception as _qgb_exc4:
            _qgb_edge_tts = None
            _qgb_edge_tts_error = '%s: %s' % (type(_qgb_exc4).__name__, _qgb_exc4)
            _qgb_missing = _qgb_missing_module(_qgb_exc4)
            if _qgb_missing and _qgb_missing not in _qgb_pkgs \
                    and _qgb_missing not in _qgb_tried:
                _qgb_pkgs.insert(0, _qgb_missing)
    _qgb_install["ok"] = _qgb_edge_tts is not None
_qgb_save_dir = @SAVE_DIR@
_qgb_save_ok = bool(_qgb_save_dir)
_qgb_save_error = '' if _qgb_save_ok else 'save_dir is empty'
try:
    if _qgb_save_ok:
        if not os.path.isdir(_qgb_save_dir):
            os.makedirs(_qgb_save_dir, exist_ok=True)
        _qgb_save_ok = bool(os.access(_qgb_save_dir, os.W_OK))
        if not _qgb_save_ok:
            _qgb_save_error = 'save_dir not writable: %r' % (_qgb_save_dir,)
except Exception as _qgb_exc:
    _qgb_save_ok = False
    _qgb_save_error = '%s: %s' % (type(_qgb_exc).__name__, _qgb_exc)
import json as _qgb_json
r = _qgb_json.dumps({
    "ok": bool(_qgb_edge_tts is not None and _qgb_save_ok),
    "tts_ready": True,
    "edge_tts": getattr(_qgb_edge_tts, "__version__", "") if _qgb_edge_tts is not None else "",
    "edge_tts_error": _qgb_edge_tts_error,
    "edge_tts_install": _qgb_install,
    "save_dir": _qgb_save_dir,
    "save_dir_ok": bool(_qgb_save_ok),
    "save_dir_error": _qgb_save_error,
}, ensure_ascii=False)
'''

SPEAK_CODE = r'''import json, os, sys, time, types
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
MediaPlayer = jclass("android.media.MediaPlayer")
AudioManager = jclass("android.media.AudioManager")
Context = jclass("android.content.Context")

TEXT = @TEXT@
VOICE = @VOICE@
FMT = @FMT@
RATE = @RATE@
PITCH = @PITCH@
SAVE_DIR = @SAVE_DIR@
STREAM = @STREAM@
PROXY = @PROXY@

# 目标端执行结果：tts 生成本身可能几十秒，这里只记录阶段便于客户端区分失败点。
r = {
    "ok": False,
    "stage": "init",
    "text_chars": len(TEXT),
    "voice": VOICE,
    "fmt": FMT,
    "rate": RATE,
    "pitch": PITCH,
    "save_dir": SAVE_DIR,
    "proxy": PROXY or "",
    "path": "",
    "file_size": 0,
    "vol_before": None,
    "vol_after": None,
    "vol_max": None,
    "duration_ms": None,
    "playing": False,
    "tts_ms": None,
    "elapsed_ms": None,
    "stopped_previous": False,
    "holder_ready": False,
    "steps": [],
    "errors": [],
}

async def _qgb_do_speak():
    started = time.time()
    # 1) edge_tts 生成（目标 _tts_cache 命中时秒回），落盘到 SAVE_DIR
    try:
        out = await tts(TEXT, voice=VOICE, fmt=FMT, save_dir=SAVE_DIR, rate=RATE,
                        pitch=PITCH, proxy=(PROXY or None))
    except BaseException as exc:
        if isinstance(exc, NameError):
            r["rebootstrap"] = True
        r["stage"] = "tts"
        r["errors"].append("tts call: " + " ".join(str(exc).split())[:300])
        r["elapsed_ms"] = int((time.time() - started) * 1000)
        return r
    r["tts_ms"] = int((time.time() - started) * 1000)
    if isinstance(out, (bytes, bytearray)):
        r["stage"] = "save"
        r["errors"].append("tts returned bytes (SAVE_DIR must be a writable dir)")
        r["elapsed_ms"] = int((time.time() - started) * 1000)
        return r
    out_text = str(out or "")
    if out_text.startswith("[tts failed @"):
        r["stage"] = "tts"
        r["errors"].append(out_text[:400])
        r["elapsed_ms"] = int((time.time() - started) * 1000)
        return r
    path = out_text
    r["path"] = path
    try:
        r["file_size"] = int(os.path.getsize(path)) if path and os.path.isfile(path) else 0
    except Exception as exc:
        r["errors"].append("stat: " + " ".join(str(exc).split())[:200])
    if not r["file_size"]:
        r["stage"] = "save"
        r["errors"].append("tts file missing or empty: %r" % path)
        r["elapsed_ms"] = int((time.time() - started) * 1000)
        return r
    r["steps"].append("tts saved %s (%d bytes, %dms)" % (path, r["file_size"], r["tts_ms"]))

    # 2) 设备音量（保证长音频能听见）+ 播放；start 后立刻返回，播放在目标继续
    ctx = ActivityThread.currentApplication()
    am = ctx.getSystemService(Context.AUDIO_SERVICE)
    try:
        max_vol = int(am.getStreamMaxVolume(STREAM))
        cur_vol = int(am.getStreamVolume(STREAM))
        r["vol_max"] = max_vol
        r["vol_before"] = cur_vol
        if cur_vol != max_vol:
            am.setStreamVolume(STREAM, max_vol, 0)
        r["vol_after"] = int(am.getStreamVolume(STREAM))
        r["steps"].append("volume %s -> %s (max=%s)" % (cur_vol, r["vol_after"], max_vol))
    except Exception as exc:
        r["errors"].append("volume: " + " ".join(str(exc).split())[:200])

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
        holder.mp = None
        holder.path = ""
        mp = MediaPlayer()
        mp.setAudioStreamType(STREAM)
        mp.setDataSource(path)
        mp.setVolume(1.0, 1.0)
        mp.prepare()
        r["duration_ms"] = int(mp.getDuration())
        mp.start()
        r["playing"] = bool(mp.isPlaying())
        holder.mp = mp
        holder.path = path
        r["holder_ready"] = True
        r["stage"] = "playing"
        r["steps"].append("started, duration=%sms" % r["duration_ms"])
        r["ok"] = True
    except Exception as exc:
        r["stage"] = "play"
        r["errors"].append("play: " + " ".join(str(exc).split())[:300])
    r["elapsed_ms"] = int((time.time() - started) * 1000)
    return r

r = await _qgb_do_speak()
r = json.dumps(r, ensure_ascii=False)
'''

STATUS_CODE = r'''import json, sys
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
Context = jclass("android.content.Context")
AudioManager = jclass("android.media.AudioManager")
STREAM = @STREAM@

r = {"ok": True, "stream": STREAM, "player": False, "playing": False,
     "position_ms": 0, "duration_ms": 0, "path": "",
     "vol": None, "vol_max": None, "error": ""}
try:
    ctx = ActivityThread.currentApplication()
    am = ctx.getSystemService(Context.AUDIO_SERVICE)
    r["vol_max"] = int(am.getStreamMaxVolume(STREAM))
    r["vol"] = int(am.getStreamVolume(STREAM))
except Exception as exc:
    r["error"] = "volume: " + " ".join(str(exc).split())[:200]

holder = sys.modules.get("_mp_holder")
mp = getattr(holder, "mp", None) if holder is not None else None
if mp is not None:
    r["player"] = True
    r["path"] = str(getattr(holder, "path", "") or "")
    try:
        r["playing"] = bool(mp.isPlaying())
        r["position_ms"] = int(mp.getCurrentPosition())
        r["duration_ms"] = int(mp.getDuration())
    except Exception as exc:
        r["player"] = False
        r["playing"] = False
        r["error"] = (r["error"] + " | " if r["error"] else "") + "player: " + " ".join(str(exc).split())[:200]
r = json.dumps(r, ensure_ascii=False)
'''

STOP_CODE = r'''import json, sys
from java import jclass

ActivityThread = jclass("android.app.ActivityThread")
Context = jclass("android.content.Context")
AudioManager = jclass("android.media.AudioManager")
STREAM = @STREAM@

r = {"ok": True, "stream": STREAM, "stopped": False, "playing": False,
     "vol": None, "vol_max": None, "error": ""}
try:
    ctx = ActivityThread.currentApplication()
    am = ctx.getSystemService(Context.AUDIO_SERVICE)
    r["vol_max"] = int(am.getStreamMaxVolume(STREAM))
    r["vol"] = int(am.getStreamVolume(STREAM))
except Exception as exc:
    r["error"] = "volume: " + " ".join(str(exc).split())[:200]

holder = sys.modules.get("_mp_holder")
mp = getattr(holder, "mp", None) if holder is not None else None
if mp is not None:
    try:
        mp.stop()
        r["stopped"] = True
    except Exception as exc:
        r["error"] = (r["error"] + " | " if r["error"] else "") + "stop: " + " ".join(str(exc).split())[:200]
    try:
        mp.release()
    except Exception:
        pass
    holder.mp = None
    holder.path = ""
r = json.dumps(r, ensure_ascii=False)
'''


def _fill(template, values):
    """把 @TOKEN@ 占位替换成 Python 字面量（JSON 编码，安全注入字符串/数字）。"""
    code = template
    for token, value in values.items():
        code = code.replace("@%s@" % token, value)
    return code


def bootstrap_code(save_dir=TTS_SAVE_DIR):
    """首次连接目标时下发的 tts() 定义（含健康检查）。"""
    return TTS_RUNTIME.replace(
        "@SAVE_DIR@", json.dumps(str(save_dir), ensure_ascii=False)
    )


def build_play_code(path=AUDIO_PATH):
    return _fill(PLAY_CODE, {
        "PATH": json.dumps(str(path), ensure_ascii=False),
    })


def build_volume_code(action="set", level=15, stream=DEFAULT_STREAM):
    """查询 / 设置某个音频流的音量。

    - action="get" 只读取，返回 max_volume / after_volume；
    - action="set" 先钳位、写入，再回读 after_volume（真实生效值）。
    """
    return _fill(VOLUME_CODE, {
        "ACTION": json.dumps(str(action)),
        "LEVEL": str(int(level)),
        "STREAM": str(int(stream)),
    })


def build_speak_code(text, voice=TTS_VOICE, rate=TTS_RATE, pitch=TTS_PITCH,
                     fmt=TTS_FORMAT, save_dir=TTS_SAVE_DIR, stream=DEFAULT_STREAM,
                     proxy=""):
    """生成一条“目标端 tts + 立即播放”的执行代码（含顶层 await）。"""
    proxy_text = str(proxy or "").strip()
    return _fill(SPEAK_CODE, {
        "TEXT": json.dumps(str(text), ensure_ascii=False),
        "VOICE": json.dumps(str(voice), ensure_ascii=False),
        "FMT": json.dumps(str(fmt), ensure_ascii=False),
        "RATE": json.dumps(str(rate), ensure_ascii=False),
        "PITCH": json.dumps(str(pitch), ensure_ascii=False),
        "SAVE_DIR": json.dumps(str(save_dir), ensure_ascii=False),
        "STREAM": str(int(stream)),
        "PROXY": json.dumps(proxy_text, ensure_ascii=False) if proxy_text else "None",
    })


def build_status_code(stream=DEFAULT_STREAM):
    return _fill(STATUS_CODE, {"STREAM": str(int(stream))})


def build_stop_code(stream=DEFAULT_STREAM):
    return _fill(STOP_CODE, {"STREAM": str(int(stream))})


# ---------------------------------------------------------------------------
# 客户端侧：tts 定义按目标只下发一次（持久命名空间复用）
# ---------------------------------------------------------------------------

# request_topic -> True，表示该目标的 executor 命名空间里已有 tts()。
_TTS_READY = {}


def _target_key():
    try:
        cfg = client_service._device_config(None) or {}
        return str(cfg.get("request_topic") or cfg.get("id") or "")
    except BaseException:
        return ""


def _clamp_seconds(value, default):
    try:
        seconds = float(default if value is None else value)
    except (TypeError, ValueError):
        seconds = float(default)
    return min(max(seconds, 1.0), 600.0)


def tts_timeout():
    """TTS 合成超时（秒）：设置页 tts_timeout 优先，缺省 90s，夹到 [1,600]。"""
    return _clamp_seconds(
        client_service.feature_setting("audio", "tts_timeout", None), TTS_TIMEOUT
    )


def tts_proxy():
    """TTS 代理（可选，如 http://192.168.1.2:7890）：留空/非法值都按直连。

    k12 实测：设备所在网络访问 speech.platform.bing.com 会被 reset，
    只能走代理。edge_tts 只接受 http/https 代理，其它写法一律当没填，
    避免一个手滑字符串把 TTS 直接打死（边缘功能只允许降级）。
    """
    try:
        raw = client_service.feature_setting("audio", "tts_proxy", None)
    except BaseException:
        return ""
    text = str(raw or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return ""
    return text


def _ensure_tts(timeout=None, force=False):
    """确保目标命名空间里有 tts()；已下发过则直接返回缓存命中（零 RPC）。

    任何失败都返回结构化 dict（ok=False + error），由 speak() 转成 JSON 给 UI，
    绝不让边缘的 TTS 准备动作抛异常影响主流程。
    """
    key = _target_key()
    if not force and _TTS_READY.get(key):
        return {"ok": True, "cached": True, "key": key}
    # bootstrap 可能要现装 edge_tts 及其依赖（k12 实测缺 typing_extensions），
    # 下载耗时不受 tts_timeout 约束，这里用独立上限，避免被设置页调小后
    # 安装还没完就先超时；target 离线时最坏也只是等这么久后降级报错。
    seconds = _clamp_seconds(float(BOOTSTRAP_TIMEOUT), BOOTSTRAP_TIMEOUT)
    try:
        result = client_service.rpc(bootstrap_code(), timeout=seconds)
        data = client_service.parse_json_result(result)
    except BaseException as error:
        return {"ok": False, "key": key,
                "error": "%s: %s" % (type(error).__name__, error)}
    if not isinstance(data, dict):
        data = {"ok": False, "error": "non-dict bootstrap result"}
    data = dict(data)
    data["key"] = key
    if data.get("ok"):
        _TTS_READY[key] = True
    else:
        _TTS_READY.pop(key, None)
    return data


def _fail(error, stage="", **extra):
    payload = {"ok": False, "stage": stage, "error": str(error)}
    payload.update(extra)
    return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# 远端动作入口（保持 compose 风格：返回 JSON 字符串）
# ---------------------------------------------------------------------------

def play(path=AUDIO_PATH):
    # 起播 + 状态采集，默认 20s 上限（设置页配了 timeout 则以设置为准）。
    result = client_service.rpc(
        build_play_code(path), timeout=client_service.feature_timeout("audio", PLAY_TIMEOUT)
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def speak(text, voice=TTS_VOICE, rate=TTS_RATE, pitch=TTS_PITCH,
          fmt=TTS_FORMAT, save_dir=TTS_SAVE_DIR, stream=DEFAULT_STREAM,
          proxy=None):
    """目标端 edge_tts 合成到 save_dir，然后立即播放（长音频不阻塞 UI）。"""
    body = str(text or "").strip()
    if not body:
        return _fail("empty text", stage="input")
    if len(body) > TTS_TEXT_LIMIT:
        return _fail(
            "text too long: %d chars > %d (split it into shorter clips)"
            % (len(body), TTS_TEXT_LIMIT),
            stage="input", text_chars=len(body),
        )
    if not str(save_dir or "").strip():
        return _fail("save_dir is empty", stage="input")
    # 代理跟着设置走；显式传参优先（None 表示读设置，空串表示本次强制直连）。
    proxy_text = tts_proxy() if proxy is None else str(proxy or "").strip()

    boot = _ensure_tts()
    if not boot.get("ok"):
        # 失败原因按「RPC/配置层 → 自动装依赖 → edge_tts import → 落盘目录」顺序取，
        # 保证 UI 能看到到底是哪一环坏了（k12 现场就是自动装依赖失败）。
        install = boot.get("edge_tts_install")
        if not isinstance(install, dict):
            install = {}
        reason = boot.get("error") or ""
        if not reason and install.get("error"):
            reason = "install edge_tts failed: " + str(install.get("error"))[:300]
        if not reason:
            reason = (boot.get("edge_tts_error") or boot.get("save_dir_error")
                      or "tts bootstrap failed")
        return json.dumps({
            "ok": False,
            "stage": "bootstrap",
            "text_chars": len(body),
            "error": reason,
            "proxy": proxy_text,
            "edge_tts": boot.get("edge_tts", ""),
            "edge_tts_error": boot.get("edge_tts_error", ""),
            "edge_tts_install": install,
            "save_dir": boot.get("save_dir", save_dir),
            "save_dir_error": boot.get("save_dir_error", ""),
        }, ensure_ascii=False)

    result = client_service.rpc(
        build_speak_code(body, voice, rate, pitch, fmt, save_dir, stream,
                          proxy=proxy_text),
        timeout=tts_timeout(),
    )
    data = client_service.parse_json_result(result)
    if isinstance(data, dict) and data.get("rebootstrap"):
        # 目标 executor 可能重启/命名空间被清空：补发一次定义后重试一次。
        _TTS_READY.pop(boot.get("key") or _target_key(), None)
        retry_boot = _ensure_tts(force=True)
        if retry_boot.get("ok"):
            result = client_service.rpc(
                build_speak_code(body, voice, rate, pitch, fmt, save_dir, stream,
                                  proxy=proxy_text),
                timeout=tts_timeout(),
            )
            data = client_service.parse_json_result(result)
    if isinstance(data, dict):
        data.setdefault("voice", voice)
        data.setdefault("proxy", proxy_text)
    return json.dumps(data, ensure_ascii=False)


def status(stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_status_code(stream),
        timeout=client_service.feature_timeout("audio", VOLUME_TIMEOUT),
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def stop(stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_stop_code(stream),
        timeout=client_service.feature_timeout("audio", VOLUME_TIMEOUT),
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def get_volume(stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_volume_code("get", 0, stream),
        timeout=client_service.feature_timeout("audio", VOLUME_TIMEOUT),
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def set_volume(level, stream=DEFAULT_STREAM):
    result = client_service.rpc(
        build_volume_code("set", int(level), stream),
        timeout=client_service.feature_timeout("audio", VOLUME_TIMEOUT),
    )
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Chaquopy 自绘 UI
# ---------------------------------------------------------------------------

def build_view(context):
    """TTS 文本输入 + Speak/Stop/Play；音量滑块与播放进度常驻可见。"""
    import time

    import bootstrap
    import pyui_kit
    from java import dynamic_proxy, jclass
    from android.view import View
    from android.widget import LinearLayout, SeekBar

    page = pyui_kit.Page(
        context, "Audio",
        "TTS (edge_tts) runs on target; audio cached in %s and played by MediaPlayer."
        % TTS_SAVE_DIR,
    )

    # 全局 UI 状态（跨目标切换共享 stream 选择；level/max 是当前流的值）
    ui_state = {
        "key": None,
        "topic": "",
        "stream": DEFAULT_STREAM,
        "level": 0,
        "max": DEFAULT_VOLUME_MAX,
        "playing": False,
        "pos_ms": 0,
        "dur_ms": 0,
        "path": "",
        "note": "Ready",
        "poll": None,
        "poll_busy": False,
        "idle_ticks": 0,
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

    # ---- TTS 文本输入（多行；长音频就靠这段文字生成） ----
    page.add(
        pyui_kit.make_text(
            context,
            "Text to speak（到 %s 生成 mp3 后立即播放）：" % TTS_SAVE_DIR,
        ),
        top=10,
    )
    try:
        text_input = pyui_kit.make_text_edit(context, DEFAULT_TEXT, lines=3)
    except Exception:
        text_input = pyui_kit.make_edit(context, DEFAULT_TEXT)
    page.add(text_input, top=4)

    # ---- 底部：Speak / Stop / Play（Play = 重播上一条 TTS 文件） ----
    speak_btn = pyui_kit.make_button(context, "Speak", lambda: None)
    stop_btn = pyui_kit.make_button(context, "Stop", lambda: None)
    play_btn = pyui_kit.make_button(context, "Play", lambda: None)
    page.bottom_add(speak_btn, weight=1.0)
    page.bottom_add(stop_btn, weight=1.0)
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

    def fmt_seconds(ms):
        try:
            return "%.1fs" % (float(ms) / 1000.0)
        except (TypeError, ValueError):
            return "-"

    # ------------------------------------------------------------------
    # 显示刷新（所有 View 写操作都在主线程）
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

    def render_status():
        name = STREAM_NAMES.get(ui_state["stream"], "Stream %d" % ui_state["stream"])
        lines = [
            "target: %s" % (ui_state["topic"] or "?"),
            "volume: %s %d / %d%s" % (
                name, ui_state["level"], ui_state["max"],
                "   (playing)" if ui_state["playing"] else "",
            ),
        ]
        if ui_state["path"]:
            lines.append("file: %s" % ui_state["path"])
        if ui_state["playing"] or ui_state["dur_ms"]:
            lines.append(
                "position: %s / %s" % (
                    fmt_seconds(ui_state["pos_ms"]), fmt_seconds(ui_state["dur_ms"])
                )
            )
        lines.append(
            "voice: %s   rate %s   pitch %s   fmt %s"
            % (TTS_VOICE, TTS_RATE, TTS_PITCH, TTS_FORMAT)
        )
        lines.append("cache: %s" % TTS_SAVE_DIR)
        if ui_state["note"]:
            lines.append(str(ui_state["note"]))
        status.setText("\n".join(lines))

    def set_note(text):
        ui_state["note"] = str(text)[:800]
        render_status()

    def apply_device_state(data):
        """把目标回传的播放/音量字段合并进 UI 状态并刷新显示。"""
        try:
            ui_state["playing"] = bool(data.get("playing"))
        except Exception:
            pass
        try:
            ui_state["pos_ms"] = int(data.get("position_ms") or 0)
            if data.get("duration_ms") is not None:
                ui_state["dur_ms"] = int(data.get("duration_ms") or 0)
        except (TypeError, ValueError):
            pass
        path = str(data.get("path") or "")
        if path:
            ui_state["path"] = path
        vol = data.get("vol", data.get("vol_after"))
        max_v = data.get("vol_max")
        if vol is not None and max_v is not None:
            try:
                sync_seekbar(int(vol), int(max_v))
            except (TypeError, ValueError):
                pass
        render_status()

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
            try:
                sync_seekbar(
                    int(data.get("after_volume", 0)),
                    int(data.get("max_volume", DEFAULT_VOLUME_MAX)),
                )
            except (TypeError, ValueError):
                pass
            render_status()

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
                set_note("Set volume failed: %s" % data.get("error", "?"))
                return
            # 以设备端回读的 after_volume 为准（可能被钳制）
            try:
                sync_seekbar(
                    int(data.get("after_volume", level)),
                    int(data.get("max_volume", ui_state["max"])),
                )
            except (TypeError, ValueError):
                pass
            render_status()

        def apply_error(error):
            set_note("Set volume failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error)

    # ------------------------------------------------------------------
    # 播放进度轮询（长音频：每 2s 拉一次 status，停止/播完自动收手）
    # ------------------------------------------------------------------
    def stop_poll():
        stopper = ui_state.get("poll")
        ui_state["poll"] = None
        ui_state["poll_busy"] = False
        if stopper:
            try:
                stopper()
            except Exception:
                pass

    def poll_tick():
        if not ui_state["key"] or ui_state["poll_busy"]:
            return
        ui_state["poll_busy"] = True
        key = ui_state["key"]
        stream = ui_state["stream"]

        def work():
            return bootstrap.call_feature("audio", "status", stream)

        def apply_ok(raw):
            ui_state["poll_busy"] = False
            if ui_state["key"] != key:
                return
            data = parse_result(raw)
            if not data.get("ok"):
                stop_poll()
                return
            apply_device_state(data)
            if data.get("playing"):
                ui_state["idle_ticks"] = 0
            else:
                ui_state["idle_ticks"] += 1
                # 连续两次“未在播放”或根本没有 player 才停轮询，避免起播瞬间误停
                if ui_state["idle_ticks"] >= 2 or not data.get("player"):
                    stop_poll()

        def apply_error(error):
            ui_state["poll_busy"] = False
            stop_poll()

        pyui_kit.run_async(work, apply_ok, apply_error)

    def start_poll():
        stop_poll()
        ui_state["idle_ticks"] = 0
        ui_state["poll"] = pyui_kit.repeat_every(2, poll_tick)

    # ------------------------------------------------------------------
    # 切换流类型
    # ------------------------------------------------------------------
    def select_stream(stream):
        if ui_state["stream"] == stream:
            return
        ui_state["stream"] = stream
        update_stream_buttons()
        refresh_volume_async()
        render_status()

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
        set_note("SeekBar listener failed: %r" % (exc,))

    def current_text():
        try:
            return text_input.getText().toString()
        except Exception:
            return ""

    # ------------------------------------------------------------------
    # Speak：目标端 tts 生成（缓存到 /sdcard/apm）→ MediaPlayer 播放
    # ------------------------------------------------------------------
    def on_speak():
        text_value = current_text()
        if not text_value.strip():
            set_note("请输入要朗读的文字")
            return
        stop_poll()
        set_note("TTS generating on target ...")
        page.set_subtitle("TTS: requesting ...")
        started = time.time()

        def work():
            def beat():
                def show():
                    page.set_subtitle(
                        "TTS/playback: %.0f s elapsed" % (time.time() - started)
                    )
                pyui_kit.on_main(show)

            stop_beat = pyui_kit.repeat_every(2, beat)
            try:
                return bootstrap.call_feature(
                    "audio", "speak", text_value, TTS_VOICE, TTS_RATE,
                    TTS_PITCH, TTS_FORMAT, TTS_SAVE_DIR, ui_state["stream"],
                )
            finally:
                stop_beat()

        def apply_ok(raw):
            page.set_subtitle("Ready")
            data = parse_result(raw)
            if not data.get("ok"):
                details = data.get("errors") or []
                reason = data.get("error") or "; ".join(
                    str(item) for item in details
                ) or "unknown"
                set_note("Speak failed @%s: %s" % (data.get("stage") or "?", reason))
                return
            apply_device_state(data)
            set_note(
                "TTS %sms → %d bytes, duration %s, play %s" % (
                    data.get("tts_ms") or 0,
                    data.get("file_size") or 0,
                    fmt_seconds(data.get("duration_ms")),
                    fmt_seconds(data.get("elapsed_ms")),
                )
            )
            if data.get("playing"):
                start_poll()

        def apply_error(error):
            page.set_subtitle("Ready")
            set_note("Speak failed: %s: %s" % (type(error).__name__, error))

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(speak_btn, play_btn))

    # ------------------------------------------------------------------
    # Stop
    # ------------------------------------------------------------------
    def on_stop():
        stop_poll()
        key = ui_state["key"]
        stream = ui_state["stream"]

        def work():
            return bootstrap.call_feature("audio", "stop", stream)

        def apply_ok(raw):
            data = parse_result(raw)
            if not data.get("ok"):
                set_note("Stop failed: %s" % (data.get("error") or "?"))
                return
            ui_state["playing"] = False
            ui_state["pos_ms"] = 0
            apply_device_state(data)
            set_note("stopped" if data.get("stopped") else "no active player")

        def apply_error(error):
            set_note("Stop failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(stop_btn,))

    # ------------------------------------------------------------------
    # Play：优先重播上一条 TTS 文件，没有则播内置 AUDIO_PATH
    # ------------------------------------------------------------------
    def on_play():
        path = ui_state["path"] or AUDIO_PATH
        stop_poll()
        set_note("Playing %s ..." % path)

        def work():
            return bootstrap.call_feature("audio", "play", path)

        def apply_ok(raw):
            data = parse_result(raw)
            if not data.get("ok"):
                details = data.get("errors") or []
                set_note("Play failed: %s" % (data.get("error") or "; ".join(
                    str(item) for item in details
                ) or "unknown"))
                return
            apply_device_state(data)
            set_note("playing %s" % path)
            if data.get("playing"):
                start_poll()

        def apply_error(error):
            set_note("Play failed: %s" % error)

        pyui_kit.run_async(work, apply_ok, apply_error, buttons=(speak_btn, play_btn))

    pyui_kit.click(speak_btn, on_speak)
    pyui_kit.click(stop_btn, on_stop)
    pyui_kit.click(play_btn, on_play)
    for s, btn in stream_buttons:
        pyui_kit.click(btn, lambda ss=s: select_stream(ss))

    update_stream_buttons()
    update_volume_label()
    render_status()

    # ------------------------------------------------------------------
    # 目标切换：预热 tts 定义（只发一次）+ 拉取音量；失败只降级
    # ------------------------------------------------------------------
    def on_target(key, topic, _cfg):
        stop_poll()
        ui_state["key"] = key
        ui_state["topic"] = topic
        ui_state["path"] = ""
        ui_state["playing"] = False
        ui_state["pos_ms"] = 0
        ui_state["dur_ms"] = 0
        page.set_target("target topic: " + str(topic))
        render_status()
        refresh_volume_async()

        def warm():
            return _ensure_tts()

        def warm_ok(boot):
            if not isinstance(boot, dict) or ui_state["key"] != key:
                return
            if boot.get("cached"):
                return
            install = boot.get("edge_tts_install")
            if not isinstance(install, dict):
                install = {}
            if boot.get("ok"):
                note = "tts ready (edge_tts %s, cache %s)" % (
                    boot.get("edge_tts") or "?", boot.get("save_dir") or TTS_SAVE_DIR
                )
                if install.get("attempted"):
                    note += " — deps just installed"
                set_note(note)
            else:
                set_note(
                    "tts unavailable: %s"
                    % (boot.get("error") or install.get("error")
                       or boot.get("edge_tts_error")
                       or boot.get("save_dir_error") or "unknown")
                )

        def warm_error(error):
            if ui_state["key"] == key:
                set_note("tts prewarm failed: %s" % error)

        pyui_kit.run_async(warm, warm_ok, warm_error)

    pyui_kit.watch_target(page.root, on_target)
    return page.root
