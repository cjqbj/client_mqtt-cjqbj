"""极简 Windows 远程桌面控制页:纯 HTTP RPC 通道,无 WebSocket。

启动 RPC 服务(默认端口 1144):
    python server_http.py

浏览器访问(URL 即代码,等价于执行 ``import rdp;rdp.html(p)``):
    http://127.0.0.1:1144/import%20rdp;rdp.html(p)
改完本文件想立即生效(不重启服务)可加一段 reload:
    http://127.0.0.1:1144/from%20client%20import%20rdp;import%20importlib;rdp=importlib.reload(rdp);rdp.html(p)
页面设置区(黑白模式那一行)末尾的「重载 rdp」按钮做的是同一件事:先在
服务端 importlib.reload 本模块(即重新读磁盘上的最新代码),再刷新页面取到
新模板,不用手拼这条 URL。

RPC 入口(仅 html/screen 需要 server_http 注入的请求级 response 对象 p):
    html(p)                           内嵌控制页(默认彩色、不自动刷新)
    html(p, bw=True)                  打开即黑白模式;页面内亦可随时勾选切换
    screen(p)                         主屏截图 JPEG(含当前光标位置红圈)
    screen(p, 1)                      黑白(灰度)截图,显著省流量
    info()                            返回 "宽x高"
    move(x, y)                        移动光标
    click(x, y, 'left')               移动并点击;left / right
    drag(x1, y1, x2, y2)              按住拖动(选择模式松手时调用)
    scroll(x, y, n)                   移动到选定坐标滚动滚轮,n 正=向上、负=向下
    input('text')                     文本逐字符敲入远端前台窗口(Unicode 键,不动剪贴板)
    clipboard()                       读取远端剪贴板文本
    setclip('text')                   把文本写入远端剪贴板
    key('ctrl+shift+esc')             按键 / 组合键,加号连接

页面设置区的「服务端日志」开关(默认开)关闭后,页面所有请求都带
``X-RDP-Log: 0``;本模块在 import 时把这条规则作为**过滤器**注入 server_http
(``add_log_filter('rdp-slog', ...)``),server_http 据此跳过这些请求的
``[RPC]`` 访问行——每点一下、每张自动刷新截图都是一条 HTTP 请求,否则日志台
会被刷满。静音只作用于本页发出的请求,其它客户端的日志照常输出,4xx/5xx 也
仍保留便于排查。server_http 不需要认识 rdp;注册失败(独立使用本模块、或
server_http 不在 sys.path)只跳过,不影响页面和主流程。

除 html/screen 外都只用返回值回传结果,不需要 p;页面里的 RPC 表达式
一律按本模块真实导入身份生成(见 _mod_expr),所以 ``import rdp``、
``from client import rdp``、``importlib.reload(client.rdp)`` 等各种导入
形式下,页面都能正常调用。

仅适配 Windows:user32 的 keybd_event / mouse_event / SetCursorPos,
截图用 Pillow 的 ImageGrab(主屏,坐标系与 SetCursorPos 一致)。
"""
import ctypes
import io
import json
import time
from ctypes import wintypes

try:
    from PIL import Image, ImageDraw, ImageGrab
except ImportError:          # 允许模块导入成功,调用截图时再给出明确报错
    Image = ImageDraw = ImageGrab = None

user32 = ctypes.windll.user32

jpeg_quality = 65            # 截图 JPEG 质量
click_delay = 0.02           # 按下/抬起之间的间隔(秒)

# ------------------------------------------------------------------ #
# 虚拟键码(VK);标点用 VK_OEM_* 固定码,避免依赖键盘布局
# ------------------------------------------------------------------ #
VK = {
    'ctrl': 0x11, 'control': 0x11, 'alt': 0x12, 'shift': 0x10, 'win': 0x5B,
    'menu': 0x5D, 'apps': 0x5D,
    'enter': 0x0D, 'return': 0x0D, 'esc': 0x1B, 'escape': 0x1B,
    'backspace': 0x08, 'tab': 0x09, 'space': 0x20, 'capslock': 0x14,
    'insert': 0x2D, 'delete': 0x2E, 'del': 0x2E,
    'home': 0x24, 'end': 0x23, 'pageup': 0x21, 'pagedown': 0x22,
    'printscreen': 0x2C, 'prtsc': 0x2C, 'scrolllock': 0x91, 'pause': 0x13,
    'numlock': 0x90,
    'left': 0x25, 'up': 0x26, 'right': 0x27, 'down': 0x28,
    'backtick': 0xC0, 'minus': 0xBD, 'equal': 0xBB,
    'lbracket': 0xDB, 'rbracket': 0xDD, 'backslash': 0xDC,
    'semicolon': 0xBA, 'quote': 0xDE,
    'comma': 0xBC, 'period': 0xBE, 'slash': 0xBF,
}
MODIFIERS = {'ctrl', 'alt', 'shift', 'win'}
# 这些 VK 必须带 KEYEVENTF_EXTENDEDKEY,否则会与小键盘/主键位混淆
EXTENDED_VK = {0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
               0x2C, 0x2D, 0x2E, 0x5B, 0x5C, 0x5D}


def screen_size():
    return user32.GetSystemMetrics(0), user32.GetSystemMetrics(1)


def _cursor_pos():
    point = wintypes.POINT()
    if user32.GetCursorPos(ctypes.byref(point)):
        return point.x, point.y
    return 0, 0


def screenshot(quality=None, bw=False):
    if ImageGrab is None:
        raise RuntimeError('截图需要 Pillow: pip install pillow')
    image = ImageGrab.grab()                     # 主屏,坐标系同 SetCursorPos
    if bw:
        image = image.convert('L')               # 灰度:体积约为彩色的一半以下
    cursor = wintypes.POINT()
    if user32.GetCursorPos(ctypes.byref(cursor)):
        ring = ImageDraw.Draw(image)
        x, y, r = cursor.x, cursor.y, 7
        ring.ellipse([x - r - 2, y - r - 2, x + r + 2, y + r + 2],
                     outline=0 if bw else (0, 0, 0), width=2)
        ring.ellipse([x - r, y - r, x + r, y + r],
                     outline=255 if bw else (255, 60, 60), width=3)
    buffer = io.BytesIO()
    image.save(buffer, 'JPEG', quality=jpeg_quality if quality is None else quality)
    return buffer.getvalue()


def move_to(x, y):
    user32.SetCursorPos(int(x), int(y))
    return f'move to ({int(x)},{int(y)})'


def click_at(x, y, button='left'):
    width, height = screen_size()
    x = max(0, min(int(x), width - 1))
    y = max(0, min(int(y), height - 1))
    user32.SetCursorPos(x, y)
    time.sleep(click_delay)
    if str(button).lower() == 'right':
        down, up = 0x0008, 0x0010                # MOUSEEVENTF_RIGHTDOWN/UP
    else:
        down, up = 0x0002, 0x0004                # MOUSEEVENTF_LEFTDOWN/UP
    user32.mouse_event(down, 0, 0, 0, 0)
    time.sleep(click_delay)
    user32.mouse_event(up, 0, 0, 0, 0)
    return f'click {str(button).lower()} at ({x},{y})'


def drag_at(x1, y1, x2, y2, button='left', steps=12):
    """从 (x1,y1) 按住拖动到 (x2,y2) 再松开,分段移动贴近真实拖动。"""
    width, height = screen_size()
    x1 = max(0, min(int(x1), width - 1)); y1 = max(0, min(int(y1), height - 1))
    x2 = max(0, min(int(x2), width - 1)); y2 = max(0, min(int(y2), height - 1))
    down, up = (0x0008, 0x0010) if str(button).lower() == 'right' else (0x0002, 0x0004)
    user32.SetCursorPos(x1, y1)
    time.sleep(click_delay)
    user32.mouse_event(down, 0, 0, 0, 0)
    time.sleep(click_delay)
    for index in range(1, steps + 1):
        user32.SetCursorPos(x1 + (x2 - x1) * index // steps,
                            y1 + (y2 - y1) * index // steps)
        time.sleep(0.01)
    time.sleep(click_delay)
    user32.mouse_event(up, 0, 0, 0, 0)
    return f'drag {str(button).lower()} ({x1},{y1})->({x2},{y2})'


def scroll_at(x, y, clicks=1):
    """移动到 (x,y) 后滚动鼠标滚轮:clicks 正数向上滚、负数向下滚,一格为 120。"""
    width, height = screen_size()
    x = max(0, min(int(x), width - 1))
    y = max(0, min(int(y), height - 1))
    user32.SetCursorPos(x, y)
    time.sleep(click_delay)
    clicks = max(-20, min(20, int(clicks)))     # 防误传巨值导致长时间连续滚动
    for _ in range(abs(clicks)):                # MOUSEEVENTF_WHEEL 逐格发送,贴近真实滚轮
        user32.mouse_event(0x0800, 0, 0, 120 if clicks > 0 else -120, 0)
        time.sleep(0.01)
    return f'scroll {clicks:+d} at ({x},{y})'


class _KEYBDINPUT(ctypes.Structure):
    _fields_ = [('wVk', wintypes.WORD), ('wScan', wintypes.WORD),
                ('dwFlags', wintypes.DWORD), ('time', wintypes.DWORD),
                ('dwExtraInfo', ctypes.c_size_t)]


class _MOUSEINPUT(ctypes.Structure):
    _fields_ = [('dx', wintypes.LONG), ('dy', wintypes.LONG),
                ('mouseData', wintypes.DWORD), ('dwFlags', wintypes.DWORD),
                ('time', wintypes.DWORD), ('dwExtraInfo', ctypes.c_size_t)]


class _HARDWAREINPUT(ctypes.Structure):
    _fields_ = [('uMsg', wintypes.DWORD),
                ('wParamL', wintypes.WORD), ('wParamH', wintypes.WORD)]


class _INPUTunion(ctypes.Union):
    _fields_ = [('mi', _MOUSEINPUT), ('ki', _KEYBDINPUT), ('hi', _HARDWAREINPUT)]


class _INPUT(ctypes.Structure):
    _fields_ = [('type', wintypes.DWORD), ('union', _INPUTunion)]


user32.SendInput.argtypes = [wintypes.UINT, ctypes.POINTER(_INPUT), ctypes.c_int]


def type_text(text):
    """把字符串逐字符敲到远端前台窗口:SendInput Unicode 键入,支持任意语言、不占剪贴板。
    keybd_event 的 bScan 是 BYTE,Unicode 码位会被截成低字节,中文全乱,故必须走 SendInput。"""
    text = str(text)
    sent = 0
    for ch in text:
        if ch == '\n':                          # 换行走真实回车键,多数编辑器不认 U+000A
            _key_event(VK['enter'])
            time.sleep(click_delay)
            _key_event(VK['enter'], up=True)
            continue
        pair = (_INPUT * 2)()
        for item in pair:
            item.type = 1                       # INPUT_KEYBOARD
            item.union.ki.wScan = ord(ch)
            item.union.ki.dwFlags = 0x0004      # KEYEVENTF_UNICODE
        pair[1].union.ki.dwFlags |= 0x0002      # 第二个事件为抬起(+KEYEVENTF_KEYUP)
        sent += user32.SendInput(2, pair, ctypes.sizeof(_INPUT))
        time.sleep(0.005)
    return f'input {len(text)} chars, {sent} events'


# 剪贴板句柄是 64 位值,必须显式声明 argtypes/restype,否则 ctypes 按 c_int 截断;
# GlobalLock/GlobalUnlock 在 kernel32,OpenClipboard 等在 user32
kernel32 = ctypes.windll.kernel32
user32.GetClipboardData.argtypes = [wintypes.UINT]
user32.GetClipboardData.restype = ctypes.c_void_p
kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
kernel32.GlobalLock.restype = ctypes.c_void_p
kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
kernel32.GlobalAlloc.argtypes = [wintypes.UINT, ctypes.c_size_t]
kernel32.GlobalAlloc.restype = ctypes.c_void_p
kernel32.GlobalFree.argtypes = [ctypes.c_void_p]
kernel32.GlobalFree.restype = ctypes.c_void_p
user32.EmptyClipboard.restype = wintypes.BOOL
user32.SetClipboardData.argtypes = [wintypes.UINT, ctypes.c_void_p]
user32.SetClipboardData.restype = ctypes.c_void_p


def get_clipboard_text():
    """读取远端剪贴板文本;空剪贴板或非文本内容返回空串。"""
    if not user32.OpenClipboard(None):
        return ''
    try:
        handle = user32.GetClipboardData(13)    # CF_UNICODETEXT
        if not handle:
            return ''
        ptr = kernel32.GlobalLock(handle)
        if not ptr:
            return ''
        try:
            return ctypes.wstring_at(ptr)
        finally:
            kernel32.GlobalUnlock(handle)
    finally:
        user32.CloseClipboard()


def set_clipboard_text(text):
    """把文本写入远端剪贴板 CF_UNICODETEXT;SetClipboardData 成功后内存归系统接管。"""
    data = str(text)
    size = (len(data) + 1) * ctypes.sizeof(ctypes.c_wchar)
    handle = kernel32.GlobalAlloc(0x0002, size)          # GMEM_MOVEABLE
    if not handle:
        return 'setclip alloc failed'
    ptr = kernel32.GlobalLock(handle)
    if not ptr:
        kernel32.GlobalFree(handle)
        return 'setclip lock failed'
    try:
        ctypes.memmove(ptr, ctypes.create_unicode_buffer(data, len(data) + 1), size)
    finally:
        kernel32.GlobalUnlock(handle)
    if not user32.OpenClipboard(None):
        kernel32.GlobalFree(handle)
        return 'setclip open failed'
    try:
        user32.EmptyClipboard()
        ok = user32.SetClipboardData(13, handle)          # CF_UNICODETEXT
        if not ok:
            kernel32.GlobalFree(handle)
            return 'setclip failed'
        return f'set clipboard {len(data)} chars'
    finally:
        user32.CloseClipboard()


def _vk_of(name):
    text = str(name).strip().lower()
    if text in VK:
        return VK[text]
    if len(text) == 1:
        if text.isdigit():
            return 0x30 + int(text)
        if 'a' <= text <= 'z':
            return 0x41 + ord(text) - 97
        code = user32.VkKeyW(ord(text)) & 0xFF   # 布局相关字符回退
        if code:
            return code
    if len(text) in (2, 3) and text[0] == 'f' and text[1:].isdigit():
        index = int(text[1:])
        if 1 <= index <= 24:
            return 0x6F + index
    raise ValueError(f'unknown key: {name!r}')


def _key_event(vk, up=False):
    flags = 0x0002 if up else 0                 # KEYEVENTF_KEYUP
    if vk in EXTENDED_VK:
        flags |= 0x0001                         # KEYEVENTF_EXTENDEDKEY
    user32.keybd_event(vk, 0, flags, 0)


def press(combo):
    """按下组合键:'ctrl+a'、'ctrl+alt+del'、'a'、'shift' 均可。"""
    parts = [s.strip().lower() for s in str(combo).split('+') if s.strip()]
    if not parts:
        raise ValueError('empty key combo')
    vks = [_vk_of(part) for part in parts]
    for vk in vks[:-1]:                         # 依次按下修饰键
        _key_event(vk)
    time.sleep(click_delay)
    _key_event(vks[-1])
    time.sleep(click_delay)
    _key_event(vks[-1], up=True)
    for vk in reversed(vks[:-1]):               # 反序抬起
        _key_event(vk, up=True)
    return 'key ' + '+'.join(parts)


# ------------------------------------------------------------------ #
# HTTP RPC 入口:p / response 由 server_http.handle_rpc 注入
# ------------------------------------------------------------------ #
def _mod_expr():
    """按本模块真实导入身份,生成页面里调用自己的表达式。

    进程里模块可能是顶层 ``rdp``(__name__ == 'rdp'),也可能是
    ``client.rdp``;后者用 ``import client.rdp`` 导入时命名空间只绑定
    ``client``,用 ``from client import rdp`` 导入时只绑定 ``rdp``,
    写死任何一种前缀都会在另一种情况下 NameError。__import__ 带
    fromlist 直接返回子模块,不依赖任何外部绑定,一次请求内开销也
    只是 sys.modules 查表。
    """
    if '.' in __name__:
        return "__import__('%s',fromlist=['*'])" % __name__
    return __name__


def html(p, bw=False):
    """控制页;bw=True 时页面默认进入黑白模式(截图走灰度,省流量)。"""
    p.set_header('Content-Type', 'text/html; charset=utf-8')
    p.set_header('Cache-Control', 'no-store')
    page = PAGE.replace('__BW__', 'true' if bw else 'false')
    p.set_data(page.replace('__MOD__', json.dumps(_mod_expr())))


def screen(p, bw=False):
    p.set_header('Content-Type', 'image/jpeg')
    p.set_header('Cache-Control', 'no-store')
    # 光标坐标随截图返回:页面据此把红点吸附到远端真实鼠标位置(与截图内空心圈重合)
    p.set_header('X-Cursor', '%d,%d' % _cursor_pos())
    p.set_data(screenshot(bw=bool(bw)))


def info():
    width, height = screen_size()
    return f'{width}x{height}'


def move(x, y):
    return move_to(x, y)


def click(x, y, button='left'):
    return click_at(x, y, button)


def drag(x1, y1, x2, y2, button='left'):
    return drag_at(x1, y1, x2, y2, button)


def scroll(x, y, clicks=1):
    return scroll_at(x, y, clicks)


def input(text):
    return type_text(text)


def clipboard():
    return get_clipboard_text()


def setclip(text):
    return set_clipboard_text(text)


def key(combo):
    return press(combo)


PAGE = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>RDP over HTTP RPC</title>
<style>
:root { color-scheme: dark; --bg: #0e1214; --panel: #151b1f; --line: #2c373d; --text: #e6ecec; --muted: #8fa0a6; --accent: #58c4b2; --err: #ee8f82; }
* { box-sizing: border-box; }
body { margin: 0; overflow-x: hidden; background: var(--bg); color: var(--text); font: 13px/1.5 ui-monospace, SFMono-Regular, Consolas, monospace; }
#stage { position: relative; background: #000; }
#shot { display: block; width: 100%; height: auto; cursor: crosshair; user-select: none; -webkit-user-drag: none; }
/* 待点击坐标标记:大小由 JS 按图片显示比例锁定(与截图内红色空心圈一致,基准14px)、半透明 */
#dot { position: absolute; width: 14px; height: 14px; border-radius: 50%; background: rgba(229,57,53,.45); border: 1px solid rgba(255,255,255,.85); box-shadow: 0 0 6px rgba(0,0,0,.5); transform: translate(-50%,-50%); display: none; pointer-events: none; z-index: 2; }
/* 拖动选择预览:半透明红色长条 */
#dragrect { position: absolute; background: rgba(229,57,53,.25); border: 1px solid rgba(229,57,53,.8); display: none; pointer-events: none; z-index: 3; }
/* 控制条+键盘+日志整体容器:页面双指放大时反向缩放,保持原始视觉大小 */
#panel { transform-origin: 0 0; }
/* 控制条+键盘+日志整体容器:缩放时反向适配可视宽度,transform 原点钉左上 */
#panel { transform-origin: 0 0; }
/* 面板内纵向 flex 排序:聚焦输入框时输入区上移到画面正下方,不隐藏任何区块 */
#panel-inner { display: flex; flex-direction: column; }
/* 控制行:绝不允许换行;left/right_click 悬浮在本行左右两端,跟随可视区域滑动,永不遮挡截图;
   touch-action:none 屏蔽移动端在本行上触发浏览器级页面拖动/缩放 */
#bar { position: relative; order: 10; min-height: 60px; display: flex; flex-wrap: nowrap; gap: 8px; align-items: center; padding: 8px 10px; background: var(--panel); border-top: 1px solid var(--line); border-bottom: 1px solid var(--line); white-space: nowrap; overflow: hidden; touch-action: none; }
#bar .slidy { position: absolute; top: 50%; transform: translateY(-50%); z-index: 5; min-height: 36px; font-weight: 700; touch-action: none; }
#bar .slidy.dragging, #bar .slidyc.dragging { background: var(--accent); color: #08201c; }
/* 刷新:浮动正方形,钉在 left/right 之间的中间 */
#bar .slidyc { position: absolute; top: 50%; transform: translate(-50%, -50%); z-index: 5; width: 48px; height: 48px; padding: 0; font-size: 13px; line-height: 1; touch-action: none; }
/* 滚轮方向箭头:大小随红点圆圈(字号在 JS 里按圆圈宽设置),半透明不遮挡画面 */
#wheeldir { position: absolute; display: none; pointer-events: none; z-index: 4; line-height: 1; color: var(--accent); opacity: .55; text-shadow: 0 0 2px rgba(0,0,0,.9); transform: translate(-50%, -50%); }
#bar > button:not(.slidy):not(.slidyc) { flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
.bspace { flex: 1 1 0; min-width: 96px; }
.cspace { flex: 1 1 36px; min-width: 36px; }
#coord { color: var(--accent); flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; text-align: center; }
button { background: #222b31; color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 6px 12px; font: 12px ui-monospace, Consolas, monospace; cursor: pointer; touch-action: manipulation; user-select: none; -webkit-touch-callout: none; }
button:hover { background: #2c3740; }
button:active { background: var(--accent); color: #08201c; }
#info { color: var(--muted); flex: 0 1 auto; min-width: 0; overflow: hidden; text-overflow: ellipsis; }
label { color: var(--muted); display: flex; align-items: center; gap: 4px; cursor: pointer; }
#keyboard { order: 20; padding: 8px 6px 8px; background: var(--panel); }
/* 开关行:输入区与日志之间 */
#switches { order: 40; display: flex; flex-wrap: wrap; gap: 6px 18px; padding: 4px 12px 8px; background: var(--panel); border-bottom: 1px solid var(--line); }
/* 开关行里的按钮(重载 rdp):与复选框同高同字号,不把整行撑高 */
#switches .swbtn { flex: 0 0 auto; padding: 4px 10px; font-size: 11px; }
/* 默认布局按钮:输入区上移(非默认布局)时高亮提示可点回 */
body.textfocus #btn-layout { background: var(--accent); color: #08201c; border-color: var(--accent); font-weight: 700; }
/* 输入区:默认排键盘下方;聚焦后上移画面正下方并保持,点默认布局按钮才复位 */
#inputzone { order: 30; background: var(--panel); }
body.textfocus #inputzone { order: 1; }
#inputrow { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; padding: 6px 10px 4px; }
#textin { flex: 1 1 140px; min-width: 120px; min-height: 36px; max-height: 160px; resize: none; overflow-y: auto; background: #0a0e10; color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 6px 8px; font: 12px/1.4 ui-monospace, Consolas, monospace; }
#inputrow button { flex: 0 0 auto; padding: 5px 10px; font-size: 11px; }
#btn-send { background: var(--accent); color: #08201c; border-color: var(--accent); font-weight: 700; }
/* 历史、剪贴板等辅助按钮行:输入行下方 */
#auxrow { display: flex; flex-wrap: wrap; gap: 6px; align-items: center; padding: 0 10px 6px; border-bottom: 1px solid var(--line); }
#auxrow button { padding: 4px 10px; font-size: 11px; }
#histsel, #switches select, #switches input[type=number] { flex: 0 0 auto; background: #222b31; color: var(--text); border: 1px solid var(--line); border-radius: 6px; padding: 5px 4px; font: 11px ui-monospace, Consolas, monospace; }
#histsel { max-width: 110px; }
#switches input[type=number] { width: 60px; text-align: center; }
/* 触屏设备输入字号提到 16px,防 iOS 聚焦时自动放大 */
@media (pointer: coarse) { #textin { font-size: 16px; } }
.krow { display: flex; gap: 3px; margin-bottom: 4px; min-width: 0; }
.key { flex: 1 1 0; min-width: 0; height: 34px; padding: 0 4px; overflow: hidden; white-space: nowrap; border-radius: 5px; background: #1d262c; border: 1px solid #33414a; color: var(--text); font: 11px ui-monospace, Consolas, monospace; cursor: pointer; touch-action: manipulation; user-select: none; -webkit-touch-callout: none; }
.key:hover { background: #28343c; }
.key.mod.held { background: var(--accent); border-color: var(--accent); color: #08201c; font-weight: 700; }
#log { order: 50; height: 220px; overflow-y: auto; padding: 8px 10px; font-size: 12px; white-space: pre-wrap; word-break: break-all; background: #0a0e10; }
.line { border-bottom: 1px dashed #1b2429; padding: 2px 0; }
.err { color: var(--err); } .act { color: var(--accent); }
@media (max-width: 640px) {
  #bar button { padding: 4px 8px; font-size: 11px; }
  #switches label { font-size: 11px; gap: 3px; }
  #info { display: none; }
  .key { font-size: 10px; }
}
@media (max-width: 480px) {
  /* 极窄屏:隐藏坐标文本,把行中间让给刷新方块 */
  #coord { display: none; }
}
</style>
</head>
<body>

<div id="stage"><img id="shot" alt="desktop"><div id="dragrect"></div><div id="dot"></div><div id="wheeldir"></div></div>

<div id="panel">
 <div id="panel-inner">
  <div id="inputzone">
    <div id="inputrow">
      <textarea id="textin" rows="1" placeholder="要发送的文本,可多行,Enter 换行,Ctrl+Enter 发送"></textarea>
      <button id="btn-send" title="把文本逐字符敲入远端前台窗口,换行转回车键">发送</button>
    </div>
    <div id="auxrow">
      <select id="histsel" title="选择历史输入填入输入框"><option>历史</option></select>
      <button id="btn-setclip" title="把输入框文本写入远端剪贴板">设剪贴板</button>
      <button id="btn-getclip" title="读取远端剪贴板文本到输入框">取剪贴板</button>
      <button id="btn-layout" title="恢复默认布局:输入区回到键盘下方">默认布局</button>
    </div>
  </div>
  <div id="bar">
    <button id="btn-left" class="slidy">left_click</button>
    <div class="bspace"></div>
    <div class="cspace"></div>
    <span id="coord">坐标: 未选择</span>
    <span id="info"></span>
    <div class="bspace"></div>
    <button id="btn-right" class="slidy">right_click</button>
    <button id="btn-refresh" class="slidyc" title="点击刷新截图;按住上下拖动=远端滚轮">刷新</button>
  </div>

  <div id="keyboard"></div>
  <div id="switches">
    <label><input type="checkbox" id="auto"> 自动刷新 2s</label>
    <label><input type="checkbox" id="bw"> 黑白模式</label>
    <label><input type="checkbox" id="rev" checked> 滚轮取反</label>
    <label>滚动距离
      <input type="number" id="speed" min="4" max="80" step="2" value="24" list="speedpresets"
             title="拖动多少像素滚一格,越小越快,可输入自定义数值">
      <datalist id="speedpresets">
        <option value="10"></option>
        <option value="16"></option>
        <option value="24"></option>
        <option value="32"></option>
        <option value="40"></option>
      </datalist>
    </label>
    <label title="关闭后本页所有请求都带 X-RDP-Log:0,server_http 不再打印这些请求的 [RPC] 访问行">
      <input type="checkbox" id="slog" checked> 服务端日志</label>
    <button id="btn-reload" class="swbtn" title="在服务端 importlib.reload 本模块,随后刷新页面:改完 rdp.py 不用重启服务即可生效;新代码有语法错误时页面保留原状并报错">重载 rdp</button>
  </div>
  <div id="log"></div>
 </div>
</div>

<script>
const BASE = location.pathname.split(';')[0] + ';';   // /import%20rdp;
// 本模块在后端命名空间里的访问表达式,由 rdp.html() 按真实导入名注入
// (client.rdp / rdp 自动适配),页面所有请求都拼在它后面
const MOD = __MOD__;
const shot = document.getElementById('shot');
const dot = document.getElementById('dot');
const coordEl = document.getElementById('coord');
const infoEl = document.getElementById('info');
const logEl = document.getElementById('log');
const autoEl = document.getElementById('auto');
const bwEl = document.getElementById('bw');
// ---------- 设置持久化:所有配置存 localStorage,刷新页面不丢 ----------
function saveSetting(key, val) { try { localStorage.setItem(key, val); } catch (err) {} }
const savedAuto = localStorage.getItem('rdp_auto');
autoEl.checked = savedAuto === null ? false : savedAuto === '1';
autoEl.onchange = () => saveSetting('rdp_auto', autoEl.checked ? '1' : '0');
const savedBw = localStorage.getItem('rdp_bw');   // 无存档时才用 URL 参数 rdp.html(p, bw=True) 注入值
bwEl.checked = savedBw === null ? __BW__ : savedBw === '1';
function screenExpr() { return bwEl.checked ? MOD + '.screen(p,1)' : MOD + '.screen(p)'; }
bwEl.onchange = () => { saveSetting('rdp_bw', bwEl.checked ? '1' : '0');
                        log('黑白模式: ' + (bwEl.checked ? '开' : '关'), 'act'); refresh(); };
// 服务端日志开关:默认开(保持原行为);关掉后页面所有请求带 X-RDP-Log:0,
// server_http 就不再为每次点击/每张自动刷新截图打一条 [RPC] 访问行
const slogEl = document.getElementById('slog');
const savedSlog = localStorage.getItem('rdp_slog');
slogEl.checked = savedSlog === null ? true : savedSlog === '1';
slogEl.onchange = () => { saveSetting('rdp_slog', slogEl.checked ? '1' : '0');
                          log('服务端日志: ' + (slogEl.checked ? '开' : '关'), 'act'); };
// 页面所有 fetch 的统一选项:同源请求带自定义头不触发预检
function fetchOpts() {
    const opts = { cache: 'no-store' };
    if (!slogEl.checked) opts.headers = { 'X-RDP-Log': '0' };
    return opts;
}
const revEl = document.getElementById('rev');    // 滚轮取反:默认开,上拖=向下滚(手机自然方向)
const savedRev = localStorage.getItem('rdp_rev');
if (savedRev !== null) revEl.checked = savedRev === '1';
revEl.onchange = () => { saveSetting('rdp_rev', revEl.checked ? '1' : '0');
                         log('滚轮取反: ' + (revEl.checked ? '开' : '关'), 'act'); };

// ---------- 文本输入区:聚焦整区上移画面正下方并保持,失焦不自动跳回 ----------
const textin = document.getElementById('textin');
textin.addEventListener('focus', () => { document.body.classList.add('textfocus'); });
// 恢复默认布局:仅明确点按钮才复位(输入区回到键盘下方),同时收起输入焦点
document.getElementById('btn-layout').onclick = () => {
    textin.blur();
    document.body.classList.remove('textfocus');
};
// 重载 rdp:服务端 importlib.reload(本模块) 后也就是读了磁盘上的最新代码,
// 再刷新页面让新模板/新接口生效;reload 失败(如新代码有语法错误)时页面
// 保持原样,错误信息落在页面日志里。表达式全用 __import__,不依赖服务端
// 命名空间里是否绑定了 rdp/importlib 这两个名字
document.getElementById('btn-reload').onclick = () => {
    log('重载 rdp 模块…', 'act');
    rpc("__import__('importlib').reload(" + MOD + ")")
        .then(() => { log('重载完成,刷新页面…', 'act'); location.reload(); })
        .catch(() => {});
};
function autoGrow() {                       // 多行时自动长高,封顶 160px 转内部滚动
    textin.style.height = 'auto';
    textin.style.height = Math.min(textin.scrollHeight, 160) + 'px';
}
textin.addEventListener('input', autoGrow);
function pyQuote(s) {                       // 串进 Python 单引号字面量:换行必须转义
    return s.replace(/\r/g, '').replace(/\\/g, '\\\\').replace(/'/g, "\\'").replace(/\n/g, '\\n');
}
// 输入历史:localStorage 持久化,发送成功才入库,下拉选择回填
const histsel = document.getElementById('histsel');
const histKey = 'rdp_input_hist';
function loadHist() { try { return JSON.parse(localStorage.getItem(histKey)) || []; } catch (err) { return []; } }
function saveHist(t) {
    if (!t.trim()) return;
    let h = loadHist().filter(x => x !== t);
    h.unshift(t);
    try { localStorage.setItem(histKey, JSON.stringify(h.slice(0, 30))); } catch (err) {}
    renderHist();
}
function renderHist() {
    const h = loadHist();
    histsel.innerHTML = '';
    const ph = document.createElement('option');
    ph.textContent = '历史' + (h.length ? '(' + h.length + ')' : '');
    histsel.appendChild(ph);
    h.forEach((t, i) => {
        const o = document.createElement('option');
        o.textContent = t.replace(/\s+/g, ' ').slice(0, 40);
        o.title = t;
        o.value = String(i);
        histsel.appendChild(o);
    });
}
histsel.onchange = () => {
    const h = loadHist(), i = parseInt(histsel.value, 10);
    if (!isNaN(i) && h[i] !== undefined) {
        textin.value = h[i];
        autoGrow();
        log('历史填入 ' + h[i].length + ' 字符', 'act');
    }
    histsel.selectedIndex = 0;               // 复位,同一项可再次选择
};
renderHist();
function sendText() {
    const text = textin.value;
    if (!text) { log('输入框为空,未发送', 'err'); return; }
    rpc(MOD + ".input('" + pyQuote(text) + "')")
        .then(() => {                        // 发送成功:清空输入,截图刷新确认远端已上屏
            textin.value = '';
            autoGrow();
            saveHist(text);
            scheduleRefresh();
        })
        .catch(() => {});
}
textin.addEventListener('keydown', e => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); sendText(); }
});
document.getElementById('btn-send').onclick = sendText;
document.getElementById('btn-setclip').onclick = () => {
    if (!textin.value) { log('输入框为空,未写入剪贴板', 'err'); return; }
    rpc(MOD + ".setclip('" + pyQuote(textin.value) + "')").then(scheduleRefresh).catch(() => {});
};
document.getElementById('btn-getclip').onclick = () => {
    rpc(MOD + '.clipboard()').then(t => {
        textin.value = t;
        autoGrow();
        log('远程剪贴板 ' + t.length + ' 字符: ' + (t.slice(0, 80).replace(/\s+/g, ' ') || '空'), 'act');
    }).catch(() => {});
};

// left/right_click 悬浮在控制行(bar)内左右两端:固定钉在行内两端、刷新方块钉在
// 行内中心(.cspace 预留位),不随缩放平移滑动,left/刷新/right 相对位置任何状态下不变,
// 想看某侧按键时自然平移页面即可
const bar = document.getElementById('bar');
const btnL = document.getElementById('btn-left');
const btnR = document.getElementById('btn-right');
const btnM = document.getElementById('btn-refresh');
const cspace = document.querySelector('.cspace');   // 行内为刷新方块预留的空位
const panel = document.getElementById('panel');
const panelInner = document.getElementById('panel-inner');
function slideButtons() {
    const vv = window.visualViewport;
    const w = bar.clientWidth;
    // 面板适配可视宽度:页面放大后面板反向缩放到恰好铺满可视片(物理尺寸≈恒定),
    // left/刷新/right 三键与整排键盘任何缩放下整行可见且相对位置恒定,
    // 放大看细节只作用于截图画面;面板占位高度同步收缩,避免底部空出大段空白
    let k = 1, pageLeft = 0;
    if (vv && vv.width && vv.width < w - 0.5) {
        k = vv.width / w;
        pageLeft = vv.pageLeft || 0;
    }
    if (k < 1) {
        panel.style.transform = 'translateX(' + pageLeft + 'px) scale(' + k + ')';
        panel.style.height = Math.round(panelInner.offsetHeight * k) + 'px';
    } else {
        panel.style.transform = '';
        panel.style.height = '';
    }
    // 三键固定钉位:left 贴行左缘、right 贴行右缘、刷新方块中心对齐行内预留位;
    // 均用布局坐标(offsetLeft/clientWidth,不受 transform 影响),与缩放平移完全解耦
    const pad = 6;
    const bwM = btnM.offsetWidth || 48;
    btnL.style.left = pad + 'px';
    btnR.style.left = (w - (btnR.offsetWidth || 70) - pad) + 'px';
    const csCenter = cspace ? cspace.offsetLeft + cspace.offsetWidth / 2 : w / 2;
    btnM.style.left = Math.max(pad + bwM / 2, Math.min(csCenter, w - pad - bwM / 2)) + 'px';
}
if (window.visualViewport) {
    window.visualViewport.addEventListener('resize', () => requestAnimationFrame(slideButtons));
    window.visualViewport.addEventListener('scroll', slideButtons);
}
window.addEventListener('resize', () => requestAnimationFrame(slideButtons));
window.addEventListener('scroll', slideButtons, true);
window.addEventListener('orientationchange', () => requestAnimationFrame(slideButtons));
window.addEventListener('load', slideButtons);
if (window.ResizeObserver) { new ResizeObserver(slideButtons).observe(bar);
                             new ResizeObserver(slideButtons).observe(cspace);
                             new ResizeObserver(slideButtons).observe(panelInner); }   // 内容高度变化后重算占位
slideButtons();
let lastX = null, lastY = null, busy = false, refreshTimer = null, snapPending = false;
let lastSx = null, lastSy = null;           // 锚点在截图内的显示像素坐标
const dragrect = document.getElementById('dragrect');

function log(msg, cls) {
    const line = document.createElement('div');
    line.className = 'line' + (cls ? ' ' + cls : '');
    line.textContent = new Date().toTimeString().slice(0, 8) + '  ' + msg;
    logEl.prepend(line);
    while (logEl.children.length > 400) logEl.lastChild.remove();
}

function rpc(expr) {
    const started = performance.now();
    return fetch(BASE + encodeURIComponent(expr), fetchOpts())
        .then(async resp => {
            const text = await resp.text();
            const ms = Math.round(performance.now() - started);
            log(expr + '  → [' + resp.status + ' ' + ms + 'ms] ' + text.slice(0, 200).trim(), resp.ok ? '' : 'err');
            if (!resp.ok) throw new Error(text.slice(0, 120));
            return text;
        })
        .catch(err => { log('请求失败: ' + err.message, 'err'); throw err; });
}

let lastCursor = null;                        // 最近一次截图携带的远端光标位置
function fmtRate(bytesPerSec) {
    if (bytesPerSec >= 1024 * 1024) return (bytesPerSec / 1048576).toFixed(1) + 'MB/s';
    if (bytesPerSec >= 1024) return (bytesPerSec / 1024).toFixed(0) + 'KB/s';
    return Math.round(bytesPerSec) + 'B/s';
}
function refresh(snap) {
    if (busy) { if (snap) snapPending = true; return; }   // 忙时记下待同步,本次截图完成后再补
    busy = true;
    const t0 = performance.now();
    let tHead = 0;
    fetch(BASE + screenExpr(), fetchOpts())
        .then(resp => {
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            tHead = performance.now();               // 响应头到达:含服务端截图+编码+网络往返
            lastCursor = resp.headers.get('X-Cursor');
            return resp.blob();
        })
        .then(blob => {
            const tBody = performance.now();         // 图片数据下载完成
            const url = URL.createObjectURL(blob);
            const old = shot.src;
            shot.src = url;
            if (old && old.startsWith('blob:')) URL.revokeObjectURL(old);
            if ((snap || snapPending) && lastCursor) {       // 手动刷新/操作后的刷新才把红点归位
                snapPending = false;
                snapDotTo(lastCursor);
            }
            const total = tBody - t0;
            const report = () => {
                const kb = blob.size / 1024;
                const size = kb >= 1024 ? (kb / 1024).toFixed(2) + 'MB' : kb.toFixed(1) + 'KB';
                log('截图 ' + shot.naturalWidth + 'x' + shot.naturalHeight + ' ' + size
                    + ' 请求' + Math.round(tHead - t0) + 'ms 传输' + Math.round(tBody - tHead)
                    + 'ms 解码' + Math.round(performance.now() - tBody) + 'ms 共' + Math.round(performance.now() - t0) + 'ms'
                    + ' ' + fmtRate(blob.size / (total / 1000))
                    + (lastCursor ? ' 光标' + lastCursor : ''));
            };
            if (shot.complete) report();
            else shot.addEventListener('load', report, { once: true });
        })
        .catch(err => log('截图失败: ' + err.message, 'err'))
        .finally(() => { busy = false; });
}

// 把红点/待点击坐标吸附到远端屏幕光标实际位置(桌面像素坐标字符串 "x,y")
function snapDotTo(cursor) {
    if (!shot.naturalWidth) return;
    const parts = cursor.split(',');
    const cx = parseInt(parts[0], 10), cy = parseInt(parts[1], 10);
    if (!(cx >= 0) || !(cy >= 0)) return;
    lastSx = cx * shot.clientWidth / shot.naturalWidth;
    lastSy = cy * shot.clientHeight / shot.naturalHeight;
    lastX = cx; lastY = cy;
    dot.style.left = lastSx + 'px';
    dot.style.top = lastSy + 'px';
    dot.style.display = 'block';
    coordEl.textContent = '坐标: (' + cx + ', ' + cy + ')';
}

// 操作后的刷新:click/drag/scroll/key/input 都会移动远端光标,统一按手动刷新处理把红点归位
function scheduleRefresh() { clearTimeout(refreshTimer); refreshTimer = setTimeout(() => refresh(true), 400); }

shot.addEventListener('load', () => { infoEl.textContent = shot.naturalWidth + 'x' + shot.naturalHeight; sizeDot(); });

// 红点大小铆定图片显示比例:与截图内"红色空心圈"(截图坐标系中直径 14px)始终保持同尺寸
function sizeDot() {
    if (!shot.naturalWidth) return;
    const ratio = shot.clientWidth / shot.naturalWidth;
    const size = 14 * ratio;
    dot.style.width = size + 'px';
    dot.style.height = size + 'px';
    dot.style.borderWidth = Math.max(0.5, ratio) + 'px';
}
if (window.ResizeObserver) new ResizeObserver(sizeDot).observe(shot);
window.addEventListener('resize', () => requestAnimationFrame(sizeDot));

shot.addEventListener('click', e => {
    const rect = shot.getBoundingClientRect();
    if (!shot.naturalWidth) return;
    const px = e.clientX - rect.left, py = e.clientY - rect.top;
    lastSx = px; lastSy = py;
    lastX = Math.round(px / rect.width * shot.naturalWidth);
    lastY = Math.round(py / rect.height * shot.naturalHeight);
    dot.style.left = px + 'px';
    dot.style.top = py + 'px';
    dot.style.display = 'block';
    coordEl.textContent = '坐标: (' + lastX + ', ' + lastY + ')';
    log('选中坐标 (' + lastX + ', ' + lastY + ')  桌面 ' + shot.naturalWidth + 'x' + shot.naturalHeight, 'act');
});

function sendClick(button) {
    if (lastX === null) { log('请先点击上方截图选择坐标', 'err'); return; }
    rpc(MOD + '.click(' + lastX + "," + lastY + ",'" + button + "')").then(scheduleRefresh).catch(() => {});
}
document.getElementById('btn-right').onclick = () => sendClick('right');

// ---------- 选择模式:按住 left_click 拖动 → 半透明红条预览,松手才发送拖动请求 ----------
const btnLeft = document.getElementById('btn-left');
let dragState = null;                        // {id, sx, sy, moved, ex, ey}
let suppressLeftClick = false;               // 拖动后吞掉紧随的 click,避免重复发送
btnLeft.addEventListener('pointerdown', e => {
    if (lastX === null) return;              // 未选坐标时保持普通点击(由 click 提示)
    dragState = { id: e.pointerId, sx: e.clientX, sy: e.clientY, moved: false, ex: null, ey: null };
    try { btnLeft.setPointerCapture(e.pointerId); } catch (err) {}
    btnLeft.classList.add('dragging');
});
btnLeft.addEventListener('pointermove', e => {
    if (!dragState || e.pointerId !== dragState.id) return;
    const dx = e.clientX - dragState.sx, dy = e.clientY - dragState.sy;
    if (!dragState.moved && Math.abs(dx) < 6 && Math.abs(dy) < 6) return;
    if (!dragState.moved) { dragState.moved = true; log('拖动选择中…松手发送', 'act'); }
    // 终点 = 红点锚点 + 指针位移(显示像素),并夹在截图范围内
    const w = shot.clientWidth, h = shot.clientHeight;
    const ex = Math.max(0, Math.min(lastSx + dx, w));
    const ey = Math.max(0, Math.min(lastSy + dy, h));
    dragState.ex = ex; dragState.ey = ey;
    dragrect.style.left = Math.min(lastSx, ex) + 'px';
    dragrect.style.top = Math.min(lastSy, ey) + 'px';
    dragrect.style.width = Math.abs(ex - lastSx) + 'px';
    dragrect.style.height = Math.abs(ey - lastSy) + 'px';
    dragrect.style.display = 'block';
});
function finishLeftDrag(send) {
    const st = dragState; dragState = null;
    btnLeft.classList.remove('dragging');
    dragrect.style.display = 'none';
    if (!st) return;
    if (send && st.moved && st.ex !== null) {
        const ex = Math.round(st.ex / shot.clientWidth * shot.naturalWidth);
        const ey = Math.round(st.ey / shot.clientHeight * shot.naturalHeight);
        rpc(MOD + '.drag(' + lastX + "," + lastY + "," + ex + "," + ey + ")").then(scheduleRefresh).catch(() => {});
    }
}
btnLeft.addEventListener('pointerup', e => {
    if (!dragState || e.pointerId !== dragState.id) return;
    const wasDrag = dragState.moved;
    if (wasDrag) { suppressLeftClick = true; setTimeout(() => { suppressLeftClick = false; }, 400); }
    finishLeftDrag(true);
});
btnLeft.addEventListener('pointercancel', () => finishLeftDrag(false));
btnLeft.addEventListener('click', () => {
    if (suppressLeftClick) { suppressLeftClick = false; return; }
    sendClick('left');
});

// ---------- 刷新按钮:点击=刷新截图;按住上下拖动=在红点坐标处滚动远端滚轮 ----------
// 滚轮取反开关默认开:上拖=向下滚、下拖=向上滚(手机自然方向),关掉则与真实滚轮同向;
// 滚动距离(每格拖动像素)由配置数字框决定;不足一格视为点击,仍走刷新
const btnRefresh = document.getElementById('btn-refresh');
let scrollState = null;                        // {id, sy, moved}
let suppressRefreshClick = false;              // 滚动过就吞掉松手紧随的 click,避免误触刷新
const speedSel = document.getElementById('speed');   // 滚动距离:每格拖动像素,越小越快
const savedSpeed = parseInt(localStorage.getItem('rdp_speed'), 10);
if (savedSpeed >= 4 && savedSpeed <= 80) speedSel.value = savedSpeed;
function notchPx() { const n = parseInt(speedSel.value, 10); return (n >= 4 && n <= 80) ? n : 24; }
speedSel.onchange = () => { saveSetting('rdp_speed', speedSel.value);
                            log('滚动距离: 每格 ' + notchPx() + 'px', 'act'); };
// 滚轮方向箭头:截图网络往返慢,远端画面延迟才更新,先在远端光标处本地画箭头即时反馈
const wheeldir = document.getElementById('wheeldir');
let wheelDirTimer = null;
function showWheelDir(clicks) {
    wheeldir.textContent = clicks > 0 ? '▲' : '▼';
    wheeldir.style.fontSize = Math.max(dot.offsetWidth || 14, 12) + 'px';   // 随红点圆圈尺寸,下限12px保可见
    wheeldir.style.left = (lastSx || 0) + 'px';
    wheeldir.style.top = (lastSy || 0) + 'px';
    wheeldir.style.display = 'block';
    clearTimeout(wheelDirTimer);
    wheelDirTimer = setTimeout(() => { wheeldir.style.display = 'none'; }, 600);
}
btnRefresh.addEventListener('pointerdown', e => {
    if (lastX === null && lastCursor) snapDotTo(lastCursor);   // 首帧图未解码完吸附失败,此处兜底
    if (lastX === null) return;                // 未选坐标时不进滚轮模式,保持纯点击刷新
    scrollState = { id: e.pointerId, sy: e.clientY, moved: false };
    try { btnRefresh.setPointerCapture(e.pointerId); } catch (err) {}
});
btnRefresh.addEventListener('pointermove', e => {
    if (!scrollState || e.pointerId !== scrollState.id) return;
    const notches = Math.trunc((e.clientY - scrollState.sy) / notchPx());   // 下拖为正
    if (!notches) return;                      // 累计不足一格:既不滚动也不算拖动
    if (!scrollState.moved) { scrollState.moved = true; btnRefresh.classList.add('dragging');
                              log('滚轮拖动中…上下拖=滚轮,松手结束', 'act'); }
    scrollState.sy += notches * notchPx();     // 只消费整格位移,余量留给后续移动
    const clicks = revEl.checked ? notches : -notches;   // 默认取反:上拖(负)=向下滚
    showWheelDir(clicks);                      // 本地箭头先行,不等远端截图
    rpc(MOD + '.scroll(' + lastX + "," + lastY + "," + clicks + ")").then(scheduleRefresh).catch(() => {});
});
function finishRefreshScroll() {
    const st = scrollState; scrollState = null;
    btnRefresh.classList.remove('dragging');
    if (st && st.moved) { suppressRefreshClick = true; setTimeout(() => { suppressRefreshClick = false; }, 400); }
}
btnRefresh.addEventListener('pointerup', finishRefreshScroll);
btnRefresh.addEventListener('pointercancel', finishRefreshScroll);
btnRefresh.addEventListener('click', () => {
    if (suppressRefreshClick) { suppressRefreshClick = false; return; }
    refresh(true);                             // 手动刷新:同时把红点同步到远端光标
});

// 屏蔽按钮/按键上的长按复制粘贴与右键菜单(移动端)
document.addEventListener('contextmenu', e => {
    if (e.target.closest('button')) e.preventDefault();
});
document.addEventListener('selectstart', e => {
    if (e.target.closest('button')) e.preventDefault();
});

// ---------- 全键盘 ----------
const MODS = new Set(['ctrl', 'alt', 'shift', 'win']);
// Shift 按住时键面改成实际会发送的字符(2→@、`→~、[→{ …):手机屏上键太小,
// 一个键放不下 2/@ 两个字符,所以按住 Shift 时数字/符号键切换显示,松开还原。
// 键按 code 索引(而不是显示文本),反斜杠等特殊字符不必再转义。
const SHIFT_LABELS = {
 '1': '!', '2': '@', '3': '#', '4': '$', '5': '%',
 '6': '^', '7': '&', '8': '*', '9': '(', '0': ')',
 'backtick': '~', 'minus': '_', 'equal': '+',
 'lbracket': '{', 'rbracket': '}', 'backslash': '|',
 'semicolon': ':', 'quote': '"', 'comma': '<', 'period': '>', 'slash': '?'
};
const ROWS = [
 [['Esc','esc'],['F1','f1'],['F2','f2'],['F3','f3'],['F4','f4'],['F5','f5'],['F6','f6'],['F7','f7'],['F8','f8'],['F9','f9'],['F10','f10'],['F11','f11'],['F12','f12'],['PrtSc','prtsc'],['ScrLk','scrolllock'],['Pause','pause']],
 [['`','backtick'],['1','1'],['2','2'],['3','3'],['4','4'],['5','5'],['6','6'],['7','7'],['8','8'],['9','9'],['0','0'],['-','minus'],['=','equal'],['Backspace','backspace',2],['Ins','insert'],['Home','home'],['PgUp','pageup']],
 [['Tab','tab',1.5],['Q','q'],['W','w'],['E','e'],['R','r'],['T','t'],['Y','y'],['U','u'],['I','i'],['O','o'],['P','p'],['[','lbracket'],[']','rbracket'],['\\','backslash'],['Del','delete'],['End','end'],['PgDn','pagedown']],
 [['Caps','capslock',1.8],['A','a'],['S','s'],['D','d'],['F','f'],['G','g'],['H','h'],['J','j'],['K','k'],['L','l'],[';','semicolon'],["'",'quote'],['Enter','enter',2.2]],
 [['Shift','shift',2.3],['Z','z'],['X','x'],['C','c'],['V','v'],['B','b'],['N','n'],['M','m'],[',','comma'],['.','period'],['/','slash'],['Shift','shift',1.7],['↑','up']],
 [['Ctrl','ctrl',1.4],['Win','win',1.2],['Alt','alt',1.2],['Space','space',6],['Alt','alt',1.2],['Win','win',1.2],['Menu','menu',1.2],['Ctrl','ctrl',1.4],['←','left'],['↓','down'],['→','right']]
];
const held = new Set();
const kb = document.getElementById('keyboard');
ROWS.forEach(row => {
    const rowEl = document.createElement('div');
    rowEl.className = 'krow';
    row.forEach(item => {
        const btn = document.createElement('button');
        btn.className = 'key' + (MODS.has(item[1]) ? ' mod' : '');
        if (item[2]) btn.style.flexGrow = item[2];
        btn.dataset.code = item[1];             // 供 Shift 键面切换用
        btn.dataset.base = item[0];             // 原始键面文字
        btn.textContent = item[0];
        btn.onclick = () => onKey(item[1], btn);
        rowEl.appendChild(btn);
    });
    kb.appendChild(rowEl);
});

function paintKeyboard() {
    const shift = held.has('shift');
    kb.querySelectorAll('.key').forEach(el => {
        const code = el.dataset.code;
        let label = el.dataset.base;
        if (shift) {
            if (SHIFT_LABELS[code]) label = SHIFT_LABELS[code];
            else if (code.length === 1 && code >= 'a' && code <= 'z') label = code.toUpperCase();
        }
        if (el.textContent !== label) el.textContent = label;
    });
}

function onKey(code, btn) {
    if (MODS.has(code)) {                       // 修饰键:点击进入/退出组合状态
        if (held.has(code)) { held.delete(code); btn.classList.remove('held'); }
        else { held.add(code); btn.classList.add('held'); }
        paintKeyboard();                        // Shift 按住时键面切换成 @ # ~ 等实际字符
        log(held.size ? '组合修饰键: ' + [...held].join('+') + '+ …' : '修饰键已清空', 'act');
        return;
    }
    let combo = code;
    if (held.size) {
        combo = [...held].join('+') + '+' + code;
        held.clear();
        document.querySelectorAll('.key.mod.held').forEach(el => el.classList.remove('held'));
        paintKeyboard();                        // 组合已发出,键面还原成原始字符
    }
    rpc(MOD + ".key('" + combo + "')").then(scheduleRefresh).catch(() => {});
}

setInterval(() => { if (autoEl.checked) refresh(); }, 2000);
log('页面就绪,加载截图中…');
refresh();                              // 首次加载不归位红点,只有一个空心红圈指示远程光标
</script>
</body>
</html>'''


# --------------------------------------------------------------------------- #
# 「服务端日志」开关的静音规则：本模块只声明“请求带 X-RDP-Log: 0 就不写
# 访问行”，通过 server_http 的过滤器注册接口注入，server_http 不需要认识
# rdp。注册失败（独立 import 本模块、或 server_http 不在 sys.path）只跳过，
# 绝不影响页面和主流程。
# --------------------------------------------------------------------------- #
_SILENCE_VALUES = ('0', 'off', 'false', 'no')


def _silence_access_log(handler):
    """server_http 日志过滤器：本页请求头带 X-RDP-Log: 0 时静音访问行。"""
    headers = getattr(handler, 'headers', None)
    if headers is None:      # 测试/WSGI 垫片可能是 __new__ 出来的 handler
        return False
    try:
        value = headers.get('X-RDP-Log', '')
    except Exception:
        return False
    return str(value).strip().lower() in _SILENCE_VALUES


try:  # 只有跑在 server_http 进程里才需要注册；独立 import 时静默跳过
    import server_http

    # 同名覆盖：页面「重载 rdp」reload 本模块时不会累积重复过滤器
    server_http.add_log_filter('rdp-slog', _silence_access_log)
except Exception:
    pass
