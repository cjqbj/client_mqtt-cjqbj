# ============================ 本地热键：按键名解析 ============================

# 多字节热键（如 Ctrl+Alt+Insert 的 ESC[2;7~）到达时，ESC 可能与后续字节
# 被拆成两次读；先缓存这么久等剩余字节，超时就把 ESC 原样放给远端。
_HOTKEY_GAP = 0.025

# 单字节控制键
_CTRL_CHARS = {
    "ctrl-@": 0, "ctrl-[": 27, "ctrl-\\": 28, "ctrl-]": 29,
    "ctrl-^": 30, "ctrl-_": 31,
}
for _i in range(26):
    _CTRL_CHARS["ctrl-%s" % chr(ord("a") + _i)] = _i + 1
_CTRL_CHARS.update({
    "space": 32, "tab": 9, "enter": 13, "cr": 13,
    "esc": 27, "escape": 27,
})
# 以 ~ 结尾的 CSI 键：名字 -> 中间数字
_CSI_TILDE_KEYS = {
    "insert": 2, "delete": 3, "pageup": 5, "pgup": 5,
    "pagedown": 6, "pgdn": 6, "home": 1, "end": 4,
    "f5": 15, "f6": 17, "f7": 18, "f8": 19, "f9": 20, "f10": 21,
    "f11": 23, "f12": 24,
}
# 单字母 CSI 键：方向键 [A..[D；Home/End [H/[F
_CSI_LETTER_KEYS = {
    "up": "A", "down": "B", "right": "C", "left": "D",
    "home": "H", "end": "F",
}
# SS3 功能键 F1-F4：OP/OQ/OR/OS
_SS3_KEYS = {"f1": "P", "f2": "Q", "f3": "R", "f4": "S"}
_MOD_WORDS = {"ctrl": 4, "control": 4, "alt": 2, "meta": 2, "shift": 1}


def _parse_key_spec(spec) -> bytes:
    """把按键名解析成终端实际上报的字节序列。

    支持：ctrl-]/ctrl-a（单字节）、f1-f12、insert/delete/home/end/方向键、
    以及 shift/alt/ctrl 修饰组合（xterm 编码），如 ``ctrl-alt-insert`` ->
    ``ESC[2;7~``。无法识别时按 latin-1 原样返回（兼容直接传原始字节）；
    none/off/空串表示禁用。
    """
    if spec is None:
        return b""
    # 只剥普通空白：str.strip() 会把 0x1c-0x1f（含 Ctrl-] 的 0x1d）当空白
    # 剥掉，导致直接传原始控制字节做 --detach-key 时被判成"禁用"
    s = str(spec).strip(" \t\r\n").lower().replace("+", "-").replace("_", "-")
    if s in ("", "none", "off", "-", "no"):
        return b""
    if s in _CTRL_CHARS:
        return bytes([_CTRL_CHARS[s]])
    tokens = s.split("-")
    mod = 0
    while tokens and tokens[0] in _MOD_WORDS:
        mod |= _MOD_WORDS[tokens.pop(0)]
    base = "-".join(tokens)
    if base in _CTRL_CHARS:
        # 修饰键配单字节键不产生稳定序列，只接受无修饰的写法
        return bytes([_CTRL_CHARS[base]]) if mod == 0 else b""
    if base in _CSI_TILDE_KEYS:
        n = _CSI_TILDE_KEYS[base]
        mid = (";%d" % (mod + 1)) if mod else ""
        return ("\x1b[%d%s~" % (n, mid)).encode("latin-1")
    if base in _CSI_LETTER_KEYS:
        ch = _CSI_LETTER_KEYS[base]
        if mod:
            return ("\x1b[1;%d%s" % (mod + 1, ch)).encode("latin-1")
        return ("\x1b[%s" % ch).encode("latin-1")
    if base in _SS3_KEYS:
        ch = _SS3_KEYS[base]
        if mod:
            return ("\x1b[1;%d%s" % (mod + 1, ch)).encode("latin-1")
        return ("\x1bO%s" % ch).encode("latin-1")
    return str(spec).encode("latin-1", "ignore")


# 字节序列 -> 人类可读名字（banner 用），由上面的表反向构建
def _build_key_display_map():
    m = {}
    for name, code in _CTRL_CHARS.items():
        m.setdefault(bytes([code]), name)
    labels = ["", "shift-", "alt-", "alt-shift-", "ctrl-", "ctrl-shift-",
              "ctrl-alt-", "ctrl-alt-shift-"]
    for label in labels:
        for name in _CSI_TILDE_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
        for name in _CSI_LETTER_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
        for name in _SS3_KEYS:
            m.setdefault(_parse_key_spec("%s%s" % (label, name)),
                         "%s%s" % (label, name))
    return m


_KEY_DISPLAY = _build_key_display_map()


def _describe_key(seq: bytes) -> str:
    if not seq:
        return "禁用"
    return _KEY_DISPLAY.get(seq, repr(seq.decode("latin-1", "replace")))


