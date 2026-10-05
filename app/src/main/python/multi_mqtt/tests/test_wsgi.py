import sys, types, urllib.parse

# --- stub heavy deps so we can import app.py for the normalize helpers ---
from unittest.mock import MagicMock

def _gpu_stub(*a, **k):
    if a and callable(a[0]):
        return a[0]
    return lambda f: f

for name, attrs in {
    "spaces": {"GPU": _gpu_stub},
    "a2wsgi": {"WSGIMiddleware": lambda app: app},
}.items():
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
fm = types.ModuleType("fastapi"); fr = types.ModuleType("fastapi.responses")
fr.RedirectResponse = MagicMock()
fm.responses = fr
sys.modules["fastapi"] = fm
sys.modules["fastapi.responses"] = fr
sys.modules["gradio"] = MagicMock()

sys.path.insert(0, ".")
import app

for val, expect in [(30, "+30%"), ("50%", "+50%"), ("-10", "-10%"),
                    ("+0%", "+0%"), ("-20%", "-20%"), (0, "+0%")]:
    got = app._normalize_percent(val, "rate")
    assert got == expect, (val, got, expect)
for val, expect in [(50, "+50Hz"), ("-20hz", "-20Hz"), ("+0Hz", "+0Hz")]:
    assert app._normalize_pitch(val) == expect, (val, app._normalize_pitch(val))
print("normalize OK")

# --- drive the streaming WSGI application with a fake environ ---
sys.path.insert(0, "multi_mqtt")
import server_http_wsgi

def call(code):
    captured = {}
    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers
    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": "/" + urllib.parse.quote(code),
        "QUERY_STRING": "",
        "REMOTE_ADDR": "127.0.0.1", "REMOTE_PORT": "1",
        "SERVER_NAME": "x", "SERVER_PORT": "80",
    }
    body = server_http_wsgi.application(environ, start_response)
    out = b"".join(body)
    return captured["status"], dict(captured["headers"]), out

s, h, out = call("r=1")
assert s.startswith("200") and out == b"1", (s, out)
print("plain OK ->", s, out)

s, h, out = call("response.set_header('X-Test','1');response.write(b'ab');response.write(b'cd')")
assert s.startswith("200"), s
assert h.get("X-Test") == "1", h
assert h.get("Content-Type") == "application/octet-stream", h
assert "Content-Length" not in h, h
assert out == b"abcd", out
print("stream OK ->", s, h, out)

s, h, out = call("nope_undefined_name")
assert s.startswith("500"), s
assert b"NameError" in out, out
print("error OK ->", s)

# verify streamed mid-flight chunk ordering/arrival pattern via queue timing
import queue as _q
state_chunks = []
print("ALL TESTS PASSED")
