"""Stable runtime loader for hot-swappable Python features."""

import hashlib
import importlib
import os
import sys
import tempfile
import time
from urllib.error import URLError
import urllib.request

_UPDATE_DIR = None
BUILTIN_FEATURES = ("files", "camera", "wifi")
MAX_FEATURE_SIZE = 2 * 1024 * 1024

import client_service

def init_env(update_dir):
    global _UPDATE_DIR,ghs
    client_service._mqtt_client_module()  #
    import server_http
    import server_http
    ghs = server_http.start_rpc_server(
        port=1166,
        ip='0.0.0.0',
        globals=globals(),
        locals=locals(),
    )
    print(f"RPC server started {ghs}")

    _UPDATE_DIR = os.path.abspath(str(update_dir))
    os.makedirs(_UPDATE_DIR, exist_ok=True)
    if _UPDATE_DIR in sys.path:
        sys.path.remove(_UPDATE_DIR)
    sys.path.insert(0, _UPDATE_DIR)
    importlib.invalidate_caches()
    return {"ok": True, "update_dir": _UPDATE_DIR}


def update_dir():
    return _UPDATE_DIR


def _module_name(feature):
    value = str(feature).strip()
    if not value.isidentifier() and not (value.startswith("feature_") and value[8:].isidentifier()):
        raise ValueError("invalid feature name")
    return value if value.startswith("feature_") else "feature_" + value


def load_feature(feature):
    module_name = _module_name(feature)
    importlib.invalidate_caches()
    module = sys.modules.get(module_name)
    if module is None:
        return importlib.import_module(module_name)
    module_file = os.path.abspath(str(getattr(module, "__file__", "")))
    runtime_file = os.path.join(_UPDATE_DIR, module_name + ".py") if _UPDATE_DIR else ""
    if runtime_file and os.path.isfile(runtime_file) and not module_file.startswith(_UPDATE_DIR + os.sep):
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()
        return importlib.import_module(module_name)
    if _UPDATE_DIR and module_file.startswith(_UPDATE_DIR + os.sep):
        cache_dir = os.path.join(_UPDATE_DIR, "__pycache__")
        if os.path.isdir(cache_dir):
            for filename in os.listdir(cache_dir):
                if filename.startswith(module_name + ".") and filename.endswith(".pyc"):
                    os.unlink(os.path.join(cache_dir, filename))
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()
        return importlib.import_module(module_name)
    return module


def list_features():
    names = set(BUILTIN_FEATURES)
    roots = [_UPDATE_DIR, os.path.dirname(__file__)]
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        for filename in os.listdir(root):
            if filename.startswith("feature_") and filename.endswith(".py"):
                names.add(filename[8:-3])
    return sorted(names)


def describe_features():
    result = []
    for name in list_features():
        try:
            module = load_feature(name)
            manifest = getattr(module, "FEATURE", {})
            result.append({
                "name": name,
                "title": str(manifest.get("title") or name),
                "version": manifest.get("version", 1),
                "actions": list(manifest.get("actions") or ["run"]),
            })
        except Exception as exc:
            result.append({"name": name, "title": name, "version": 0, "actions": [], "error": repr(exc)})
    return result


def call_feature(feature, action="run", *args):
    module = load_feature(feature)
    function = getattr(module, str(action), None)
    if not callable(function):
        raise AttributeError(f"feature {feature!r} has no action {action!r}")
    return function(*args)


def install_feature(
    url,
    filename,
    sha256="",
    update_dir=None,
    retries=4,
    timeout=20,
    retry_delay=1,
    fallback_urls=(),
    progress=None,
):
    """Download a feature atomically, retrying across configured source URLs."""
    target_root = update_dir or _UPDATE_DIR
    if not target_root:
        raise RuntimeError("bootstrap is not initialized")
    target_dir = os.path.abspath(str(target_root))
    name = os.path.basename(str(filename))
    if name != str(filename) or not (name.startswith("feature_") and name.endswith(".py")):
        raise ValueError("only feature_*.py modules can be installed")
    urls = [str(url), *(str(item) for item in fallback_urls)]
    max_attempts = max(1, min(int(retries), 8))
    os.makedirs(target_dir, exist_ok=True)
    target = os.path.join(target_dir, name)
    last_error = None

    def report(message):
        if progress:
            try:
                progress(message)
            except Exception:
                pass

    for attempt in range(1, max_attempts + 1):
        source = urls[(attempt - 1) % len(urls)]
        host = source.split("/", 3)[2] if "://" in source else "source"
        report(f"{name}: attempt {attempt}/{max_attempts} via {host}")
        temporary = None
        try:
            request = urllib.request.Request(source, headers={"User-Agent": "ClientMqtt/1.0"})
            with urllib.request.urlopen(request, timeout=float(timeout)) as response:
                content = response.read(MAX_FEATURE_SIZE + 1)
            if len(content) > MAX_FEATURE_SIZE:
                raise ValueError("feature file exceeds size limit")
            compile(content, name, "exec")
            digest = hashlib.sha256(content).hexdigest()
            if sha256 and digest.lower() != str(sha256).lower():
                raise ValueError("feature checksum mismatch")
            fd, temporary = tempfile.mkstemp(prefix=name + ".", dir=target_dir)
            with os.fdopen(fd, "wb") as output:
                output.write(content)
            os.replace(temporary, target)
            temporary = None
            importlib.invalidate_caches()
            report(f"{name}: installed, sha256={digest}")
            return {"ok": True, "filename": name, "sha256": digest, "attempts": attempt}
        except (URLError, TimeoutError, OSError, SyntaxError) as error:
            last_error = error
            report(f"{name}: attempt {attempt} failed: {type(error).__name__}: {error}")
            if attempt < max_attempts:
                time.sleep(min(float(retry_delay) * (2 ** (attempt - 1)), 8))
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
    raise RuntimeError(f"failed to download {name} after {max_attempts} attempts: {last_error}")
