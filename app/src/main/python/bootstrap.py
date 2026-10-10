"""Stable runtime loader for hot-swappable Python features."""

import hashlib
import importlib
import os
import sys
import tempfile
import threading
import time
from urllib.error import URLError
import urllib.request

_UPDATE_DIR = None


def _scan_builtin_features():
    """内建 feature 不再硬编码：编译打包进来的每个 feature_*.py 自动算内建。

    名字取文件名 feature_<name>.py 的 <name> 段，与 py_updates 热更文件
    使用同一套命名规则；FEATURE manifest 里无需再写 name/title。
    """
    root = os.path.dirname(os.path.abspath(__file__))
    names = set()
    # 源码/桌面布局：物理目录里就是 .py 文件，直接 os.listdir。
    try:
        for filename in os.listdir(root):
            if filename.startswith("feature_") and filename.endswith(".py"):
                name = filename[8:-3]
                if name.isidentifier():
                    names.add(name)
    except OSError:
        pass
    # APK 打包布局：.pyc 全部收在 app.imy 里，AssetFinder 物理目录
    # os.listdir 看不到模块文件，必须经 Chaquopy AssetFinder.listdir
    # 枚举（SourcelessAssetLoader.finder），否则内置 feature 列表为空。
    if not names:
        try:
            loader = getattr(sys.modules.get(__name__), "__loader__", None)
            finder = getattr(loader, "finder", None)
            if finder is not None:
                for filename in finder.listdir(""):
                    if filename.startswith("feature_") and filename.endswith(".pyc"):
                        name = filename[8:-4]
                        if name.isidentifier():
                            names.add(name)
        except Exception:
            pass
    return tuple(sorted(names))


BUILTIN_FEATURES = _scan_builtin_features()
MAX_FEATURE_SIZE = 2 * 1024 * 1024
_FEATURE_LOCK = threading.RLock()
_RPC_SERVER_STARTED = False

# 历史改名遗留：py_updates 里的旧文件 feature_call.py 会和新内置
# feature_dialer.py 并存，feature 列表冒出两个名字（Dialer / call）。
# 新名字已内置时，启动即清掉旧覆盖文件。
_RENAMED_FEATURES = {"call": "dialer"}

import client_service

def cleanup_legacy_updates():
    """删除改名前遗留在 py_updates 的旧 feature 覆盖文件，返回清掉的旧名列表。"""
    if not _UPDATE_DIR or not os.path.isdir(_UPDATE_DIR):
        return []
    removed = []
    for old_name, new_name in _RENAMED_FEATURES.items():
        if new_name not in BUILTIN_FEATURES:
            continue
        module_name = "feature_%s" % old_name
        old_file = os.path.join(_UPDATE_DIR, module_name + ".py")
        if os.path.isfile(old_file):
            try:
                os.unlink(old_file)
                _purge_feature_pyc(module_name)
                sys.modules.pop(module_name, None)
                removed.append(old_name)
            except OSError:
                pass
    return removed


def init_env(update_dir):
    global _UPDATE_DIR, ghs, _RPC_SERVER_STARTED
    client_service._mqtt_client_module()  #
    # App 启动只初始化一次；重复调用（如测试进程内多次 init_env）不重复绑定端口。
    if not _RPC_SERVER_STARTED:
        import server_http
        ghs = server_http.start_rpc_server(
            port=1166,
            ip='0.0.0.0',
            globals=globals(),
            locals=locals(),
        )
        _RPC_SERVER_STARTED = True
        print(f"RPC server started {ghs}")

    _UPDATE_DIR = os.path.abspath(str(update_dir))
    os.makedirs(_UPDATE_DIR, exist_ok=True)
    # 必须在 py_updates 进 sys.path 之前清理，避免旧模块（feature_call）被导入。
    stale = cleanup_legacy_updates()
    if stale:
        print("removed legacy feature overrides: %s" % ",".join(stale))
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


def _runtime_module_path(module_name):
    return os.path.join(_UPDATE_DIR, module_name + ".py") if _UPDATE_DIR else ""


def _is_runtime_module(module):
    module_file = os.path.abspath(str(getattr(module, "__file__", "") or ""))
    return bool(_UPDATE_DIR) and module_file.startswith(_UPDATE_DIR + os.sep)


def _purge_feature_pyc(module_name):
    """Drop stale bytecode for an updatable module before a fresh import."""
    if not _UPDATE_DIR:
        return
    cache_dir = os.path.join(_UPDATE_DIR, "__pycache__")
    if not os.path.isdir(cache_dir):
        return
    for filename in os.listdir(cache_dir):
        if filename.startswith(module_name + ".") and filename.endswith((".pyc", ".pyo")):
            try:
                os.unlink(os.path.join(cache_dir, filename))
            except OSError:
                pass


def load_feature(feature, force_reload=False):
    """Resolve a feature module.

    热加载策略：
    - 模块未导入时正常导入（``sys.path`` 中 ``py_updates`` 优先，自动命中
      下载的覆盖文件）。
    - 已导入的是内置模块、但 ``py_updates`` 出现同名 ``feature_*.py`` 时，
      只做一次影子接管：弹出 ``sys.modules``、清 pyc 后重新导入；之后保持
      缓存，不再每次调用都重载。
    - 已从 ``py_updates`` 加载的模块保持缓存，文件被外部改写也不自动重载，
      只有显式 :func:`reload_feature`（UI 长按 feature）才重新导入。
    """
    module_name = _module_name(feature)
    with _FEATURE_LOCK:
        runtime_file = _runtime_module_path(module_name)
        module = sys.modules.get(module_name)
        if force_reload and module is not None:
            _purge_feature_pyc(module_name)
            sys.modules.pop(module_name, None)
            module = None
        if module is None:
            importlib.invalidate_caches()
            return importlib.import_module(module_name)
        # 内置模块已在内存中，而更新目录新放入了同名文件：热覆盖一次。
        if runtime_file and os.path.isfile(runtime_file) and not _is_runtime_module(module):
            _purge_feature_pyc(module_name)
            sys.modules.pop(module_name, None)
            importlib.invalidate_caches()
            return importlib.import_module(module_name)
        return module


def reload_feature(feature):
    """Always drop the cached module (and py_updates pyc) and re-import it."""
    module_name = _module_name(feature)
    with _FEATURE_LOCK:
        _purge_feature_pyc(module_name)
        sys.modules.pop(module_name, None)
        importlib.invalidate_caches()
        return importlib.import_module(module_name)


def feature_source_info(feature):
    """Describe where the currently loaded module comes from."""
    module_name = _module_name(feature)
    module = sys.modules.get(module_name)
    module_file = str(getattr(module, "__file__", "") or "") if module else ""
    runtime_file = _runtime_module_path(module_name)
    return {
        "name": module_name[8:] if module_name.startswith("feature_") else module_name,
        "module": module_name,
        "module_file": module_file,
        "runtime_file": runtime_file,
        "runtime_file_present": bool(runtime_file) and os.path.isfile(runtime_file),
        "loaded": module is not None,
        "source": "py_updates" if module is not None and _is_runtime_module(module) else "builtin",
    }


def feature_file_info(feature):
    """Return metadata for the file backing the currently loaded feature module."""
    source = feature_source_info(feature)
    path = source["module_file"]
    info = {
        "module_file": path,
        "modified": "",
        "size": None,
        "sha256": "",
        "metadata_error": "",
    }
    if not source["loaded"]:
        info["metadata_error"] = "feature module is not loaded"
        return info
    if not path:
        info["metadata_error"] = "loaded module has no file path"
        return info
    try:
        with open(path, "rb") as module_file:
            stat = os.fstat(module_file.fileno())
            digest = hashlib.sha256()
            for chunk in iter(lambda: module_file.read(64 * 1024), b""):
                digest.update(chunk)
        info.update({
            "modified": time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime)
            ),
            "size": stat.st_size,
            "sha256": digest.hexdigest(),
        })
    except OSError as error:
        info["metadata_error"] = f"{type(error).__name__}: {error}"
    return info


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
            icon = manifest.get("icon")
            # 名字永远以文件名为准（manifest.name 被忽略，避免两边不一致）；
            # title 缺省直接美化文件名：dialer -> Dialer。
            default_title = name[:1].upper() + name[1:]
            result.append({
                "name": name,
                "title": str(manifest.get("title") or default_title),
                "version": manifest.get("version", 1),
                "actions": list(manifest.get("actions") or ["run"]),
                # ui=python 表示界面由 feature 脚本用 Chaquopy 自绘，
                # APK 端走通用宿主，其余走 Compose 通用动作页。
                "ui": str(manifest.get("ui") or "compose"),
                "icon": str(icon) if icon else None,
                "module_file": str(getattr(module, "__file__", "") or ""),
                "source": "py_updates" if _is_runtime_module(module) else "builtin",
            })
        except BaseException as exc:  # noqa: BLE001 - 含 SystemExit：脚本里 sys.exit/导入期
            # FATAL 属于常见死法，单个 feature 绝不能拖垮整个 feature 目录的枚举。
            result.append({
                "name": name,
                "title": name,
                "version": 0,
                "actions": [],
                "ui": "compose",
                "icon": None,
                "module_file": "",
                "source": "unknown",
                "error": repr(exc),
            })
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
