"""Remote filesystem feature. Replaceable at runtime."""

import client_service
import json

FEATURE = {"name": "files", "title": "Files", "version": 1, "actions": ["scan", "upload"]}


def run():
    return FEATURE


def build_scan_code(root, offset=0, limit=100, recursive=False):
    root_value = json.dumps(str(root), ensure_ascii=False)
    offset_value = max(0, int(offset))
    limit_value = max(1, min(int(limit), 10000))
    recursive_value = bool(recursive)
    return f'''import json, os
root = os.path.abspath({root_value})
start = {offset_value}
limit = {limit_value}
recursive = {recursive_value!r}
items = []
errors = []
has_more = False
seen = 0
base = os.path.realpath(root)
if not os.path.isdir(root):
    r = json.dumps({{"ok": False, "error": "root is not a directory", "root": root}}, ensure_ascii=False)
else:
    for current, dirs, names in os.walk(root, followlinks=False):
        dirs.sort()
        names.sort()
        current_real = os.path.realpath(current)
        if not (current_real == base or current_real.startswith(base + os.sep)):
            dirs[:] = []
            continue
        dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(current, d))]
        entries = [(name, "directory") for name in dirs]
        entries.extend((name, "file") for name in names)
        entries.sort(key=lambda item: (item[1] != "directory", item[0].casefold()))
        for name, kind in entries:
            path = os.path.join(current, name)
            try:
                real = os.path.realpath(path)
                if not (real == base or real.startswith(base + os.sep)):
                    continue
                stat = os.stat(path, follow_symlinks=False)
                if seen < start:
                    seen += 1
                    continue
                if len(items) >= limit:
                    has_more = True
                    break
                items.append({{"path": os.path.relpath(path, root), "kind": kind, "size": stat.st_size, "modified": stat.st_mtime}})
                seen += 1
            except OSError as exc:
                errors.append({{"path": os.path.relpath(path, root), "error": repr(exc)}})
        if has_more or not recursive:
            break
    r = json.dumps({{"ok": True, "root": root, "items": items, "errors": errors, "has_more": has_more, "next_offset": start + len(items)}}, ensure_ascii=False)
'''


def build_upload_code(remote_path):
    path_value = json.dumps(str(remote_path), ensure_ascii=False)
    return f'''import json
import aliyun_git
path = {path_value}
try:
    url = aliyun_git.upload(path, file_path=path.rsplit("/", 1)[-1])
    r = json.dumps({{"ok": True, "url": url, "name": path.rsplit("/", 1)[-1]}}, ensure_ascii=False)
except Exception as exc:
    r = json.dumps({{"ok": False, "error": repr(exc)}}, ensure_ascii=False)
'''


def scan(root, offset=0, limit=100):
    result = client_service.rpc(build_scan_code(root, offset, limit))
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)


def upload(remote_path):
    result = client_service.rpc(build_upload_code(remote_path))
    return json.dumps(client_service.parse_json_result(result), ensure_ascii=False)
