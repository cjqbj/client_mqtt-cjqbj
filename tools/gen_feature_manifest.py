"""Generate app/src/main/python/feature_manifest.json from feature_*.py.

用法（仓库根目录）:
    python3 tools/gen_feature_manifest.py
    python3 tools/gen_feature_manifest.py --root app/src/main/python --min-client 1

清单供客户端在下载任何 feature 正文之前判断“有没有新版本”：每个
feature_*.py 顶部的 FEATURE 字典（version/title/actions/ui）加上文件
sha256/size，由本脚本静态解析生成（不执行脚本）。
"""

import argparse
import ast
import hashlib
import json
import os
import time

MANIFEST_NAME = "feature_manifest.json"


def read_feature(path):
    with open(path, "rb") as handle:
        content = handle.read()
    try:
        tree = ast.parse(content)
    except SyntaxError as error:
        raise ValueError("%s: %s" % (path, error))
    manifest = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "FEATURE"
                   for target in node.targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (ValueError, TypeError) as error:
            raise ValueError("%s: FEATURE must be a literal dict: %s" % (path, error))
        if isinstance(value, dict):
            manifest = value
        break
    if not manifest:
        raise ValueError("%s: missing FEATURE dict" % path)
    name = os.path.basename(path)[len("feature_"):-len(".py")]
    try:
        version = int(manifest.get("version", 1))
    except (TypeError, ValueError):
        raise ValueError("%s: FEATURE.version must be an int" % path)
    return {
        "name": name,
        "filename": os.path.basename(path),
        "title": str(manifest.get("title") or name.title()),
        "version": version,
        "actions": [str(item) for item in manifest.get("actions") or ["run"]],
        "ui": str(manifest.get("ui") or "compose"),
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
    }


def build_manifest(python_root, min_client=""):
    entries = []
    for filename in sorted(os.listdir(python_root)):
        if not (filename.startswith("feature_") and filename.endswith(".py")):
            continue
        path = os.path.join(python_root, filename)
        if not os.path.isfile(path):
            continue
        entry = read_feature(path)
        entry["url"] = entry["filename"]
        if min_client:
            entry["min_client_version"] = str(min_client)
        entries.append(entry)
    return {
        "schema": 1,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "features": entries,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate feature_manifest.json")
    default_root = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "app", "src", "main", "python",
    )
    parser.add_argument("--root", default=default_root,
                        help="directory containing feature_*.py")
    parser.add_argument("--out", default="",
                        help="output path (default <root>/%s)" % MANIFEST_NAME)
    parser.add_argument("--min-client", default="",
                        help="optional min client version applied to every feature")
    args = parser.parse_args(argv)
    root = os.path.abspath(args.root)
    if not os.path.isdir(root):
        raise SystemExit("feature root not found: %s" % root)
    manifest = build_manifest(root, args.min_client)
    output = args.out or os.path.join(root, MANIFEST_NAME)
    with open(output, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print("wrote %s with %d features" % (output, len(manifest["features"])))
    for entry in manifest["features"]:
        print("  %-28s v%-3s %8d  %s" % (entry["filename"], entry["version"],
                                         entry["size"], entry["sha256"][:12]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
