#!/usr/bin/env python3
"""Upload one file to the HF Space through the MQTT client (put subcommand).

The CLI prints its OK line on stderr, so both streams are merged here.
Success = process rc 0 AND an 'OK <remote> ... sha256=...' line.
Exit codes: 0 uploaded | 1 failed.
"""
import argparse
import subprocess
import sys
from pathlib import Path

# scripts/ -> apk-device-deploy/ -> skills/ -> .trae/ -> workspace root
REPO_ROOT = Path(__file__).resolve().parents[4]
CMD_PY = REPO_ROOT / "client" / "cmd_client_mqtt.py"

# The bundled miniforge python is the interpreter known to carry paho/deps.
DEFAULT_PYTHON = r"C:\QGB\miniforge3\python.exe"


def main():
    parser = argparse.ArgumentParser(description="put a local file onto the remote build host via MQTT")
    parser.add_argument("local", help="Local file path")
    parser.add_argument("remote", help="Absolute remote destination path")
    parser.add_argument("-t", "--topic", default="q")
    parser.add_argument("-k", "--key", default="2**128")
    parser.add_argument("--python", default=DEFAULT_PYTHON, help="Interpreter to run cmd_client_mqtt.py")
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args()

    local = Path(args.local)
    if not local.is_file():
        print(f"local file not found: {local}", file=sys.stderr)
        return 1

    cmd = [
        args.python, str(CMD_PY),
        "-t", args.topic, "-k", args.key,
        "put", str(local), args.remote,
    ]
    r = subprocess.run(
        cmd, cwd=str(REPO_ROOT), capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=args.timeout,
    )
    out = (r.stdout or "") + (r.stderr or "")
    print(out)

    ok = r.returncode == 0 and f"OK {args.remote}" in out and "sha256=" in out
    if not ok:
        print(f"UPLOAD FAILED rc={r.returncode}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
