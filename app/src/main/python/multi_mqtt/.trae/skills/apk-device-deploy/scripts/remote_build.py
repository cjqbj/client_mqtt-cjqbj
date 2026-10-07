#!/usr/bin/env python3
"""Build the qgb client APK on the HF Space through the shared MQTT PTY.

Serial-only: the ai_pty_* channel multiplexes one remote shell (the monitor
window is already bound to topic q / key 2**128). An overlapping call returns
{'ok': False, 'busy': True} immediately, so this script retries with backoff
until it acquires the shell, then blocks until the build finishes.

Outputs BUILD_RC, artifact size and sha256 (the sha feeds pull_apk.py).
Exit codes: 0 build ok | 1 build failed / PTY error / output unparseable.
"""
import argparse
import re
import sys
import time
from pathlib import Path

# scripts/ -> apk-device-deploy/ -> skills/ -> .trae/ -> workspace root
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from client.pty_client_mqtt import ai_pty_run  # noqa: E402


def run_serial(cmd, timeout, busy_wait_s=300.0):
    """Acquire the single PTY slot, retrying while another command holds it."""
    deadline = time.time() + busy_wait_s
    attempt = 0
    while True:
        attempt += 1
        r = ai_pty_run(cmd, timeout=timeout)
        if not r.get("busy"):
            return r
        if time.time() >= deadline:
            return r
        print(f"PTY busy (another command running), retry {attempt} in 10s...", flush=True)
        time.sleep(10)


def main():
    parser = argparse.ArgumentParser(description="Remote-build qgb client APK via MQTT PTY")
    parser.add_argument("--workdir", default="/root/build_xime_home/client")
    parser.add_argument("--artifact", default="out/com.qgb.client-1-arm64-v8a.apk")
    parser.add_argument("--timeout", type=float, default=540.0,
                        help="Max seconds for the build itself")
    parser.add_argument("--busy-wait", type=float, default=300.0,
                        help="Max seconds to wait for the PTY slot when busy")
    args = parser.parse_args()

    # Log to /tmp so PTY output stays tiny; print only rc, first compile errors,
    # size and sha. grep pattern matches Kotlin 'e:' and generic 'error:' lines.
    cmd = (
        f"cd {args.workdir} && "
        "./debug_build_secexp.sh > /tmp/client_build.log 2>&1; "
        "echo BUILD_RC=$?; "
        "grep -E 'e: |error:' /tmp/client_build.log | head -20; "
        f"stat -c %s {args.artifact}; "
        f"sha256sum {args.artifact}"
    )
    r = run_serial(cmd, timeout=args.timeout, busy_wait_s=args.busy_wait)
    out = r.get("out", "") or ""
    print(out)

    m_rc = re.search(r"BUILD_RC=(\d+)", out)
    m_sha = re.search(r"^([0-9a-f]{64})\s+" + re.escape(args.artifact), out, re.M)
    m_size = re.search(r"^(\d{6,})\s*$", out, re.M)

    if r.get("busy"):
        print("PTY stayed busy for the whole busy-wait window", file=sys.stderr)
        return 1
    if not r.get("ok"):
        print("PTY_OK=False timed_out=", r.get("timed_out"), file=sys.stderr)
        return 1

    rc = int(m_rc.group(1)) if m_rc else -1
    if rc != 0:
        print(f"BUILD FAILED rc={rc}; full log on remote: /tmp/client_build.log", file=sys.stderr)
        return 1
    if not m_sha:
        print("BUILD rc=0 but sha256 line missing — artifact may be stale/missing", file=sys.stderr)
        return 1

    print("=" * 60)
    print(f"ARTIFACT : {args.workdir}/{args.artifact}")
    print(f"SIZE     : {m_size.group(1) if m_size else '?'} bytes")
    print(f"SHA256   : {m_sha.group(1)}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
