# Feature Script Development

## File location

A feature is a Python file named `feature_<name>.py`.

- Bundled feature: `app/src/main/python/feature_<name>.py`
- Runtime feature: `<script-root>/py_updates/feature_<name>.py`
- Runtime feature files override bundled files with the same name.
- The selected script root is internal app storage by default, or `/sdcard/apm/client_mqtt/` after the user authorizes external storage and enables it in Settings.

## Required manifest

```python
FEATURE = {
    "name": "demo",
    "title": "Demo",
    "version": 1,
    "actions": ["run"],
}
```

The `title` appears in the script drawer and feature page. If omitted, the filename name is used. `actions` controls the buttons shown on the generic page.

## Action API

Each action is a top-level function with positional arguments:

```python
import json


def run():
    return json.dumps({"ok": True, "message": "hello"}, ensure_ascii=False)
```

The main app calls the stable Python bridge:

```text
client_service.call_feature(feature_name, action_name, *args)
```

A feature must not import Compose or Android Activity classes. It owns its domain-specific code generator and action, uses the generic `client_service.rpc(code)` bridge, and returns JSON for the UI. Long files and images use the target-side Aliyun flow and return short metadata; never return large bytes through MQTT.

`client_service.call_feature` converts feature Python exceptions, including `SystemExit`, into JSON with `ok`, `feature`, `action`, and `error`. A broken downloadable Python feature therefore reports an action failure instead of propagating the Python exception into the Android Activity. This cannot catch native/JVM process crashes or forced process termination.

## Shared services

- `client_service.rpc(code, device=None)` sends short target control code through the selected target's request topic, or an explicitly supplied device.
- `client_service.device_catalog()`, `device_settings(topic)`, `select_device(topic)`, and `update_device_settings(topic, values)` manage per-topic target configuration.
- `client_service.aliyun_settings()` and `update_aliyun_settings(values)` read and write the single shared Aliyun configuration.
- `client_service.install_builtin_features(script_root, retries, timeout)` installs the three built-in feature scripts into the selected root's `py_updates/` directory.
- `client_service.operation_logs()` returns the rolling downloader log for UI display.
- `client_service.rpc_logs()` and `clear_rpc_logs()` expose and clear the redacted RPC diagnostic ring buffer.
- `client_service.standardize_private_key(value)` normalizes key expressions and PEM/OpenSSH inputs through upstream `get_standard_pem_bytes`.
- `client_service.general_settings()` and `update_general_settings(values)` manage the app-wide online probe toggle and interval.
- `client_service.target_health(device_ref)` returns per-target last RPC/probe state and in-flight count.
- Successful feature results retain transport fields such as request ID, server time, responding brokers, latency, and elapsed time in `_rpc`; Compose can display or copy them.
- `feature_files.scan(...)` generates and sends bounded target scanning code.
- `feature_files.upload(path)` generates and sends target-side Aliyun upload code.
- `feature_wifi.info()` builds the Wi-Fi query code in `feature_wifi.py` and sends it through the generic RPC bridge.
- `feature_camera.capture(facing)` builds capture code in `feature_camera.py`; Compose downloads and displays the returned photo URL.
- `client_service.download_transfer(url, config, save_to)` downloads outside MQTT.
- `client_service.update_settings(...)` persists app-level configuration beside the active script root; use `update_device_settings(...)` for target-specific values.

Each target record has a stable `id` and its own `request_topic`, `remote_root`, private key, timeout, and server-signature fallback option. Aliyun JSON is global and stored once at the root of `client_mqtt.json`, not in target records. Both forms automatically write edits after a short debounce and poll the file once per second for external changes. Invalid in-progress Aliyun JSON is retained as a draft while the last valid object remains active. Before executing feature code, the RPC bridge initializes the target-side Aliyun configuration from the global setting. Private-key strings are passed unchanged to `multi_mqtt.get_standard_pem_bytes`; it supports integer expressions, including the configured value `233`, and key-file/PEM inputs.

Each feature should be independently callable through `client_service.call_feature` and must not rely on another feature's code generator. Keep Android pages limited to presentation and dispatch; do not duplicate Wi-Fi, scan, upload, or camera RPC code in the Activity or generic service. The settings page downloads missing scripts through Python with per-request timeouts, alternating GitHub URLs, up to four attempts, and progress messages shown in the download log.

## Runtime installation

A downloaded module is installed atomically and optionally verified:

```python
client_service.install_feature(url, "feature_demo.py", sha256)
```

Only `feature_*.py` filenames are accepted. The module is discovered on the next catalog refresh and loaded on its next action call. Do not execute untrusted scripts without verifying the SHA-256 digest and transport authentication.

## UI communication

The UI never imports feature modules directly. It polls `client_service.feature_catalog()`, creates a pager entry for each descriptor, and invokes actions through `client_service.call_feature`. This keeps the Android APK stable while allowing feature scripts to evolve independently.

Successful MQTT envelopes are preserved under the feature result's `_rpc` key with request ID, server time, broker names, latency, and elapsed time. The RPC diagnostics page shows and copies the full submitted code and response envelope/raw `r`, after redacting configured key and Aliyun values. Use this metadata and the diagnostics page to inspect target traceback/error details and transport behavior. The periodic online probe is shared-configurable and is deferred after any successful RPC for that target.
