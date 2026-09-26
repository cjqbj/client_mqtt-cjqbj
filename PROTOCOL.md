# Protocol

## Target selection and setup
The selected target record determines the MQTT `request_topic` and connection options for each RPC. Shared Aliyun configuration is stored once at the root of `client_mqtt.json`. Before submitted feature code runs, the client prepends a short Python setup block which merges this global Aliyun JSON object into `sys._qgb_dict["aliyun_git"]`. This does not add fields to the MQTT request envelope; it initializes the target interpreter for the existing code payload. Do not log the configuration because it may contain credentials.

## RPC envelope metadata
The feature result retains MQTT response metadata under `_rpc`: `req_id`, `server_time`, `server_from`, `client_from`, `latency_ms`, and local `elapsed_ms`/`request_id` when available. The app diagnostics log records the sanitized request code and full MQTT response envelope/raw `r`, including target `error`, plus request/reply topic, key configured/type/normalized length, phase, and broker states. Key contents and Aliyun values must be redacted before storing/copying request or response text.

## Browse request
The generated target code receives:

- `root`: configured allowed root on the target device
- `offset`: number of sorted file entries to skip
- `limit`: maximum entries for this page
- `recursive`: defaults to `false` for the interactive browser; recursive scans may be requested explicitly.

The result must be JSON with `ok`, `root`, `items`, `has_more`, and `next_offset`. Each item contains a relative path, `kind` (`directory` or `file`), size, modified time, and optional error. Directory entries use `os.stat` without following symlinks and are listed before files. Pagination offset counts every entry encountered, including skipped entries. RPC `r` must be a JSON string so the client never parses Python pretty repr.

## Transfer result
A file transfer RPC returns only short metadata: `ok`, `url`, `name`, `size`, `content_type`, and optional `error`. The JPEG or file bytes never appear in the MQTT response.

## Wi-Fi result
The Wi-Fi feature returns JSON metadata under `wifi`, including `ssid`, `bssid`, `rssi`, `link_speed`, `frequency`, `ip`, and `mac`. The UI renders this JSON; it does not depend on Python repr formatting.

## Photo result
Photo RPC returns the same transfer metadata plus camera facing and capture duration. The target must convert the Java callback buffer to Python `bytes`, upload it directly, and release the camera in `finally` without creating a photo file.

## Errors
RPC responses use structured error fields. A server response with `ok: false` and `r: null` must preserve the server `error` traceback/details as a structured JSON error; never pass JSON null to a UI object parser. The client feature dispatcher returns `ok: false`, `feature`, `action`, and `error` when Python feature code raises, including `SystemExit`. The diagnostic log records only a redacted final error summary, never a private key, token, authorization header, or submitted RPC source. Native/JVM crashes and forced process termination cannot be recovered by the Python exception boundary.

## Hot-updatable features
Feature modules use the names `feature_files`, `feature_camera`, and `feature_wifi`. The stable dispatcher calls `bootstrap.call_feature(feature, action, *args)`. Runtime updates must be `feature_*.py` files written atomically into the app-private `files/py_updates/` directory. Optional SHA-256 verification is supported by `bootstrap.install_feature`; unsigned or path-traversal filenames are rejected.

The UI catalog refresh interval is three seconds. A script is not required to ship an Android class: its `FEATURE.title` is the pager title and its `FEATURE.actions` become generic action buttons. See `FEATURE_DEVELOPMENT.md` for the complete contract.
