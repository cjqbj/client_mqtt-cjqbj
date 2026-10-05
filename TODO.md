# TODO

## Remaining
- Install the latest APK and verify on device: cold start restores the last selected target topic, bottom WeChat-style feature bar + drawer stay in sync, long-press feature shows the reload dialog and picks up edited `py_updates` files, and raw Python stdout/stderr render verbatim in feature pages and RPC logs.
- Install the import-fix APK on the reported device and confirm Wi-Fi RPC no longer crashes; the exact host-side `233` request succeeds, but ADB is unavailable in the container.
- Run device tests for bidirectional target settings sync, first-use external script download and retry log on the target network, two responders, remote scan pagination, file/Aliyun transfer, both cameras, permission flows, external storage, and runtime feature installation.

## Completed
- Selected target (`selected_device_id`) persisted in `client_mqtt.json` and restored on app restart.
- Feature switching moved to a WeChat-style bottom navigation bar; drawer/bottom lists are fully catalog-driven (manifest `icon`, dynamic missing-files list).
- Feature hot reload: one-time py_updates shadow takeover, cached afterwards, explicit reload via long-press menu (`reload_feature`).
- Raw Python stdout/stderr/error surfaced verbatim in RPC logs and feature result views.
- Python service and structured feature dispatch.
- Per-topic request routing and remote-root settings, plus one shared Aliyun JSON configuration.
- Stable per-target IDs, automatic form-to-file persistence, external-file polling, and private-key/timeout/signature-fallback controls.
- Bounded recursive scan with path and symlink protections.
- Target-side file/photo upload and client-side download/preview flow.
- Wi-Fi RPC validation and focused Python tests.
- Debug APK compilation, signing, and package metadata verification.
- Chaquopy-safe static discovery of the three built-in feature modules.
- Python downloader with timeout, GitHub host fallback, retries, and visible progress logs.
- Camera RPC generation and capture logic centralized in `feature_camera.py`.
- Wi-Fi query generation and files scan/upload code generation owned by their respective feature modules.
- Compose JSON rendering and camera photo download/preview bridge.
- Catchable Python feature failures, including `SystemExit`, reported as structured UI errors instead of escaping into the Activity.
- Current-directory file browsing with folder-first rows, tap-to-enter, parent navigation, corrected pagination, and file-only downloads.
- Preserve and display server error details when target RPC returns `ok: false` and `r: null`.
- In-app RPC diagnostics with broker state and credential/source redaction.
- Private-key standardization button backed by the upstream PEM normalizer.
- Copyable RPC diagnostics and Wi-Fi result, including sanitized key configuration state.
- Configurable per-target online health probing deferred by successful requests.
