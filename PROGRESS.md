# Progress

## Current state
- Phase: feature implementation complete; device integration validation remains.
- Completed: Android Compose app with feature pager, left-edge drawer for scripts and targets, online status, permissions, and local script-root selection.
- Completed: per-topic target configuration with stable IDs; request topic, remote root, private key, timeout, and signature fallback are automatically saved and refreshed from external file edits. Aliyun JSON is one shared, synchronized app setting.
- Completed: Python service, bounded remote scan pagination, file transfer, in-memory photo capture, Wi-Fi query, and runtime feature loading.
- Completed: compatible loading of the vendored flat `multi_mqtt.py` library from the app's same-name directory; generated library files remain build-synchronized.
- Completed: static catalog for the three Chaquopy-bundled features, even when the APK has no loose `.py` source entries.
- Completed: Python installer for missing files/camera/wifi scripts in an external `py_updates/`, with alternate GitHub URLs, timeouts, retries, atomic writes, and a visible polling log.
- Completed: moved camera target-code generation into `feature_camera.py`; the Activity only dispatches capture actions.
- Completed: moved Wi-Fi query generation and bounded scan/upload generation into `feature_wifi.py` and `feature_files.py`; `client_service` no longer exposes those feature-specific helpers.
- Completed: feature actions return explicit JSON; Compose formats Wi-Fi data and downloads/decodes photo URLs for preview.
- Completed: files scan includes directory entries and correct pagination; Compose supports folder navigation, parent navigation, and file-only download actions.
- Completed: null RPC results become structured errors with target traceback summaries; camera choice uses a dropdown and camera failures render server error details.
- Completed: fixed Chaquopy flat-module import crash reported by device log; `client_mqtt.py` loads as a top-level sibling of flat `multi_mqtt.py`.
- Completed: added settings back handling, migrated Aliyun to shared configuration, passed key expressions to upstream normalization, and surfaced Wi-Fi failures in the page.
- Completed: feature dispatcher converts Python exceptions and `SystemExit` to structured UI errors; broken runtime feature regression test passes.
- Completed: topic settings can normalize private-key expressions to PEM using upstream `get_standard_pem_bytes`.
- Completed: Add target defaults to `sys/device/request`; existing target edits keep their stored topic.
- Completed: the old private-key test fixture was replaced with `233` in upstream `multi_mqtt` and synchronized client/xime copies; no tracked file contains the old literal.
- Completed: Add target initializes request topic to `sys/device/request` while edits to existing targets retain their stored topic.
- Completed: removed the previously embedded key-expression example from the upstream and synchronized MQTT test fixtures; placeholders now use `233`.
- Completed: App RPC diagnostics show/copy redacted request code, complete MQTT envelope/raw `r` and target errors, topic/reply topic, key presence/type/normalized length, elapsed time, and broker states; Wi-Fi output is selectable/copyable.
- Completed: RPC metadata is attached to feature results; shared online probe enable/interval settings track health per topic and successful RPCs defer the next probe.
- Completed: Wi-Fi RPC through `feature_wifi.info()` with the user's `233` key, `sys/device/k12`, and 5-second timeout returned `192.168.1.106`; a prior probe used a different test key and timed out.
- Completed: Python tests pass (`33/33`); `./debug_build_secexp.sh` generated and verified `out/com.qgb.client-1-arm64-v8a.apk` with hierarchical file browsing and target-error reporting.
- Pending: install and exercise the app UI on Android; validate two responders, pagination, file/Aliyun transfer, both cameras, permissions, external storage, and runtime feature installation.

## Last validation
- `bash -n debug_build_secexp.sh`: passed.
- `python3 -m unittest discover -s tests`: passed (`33/33`), including folder pagination, server traceback/raw response, key status, broker diagnostics, copy-safe redaction, and null handling.
- Upstream and xime MQTT fixture suites: eight tests pass; one pre-existing test fails because it passes unsupported `server_public_key_bytes` to `MQTTClientNode`.
- Wi-Fi feature RPC with the exact `233` sample parameters: passed; returned `192.168.1.106` and MAC metadata.
- `./debug_build_secexp.sh`: passed; APK metadata reports version code `1` and signature verification passed.
- Client tests passed after upstream sync (`30/30`); exact private-key example search is clean in client, multi_mqtt, and xime tracked files.
- Kotlin `:app:compileDebugKotlin`: passed; only existing deprecation warnings remain.
- Kotlin/Python workspace diagnostics and `git diff --check`: passed.
- Full Android UI/device integration suite, including live external-file refresh and GitHub download on a device: not run.
- The reported Android runtime stack trace identifies the flat-module import issue; the fix is compiled and covered by a flat-layout test, but not yet installed on that device.
- ADB is not installed in the development container, so installation-level verification on the reported device could not be run.

## Next step
Install the generated APK and validate external script downloads on the target network, the visible retry log, feature override, camera capture/preview, and remaining device workflows.
