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
- Completed: selected target is persisted as `selected_device_id` in `client_mqtt.json` and restored on cold start; the UI no longer jumps to the first catalog topic after restart.
- Completed: feature switching moved from the top tab row to a WeChat-style bottom navigation bar; drawer and bottom bar render from the same catalog state, icons resolve from the manifest `icon` hint, and the settings missing-files list comes from `client_service.builtin_feature_files()` instead of a hardcoded Kotlin list.
- Completed: feature module caching policy reworked — built-in shadowed by a new `py_updates` file is taken over once, loaded modules stay cached, and long-pressing a feature (drawer/bottom bar) shows a reload dialog backed by `client_service.reload_feature(name)`.
- Completed: feature result pages and the RPC diagnostics page show raw Python stdout/stderr/error text verbatim (Python-side sections, no Kotlin reformatting).
- Completed: real-device UI fix for unresponsive bottom-feature switching — the old items wrapped the icon/label in an empty `combinedClickable` whose child swallowed taps on the icon touch area; the bottom bar and drawer feature rows are now custom rows driven by a single `combinedClickable` (tap switches, long-press reloads), and the cold-start restore race that overwrote the persisted target with the `sys/device/request` placeholder was removed (`restoreDone` gate, stable-ID sync only). Page composables receive the Python service handle as a parameter, eliminating first-switch JNI lookups on the main thread.
- Completed: on a real device (angler, 1440x2560) the rebuilt APK restores the persisted `k12` target after force-stop restart, switches tabs instantly on icon tap, opens the long-press reload dialog from both the bottom bar and the drawer, and switches drawer targets without jank.
- Completed: fixed the Aliyun settings editor wiping its content a moment after pasting — the 1s shared-config poll used `has()` + `optString()` on a always-present `aliyun_json_draft` key (null when empty); on old Android org.json the null collapses to the literal `"null"` (newer versions to `""`), overwriting the field after the 1.2s idle gate. Poll now distinguishes null with `isNull()`, never overwrites unparseable user text (only historical garbage drafts `""`/`"null"` self-heal), parsing tolerates a leading BOM and trims whitespace, and the text field shows an inline red error with the parser message while keeping the text editable and saved as a draft; the last valid `aliyun` object stays active.
- Completed: per-target UI memory framework — `selected_feature` remembers each target's last visible feature tab (restored once per target on cold start/switch, `featureRestoredFor` gate prevents the placeholder page from overwriting the persisted value), and `feature_settings` persists an opaque JSON object per `(target, feature)` that any feature can read/write through `client_service.feature_settings(name)` / `update_feature_settings(name, values)` (shallow-merge, keyed by stable target id); the camera page replaced its button+dropdown with two explicit FilterChips (Back/Front) whose `lens_facing` choice survives restarts. Convention documented in FEATURE_DEVELOPMENT.md "Per-target memory".
- Pending: continue exercising two responders, pagination, file/Aliyun transfer, both cameras, permissions, external storage, and runtime feature installation on the device.

## Last validation
- Real-device round (Nexus 6P, SDK 27, `192.168.1.111:5555`): APK rebuilt after the per-target memory change (`BUILD_RC=0`, sha256 `8b8cb957…453d83`) and installed. Verified: bjhtw picks Front camera chip + Wi-Fi/Files tabs → `client_mqtt.json` gains `feature_settings.61dd….camera.lens_facing=1` and `selected_feature.61dd…`; k12 picks Camera → `selected_feature.18fc…="camera"`; switching drawer targets auto-restores each target's own tab (bjhtw→Files); force-stop cold start restores bjhtw+Files; Camera page shows two explicit FilterChips with Front still selected after restart.
- `python3 -m unittest discover -s tests`: passed (`42/42` locally and on the remote build host), including the new per-device `selected_feature` persistence, per-device/per-feature `feature_settings` isolation + shallow merge, and invalid-input rejection tests.
- Real-device round (Nexus 6P, SDK 27, `192.168.1.111:5555`): APK rebuilt after the Aliyun editor fix (`BUILD_RC=0`, sha256 `e55d5420…1dc37`) and installed; the stale `"null"` draft left by the old bug self-healed on open (field restored to the saved Aliyun object, draft popped on disk), intentionally malformed JSON keeps its text across poll cycles and settings reopen with an inline red parser error while the valid config stays active, and repairing the JSON saves normally.
- Real-device round (`192.168.1.111:5555`, angler): APK `out/com.qgb.client-1-arm64-v8a.apk` rebuilt after the custom-row/restore-race fixes (`BUILD_RC=0`, sha256 `9f4d03a0…99bf`) and installed; force-stop cold start restores `k12`, icon taps switch pages immediately (steady-state gfxinfo: 99th percentile 150 ms across rapid switching), and long-press reload dialogs were confirmed on both the bottom Wi-Fi item and the drawer Files item via raw touch-event injection.
- `bash -n debug_build_secexp.sh`: passed.
- `python3 -m unittest discover -s tests`: passed (`39/39` locally and on the remote build host), including the new selected-device persistence, raw stdout/stderr passthrough and RPC log sections, built-in catalog bridge, manual reload bridge, cache-stability/shadow-takeover, folder pagination, server traceback/raw response, key status, broker diagnostics, copy-safe redaction, and null handling. `init_env` starts its RPC server once per process so repeated inits no longer collide on port 1166 under Linux.
- `./debug_build_secexp.sh`: passed after the selected-device/bottom-nav/reload/raw-output changes; `out/com.qgb.client-1-arm64-v8a.apk` regenerated, signed (apksigner V3 verify passed), aapt badging reports version code `1`.
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
