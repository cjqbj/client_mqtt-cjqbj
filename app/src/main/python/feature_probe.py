"""最小在线探针（feature）。

向指定 request topic 发一段最轻量的代码，2 秒超时，目标在线时返回：
sys.executable / platform.node() / platform.machine() / platform.release()。

用途：
- 在跑重 RPC 前快速判断目标在不在线、架构对不对；
- 不依赖任何业务模块，目标端是裸 multi_mqtt 也能应答。

只依赖 client_service 里稳定的配置/RPC 桥，不假设当前选中设备的超时
（默认全局超时 10s 对探针太长，这里固定 2s、topic 默认 q）。
"""
import ast
import json

import client_service

FEATURE = {
    "title": "Probe",
    "actions": ["run"],
    "icon": "bug",
    "version": 1,
}

# 注意：末行必须是赋值给 r 的表达式。远端执行器取末尾表达式/ r 变量，
# 经 repr 风格格式化后回传，这里用 ast.literal_eval 还原成 tuple。
PROBE_CODE = (
    "import platform,sys\n"
    "r = (sys.executable, platform.node(), platform.machine(), platform.release())"
)
DEFAULT_TOPIC = "q"
DEFAULT_TIMEOUT = 2.0


def _request(topic=DEFAULT_TOPIC, timeout=DEFAULT_TIMEOUT):
    """直接复用底层共享 MQTT 客户端，绕过全局 10s 超时。"""
    selected = client_service._device_config(None)
    mqtt_client = client_service._mqtt_client_module()
    return mqtt_client.rpc(
        PROBE_CODE,
        request_topic=str(topic or DEFAULT_TOPIC),
        timeout=float(timeout or DEFAULT_TIMEOUT),
        client_private_key_bytes=selected.get("private_key") or None,
        allow_no_server_pubkey_response=bool(
            selected.get("allow_no_server_pubkey_response", False)
        ),
    )


def _parse_tuple(raw):
    """远端回传的是 tuple 的 repr/pprint 文本，literal_eval 安全还原。"""
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = ast.literal_eval(raw.strip())
    except (SyntaxError, ValueError):
        return None
    return value if isinstance(value, tuple) else None


def run(topic=DEFAULT_TOPIC, timeout=DEFAULT_TIMEOUT):
    """UI 动作入口；始终返回 JSON 字符串。"""
    topic = str(topic or DEFAULT_TOPIC)
    timeout = float(timeout or DEFAULT_TIMEOUT)
    try:
        response = _request(topic, timeout)
    except Exception as error:
        return json.dumps(
            {"ok": False, "topic": topic, "timeout": timeout,
             "error": f"{type(error).__name__}: {error}"},
            ensure_ascii=False,
        )

    if not isinstance(response, dict) or response.get("r") is None:
        detail = ""
        if isinstance(response, dict):
            detail = str(response.get("error") or "").strip()
        return json.dumps(
            {"ok": False, "topic": topic, "timeout": timeout,
             "elapsed_ms": response.get("elapsed_ms") if isinstance(response, dict) else None,
             "error": detail or "no result before timeout (target offline?)"},
            ensure_ascii=False,
        )

    raw = response.get("r")
    result = {
        "ok": bool(response.get("ok", True)),
        "topic": topic,
        "timeout": timeout,
        "elapsed_ms": response.get("elapsed_ms"),
        "raw": raw,
    }
    values = _parse_tuple(raw)
    if values and len(values) >= 4:
        result.update(
            executable=values[0],
            node=values[1],
            machine=values[2],
            release=values[3],
        )
    else:
        # 目标有应答但回的不是约定 4-tuple：契约不符，整体按失败呈现（raw 保留备查）。
        result["ok"] = False
        result["error"] = "target answered but result is not a 4-tuple (see raw)"
    return json.dumps(result, ensure_ascii=False)
