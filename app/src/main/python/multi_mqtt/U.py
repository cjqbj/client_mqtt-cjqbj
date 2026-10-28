# -*- coding: utf-8 -*-
import os, re, platform, subprocess

def _cmd(args, timeout=5):
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=timeout, errors="ignore")
        if out.returncode == 0:
            v = (out.stdout or "").strip()
            return v or None
    except Exception:
        pass
    return None

def _file(path, n=512):
    try:
        with open(path, "r", errors="ignore") as f:
            v = f.read(n).strip()
            return v or None
    except Exception:
        return None

def _prop(name):
    return _cmd(["getprop", name])

def _is_android():
    return (os.path.exists("/system/build.prop") or os.path.exists("/system/bin/getprop")
            or os.environ.get("ANDROID_ROOT") or os.environ.get("ANDROID_DATA")
            or "android" in platform.platform().lower())

def _safe_getter(fn):
    def wrapper(*args, **kwargs):
        try:
            v = fn(*args, **kwargs)
            return v if isinstance(v, str) and v.strip() else ""
        except Exception:
            return ""
    return wrapper


@_safe_getter
def get_manufacturer():
    if _is_android():
        v = (_prop("ro.product.manufacturer") or _prop("ro.product.vendor.manufacturer")
             or _prop("ro.product.system.manufacturer"))
        if v:
            return v
        try:
            from java import jclass
            v = str(jclass("android.os.Build").MANUFACTURER)
            if v and v != "unknown":
                return v
        except Exception:
            pass
        for p in ("/system/build.prop", "/system/system/build.prop", "/vendor/build.prop", "/odm/etc/build.prop"):
            txt = _file(p, 8192)
            if txt:
                m = re.search(r"^ro\.product\.manufacturer=(.+)$", txt, re.M)
                if m:
                    return m.group(1).strip()
        return ""

    sysname = platform.system().lower()
    if sysname == "windows":
        out = _cmd(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                    "$cs=Get-CimInstance Win32_ComputerSystem; Write-Output $cs.Manufacturer"])
        if out:
            return out
        try:
            import wmi
            v = wmi.WMI().Win32_ComputerSystem()[0].Manufacturer
            if v:
                return v
        except Exception:
            pass
        out = _cmd(["wmic", "csproduct", "get", "vendor", "/format:list"])
        if out:
            for line in out.splitlines():
                if line.strip().lower().startswith("vendor="):
                    v = line.split("=", 1)[1].strip()
                    if v:
                        return v
        return ""
    if sysname == "darwin":
        return "Apple"
    for p in ("/sys/devices/virtual/dmi/id/sys_vendor", "/sys/class/dmi/id/sys_vendor",
              "/sys/firmware/devicetree/base/vendor", "/proc/device-tree/vendor"):
        v = _file(p)
        if v:
            return v
    out = _cmd(["dmidecode", "-s", "system-manufacturer"])
    return out or ""


@_safe_getter
def get_model():
    if _is_android():
        v = (_prop("ro.product.model") or _prop("ro.product.vendor.model")
             or _prop("ro.product.system.model"))
        if v:
            return v
        try:
            from java import jclass
            v = str(jclass("android.os.Build").MODEL)
            if v and v != "unknown":
                return v
        except Exception:
            pass
        for p in ("/system/build.prop", "/system/system/build.prop", "/vendor/build.prop", "/odm/etc/build.prop"):
            txt = _file(p, 8192)
            if txt:
                m = re.search(r"^ro\.product\.model=(.+)$", txt, re.M)
                if m:
                    return m.group(1).strip()
        return ""

    sysname = platform.system().lower()
    if sysname == "windows":
        out = _cmd(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
                    "$cs=Get-CimInstance Win32_ComputerSystem; Write-Output $cs.Model"])
        if out:
            return out
        try:
            import wmi
            v = wmi.WMI().Win32_ComputerSystem()[0].Model
            if v:
                return v
        except Exception:
            pass
        out = _cmd(["wmic", "csproduct", "get", "model", "/format:list"])
        if out:
            for line in out.splitlines():
                if line.strip().lower().startswith("model="):
                    v = line.split("=", 1)[1].strip()
                    if v:
                        return v
        return ""
    if sysname == "darwin":
        out = _cmd(["sysctl", "-n", "hw.model"])
        if out:
            return out
        out = _cmd(["system_profiler", "SPHardwareDataType"])
        if out:
            m = re.search(r"Model Identifier:\s*(.+)", out)
            if m:
                return m.group(1).strip()
        return ""
    for p in ("/sys/devices/virtual/dmi/id/product_name", "/sys/class/dmi/id/product_name",
              "/proc/device-tree/model"):
        v = _file(p)
        if v:
            return v
    out = _cmd(["dmidecode", "-s", "system-product-name"])
    return out or ""


@_safe_getter
def get_system_id():
    if _is_android():
        # 优先取硬件序列号，刷机不易变化；Android ID 作为次选
        v = (_prop("ro.serialno") or _prop("ro.boot.serialno") or _prop("ro.boot.serial")
             or _file("/sys/block/mmcblk0/device/cid")
             or _file("/sys/block/mmcblk0/device/serial")
             or _file("/sys/block/sda/device/serial"))
        if v:
            return v
        try:
            from java import jclass
            from android.provider import Settings
            from android.app import ActivityThread
            ctx = ActivityThread.currentApplication()
            v = str(Settings.Secure.getString(ctx.getContentResolver(), Settings.Secure.ANDROID_ID))
            if v and v != "9774d56d682e549c":   # 过滤已知异常值
                return v
        except Exception:
            pass
        return ""

    sysname = platform.system().lower()
    if sysname == "windows":
        out = _cmd(["reg", "query", r"HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Cryptography",
                    "/v", "MachineGuid"])
        if out:
            m = re.search(r"MachineGuid\s+REG_SZ\s+(\S+)", out)
            if m:
                return m.group(1).strip()
        return ""
    if sysname == "darwin":
        out = _cmd(["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"])
        if out:
            m = re.search(r'"IOPlatformUUID"\s*=\s*"([^"]+)"', out)
            if m:
                return m.group(1).strip()
        return ""
    # Linux / 其它类 Unix
    for p in ("/var/lib/dbus/machine-id", "/etc/machine-id"):
        v = _file(p, 64)
        if v:
            return v
    return ""
    
def get_machine_id():
    mf=get_manufacturer().replace(" ", "")[:9]
    mo=get_model().replace(" ", "")[:14]
    return f"{mf}_{mo}_{get_system_id()[:6]}"
get_machine_request_topic=get_machine_id
