@echo on
chcp 65001
cd /d "%~dp0"
:: ===== 基础配置（可按需修改） =====
set "PY_PATH=C:\QGB\miniforge3\python.exe"

"%PY_PATH%" client_mqtt.py

