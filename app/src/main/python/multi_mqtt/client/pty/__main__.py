#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""``python -m client.pty`` 等价 ``python client/pty_client_mqtt.py``。"""
import sys

from . import main

if __name__ == "__main__":
    sys.exit(main())
