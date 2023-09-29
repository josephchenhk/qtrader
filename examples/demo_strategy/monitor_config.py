# -*- coding: utf-8 -*-
# @Time    : 27/4/2023 4:31 pm
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: monitor_config.py

"""
Copyright (C) 2022 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the terms of the JXW license, 
which unfortunately won't be written for another century.

You should have received a copy of the JXW license with this file. If not, 
please write to: josephchenhk@gmail.com
"""

instruments = {
    "Demo_Strategy": {
        "Backtest": {
                "security": ["FUT.GC", "FUT.SI"],
                "lot": [100, 5000],
                "commission": [1.98, 1.98],
                "slippage": [0.0, 0.0],
                "show_fields": {}
            }
    },
}