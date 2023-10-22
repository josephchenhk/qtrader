# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: qtrader_config.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

"""
Usage: Modify this config and put it in the same folder of your main script
"""

BACKTEST_GATEWAY = {
    "broker_name": "BACKTEST",
    "broker_account": "",
    "host": "",
    "port": -1,
    "pwd_unlock": -1,
}

GATEWAYS = {
    "Backtest": BACKTEST_GATEWAY,
}

# time step in milliseconds
TIME_STEP = 1 * 60 * 1000

DATA_PATH = {
    "kline": "path_to_your_data_folder/data/k_line",
}

DATA_MODEL = {
    "kline": "Bar",
}

ACTIVATED_PLUGINS = ["analysis"]

LOCAL_PACKAGE_PATHS = [
    "path_to_your_lib_folder/qtrader",
    "path_to_your_lib_folder/qtalib",
]
ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH = True

if "monitor" in ACTIVATED_PLUGINS:
    import monitor_config as MONITOR_CONFIG

IGNORE_TIMESTEP_OVERFLOW = False
AUTO_OPEN_PLOT = True

# if true, ffill the historical data
DATA_FFILL = True

# end: timestamp for bar 9:15-9:16 is stamped as 9:16
# start: timestamp for bar 9:15-9:16 is stamped as 9:15
BAR_CONVENTION = {
    'FUT.GC': 'start',
    'FUT.SI': 'start',
}

