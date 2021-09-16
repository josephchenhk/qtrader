# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: config.py
# @Software: PyCharm

BACKTEST_GATEWAY = {
    "broker_name": "BACKTEST",
    "broker_account": "",
    "host": "",
    "port": -1,
    "pwd_unlock": -1,
}

GATEWAYS = {
    "Backtest": BACKTEST_GATEWAY
}

TIME_STEP = 60000  # 设置事件框架监测步长 ms

DATA_PATH = {
    "k1m": "C:/Users/ABC/data/k_line/K_1M", # "k1m" is must have
}

DATA_MODEL = {
    "k1m": "Bar",
}

DB = {
    "sqlite3": "examples/data/sqlite3"
}

ACTIVATED_PLUGINS = ["analysis", "sqlite3"]