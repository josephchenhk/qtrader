# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: qtrader_config.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""


BACKTEST_GATEWAY = {
    "broker_name": "BACKTEST",
    "broker_account": "",
    "host": "",
    "port": -1,
    "pwd_unlock": -1,
}

IB_GATEWAY = {
    "broker_name": "IB",
    "broker_account": "",
    "host": "127.0.0.1",
    "port": 7497,
    "clientid": 1,
    "pwd_unlock": -1,
}

CQG_GATEWAY = {
    "broker_name": "CQG",
    "broker_account": "Demo",
    "password": "pass",
    "host": "127.0.0.1",
    "port": 2823,
}

FUTU_GATEWAY = {
    "broker_name": "FUTU",
    "broker_account": "TEST123456",
    "host": "127.0.0.1",
    "port": 11111,
    "pwd_unlock": 123456,
}

FUTUFUTURES_GATEWAY = {
    "broker_name": "FUTUFUTURES",
    "broker_account": "TEST123456",
    "host": "127.0.0.1",
    "port": 11111,
    "pwd_unlock": 123456,
}

GATEWAYS = {
    "Ib": IB_GATEWAY,
    "Backtest": BACKTEST_GATEWAY,
    "Cqg": CQG_GATEWAY,
    "Futu": FUTU_GATEWAY,
    "Futufutures": FUTUFUTURES_GATEWAY
}

TIME_STEP = 60000  # time step in milliseconds

DATA_PATH = {
    "kline": "C:/Users/josephchen/data/k_line",
}

DATA_MODEL = {
    "kline": "Bar",
}

DB = {
    "sqlite3": "/Users/qtrader/data"
}

CLICKHOUSE = {
    "host": "localhost",
    "port": 9000,
    "user": "default",
    "password": ""
}

ACTIVATED_PLUGINS = ["analysis"]
LOCAL_PACKAGE_PATHS = []
ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH = False

AUTO_OPEN_PLOT = True
IGNORE_TIMESTEP_OVERFLOW = False
TELEGRAM_TOKEN = ""
TELEGRAM_CHAT_ID = 1
DATA_FFILL = True