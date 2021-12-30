# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: config.py

BACKTEST_GATEWAY = {
    "broker_name": "BACKTEST",
    "broker_account": "",
    "host": "",
    "port": -1,
    "pwd_unlock": -1,
}

FUTU_GATEWAY = {
    "broker_name": "FUTU",
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
    "broker_account": "",
    "GWAccountID": 1,
    "password": "",
    "host": "127.0.0.1",
    "port": 2823,
}

GATEWAYS = {
    "Backtest": BACKTEST_GATEWAY,
    "Futu": FUTU_GATEWAY,
    "Ib": IB_GATEWAY,
    "Cqg": CQG_GATEWAY
}

TIME_STEP = 60000  # 设置事件框架监测步长 ms

DATA_PATH = {
    "kline": "path_to_your_qtrader_folder/examples/data/k_line/K_1M",
}

DATA_MODEL = {
    "kline": "Bar",
}

DB = {
    "sqlite3": "path_to_your_qtrader_folder/examples/data/sqlite3"
}

ACTIVATED_PLUGINS = ["analysis"]