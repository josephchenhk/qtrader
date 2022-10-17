# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: config.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

"""
ClickHouse：

# 寻找clickhouse镜像
> docker search clickhouse
# 拉取镜像
> docker pull yandex/clickhouse-server
# 启动clickhouse容器
> docker run -d --name clickhouse-server --volume=/Users/joseph/Dropbox/code/qtrader/strategies/data/clickhouse:/var/lib/clickhouse --ulimit nofile=262144:262144 -p 8123:8123 -p 9000:9000 yandex/clickhouse-server

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
    "broker_account": "U6680457",  # "DU4267228", # "U6680457", # "DU4267228",
    "host": "127.0.0.1",
    "port": 7496,  # 7497, # 7496, # 7497,
    "clientid": 1,
    "pwd_unlock": -1,
}

CQG_GATEWAY = {
    "broker_name": "CQG",
    "broker_account": "JChenSim",
    "password": "pass",
    "host": "127.0.0.1",
    "port": 2823,
}

FUTU_GATEWAY = {
    "broker_name": "FUTU",
    "broker_account": "17149563",
    "host": "127.0.0.1",
    "port": 11111,
    "pwd_unlock": 314159,
}

FUTUFUTURES_GATEWAY = {
    "broker_name": "FUTUFUTURES",
    "broker_account": "715823",  # "17149563",
    "host": "127.0.0.1",
    "port": 11111,
    "pwd_unlock": 314159,
}

GATEWAYS = {
    "Ib": IB_GATEWAY,
    "Backtest": BACKTEST_GATEWAY,
    # "Cqg": CQG_GATEWAY,
    # "Futufutures": FUTUFUTURES_GATEWAY
}

TIME_STEP = 15 * 60 * 1000  # time step in milliseconds

DATA_PATH = {
    "kline": "/Users/joseph/Dropbox/code/data/data/k_line", # "k1m" is must have
    # "kline": "C:/Users/josephchenj/data/k_line/K_1M", # "k1m" is must have
}

DATA_MODEL = {
    "kline": "Bar",
}

DB = {
    "sqlite3": "/Users/joseph/Dropbox/code/stat-arb/data/sqlite3"
}

ACTIVATED_PLUGINS = ["analysis"]#, "sqlite3"]  # "telegram", "sqlite3", "monitor"

TELEGRAM_TOKEN = "XXX"
TELEGRAM_CHAT_ID = 123
IGNORE_TIMESTEP_OVERFLOW = True  # default False
DATA_FFILL = True
AUTO_OPEN_PLOT = False

