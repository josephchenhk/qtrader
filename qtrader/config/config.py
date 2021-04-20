# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:04 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: config.py
# @Software: PyCharm

GATEWAY = {
    "broker_name": "FUTU",
    "broker_account": "654321",
    "host": "127.0.0.1",
    "port": 11111,
    "pwd_unlock": 12345,
}


DATA_PATH = {
    "k1m": "examples/data/k_line/K_1M", # "k1m" is must have
}

DATA_MODEL = {
    "k1m": "Bar",
}

DB = {
    "sqlite3": "examples/data/sqlite3"
}

ACTIVATED_PLUGINS = ["analysis", "sqlite3"]