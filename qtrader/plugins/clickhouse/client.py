# -*- coding: utf-8 -*-
# @Time    : 23/3/2021 6:57 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: client.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from clickhouse_driver import Client

from qtrader_config import CLICKHOUSE

client = Client(
    host=CLICKHOUSE["host"],
    port=CLICKHOUSE["port"],
    user=CLICKHOUSE["user"],
    password=CLICKHOUSE["password"]
)

sql = "show databases"

ans = client.execute(sql)
print(ans)
client.disconnect()
