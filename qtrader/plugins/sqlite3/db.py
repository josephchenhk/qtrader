# -*- coding: utf-8 -*-
# @Time    : 16/4/2021 3:48 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: db.py
# @Software: PyCharm

import sqlite3
from datetime import datetime
from typing import Iterable

import pandas as pd

try:
    from qtrader.config import DB
    db_path = DB["sqlite3"]
except:
    raise ValueError("`sqlite3` is activated, and its path must be specified in `DB` variable in config.py")

class DB:

    def __init__(self):
        self.conn = sqlite3.connect(f"{db_path}/qtrader.db")
        self.cursor = self.conn.cursor()
        self.create_balance_table()
        print()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def execute(self, sql:str, parameters:Iterable=None):
        if parameters is None:
            self.cursor.execute(sql)
            self.commit()
            return
        self.cursor.execute(sql, parameters)
        self.commit()

    def delete_table(self, table_name:str):
        sql = f"DROP TABLE {table_name}"
        self.execute(sql)

    def select_records(self, table_name:str, **kwargs):
        sql = f"SELECT * FROM {table_name} "
        if len(kwargs)>0:
            sql += "WHERE "
        for idx, (k,v) in enumerate(kwargs.items()):
            if idx!=0:
                sql += "AND "
            if isinstance(v, str):
                sql += f"{k}=\"{v}\" "
            elif isinstance(v, float) or isinstance(v, int):
                sql += f"{k}={v} "
            elif isinstance(v, datetime):
                sql += f"{k}=\"{v.strftime('%Y-%m-%d %H:%M:%S')}\" "
            else:
                raise ValueError(f"Data format is not support! type({v})={type(v)}")
        self.execute(sql)
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=[d[0] for d in self.cursor.description])

    def create_balance_table(self):
        sql = (
            "CREATE TABLE IF NOT EXISTS balance " +
            "(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, " +
            "broker_name VARCHAR(20) NOT NULL, " +
            "broker_environment VARCHAR(20) NOT NULL, " +
            "broker_account_id INTEGER NOT NULL, " +
            "broker_account VARCHAR(20) NOT NULL, " +
            "strategy_account_id INTEGER NOT NULL, " +
            "strategy_account VARCHAR(20) NOT NULL, " +
            "strategy_version VARCHAR(20) NOT NULL, " +
            "strategy_version_desc VARCHAR(300), "
            "strategy_status VARCHAR(15), "
            "cash DOUBLE NOT NULL, " +
            "power DOUBLE NOT NULL, " +
            "max_power_short DOUBLE, " +
            "net_cash_power DOUBLE, " +
            "update_time DATETIME NOT NULL, "
            "remark VARCHAR(300))"
        )
        self.execute(sql)

    def insert_balance_table(self,
                             broker_name:str,
                             broker_environment:str,
                             broker_account_id:int,
                             broker_account:str,
                             strategy_account_id:int,
                             strategy_account:str,
                             strategy_version:str,
                             strategy_version_desc:str,
                             strategy_status:str,
                             cash:float,
                             power:float,
                             max_power_short:float,
                             net_cash_power:float,
                             update_time:datetime,
                             remark:str
        ):
        sql = (
            "INSERT INTO balance " +
            "(broker_name, broker_environment,broker_account_id, broker_account, strategy_account_id, strategy_account, " +
            "strategy_version, strategy_version_desc, strategy_status, cash, power, max_power_short, net_cash_power, update_time, remark) " +
            f"VALUES (\"{broker_name}\", \"{broker_environment}\", {broker_account_id}, \"{broker_account}\", " +
            f"{strategy_account_id}, \"{strategy_account}\", \"{strategy_version}\", \"{strategy_version_desc}\", \"{strategy_status}\", " +
            f"{cash}, {power}, {max_power_short}, {net_cash_power}, \"{update_time.strftime('%Y-%m-%d %H:%M:%S')}\", \"{remark}\")"
        )
        self.execute(sql)
        print()



if __name__=="__main__":
    db = DB()
    # db.delete_table("balance")
    # db.create_balance_table()
    # db.insert_balance_table(
    #     broker_name = "FUTU",
    #     broker_environment = "SIMULATE",
    #     broker_account_id = 1,
    #     broker_account = "123456",
    #     strategy_account_id = 1,
    #     strategy_account = "default",
    #     strategy_version = "1.0",
    #     strategy_version_desc = "manual trading",
    #     strategy_status="active",
    #     cash = 100000.0,
    #     power = 99000,
    #     max_power_short = -1,
    #     net_cash_power = -1,
    #     update_time = datetime.now(),
    #     remark = "N/A"
    # )
    records = db.select_records(
        table_name="balance",
        broker_name="FUTU",
        broker_environment="SIMULATE",
        broker_account="123456",
        # strategy_account_id=1,
    )
    print()