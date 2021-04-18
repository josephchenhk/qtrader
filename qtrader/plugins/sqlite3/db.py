# -*- coding: utf-8 -*-
# @Time    : 16/4/2021 3:48 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: db.py
# @Software: PyCharm

import sqlite3
from datetime import datetime
from typing import Iterable, List, Dict, Union

import pandas as pd
import numpy as np

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

    def _parse_sql_value(self, value:Union[str,float,int,datetime]):
        if isinstance(value, str):
            return f"\"{value}\""
        elif isinstance(value, float) or isinstance(value, int) or isinstance(value, np.int64):
            return f"{value}"
        elif isinstance(value, datetime):
            return f"\"{value.strftime('%Y-%m-%d %H:%M:%S')}\""
        else:
            raise ValueError(f"Data format is not support! type({value})={type(value)}")

    def _parse_sql_where_condition(self, **kwargs):
        sql = ""
        if len(kwargs)>0:
            sql += "WHERE "
        for idx, (k,v) in enumerate(kwargs.items()):
            if idx!=0:
                sql += "AND "
            if k=="condition_str": # 覆盖所有非"="形式的条件
                sql += f"{v} "
            else:
                sql += f"{k}={self._parse_sql_value(v)} "
        return sql

    def delete_table(self, table_name:str):
        sql = f"DROP TABLE {table_name}"
        self.execute(sql)

    def select_records(self, table_name:str, columns:List[str]=None, **kwargs):
        if columns is None:
            columns = "*"
        else:
            columns = ",".join(columns)
        sql = f"SELECT {columns} FROM {table_name} "
        sql += self._parse_sql_where_condition(**kwargs)
        self.execute(sql)
        data = self.cursor.fetchall()
        return pd.DataFrame(data, columns=[d[0] for d in self.cursor.description])

    def update_records(self, table_name:str, columns:Dict[str, Union[str,float,int,datetime]], **kwargs):
        assert len(columns)>0, "At least one column needs to be updated!"
        sql = f"UPDATE {table_name} "
        for idx, (k,v) in enumerate(columns.items()):
            if idx==0:
                sql += "SET "
            sql += f"{k}={self._parse_sql_value(v)} "
            if idx!=len(columns)-1:
                sql += ","
        sql += self._parse_sql_where_condition(**kwargs)
        self.execute(sql)

    def insert_records(self, table_name:str, **kwargs):
        assert len(kwargs)>0 ,"Must provide columns and values when inserting!"
        sql = f"INSERT INTO {table_name} ("
        sql += ",".join(kwargs.keys())
        sql += ") VALUES ("
        sql += ",".join([self._parse_sql_value(v) for v in kwargs.values()])
        sql += ")"
        self.execute(sql)

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






if __name__=="__main__":
    db = DB()
    # db.delete_table("balance")
    # db.create_balance_table()

    db.insert_records(
        table_name="balance",
        broker_name = "FUTU2",
        broker_environment = "SIMULATE",
        broker_account_id = 1,
        broker_account = "123456",
        strategy_account_id = 1,
        strategy_account = "default",
        strategy_version = "1.0",
        strategy_version_desc = "manual trading",
        strategy_status="active",
        cash = 100000.0,
        power = 99000,
        max_power_short = -1,
        net_cash_power = -1,
        update_time = datetime.now(),
        remark = "N/A"
    )

    records = db.select_records(
        table_name="balance",
        broker_name="FUTU2",
        broker_environment="SIMULATE",
        broker_account="123456",
        # strategy_account_id=1,
    )

    db.update_records(
        table_name="balance",
        columns={"cash":950000},
        id=4
    )
    print()