# -*- coding: utf-8 -*-
# @Time    : 6/3/2021 5:47 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: data.py
# @Software: PyCharm

import os
from datetime import datetime
from typing import List
import pandas as pd

from qtrader.core.security import Stock
from qtrader.config import DATA_PATH

DATA_FILES = [f for f in os.listdir(f"{DATA_PATH}/k_line/K_1M") if ".csv" in f]

class Bar:
    """Bar数据的基本属性"""
    def __init__(self,
                 datetime:datetime,
                 security:Stock,
                 open:float = None,
                 high:float = None,
                 low:float = None,
                 close:float = None,
                 volume:float = None):
        self.datetime = datetime
        self.security = security
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    def __str__(self):
        return f"Bar[{self.datetime}, {self.security}, open={self.open}, high={self.high}, low={self.low}, close={self.close}, volume={self.volume}]"
    __repr__ = __str__


def _get_data_files(security:Stock):
    data_files = [f for f in DATA_FILES if security.code in f]
    data_files.sort()
    return data_files


def _get_full_data(security:Stock,
                   start:datetime,
                   end:datetime,
                   dtype:List[str]=["open", "high", "low", "close", "volume"]
    )->pd.DataFrame:
    # 该证券的所有历史数据文件（以日期为单位）
    data_files = _get_data_files(security)
    # 提取在指定时间段内的数据文件
    data_files_in_range = []
    for data_file in data_files:
        dt = datetime.strptime(data_file[-14:].replace(".csv",""), "%Y-%m-%d").date()
        if start.date()<=dt<=end.date():
            data_files_in_range.append(data_file)
    # 合并指定时间段内的历史数据
    full_data = pd.DataFrame()
    for data_file in data_files_in_range:
        data = pd.read_csv(f"{DATA_PATH}/k_line/K_1M/{data_file}")
        data = data[["time_key"] + dtype]
        full_data = full_data.append(data, ignore_index=True)
    if full_data.empty:
        raise ValueError(f"{security} 在时间段 [{start} - {end}] 内的历史数据不存在！")
    full_data = full_data.sort_values(by=['time_key'])
    return full_data


def _get_data_iterator(security:Stock, full_data:pd.DataFrame)->Bar:
    # Bar数据生成器
    for _,row in full_data.iterrows():
        cur_time = datetime.strptime(row["time_key"], "%Y-%m-%d %H:%M:%S")
        bar = Bar(
            datetime = cur_time,
            security = security,
            open = row["open"],
            high = row["high"],
            low = row["low"],
            close = row["close"],
            volume = row["volume"]
        )
        yield bar