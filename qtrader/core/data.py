# -*- coding: utf-8 -*-
# @Time    : 6/3/2021 5:47 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: data.py
# @Software: PyCharm

import os
from dataclasses import dataclass
from datetime import datetime
from typing import List
import pandas as pd

from qtrader.core.constants import Exchange
from qtrader.core.security import Stock
from qtrader.config import DATA_PATH

DATA_FILES = [f for f in os.listdir(f"{DATA_PATH}/k_line/K_1M") if ".csv" in f]

class Bar:
    """
    Bar数据的基本属性
    """
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
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    full_data = full_data[(full_data['time_key']>=start_str) & (full_data['time_key']<=end_str)]
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


@dataclass
class OrderBook:
    """
    Orderbook
    订单簿
    """

    security: Stock
    exchange: Exchange
    datetime: datetime

    bid_price_1: float = 0
    bid_price_2: float = 0
    bid_price_3: float = 0
    bid_price_4: float = 0
    bid_price_5: float = 0
    bid_price_6: float = 0
    bid_price_7: float = 0
    bid_price_8: float = 0
    bid_price_9: float = 0
    bid_price_10: float = 0

    ask_price_1: float = 0
    ask_price_2: float = 0
    ask_price_3: float = 0
    ask_price_4: float = 0
    ask_price_5: float = 0
    ask_price_6: float = 0
    ask_price_7: float = 0
    ask_price_8: float = 0
    ask_price_9: float = 0
    ask_price_10: float = 0

    bid_volume_1: float = 0
    bid_volume_2: float = 0
    bid_volume_3: float = 0
    bid_volume_4: float = 0
    bid_volume_5: float = 0
    bid_volume_6: float = 0
    bid_volume_7: float = 0
    bid_volume_8: float = 0
    bid_volume_9: float = 0
    bid_volume_10: float = 0

    ask_volume_1: float = 0
    ask_volume_2: float = 0
    ask_volume_3: float = 0
    ask_volume_4: float = 0
    ask_volume_5: float = 0
    ask_volume_6: float = 0
    ask_volume_7: float = 0
    ask_volume_8: float = 0
    ask_volume_9: float = 0
    ask_volume_10: float = 0

    bid_num_1: float = 0
    bid_num_2: float = 0
    bid_num_3: float = 0
    bid_num_4: float = 0
    bid_num_5: float = 0
    bid_num_6: float = 0
    bid_num_7: float = 0
    bid_num_8: float = 0
    bid_num_9: float = 0
    bid_num_10: float = 0

    ask_num_1: float = 0
    ask_num_2: float = 0
    ask_num_3: float = 0
    ask_num_4: float = 0
    ask_num_5: float = 0
    ask_num_6: float = 0
    ask_num_7: float = 0
    ask_num_8: float = 0
    ask_num_9: float = 0
    ask_num_10: float = 0

@dataclass
class Quote:
    """
    Quote
    报价
    """
    security: Stock
    exchange: Exchange
    datetime: datetime

    last_price: float = 0
    open_price: float = 0
    high_price: float = 0
    low_price: float = 0
    prev_close_price: float = 0
    volume: float = 0
    turnover: float = 0
    turnover_rate: float = 0
    amplitude: float = 0
    suspension: bool = False
    price_spread: float = 0
    sec_status: str = "NORMAL"


@dataclass
class CapitalDistribution:
    """资金分布"""
    datetime: datetime
    security: Stock
    capital_in_big: float
    capital_in_mid: float
    capital_in_small: float
    capital_out_big: float
    capital_out_mid: float
    capital_out_small: float
