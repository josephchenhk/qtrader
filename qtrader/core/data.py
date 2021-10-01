# -*- coding: utf-8 -*-
# @Time    : 6/3/2021 5:47 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: data.py
# @Software: PyCharm

import os
import importlib
from dataclasses import dataclass
from datetime import datetime
from typing import List, Any
import pandas as pd

from qtrader.core.constants import Exchange
from qtrader.core.security import Stock
from qtrader.config import DATA_PATH


@dataclass
class Bar:
    """Bar数据的基本属性"""
    datetime: datetime
    security: Stock
    open: float
    high: float
    low: float
    close: float
    volume: float


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
    bid_price: float = 0
    ask_price: float = 0
    sec_status: str = "NORMAL"


def _get_data_files(security:Stock, dfield:str)->List[str]:
    """获取csv文件"""
    if not os.path.exists(f"{DATA_PATH[dfield]}/{security.code}"):
        raise FileNotFoundError(f"{DATA_PATH[dfield]}/{security.code}数据不存在！")
    data_files = [f for f in os.listdir(f"{DATA_PATH[dfield]}/{security.code}") if ".csv" in f]
    return data_files


def _get_data(security:Stock,
              start:datetime,
              end:datetime,
              dfield:str,
              dtype:List[str]=None,
    )->pd.DataFrame:
    """
    读取数据
    :param security:
    :param start:
    :param end:
    :param dfield: "k1m"
    :param dtype: ["time_key", "open", "high", "low", "close", "volume"], 第一个字段必须是时间
    :return:
    """
    # 该证券的所有历史数据文件（以日期为单位）
    data_files = _get_data_files(security, dfield)
    # 提取在指定时间段内的数据文件
    data_files_in_range = []
    for data_file in data_files:
        dt = datetime.strptime(data_file[-14:].replace(".csv",""), "%Y-%m-%d").date()
        if start.date()<=dt<=end.date():
            data_files_in_range.append(data_file)
    # 合并指定时间段内的历史数据
    full_data = pd.DataFrame()
    for data_file in data_files_in_range:
        data = pd.read_csv(f"{DATA_PATH[dfield]}/{security.code}/{data_file}")
        if dtype is None:
            # 检查数据是否有时间戳
            inspect_time_cols = [c for c in data.columns if "time" in c or "Time" in c]
            assert len(inspect_time_cols)>0, f"Data must contains at least one `*time*` column. Invalid data: {DATA_PATH[dfield]}/{security.code}/{data_file}"
            if "update_time" in inspect_time_cols:
                time_col = "update_time"
            else:
                time_col = inspect_time_cols[0]
        elif dtype is not None:
            assert sum([1 for d in dtype if "time" in d or "Time" in d])>0, f"Input params `dtype` must contains at least one `*time*` column. Invalid data: {DATA_PATH[dfield]}/{security.code}/{data_file}"
            assert set(dtype).issubset(set(data.columns)), f"Input params `dtype` must be a subset of the data columns in {DATA_PATH[dfield]}/{security.code}/{data_file}"
            time_col = dtype[0] # 第一个元素必须是代表时间
            data = data[dtype]

        # 确保该列数据可以被解析为datetime
        try:
            datetime.strptime(data.iloc[0][time_col], "%Y-%m-%d %H:%M:%S")
        except:
            raise ValueError(f"{time_col} data {data.iloc[0][time_col]} can not convert to datetime")
        full_data = full_data.append(data, ignore_index=True)
    if full_data.empty:
        raise ValueError(f"{security} 在时间段 [{start} - {end}] 内的历史数据不存在！")
    full_data = full_data.sort_values(by=[time_col])
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    full_data = full_data[(full_data[time_col]>=start_str) & (full_data[time_col]<=end_str)]
    return full_data


def _get_data_iterator(security:Stock, full_data:pd.DataFrame, class_name:str)->Any:
    """数据生成器"""
    # `class_name`数据生成器
    data_cls = getattr(importlib.import_module("qtrader.core.data"), class_name)
    time_col = full_data.columns[0]
    assert "time" in time_col or "Time" in time_col, f"The first column in `full_data` must be a `*time*` column, but {time_col} was given."
    for _,row in full_data.iterrows():
        cur_time = datetime.strptime(row[time_col], "%Y-%m-%d %H:%M:%S")
        kwargs = {"datetime": cur_time, "security": security}
        for col in full_data.columns:
            if col == time_col:
                continue
            kwargs[col] = row[col]
        data = data_cls(**kwargs)
        yield data
