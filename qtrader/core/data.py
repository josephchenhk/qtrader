# -*- coding: utf-8 -*-
# @Time    : 6/3/2021 5:47 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: data.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import os
import importlib
from dataclasses import dataclass
from datetime import datetime
from datetime import time as Time
from datetime import date as Date
from datetime import timedelta
from typing import List, Any
import pandas as pd

from qtrader.core.constants import Exchange
from qtrader.core.security import Stock, Security
from qtrader.core.utility import get_kline_dfield_from_seconds
from qtrader.config import DATA_PATH, TIME_STEP


@dataclass
class Bar:
    """OHLCV"""
    datetime: datetime
    security: Stock
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class CapitalDistribution:
    """Capital Distributions"""
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
    """Orderbook"""
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
    """Quote"""
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


def _get_data_path(security: Security, dfield: str, **kwargs) -> str:
    """Get the path to corresponding csv files."""
    if dfield == "kline":
        if "interval" in kwargs:
            interval = kwargs.get("interval")
            if "min" in interval:
                interval_in_sec = int(interval.replace('min', '')) * 60
            elif "hour" in interval:
                interval_in_sec = int(interval.replace('hour', '')) * 3600
            elif "day" in interval:
                interval_in_sec = int(interval.replace('day', '')) * 3600 * 24
            else:
                raise ValueError(f"interval {interval} is NOT valid!")
        else:
            interval_in_sec = TIME_STEP // 1000  # TIME_STEP is in millisecond
        kline_name = get_kline_dfield_from_seconds(time_step=interval_in_sec)
        data_path = f"{DATA_PATH[dfield]}/{kline_name}/{security.code}"
    else:
        data_path = f"{DATA_PATH[dfield]}/{security.code}"
    return data_path


def _get_data_files(security: Stock, dfield: str, **kwargs) -> List[str]:
    """Fetch csv files"""
    data_path = _get_data_path(security, dfield, **kwargs)
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data was NOT found in {data_path}!")
    data_files = [f for f in os.listdir(data_path) if ".csv" in f]
    return data_files


def _get_data(
        security: Stock,
        start: datetime,
        end: datetime,
        dfield: str,
        dtype: List[str] = None,
        **kwargs
) -> pd.DataFrame:
    """Get historical data"""
    # Get all csv files of the security given
    data_files = _get_data_files(security, dfield, **kwargs)
    # Filter out the data that is within the time range given
    data_files_in_range = []
    for data_file in data_files:
        dt = datetime.strptime(
            data_file[-14:].replace(".csv", ""), "%Y-%m-%d").date()
        if start.date() <= dt <= end.date():
            data_files_in_range.append(data_file)
    # Aggregate the data to a dataframe
    full_data = pd.DataFrame()
    for data_file in sorted(data_files_in_range):
        data_path = _get_data_path(security, dfield)
        data = pd.read_csv(f"{data_path}/{data_file}")
        if dtype is None:
            # Identify the time_key/timestamp column
            inspect_time_cols = [
                c for c in data.columns if "time" in c or "Time" in c]
            assert len(inspect_time_cols) > 0, (
                "Data must contains at least one `*time*` column. Invalid "
                f"data: {DATA_PATH[dfield]}/{security.code}/{data_file}"
            )
            if "update_time" in inspect_time_cols:
                time_col = "update_time"
            else:
                time_col = inspect_time_cols[0]
        else:
            assert sum([1 for d in dtype if "time" in d or "Time" in d]) > 0, (
                "Input params `dtype` must contains at least one `*time*` "
                f"column. Invalid data: {DATA_PATH[dfield]}/{security.code}"
                f"/{data_file}"
            )
            assert set(dtype).issubset(set(data.columns)), (
                f"Input params `dtype` must be a subset of the data columns in "
                f"{DATA_PATH[dfield]}/{security.code}/{data_file}"
            )
            time_col = dtype[0]  # The first element must be time
            data = data[dtype]

        # Confirm the time column could be parsed into datetime
        try:
            datetime.strptime(data.iloc[0][time_col], "%Y-%m-%d %H:%M:%S")
        except BaseException:
            raise ValueError(
                f"{time_col} data {data.iloc[0][time_col]} can not convert to "
                "datetime")
        full_data = full_data.append(data, ignore_index=True)
    if full_data.empty:
        raise ValueError(
            f"There is no historical data for {security.code} within time range"
            f": [{start} - {end}]!")
    full_data = full_data.sort_values(by=[time_col])
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    full_data = full_data[(full_data[time_col] >= start_str)
                          & (full_data[time_col] <= end_str)]
    full_data["time_key"] = pd.to_datetime(full_data["time_key"])
    return full_data


def _get_data_iterator(
        security: Stock,
        full_data: pd.DataFrame,
        class_name: str
) -> Any:
    """Data generator"""
    # `class_name` could be Bar, CapitalDistribution, Quote, Orderbook, etc
    data_cls = getattr(importlib.import_module(
        "qtrader.core.data"), class_name)
    time_col = full_data.columns[0]
    assert "time" in time_col or "Time" in time_col, (
        "The first column in `full_data` must be a `*time*` column, but "
        f"{time_col} was given."
    )
    for _, row in full_data.iterrows():
        cur_time = row[time_col].to_pydatetime()
        kwargs = {"datetime": cur_time, "security": security}
        for col in full_data.columns:
            if col == time_col:
                continue
            kwargs[col] = row[col]
        data = data_cls(**kwargs)
        yield data


def _load_historical_bars_in_reverse(
        security: Security,
        cur_datetime: datetime,
        interval: str = "1min"
) -> List[str]:
    """Load historical csv data in reversed order."""
    data_path = _get_data_path(security, "kline", interval=interval)
    csv_files = os.listdir(data_path)
    csv_files = [f for f in csv_files if ".csv" in f]
    hist_csv_files = []
    for f in csv_files:
        if "_" in f:
            hist_csv_files.append(f)
        else:
            if datetime.strptime(
                    f, "%Y-%m-%d.csv").date() <= cur_datetime.date():
                hist_csv_files.append(f)
    hist_csv_files = sorted(hist_csv_files, reverse=True)
    return hist_csv_files


def get_trading_day(
        dt: datetime,
        daily_open_time: Time,
        daily_close_time: Time
) -> Date:
    """Get futures trading day according to daily_open_time and daily_close_time
    given."""
    if daily_open_time <= dt.time() <= daily_close_time:
        return dt.date()
    elif daily_close_time < daily_open_time <= dt.time():
        return (dt + timedelta(days=1)).date()
    elif dt.time() <= daily_close_time < daily_open_time:
        return dt.date()
    else:
        raise ValueError(
            f"{dt} is NOT within {daily_open_time} and {daily_close_time}")
