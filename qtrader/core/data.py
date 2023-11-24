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
import warnings
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
from qtrader.core.utility import read_row_from_csv
from qtrader_config import DATA_PATH, TIME_STEP, BAR_CONVENTION


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
    num_trds: int = 0
    value: float = 0
    ticker: str = ''


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


def _get_data_path(security: Security, dtype: str, **kwargs) -> str:
    """Get the path to corresponding csv files."""
    if dtype == "kline":
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
        data_path = f"{DATA_PATH[dtype]}/{kline_name}/{security.code}"
    else:
        data_path = f"{DATA_PATH[dtype]}/{security.code}"
    return data_path


def _get_data_files(security: Security, dtype: str, **kwargs) -> List[str]:
    """Fetch csv files"""
    data_path = _get_data_path(security, dtype, **kwargs)
    if not os.path.exists(data_path):
        raise FileNotFoundError(f"Data was NOT found in {data_path}!")
    data_files = [f for f in os.listdir(data_path) if ".csv" in f]
    return data_files


def _get_data(
        security: Stock,
        start: datetime,
        end: datetime,
        dtype: str,
        dfield: List[str] = None,
        **kwargs
) -> pd.DataFrame:
    """Get historical data"""
    time_col = 'time_key'
    if kwargs.get('interval') and 'min' in kwargs.get('interval'):
        # Get all csv files of the security given
        data_files = _get_data_files(security, dtype, **kwargs)
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
            data_path = _get_data_path(security, dtype)
            if 'open' in read_row_from_csv(f"{data_path}/{data_file}", 1):
                data = pd.read_csv(f"{data_path}/{data_file}")
            elif 'open' in read_row_from_csv(f"{data_path}/{data_file}", 2):
                data = pd.read_csv(f"{data_path}/{data_file}", header=[0,1], index_col=[0])
                # get only the principal contract
                levels = [lvl for lvl in set(data.columns.get_level_values(0)) if lvl!='meta']
                volumes = {lvl: data.xs(lvl, level=0, axis=1).dropna()['volume'].sum() for lvl in levels}
                principal_level = max(volumes, key=volumes.get)
                data = data.xs(principal_level, level=0, axis=1).reset_index()
            # Confirm the time column could be parsed into datetime
            try:
                datetime.strptime(data.iloc[0][time_col], "%Y-%m-%d %H:%M:%S")
            except BaseException:
                raise ValueError(
                    f"{time_col} data {data.iloc[0][time_col]} can not convert to datetime")
            full_data = pd.concat([full_data, data])
        if full_data.empty:
            raise ValueError(
                f"There is no historical data for {security.code} within time range"
                f": [{start} - {end}]!")
        full_data = full_data.sort_values(by=[time_col])
        if BAR_CONVENTION.get(security.code) == 'start':
            start -= timedelta(minutes=int(TIME_STEP/60/1000))
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
        full_data = full_data[(full_data[time_col] >= start_str)
                              & (full_data[time_col] <= end_str)]
        full_data["time_key"] = pd.to_datetime(full_data["time_key"])
        full_data = full_data.dropna()
        full_data.reset_index(drop=True, inplace=True)
    elif kwargs.get('interval') == '1day':
        data_path = _get_data_path(security, dtype, interval='1day')
        if 'open' in read_row_from_csv(f"{data_path}/ohlcv.csv", 1):
            data = pd.read_csv(f"{data_path}/ohlcv.csv")
        elif 'open' in read_row_from_csv(f"{data_path}/ohlcv.csv", 2):
            data = pd.read_csv(f"{data_path}/ohlcv.csv", header=[0, 1], index_col=[0])
            # get only the principal contract
            levels = [lvl for lvl in set(data.columns.get_level_values(0)) if lvl != 'meta']
            volumes = {lvl: data.xs(lvl, level=0, axis=1).dropna()['volume'].sum() for lvl in levels}
            principal_level = max(volumes, key=volumes.get)
            data = data.xs(principal_level, level=0, axis=1).reset_index()
        full_data = data
        full_data['time_key'] = pd.to_datetime(data['time_key'])

    # build continuous contracts for futures
    #
    # | idx |     full_data   |  full_data.shift(1)  | full_data.shift(-1) |  switch1  |  switch2  |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    # |  0  |       1         |        NaN           |         1           |     Y     |           |
    # |  1  |       1         |         1            |         2           |           |     Y     |
    # |  2  |       2         |         1            |         2           |     Y     |           |
    # |  3  |       2         |         2            |         2           |           |           |
    # |  4  |       2         |         2            |         3           |           |     Y     |
    # |  5  |       3         |         2            |         4           |     Y     |     Y     |
    # |  6  |       4         |         3            |        NaN          |     Y     |     Y     |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    #
    # we need to find even number of rows, where for each consecutive two rows, the second row with `switch1=Y`, and
    # first row with `switch2=Y`. In the above example, this means we want to get the following rows (the `roll`):
    #
    # | idx |     full_data   |  full_data.shift(1)  | full_data.shift(-1) |  switch1  |  switch2  |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    # |  1  |       1         |         1            |         2           |           |     Y     |
    # |  2  |       2         |         1            |         2           |     Y     |           |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    # |  4  |       2         |         2            |         3           |           |     Y     |
    # |  5  |       3         |         2            |         4           |     Y     |           |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    # |  5  |       3         |         2            |         4           |           |     Y     |
    # |  6  |       4         |         3            |        NaN          |     Y     |           |
    # |-----|-----------------|----------------------|---------------------|-----------|-----------|
    #
    # where row 5 has been used twice.

    if 'ticker' in full_data.columns:
        switch1 = (full_data['ticker'] != full_data['ticker'].shift(1)).astype(int)
        switch2 = (full_data['ticker'] != full_data['ticker'].shift(-1)).astype(int)
        if switch1[switch1>0].empty:
            roll = pd.DataFrame()
        else:
            switch_rows = []
            for i in switch1[switch1>0].index:
                if i == 0:
                    continue
                if switch2.loc[i-1] == 1:
                    switch_rows.extend([i-1, i])
            roll = full_data.loc[switch_rows]
        assert roll.shape[0] % 2 == 0, 'roll records should be an even number'
        # get adjustment factors and corresponding indices
        adj_factors = []
        adj_indices = []
        for i in range(1, len(roll), 2):
            factor = roll.iloc[i]['close'] / roll.iloc[i - 1]['close']
            adj_factors.append(factor)
            adj_indices.append(roll.iloc[[i-1]].index[0])
        # Adjust historical prices and make continuous data
        for idx, factor in zip(adj_indices, adj_factors):
            # print(idx, factor)
            full_data.loc[:idx, 'close'] *= factor
            full_data.loc[:idx, 'open'] *= factor
            full_data.loc[:idx, 'high'] *= factor
            full_data.loc[:idx, 'low'] *= factor
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
    if daily_open_time <= daily_close_time:
        return dt.date()
    elif daily_close_time < daily_open_time and dt.time() > daily_close_time:
        return (dt + timedelta(days=1)).date()
    elif daily_close_time < daily_open_time and dt.time() <= daily_close_time:
        return dt.date()
    else:
        warnings.warn(
            f"{dt} is NOT within {daily_open_time} and {daily_close_time}")
        return None
