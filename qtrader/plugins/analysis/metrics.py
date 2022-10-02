# -*- coding: utf-8 -*-
# @Time    : 3/5/2022 11:01 am
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: metrics.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from datetime import datetime

import numpy as np
import pandas as pd


def convert_time(time_: str) -> str:
    """time_ in the format: %H:%M:%S"""
    hour, min, *sec = time_.split(":")
    if int(min) < 30:
        return f"{hour}:00:00"
    else:
        return f"{hour}:30:00"


def holding_period(time_: pd.Series) -> float:
    """open_datetime and close_datetime are in the format of %Y-%m-%d %H:%M:%S
    (
        time_['close_datetime'].astype('datetime64')
        - time_['open_datetime'].astype('datetime64')).apply(lambda x:
        x.total_seconds() / 60.
    )
    """
    begin_dt = datetime.strptime(time_["open_datetime"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(time_["close_datetime"], "%Y-%m-%d %H:%M:%S")
    return (end_dt - begin_dt).total_seconds() / 60.


def percentile(n: float):
    """Ref: https://stackoverflow.com/questions/17578115/pass-percentiles-to-pandas-agg-function"""
    def percentile_(x):
        return x.quantile(n / 100.)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


def sharpe_ratio(returns: np.array, days: int = 252) -> float:
    volatility = returns.std()
    sharpe_ratio = np.sqrt(days) * returns.mean() / volatility
    return sharpe_ratio


def information_ratio(
        returns: np.array,
        benchmark_returns: np.array,
        days: int = 252
) -> float:
    return_difference = returns - benchmark_returns
    volatility = return_difference.std()
    information_ratio = np.sqrt(days) * return_difference.mean() / volatility
    return information_ratio


def modigliani_ratio(returns: np.array, benchmark_returns, days=252) -> float:
    volatility = returns.std()
    sharpe_ratio = np.sqrt(days) * returns.mean() / volatility
    benchmark_volatility = benchmark_returns.std()
    m2_ratio = sharpe_ratio * benchmark_volatility
    return m2_ratio


def rolling_maximum_drawdown(
        portfolio_value: np.array,
        window: int = 252
) -> float:
    """(default) use a trailing 252 trading day window
    portfolio_value: the *daily* portfolio values
    """
    df = pd.Series(portfolio_value, name="pv").to_frame()
    # Calculate max drawdown in the past window days for each day in the
    # series.
    roll_max = df['pv'].rolling(window, min_periods=1).max()
    daily_drawdown = df['pv'] / roll_max - 1.0
    # Calculate the minimum (negative) daily drawdown in that window.
    max_daily_drawdown = daily_drawdown.rolling(window, min_periods=1).min()
    return max_daily_drawdown
