# -*- coding: utf-8 -*-
# @Time    : 4/22/2022 9:20 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: livemonitor.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import os
from pathlib import Path
import matplotlib.pyplot as plt
import ast

import pandas as pd

results_path = Path(os.path.abspath(__file__)
                    ).parent.parent.parent.parent.joinpath("results")
date = "2022-04-21"
security_indices = [0, 1]

qty = 2
df_backtest = pd.read_csv(results_path.joinpath(f"{date} Backtest/result.csv"))
df_backtest = df_backtest.sort_values(by="bar_datetime")
df_backtest["portfolio_value"] = (
    df_backtest["portfolio_value"]
    - df_backtest["portfolio_value"].iloc[0]
)
df_backtest["portfolio_value"] = df_backtest["portfolio_value"] * qty
df_backtest = df_backtest[
    ["bar_datetime",
     "portfolio_value",
     "action",
     "close",
     "volume"]
]

df_live = pd.read_csv(results_path.joinpath(f"{date} Live/result.csv"))
df_live = df_live.sort_values(by="bar_datetime")
df_live["portfolio_value"] = (
    df_live["portfolio_value"]
    - df_live["portfolio_value"].iloc[0]
)
df_live = df_live[
    ["bar_datetime",
     "portfolio_value",
     "action",
     "close",
     "volume"]
]

df = df_live.merge(
    df_backtest,
    on="bar_datetime",
    how="inner",
    suffixes=[
        "_live",
        "_backtest"])
# df.set_index("bar_datetime", inplace=True)

# compare portfolio value
ax = df.plot('bar_datetime', 'portfolio_value_live', color="red", alpha=0.9)
df.plot(
    'bar_datetime',
    'portfolio_value_backtest',
    ax=ax,
    color='green',
    alpha=0.8)
plt.xticks(rotation=60)
plt.tight_layout()
plt.show()


for security_index in security_indices:
    # compare close
    df_close = pd.concat([
        df.bar_datetime,
        df.close_live.apply(lambda x: ast.literal_eval(x)[security_index]),
        df.close_backtest.apply(lambda x: ast.literal_eval(x)[security_index])
    ], axis=1)
    df_close["close_diff"] = (
        df_close["close_live"]
        - df_close["close_backtest"]
    )
    ax = df_close.plot('bar_datetime', 'close_live', color="red", alpha=0.9)
    df_close.plot(
        'bar_datetime',
        'close_backtest',
        ax=ax,
        color='green',
        alpha=0.8)
    plt.xticks(rotation=60)
    ax1 = ax.twinx()
    df_close.plot('bar_datetime', 'close_diff', ax=ax1, color='orange')
    plt.tight_layout()
    plt.show()

    # compare volume
    df_volume = pd.concat([
        df.bar_datetime,
        df.volume_live.apply(lambda x: ast.literal_eval(x)[security_index]),
        df.volume_backtest.apply(lambda x: ast.literal_eval(x)[security_index])
    ], axis=1)
    df_volume["volume_diff"] = (
        df_volume["volume_live"]
        - df_volume["volume_backtest"]
    )

    ax = df_volume.plot('bar_datetime', 'volume_live', color="red", alpha=0.9)
    df_volume.plot(
        'bar_datetime',
        'volume_backtest',
        ax=ax,
        color='green',
        alpha=0.8)
    plt.xticks(rotation=60)
    ax1 = ax.twinx()
    df_volume.plot('bar_datetime', 'volume_diff', ax=ax1, color='orange')
    plt.tight_layout()
    plt.show()
