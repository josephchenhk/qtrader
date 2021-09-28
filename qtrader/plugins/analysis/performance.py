# -*- coding: utf-8 -*-
# @Time    : 9/27/2021 9:51 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: performance.py
# @Software: PyCharm
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

def convert_time(time_:str):
    """time_ in the format: %H:%M:%S"""
    hour, min, *sec = time_.split(":")
    if int(min)<30:
        return f"{hour}:00:00"
    else:
        return f"{hour}:30:00"

def holding_period(time_:pd.Series):
    """open_datetime and close_datetime are in the format of %Y-%m-%d %H:%M:%S"""
    begin_dt = datetime.strptime(time_["open_datetime"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(time_["close_datetime"], "%Y-%m-%d %H:%M:%S")
    return (end_dt - begin_dt).total_seconds() / 60.

result_path = "results/2021-09-24 10-49-10.569006-3min/result.csv"
result = Path(os.getcwd()).parent.parent.parent.joinpath(result_path)
df = pd.read_csv(result)
df_action = df[df["action"].notna()]

df_action.reset_index(drop=True, inplace=True)

# Do not count fees when considering win/loss of each trade
df_action["pv_without_fees"] = df_action[["portfolio_value"]].apply(lambda x: (x.index+1)*1.92 + x)

win_trades = []
loss_trades = []
flat_trades = []

for idx in range(df_action.shape[0]):
    if idx%2==1:
        continue
    # print(idx, idx + 1)
    open_trade = df_action.loc[idx, :]
    close_trade = df_action.loc[idx+1, :]

    # Remove the impact of fees
    # pnl = close_trade["pv_without_fees"] - open_trade["pv_without_fees"]

    # Keep the impact of fees
    pnl = close_trade["portfolio_value"] - open_trade["portfolio_value"]

    if pnl>1e-8:
        win_trades.append([
            open_trade["datetime"],
            close_trade["datetime"],
            open_trade["datetime"].split(" ")[1],
            close_trade["datetime"].split(" ")[1],
            open_trade["close"],
            close_trade["close"],
            pnl,
            open_trade["action"]
        ])
    elif pnl<-1e-8:
        loss_trades.append([
            open_trade["datetime"],
            close_trade["datetime"],
            open_trade["datetime"].split(" ")[1],
            close_trade["datetime"].split(" ")[1],
            open_trade["close"],
            close_trade["close"],
            pnl,
            open_trade["action"]
        ])
    else:
        flat_trades.append([
            open_trade["datetime"],
            close_trade["datetime"],
            open_trade["datetime"].split(" ")[1],
            close_trade["datetime"].split(" ")[1],
            open_trade["close"],
            close_trade["close"],
            pnl,
            open_trade["action"]
        ])


cols = ["open_datetime", "close_datetime", "open_time", "close_time", "close_at_open", "close_at_close", "pnl", "action"]
if win_trades:
    win_trades_df = pd.DataFrame(win_trades, columns=cols)
    win_trades_df["open_session"] = win_trades_df["open_time"].apply(convert_time)
    win_trades_df["close_session"] = win_trades_df["close_time"].apply(convert_time)
    win_trades_df["holding_period"] = win_trades_df[["open_datetime", "close_datetime"]].apply(holding_period, axis=1)
if loss_trades:
    loss_trades_df = pd.DataFrame(loss_trades, columns=cols)
    loss_trades_df["open_session"] = loss_trades_df["open_time"].apply(convert_time)
    loss_trades_df["close_session"] = loss_trades_df["close_time"].apply(convert_time)
    loss_trades_df["holding_period"] = loss_trades_df[["open_datetime", "close_datetime"]].apply(holding_period, axis=1)
if flat_trades:
    flat_trades_df = pd.DataFrame(flat_trades, columns=cols)
    flat_trades_df["open_session"] = flat_trades_df["open_time"].apply(convert_time)
    flat_trades_df["close_session"] = flat_trades_df["close_time"].apply(convert_time)
    flat_trades_df["holding_period"] = flat_trades_df[["open_datetime", "close_datetime"]].apply(holding_period, axis=1)


with pd.ExcelWriter(result.parent.joinpath("stats.xlsx")) as writer:
    total_trades = []
    total_holding_periods = []
    total_holding_periods_index = []
    if win_trades:
        win_trades_df.to_excel(
            writer,
            sheet_name="win_trades",
            index=False
        )
        win_trades_df.groupby("open_session").pnl.agg(["sum", "count", "mean"]).to_excel(
            writer,
            sheet_name="win_trades_sessions",
        )

        win_trades_holding_period = win_trades_df.agg({"holding_period": ["max", "min", "mean", "std"]})
        total_trades.append(win_trades_df)
        total_holding_periods.append(win_trades_holding_period.T)
        total_holding_periods_index.append("win_trades")

    if loss_trades:
        loss_trades_df.to_excel(
            writer,
            sheet_name="loss_trades",
            index=False
        )
        loss_trades_df.groupby("open_session").pnl.agg(["sum", "count", "mean"]).to_excel(
            writer,
            sheet_name="loss_trades_sessions",
        )

        loss_trades_holding_period = loss_trades_df.agg({"holding_period": ["max", "min", "mean", "std"]})
        total_trades.append(loss_trades_df)
        total_holding_periods.append(loss_trades_holding_period.T)
        total_holding_periods_index.append("loss_trades")

    if flat_trades:
        flat_trades_df.to_excel(
            writer,
            sheet_name="flat_trades",
            index=False
        )
        flat_trades_df.groupby("open_session").pnl.agg(["sum", "count", "mean"]).to_excel(
            writer,
            sheet_name="flat_trades_sessions",
        )

        flat_trades_holding_period = flat_trades_df.agg({"holding_period": ["max", "min", "mean", "std"]})
        total_trades.append(flat_trades_df)
        total_holding_periods.append(flat_trades_holding_period.T)
        total_holding_periods_index.append("flat_trades")


    pd.concat(total_trades).groupby("open_session").pnl.agg(["sum", "count", "mean"]).to_excel(
        writer,
        sheet_name="trades_sessions",
    )

    holding_period_df = pd.concat(total_holding_periods)
    holding_period_df.index = total_holding_periods_index
    holding_period_df.to_excel(
        writer,
        sheet_name="holding_time",
    )

print(f"Saved to {result.parent.joinpath('stats.xlsx')}")
