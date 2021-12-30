# -*- coding: utf-8 -*-
# @Time    : 9/27/2021 9:51 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: performance.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import os
import ast
from typing import List
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.offline as offline
from plotly.subplots import make_subplots

from qtrader.core.utility import try_parsing_datetime


def plot_pnl(
        date_time: List[datetime],
        portfolio_value: List[float],
        *args: List,
        **kwargs):
    """Plot and show profit and loss curve and other curves as given"""
    df = pd.DataFrame({"datetime": date_time,
                       "portfolio_value": portfolio_value})
    fig = px.line(df, x="datetime", y="portfolio_value", title='strategy')
    now = sorted(next(os.walk('results'))[1])[-1]
    filename = Path(os.getcwd()).joinpath(f"results/{now}/pnl.html")
    offline.plot(fig, filename=str(filename))


def convert_time(time_: str):
    """time_ in the format: %H:%M:%S"""
    hour, min, *sec = time_.split(":")
    if int(min) < 30:
        return f"{hour}:00:00"
    else:
        return f"{hour}:30:00"


def holding_period(time_: pd.Series):
    """open_datetime and close_datetime are in the format of %Y-%m-%d %H:%M:%S"""
    begin_dt = datetime.strptime(time_["open_datetime"], "%Y-%m-%d %H:%M:%S")
    end_dt = datetime.strptime(time_["close_datetime"], "%Y-%m-%d %H:%M:%S")
    return (end_dt - begin_dt).total_seconds() / 60.


def percentile(n):
    """Ref: https://stackoverflow.com/questions/17578115/pass-percentiles-to-pandas-agg-function"""
    def percentile_(x):
        return x.quantile(n / 100.)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


class PerformanceCTA:

    metrics = [
        "sum", "count", "mean", "max", "min", np.std,
        percentile(5), percentile(10), percentile(15), percentile(20),
        percentile(80), percentile(85), percentile(90), percentile(95)
    ]

    def __init__(
            self,
            lot: int,
            commission: float,
            slippage: float,
            result_path: str):
        self.lot = lot
        self.commission = commission
        self.slippage = slippage
        self.result_path = result_path

    def calc_statistics(self):
        # read result csv
        result = Path(
            os.getcwd()).parent.parent.parent.joinpath(
            self.result_path)
        df = pd.read_csv(result)
        df_action = df[df["action"].notna()]

        # only take care of rows with actions
        df_action = df_action.reset_index(drop=True).copy()

        # Do not count fees when considering win/loss of each trade
        df_action.loc[:, "count_trades"] = None
        df_action.loc[:, "pv_without_fees"] = None
        for idx, row in df_action.iterrows():
            action_str = row["action"]
            num_trades = action_str.count("|")
            df_action.loc[idx, "count_trades"] = num_trades
            df_action.loc[idx,
                          "pv_without_fees"] = df_action.loc[idx,
                                                             "portfolio_value"] + self.commission * num_trades

        win_trades = []
        loss_trades = []
        flat_trades = []
        open_trades = {}
        for idx, row in df_action.iterrows():
            action_str = row["action"]
            actions = action_str.split("|")
            actions = [ast.literal_eval(act) for act in actions if act]
            for action in actions:
                action["close"] = row["close"]
                action["datetime"] = row["datetime"]
                if action["offset"] == "OPEN":
                    open_trades[action['no']] = action
                elif action["offset"] == "CLOSE":
                    close_trd = action
                    open_trd = open_trades[action['no']]
                    assert open_trd["qty"] == close_trd["qty"], "Qty doesn't match in open and close trade!"
                    qty = open_trd["qty"]
                    assert qty % self.lot == 0, f"Qty {qty} is NOT a multiplier of lot {lot}!"
                    if open_trd["side"] == "LONG":
                        pnl = (close_trd["close"] - open_trd["close"]) * qty - self.commission * (
                            qty // self.lot) * 2 - self.slippage * qty * 2  # 2 for open&close
                    elif open_trd["side"] == "SHORT":
                        pnl = (open_trd["close"] - close_trd["close"]) * qty - self.commission * (
                            qty // self.lot) * 2 - self.slippage * qty * 2  # 2 for open&close
                    # remove open trades in the dict
                    del open_trades[action['no']]

                    # record
                    if pnl > 1e-8:
                        win_trades.append([
                            open_trd["datetime"],
                            close_trd["datetime"],
                            open_trd["datetime"].split(" ")[1],
                            close_trd["datetime"].split(" ")[1],
                            open_trd["close"],
                            close_trd["close"],
                            qty,
                            pnl,
                        ])
                    elif pnl < -1e-8:
                        loss_trades.append([
                            open_trd["datetime"],
                            close_trd["datetime"],
                            open_trd["datetime"].split(" ")[1],
                            close_trd["datetime"].split(" ")[1],
                            open_trd["close"],
                            close_trd["close"],
                            qty,
                            pnl,
                        ])
                    else:
                        flat_trades.append([
                            open_trd["datetime"],
                            close_trd["datetime"],
                            open_trd["datetime"].split(" ")[1],
                            close_trd["datetime"].split(" ")[1],
                            open_trd["close"],
                            close_trd["close"],
                            qty,
                            pnl,
                        ])

        cols = [
            "open_datetime",
            "close_datetime",
            "open_time",
            "close_time",
            "close_at_open",
            "close_at_close",
            "qty",
            "pnl"]
        if win_trades:
            win_trades_df = pd.DataFrame(win_trades, columns=cols)
            win_trades_df["open_session"] = win_trades_df["open_time"].apply(
                convert_time)
            win_trades_df["close_session"] = win_trades_df["close_time"].apply(
                convert_time)
            win_trades_df["holding_period"] = win_trades_df[[
                "open_datetime", "close_datetime"]].apply(holding_period, axis=1)
        if loss_trades:
            loss_trades_df = pd.DataFrame(loss_trades, columns=cols)
            loss_trades_df["open_session"] = loss_trades_df["open_time"].apply(
                convert_time)
            loss_trades_df["close_session"] = loss_trades_df["close_time"].apply(
                convert_time)
            loss_trades_df["holding_period"] = loss_trades_df[[
                "open_datetime", "close_datetime"]].apply(holding_period, axis=1)
        if flat_trades:
            flat_trades_df = pd.DataFrame(flat_trades, columns=cols)
            flat_trades_df["open_session"] = flat_trades_df["open_time"].apply(
                convert_time)
            flat_trades_df["close_session"] = flat_trades_df["close_time"].apply(
                convert_time)
            flat_trades_df["holding_period"] = flat_trades_df[[
                "open_datetime", "close_datetime"]].apply(holding_period, axis=1)

        self.result = result
        self.win_trades = win_trades
        self.loss_trades = loss_trades
        self.flat_trades = flat_trades
        if win_trades:
            self.win_trades_df = win_trades_df
        if loss_trades:
            self.loss_trades_df = loss_trades_df
        if flat_trades:
            self.flat_trades_df = flat_trades_df

    def save(self):
        metrics = self.metrics
        with pd.ExcelWriter(self.result.parent.joinpath("stats.xlsx")) as writer:
            total_trades = []
            total_holding_periods = []
            total_holding_periods_index = []
            if self.win_trades:
                self.win_trades_df.to_excel(
                    writer,
                    sheet_name="win_trades",
                    index=False
                )
                self.win_trades_df.groupby("open_session").pnl.agg(
                    ["sum", "count", "mean"]).to_excel(writer, sheet_name="win_trades_sessions", )

                win_trades_holding_period = self.win_trades_df.agg(
                    {"holding_period": ["max", "min", "mean", "std"]})
                total_trades.append(self.win_trades_df)
                total_holding_periods.append(win_trades_holding_period.T)
                total_holding_periods_index.append("win_trades")

            if self.loss_trades:
                self.loss_trades_df.to_excel(
                    writer,
                    sheet_name="loss_trades",
                    index=False
                )
                self.loss_trades_df.groupby("open_session").pnl.agg(
                    ["sum", "count", "mean"]).to_excel(writer, sheet_name="loss_trades_sessions", )

                loss_trades_holding_period = self.loss_trades_df.agg(
                    {"holding_period": ["max", "min", "mean", "std"]})
                total_trades.append(self.loss_trades_df)
                total_holding_periods.append(loss_trades_holding_period.T)
                total_holding_periods_index.append("loss_trades")

            if self.flat_trades:
                self.flat_trades_df.to_excel(
                    writer,
                    sheet_name="flat_trades",
                    index=False
                )
                self.flat_trades_df.groupby("open_session").pnl.agg(
                    ["sum", "count", "mean"]).to_excel(writer, sheet_name="flat_trades_sessions", )

                flat_trades_holding_period = self.flat_trades_df.agg(
                    {"holding_period": ["max", "min", "mean", "std"]})
                total_trades.append(self.flat_trades_df)
                total_holding_periods.append(flat_trades_holding_period.T)
                total_holding_periods_index.append("flat_trades")

            pd.concat(total_trades).pnl.agg(metrics).to_excel(
                writer,
                sheet_name="total_trades_summary",
            )

            df = pd.concat(total_trades)
            df["date"] = df["close_datetime"].apply(
                lambda x: datetime.strptime(
                    x, "%Y-%m-%d %H:%M:%S").date())
            df = df.sort_values(by=['close_datetime'])

            df.to_excel(
                writer,
                sheet_name="total_trades",
                index=False
            )

            self.win_trades_df.pnl.agg(metrics).to_excel(
                writer,
                sheet_name="win_trades_summary",
            )

            self.loss_trades_df.pnl.agg(metrics).to_excel(
                writer,
                sheet_name="loss_trades_summary",
            )

            pd.concat(total_trades).groupby("open_session").pnl.agg(
                ["sum", "count", "mean"]).to_excel(writer, sheet_name="trades_sessions", )

            holding_period_df = pd.concat(total_holding_periods)
            holding_period_df.index = total_holding_periods_index
            holding_period_df.to_excel(
                writer,
                sheet_name="holding_time",
                index=False
            )

        print(f"Saved to {self.result.parent.joinpath('stats.xlsx')}")


def plot_monthly_pnl(stats_path: str, target: str = "total_trades"):
    """Plot monthlly pnl of the equity curve
    """
    df = pd.read_excel(stats_path, sheet_name=target)
    df_monthly = df.groupby(pd.Grouper(key='date', freq='1M')).sum()
    df_monthly["date"] = df_monthly.index

    fig = px.bar(df_monthly, x='date', y='pnl')
    fig.update_layout(bargap=0.2)
    fig.layout.update(title="Monthly P&L")
    fig.update(layout_xaxis_rangeslider_visible=False)

    # fig.show()
    offline.plot(fig, filename=f"{str(Path(result_path).parent)}/monthly_pnl.html")
    print(f"Saved to {str(Path(result_path).parent)}/monthly_pnl.html")


def plot_pnl(result_path: str, start: datetime = None, end: datetime = None):
    """Plot portfolio equity curve, and specify maximum drawdown of the curve

    Ref: Start, End and Duration of Maximum Drawdown in Python
    (https://stackoverflow.com/questions/22607324/start-end-and-duration-of-maximum-drawdown-in-python)
    """
    df = pd.read_csv(result_path)
    if start is None:
        start = try_parsing_datetime(df.iloc[0]["datetime"])
    if end is None:
        end = try_parsing_datetime(df.iloc[-1]["datetime"])
    df["datetime"] = df["datetime"].apply(lambda x: try_parsing_datetime(x))
    df = df[(df["datetime"] >= start) & (df["datetime"] <= end)]
    df["datetime"] = df["datetime"].apply(lambda x: str(x))

    dt = np.array(df['datetime'])
    pv = np.array(df["portfolio_value"])

    # Maximum Drawdown (Nominal)
    ni = np.argmax(np.maximum.accumulate(pv) - pv)  # end of the period
    nj = np.argmax(pv[:ni]) if ni else 0            # start of period

    # Maximum Drawdown (Percentage)
    pi = np.argmax((np.maximum.accumulate(pv) - pv) /
                   np.maximum.accumulate(pv))  # end of the period
    # start of period
    pj = np.argmax(pv[:pi]) if pi else 0

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dt,
        y=pv,
        mode='lines',
        name='Portfolio value'))

    fig.add_trace(go.Scatter(
        x=[dt[ni], dt[nj]],
        y=[pv[ni], pv[nj]],
        mode='markers',
        name=f'Nominal MDD {pv[ni]-pv[nj]:.2f}({(pv[ni]-pv[nj])/pv[nj]*100:.2f}%)'))

    fig.add_trace(go.Scatter(
        x=[dt[pi], dt[pj]],
        y=[pv[pi], pv[pj]],
        mode='markers',
        name=f'Percentage MDD {pv[pi]-pv[pj]:.2f}({(pv[pi]-pv[pj])/pv[pj]*100:.2f}%)'))

    fig.layout.update(title="P&L")
    fig.update(layout_xaxis_rangeslider_visible=False)

    # fig.show()
    offline.plot(fig, filename=f"{str(Path(result_path).parent)}/pnl.html")
    print(f"Saved to {str(Path(result_path).parent)}/pnl.html")


def plot_pnl_with_category(
        result_path: str,
        category: str = None,
        start: datetime = None,
        end: datetime = None):
    """

    :param result_path:
    :param category:
    :param start: specify the start of period
    :param end: specify the end of period
    :return:
    """
    result_1m = pd.read_csv(result_path)
    category = "action" if category is None else category
    assert category in result_1m.columns, f"{category} is not in {result_path}!"

    if start is None:
        start = datetime.strptime(
            result_1m.iloc[0]["datetime"],
            "%Y-%m-%d %H:%M:%S")
    if end is None:
        end = datetime.strptime(
            result_1m.iloc[-1]["datetime"], "%Y-%m-%d %H:%M:%S")

    result_1m["datetime"] = result_1m["datetime"].apply(
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
    df = result_1m[(start <= result_1m["datetime"])
                   & (result_1m["datetime"] <= end)]

    df_category = df[df[category].notna()]
    # df_action["signal"] = df_action["action"].apply(lambda x: "Buy" if "Buy" in x else "Sell")

    has_ohlc = False
    if "open" in df.columns and "high" in df.columns and "low" in df.columns:
        candlestick = go.Candlestick(
            x=df['datetime'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name="OHLC"
        )
        has_ohlc = True

    has_short_super_trend = False
    if "short_super_trend" in df.columns:
        close_up = go.Scatter(
            mode="markers",
            x=df[df.short_super_trend == "UP"]['datetime'],
            y=df[df.short_super_trend == "UP"]['close'],
            name="Close (UP)",
            fillcolor="red"
        )
        close_down = go.Scatter(
            mode="markers",
            x=df[df.short_super_trend == "DOWN"]['datetime'],
            y=df[df.short_super_trend == "DOWN"]['close'],
            name="Close (DOWN)",
            fillcolor="green"
        )
        has_short_super_trend = True

    volume = go.Bar(
        x=df['datetime'],
        y=df['volume'],
        name="Volume",
        marker=dict(color="blue")
    )

    pnl = go.Scatter(
        mode="lines",
        x=df['datetime'],
        y=df['portfolio_value'],
        marker=dict(color="orange"),
        name="PnL",

    )

    # fig = go.Figure(data=[candlestick])
    # fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Create subplots and mention plot grid size
    # `specs` makes secondary axis
    # `rows` and `cols` make subplots
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=('Portfolio', 'Volume'),
        row_width=[0.2, 0.7],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
    )

    if has_ohlc:
        fig.add_trace(
            candlestick,
            row=1,
            col=1,
            secondary_y=True
        )

    if has_short_super_trend:
        fig.add_trace(
            close_up,
            row=1,
            col=1,
            secondary_y=True
        )
        fig.add_trace(
            close_down,
            row=1,
            col=1,
            secondary_y=True
        )

    fig.add_trace(
        pnl,
        row=1,
        col=1,
        secondary_y=False
    )

    # include a go.Bar trace for volumes
    fig.add_trace(
        volume,
        row=2,
        col=1
    )

    # mark the actions/categories
    for idx, cat in enumerate(df_category[category].unique()):
        fig.add_trace(
            go.Scatter(
                mode='markers',
                x=df_category.query(f'{category}=="{cat}"')['datetime'].tolist(),
                y=[p for p in df_category.query(f'{category}=="{cat}"')['low'].tolist()],
                marker=dict(symbol=idx, size=10, color=idx, line=dict(width=2)),
                name=f'{cat}'),
            row=1,
            col=1,
            secondary_y=True
        )

    fig.layout.update(
        title=f"Strategy {start.strftime('%Y-%m-%d %H:%M:%S')}-{end.strftime('%Y-%m-%d %H:%M:%S')}",
    )

    fig.update(layout_xaxis_rangeslider_visible=False)

    fig.layout.yaxis2.showgrid = False

    # fig.show()
    offline.plot(
        fig,
        filename=f"{str(Path(result_path).parent)}/pnl_with_category.html")
    print(f"Saved to {str(Path(result_path).parent)}/pnl_with_category.html")


if __name__ == "__main__":
    lot = 100         # per contract 100 oz
    commission = 1.92  # per contract $1.92
    slippage = 0.0    # depends on csv result has slippage or not
    result_path = "/Users/joseph/Dropbox/code/qtrader_private/results/2021-12-14/result.csv"
    stats_path = result_path.replace("result.csv", "stats.xlsx")

    # perf_cta = PerformanceCTA(lot=lot, commission=commission, slippage=slippage, result_path=result_path)
    # perf_cta.calc_statistics()
    # perf_cta.save()

    # plot_monthly_pnl(stats_path=stats_path, target="total_trades")

    # plot_pnl(result_path=result_path)

    plot_pnl_with_category(
        result_path=result_path,
        category="action",
        start=datetime(2021, 10, 1, 0, 0, 0),
        end=datetime(2021, 10, 31, 23, 59, 59)
    )
