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
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.offline as offline
from plotly.subplots import make_subplots

from qtrader.core.utility import try_parsing_datetime
from qtrader.plugins.analysis.metrics import percentile
from qtrader.plugins.analysis.metrics import convert_time
from qtrader.plugins.analysis.metrics import holding_period
from qtrader.plugins.analysis.metrics import sharp_ratio
from qtrader.plugins.analysis.metrics import information_ratio
from qtrader.plugins.analysis.metrics import modigliani_ratio
from qtrader.plugins.analysis.metrics import rolling_maximum_drawdown


class PerformanceCTA:
    """Performance of CTA strategies"""

    metrics = [
        "sum", "count", "mean", "max", "min", np.std,
        percentile(5), percentile(10), percentile(15), percentile(20),
        percentile(80), percentile(85), percentile(90), percentile(95)
    ]

    def __init__(
            self,
            instruments: Dict[str, Dict[str, List[Any]]],
            result_path: str,
    ):
        self.instruments = instruments
        self.result_path = result_path

    def calc_statistics(self):
        # read result csv
        result = Path(
            os.getcwd()).parent.parent.parent.joinpath(
            self.result_path)
        df = pd.read_csv(result)

        df.datetime = [try_parsing_datetime(
            max(ast.literal_eval(dt))) for dt in df["datetime"]]
        df.strategy_portfolio_value = [sum(ast.literal_eval(
            spv)) for spv in df["strategy_portfolio_value"]]

        # cal performance indicators on : sharpe ratio, info ratio, m2, mdd
        df1 = df.copy()
        agg_dict = {"strategy_portfolio_value": "last"}
        for i, gateway_name in enumerate(self.instruments["security"]):
            for j, security in enumerate(
                    self.instruments["security"][gateway_name]):
                df1[f"{gateway_name}_{security}_close"] = df1["close"].apply(
                    lambda x: ast.literal_eval(x)[i][j])
                agg_dict[f"{gateway_name}_{security}_close"] = "last"

        df_d = df1.set_index('datetime').resample('D').agg(agg_dict)
        df_d = df_d.dropna()
        returns = df_d['strategy_portfolio_value'].pct_change()

        df_d["benchmark"] = 0
        for i, gateway_name in enumerate(self.instruments["security"]):
            for j, security in enumerate(
                    self.instruments["security"][gateway_name]):
                df_d["benchmark"] += (
                    df_d[f"{gateway_name}_{security}_close"]
                    *self.instruments["lot"][gateway_name][j]
                )
        benchmark_returns = df_d["benchmark"].pct_change()

        sr = sharp_ratio(returns, 252)
        ir = information_ratio(returns, benchmark_returns, 252)
        m2_ratio = modigliani_ratio(returns, benchmark_returns, 252)
        df_mdd = rolling_maximum_drawdown(df_d['strategy_portfolio_value'])
        mdd = df_mdd.iloc[-1]
        # metrics to be saved
        self.strategy_metrics = pd.DataFrame(
            {
                "Sharpe Ratio": [sr],
                "Information Ratio": [ir],
                "M2 Ratio": [m2_ratio],
                "Rolling MDD": [mdd]
            }
        ).T

        # only take care of rows with actions
        df_action = df[df["action"].apply(lambda x: x.count("|") > 0)]
        df_action = df_action.reset_index(drop=True).copy()

        # Do not count fees when considering win/loss of each trade
        df_action.loc[:, "count_trades"] = None
        df_action.loc[:, "pv_without_fees"] = None
        for idx, row in df_action.iterrows():
            action_str = row["action"]
            num_trades = action_str.count("|")
            df_action.loc[idx, "count_trades"] = num_trades
            df_action.loc[idx, "pv_without_fees"] = (
                df_action.loc[idx, "strategy_portfolio_value"]
            )
            if num_trades > 0:
                gateway_action_strs = ast.literal_eval(action_str)
                for i, gateway_action_str in enumerate(gateway_action_strs):
                    gateway_name = list(self.instruments["security"].keys())[i]
                    gateway_actions = [
                        ast.literal_eval(a)
                        for a in gateway_action_str.split("|")
                        if "{" in a]
                    for gateway_action in gateway_actions:
                        sec_idx = self.instruments["security"][gateway_name].index(
                            gateway_action['sec'])
                        commission = self.instruments["commission"][gateway_name][sec_idx]
                        df_action.loc[idx, "pv_without_fees"] = (
                            df_action.loc[idx, "pv_without_fees"]
                            + commission * gateway_action['qty']
                        )

        win_trades = []
        loss_trades = []
        flat_trades = []

        open_trades = {gw: {sec: {} for sec in self.instruments["security"][gw]}
                       for gw in self.instruments["security"]}

        for idx, row in df_action.iterrows():
            action_str = row["action"]
            gateway_action_strs = ast.literal_eval(action_str)
            for gw_idx, gateway_action_str in enumerate(gateway_action_strs):
                gateway_name = list(
                    self.instruments["security"].keys())[gw_idx]
                gateway_actions = [
                    ast.literal_eval(a)
                    for a in gateway_action_str.split("|")
                    if "{" in a]
                for action in gateway_actions:
                    security = action['sec']
                    sec_idx = self.instruments["security"][gateway_name].index(
                        security)
                    lot = self.instruments["lot"][gateway_name][sec_idx]
                    commission = self.instruments["commission"][gateway_name][sec_idx]
                    slippage = self.instruments["slippage"][gateway_name][sec_idx]

                    action["datetime"] = row["datetime"].to_pydatetime().strftime(
                        "%Y-%m-%d %H:%M:%S")
                    if action["offset"] == "OPEN":
                        open_trades[gateway_name][security][action['no']] = action
                    elif action["offset"] == "CLOSE":
                        close_trd = action
                        open_trd = open_trades[gateway_name][security][action['no']]
                        assert open_trd["qty"] == close_trd["qty"], (
                            "Qty doesn't match in open and close trade!"
                        )
                        qty = open_trd["qty"]
                        close_trd_price = close_trd["close"]
                        open_trd_price = open_trd["close"]
                        # make a copy, instead of a reference!
                        side = open_trd["side"] + ""
                        if open_trd["side"] == "LONG":
                            pnl = (
                                (close_trd_price - open_trd_price) * qty * lot
                                - commission * qty * 2
                                - slippage * qty * 2
                            )  # 2 for open&close
                        elif open_trd["side"] == "SHORT":
                            pnl = (
                                (open_trd_price - close_trd_price) * qty * lot
                                - commission * qty * 2
                                - slippage * qty * 2
                            )  # 2 for open&close
                        # remove open trades in the dict
                        del open_trades[gateway_name][security][action['no']]

                        # record
                        if pnl > 1e-8:
                            win_trades.append([
                                security,
                                open_trd["datetime"],
                                close_trd["datetime"],
                                open_trd["datetime"].split(" ")[1],
                                close_trd["datetime"].split(" ")[1],
                                open_trd_price,
                                close_trd_price,
                                side,
                                qty,
                                pnl,
                            ])
                        elif pnl < -1e-8:
                            loss_trades.append([
                                security,
                                open_trd["datetime"],
                                close_trd["datetime"],
                                open_trd["datetime"].split(" ")[1],
                                close_trd["datetime"].split(" ")[1],
                                open_trd_price,
                                close_trd_price,
                                side,
                                qty,
                                pnl,
                            ])
                        else:
                            flat_trades.append([
                                security,
                                open_trd["datetime"],
                                close_trd["datetime"],
                                open_trd["datetime"].split(" ")[1],
                                close_trd["datetime"].split(" ")[1],
                                open_trd_price,
                                close_trd_price,
                                side,
                                qty,
                                pnl,
                            ])

        cols = [
            "security",
            "open_datetime",
            "close_datetime",
            "open_time",
            "close_time",
            "close_at_open",
            "close_at_close",
            "side",
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

        num_win_trades = self.win_trades_df.shape[0]
        num_loss_trades = self.loss_trades_df.shape[0]
        num_trades = num_win_trades + num_loss_trades
        total_pnl = self.win_trades_df["pnl"].sum(
        ) + self.loss_trades_df["pnl"].sum()
        win_trades_ratio = num_win_trades / (num_win_trades + num_loss_trades)
        max_trade_pnl = self.win_trades_df["pnl"].max()
        min_trade_pnl = self.loss_trades_df["pnl"].min()
        avg_win_trade_pnl = self.win_trades_df["pnl"].sum() / num_win_trades
        avg_loss_trade_pnl = self.loss_trades_df["pnl"].sum() / num_loss_trades
        avg_trade_pnl = total_pnl / num_trades

        self.win_trades_df["date"] = self.win_trades_df["close_datetime"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
        self.loss_trades_df["date"] = self.loss_trades_df["close_datetime"].apply(
            lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())
        total_trades_df = pd.concat([self.win_trades_df, self.loss_trades_df])
        daily_pnl_df = total_trades_df.groupby("date").pnl.agg(["sum"])
        num_days = len(df["datetime"].apply(lambda x: x.date()).unique())
        num_active_days = daily_pnl_df.shape[0]
        active_days_ratio = num_active_days / num_days
        num_win_days = (daily_pnl_df > 0).sum().values[0]
        num_loss_days = (daily_pnl_df < 0).sum().values[0]
        win_days_ratio = num_win_days / num_active_days
        max_daily_pnl = daily_pnl_df.max().values[0]
        min_daily_pnl = daily_pnl_df.min().values[0]
        avg_daily_pnl = total_pnl / num_active_days

        # Add more statistics to strategy metrics
        self.strategy_metrics.loc["total_pnl"] = total_pnl
        # count per trade
        self.strategy_metrics.loc["num_trades"] = num_trades
        self.strategy_metrics.loc["num_win_trades"] = num_win_trades
        self.strategy_metrics.loc["num_loss_trades"] = num_loss_trades
        self.strategy_metrics.loc["win_trades_ratio"] = win_trades_ratio
        self.strategy_metrics.loc["max_trade_pnl"] = max_trade_pnl
        self.strategy_metrics.loc["min_trade_pnl"] = min_trade_pnl
        self.strategy_metrics.loc["avg_win_trade_pnl"] = avg_win_trade_pnl
        self.strategy_metrics.loc["avg_loss_trade_pnl"] = avg_loss_trade_pnl
        self.strategy_metrics.loc["avg_trade_pnl"] = avg_trade_pnl

        # count per day
        self.strategy_metrics.loc["num_days"] = num_days
        self.strategy_metrics.loc["num_active_days"] = num_active_days
        self.strategy_metrics.loc["active_days_ratio"] = active_days_ratio
        self.strategy_metrics.loc["num_win_days"] = num_win_days
        self.strategy_metrics.loc["num_loss_days"] = num_loss_days
        self.strategy_metrics.loc["win_days_ratio"] = win_days_ratio
        self.strategy_metrics.loc["max_daily_pnl"] = max_daily_pnl
        self.strategy_metrics.loc["min_daily_pnl"] = min_daily_pnl
        self.strategy_metrics.loc["avg_daily_pnl"] = avg_daily_pnl

        metrics = self.strategy_metrics.T
        float_values = (
            'Sharpe Ratio',
            'Information Ratio',
            'M2 Ratio',
            'total_pnl',
            'max_trade_pnl',
            'min_trade_pnl',
            'avg_win_trade_pnl',
            'avg_loss_trade_pnl',
            'avg_trade_pnl',
            'max_daily_pnl',
            'min_daily_pnl',
            'avg_daily_pnl',
        )
        int_values = (
            'num_trades',
            'num_win_trades',
            'num_loss_trades',
            'num_days',
            'num_active_days',
            'num_win_days',
            'num_loss_days',
        )
        pct_values = (
            'Rolling MDD',
            'win_trades_ratio',
            'active_days_ratio',
            'win_days_ratio',
        )
        for float_value in float_values:
            metrics[float_value] = metrics[float_value].apply(
                lambda x: "{:,.2f}".format(x))
        for int_value in int_values:
            metrics[int_value] = metrics[int_value].apply(
                lambda x: "{:,.0f}".format(x))
        for pct_value in pct_values:
            metrics[pct_value] = metrics[pct_value].apply(
                lambda x: "{:,.2%}".format(x))

        self.strategy_metrics = metrics.T
        self.strategy_metrics = self.strategy_metrics.rename(columns={
                                                             0: "Metrics"})
        print(self.strategy_metrics)

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
                    ["sum", "count", "mean"]).to_excel(
                    writer,
                    sheet_name="win_trades_sessions")

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
                    ["sum", "count", "mean"]).to_excel(
                    writer,
                    sheet_name="loss_trades_sessions")

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
                    ["sum", "count", "mean"]).to_excel(
                    writer,
                    sheet_name="flat_trades_sessions")

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

            if hasattr(self, "win_trades_df"):
                self.win_trades_df.pnl.agg(metrics).to_excel(
                    writer,
                    sheet_name="win_trades_summary",
                )

            if hasattr(self, "loss_trades_df"):
                self.loss_trades_df.pnl.agg(metrics).to_excel(
                    writer,
                    sheet_name="loss_trades_summary",
                )

            pd.concat(total_trades).groupby("open_session").pnl.agg(
                ["sum", "count", "mean"]).to_excel(
                writer,
                sheet_name="trades_sessions")

            holding_period_df = pd.concat(total_holding_periods)
            holding_period_df.index = total_holding_periods_index
            holding_period_df.to_excel(
                writer,
                sheet_name="holding_time",
                index=False
            )

            self.strategy_metrics.to_excel(
                writer,
                sheet_name="metrics",
                index=True
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
    offline.plot(
        fig,
        filename=f"{str(Path(result_path).parent)}/monthly_pnl.html")
    print(f"Saved to {str(Path(result_path).parent)}/monthly_pnl.html")


def plot_pnl(
        result_path: str,
        start: datetime = None,
        end: datetime = None,
        freq: str = "1min",
        title: str = "P&L",
        auto_open=False
):
    """Plot portfolio equity curve, and specify maximum drawdown of the curve

    Ref: Start, End and Duration of Maximum Drawdown in Python
    (https://stackoverflow.com/questions/22607324/start-end-and-duration-of-maximum-drawdown-in-python)
    """
    df = pd.read_csv(result_path)
    df["datetime"] = df["datetime"].apply(lambda dt: max(
        try_parsing_datetime(x) for x in ast.literal_eval(dt)))
    df["strategy_portfolio_value"] = df["strategy_portfolio_value"].apply(
        lambda pv: sum(ast.literal_eval(pv)))

    if start is None:
        start = df.iloc[0]["datetime"].to_pydatetime()
    if end is None:
        end = df.iloc[-1]["datetime"].to_pydatetime()

    # Turn into daily pnl
    # Ref:
    # https://stackoverflow.com/questions/43009761/pandas-identify-last-row-by-date
    if freq == "daily":
        df["date"] = df["datetime"].apply(lambda x: x.date())
        last_row_index = df.groupby(df.date,
                                    as_index=True).apply(lambda g: g.index[-1])
        df = df.loc[last_row_index.to_list()]

    df = df[(df["datetime"] >= start) & (df["datetime"] <= end)]
    df["datetime"] = df["datetime"].apply(lambda x: str(x))

    dt = np.array(df['datetime'])
    spv = np.array(df["strategy_portfolio_value"])

    # Maximum Drawdown (Nominal)
    ni = np.argmax(np.maximum.accumulate(spv) - spv)  # end of the period
    nj = np.argmax(spv[:ni]) if ni else 0            # start of period

    # Maximum Drawdown (Percentage)
    pi = np.argmax(
        (np.maximum.accumulate(spv) - spv) / np.maximum.accumulate(spv)
    )

    # start of period
    pj = np.argmax(spv[:pi]) if pi else 0

    # Plot P & L
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=('Portfolio P&L', 'Daily P&L'),
        row_width=[0.2, 0.7],
        specs=[[{"secondary_y": False}], [{"secondary_y": False}]]
    )

    fig.add_trace(
        go.Scatter(
            x=dt,
            y=spv,
            mode='lines+markers',
            name='Portfolio value'),
        row=1,
        col=1
    )

    fig.add_trace(
        go.Scatter(
            x=[dt[ni], dt[nj]],
            y=[spv[ni], spv[nj]],
            mode='markers',
            name=(f'Nominal MDD {spv[ni]-spv[nj]:.2f}'
                  f'({(spv[ni]-spv[nj])/spv[nj]*100:.2f}%)')
        ),
        row=1,
        col=1
    )

    fig.add_trace(
        go.Scatter(
            x=[dt[pi], dt[pj]],
            y=[spv[pi], spv[pj]],
            mode='markers',
            name=(f'Percentage MDD {spv[pi]-spv[pj]:.2f}'
                  f'({(spv[pi]-spv[pj])/spv[pj]*100:.2f}%)')
        ),
        row=1,
        col=1
    )

    daily_max = np.diff(spv).max()
    daily_min = np.diff(spv).min()
    num_days = len(np.diff(spv))
    num_positive_days = sum(np.diff(spv) > 0)
    num_negative_days = sum(np.diff(spv) < 0)
    fig.add_trace(go.Bar(x=dt[1:],
                         y=np.diff(spv),
                         name=("{0} {1:.0f}, {2:.0f}; {3}, {4}, {5}"
                               "").format(freq,
                                          daily_max,
                                          daily_min,
                                          num_positive_days,
                                          num_negative_days,
                                          num_days),
                         marker=dict(color="red")),
                  row=2,
                  col=1)

    fig.layout.update(
        title=title,
        bargap=0
    )
    fig.update(layout_xaxis_rangeslider_visible=False)

    offline.plot(
        fig,
        filename=f"{str(Path(result_path).parent)}/pnl_{title}.html",
        auto_open=auto_open
    )
    print(f"Saved to {str(Path(result_path).parent)}/pnl_{title}.html")


def get_signal_from_action(actions: str) -> str:
    actions = ast.literal_eval(actions)
    signal = ""
    for gw_idx, gateway_action_str in enumerate(actions):
        gateway_actions = gateway_action_str.split("|")
        gateway_actions = [ast.literal_eval(a)
                           for a in gateway_actions if a != ""]
        for act in gateway_actions:
            if act['offset'] == "OPEN":
                side = act['side']
            elif act['offset'] == "CLOSE":
                side = "LONG" if act['side'] == "SHORT" else "SHORT"
            act_str = f"gw{gw_idx+1}_{act['sec']}_{act['offset']}_{side}|"
            if act_str not in signal:
                signal += act_str
    if signal:
        signal = signal[:-1]
    return signal


def plot_pnl_with_category(
        instruments: List[str],
        result_path: str,
        category: str = None,
        start: datetime = None,
        end: datetime = None
):
    """

    :param result_path:
    :param category:
    :param start: specify the start of period
    :param end: specify the end of period
    :return:
    """
    result_1m = pd.read_csv(result_path)

    # use last timestamp for datetime
    # use sum values for strategy_portfolio_value
    result_1m.datetime = [try_parsing_datetime(
        max(ast.literal_eval(dt))) for dt in result_1m["datetime"]]
    result_1m.strategy_portfolio_value = [sum(ast.literal_eval(
        spv)) for spv in result_1m["strategy_portfolio_value"]]

    category = "action" if category is None else category
    assert category in result_1m.columns, f"{category} is not in {result_path}!"

    if start is None:
        start = datetime.strptime(
            result_1m.iloc[0]["datetime"],
            "%Y-%m-%d %H:%M:%S")
    if end is None:
        end = datetime.strptime(
            result_1m.iloc[-1]["datetime"], "%Y-%m-%d %H:%M:%S")

    df = result_1m[(start <= result_1m["datetime"])
                   & (result_1m["datetime"] <= end)]

    # only take out non-empty category/action
    df_category = df[df[category].apply(lambda x: len(
        [c for c in ast.literal_eval(x) if c != ""]) > 0)].copy()
    df_category["signal"] = df_category["action"].apply(
        lambda x: get_signal_from_action(x))

    has_ohlc = False
    candlesticks = {gw: {sec: {} for sec in instruments["security"][gw]}
                    for gw in instruments["security"]}
    if (
        "open" in df.columns
        and "high" in df.columns
        and "low" in df.columns
        and "close" in df.columns
    ):
        for gw_idx, gateway_name in enumerate(instruments["security"]):
            for sec_idx, security in enumerate(
                    instruments["security"][gateway_name]):
                candlesticks[gateway_name][security] = go.Candlestick(
                    x=df['datetime'],
                    open=df['open'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    high=df['high'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    low=df['low'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    close=df['close'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    name=f"OHLC_{gateway_name}_{security}"
                )
        has_ohlc = True

    has_trend = False
    close_ups = {gw: {sec: {} for sec in instruments["security"][gw]}
                 for gw in instruments["security"]}
    close_downs = {gw: {sec: {} for sec in instruments["security"][gw]}
                   for gw in instruments["security"]}
    close_line = {gw: {sec: {} for sec in instruments["security"][gw]}
                  for gw in instruments["security"]}
    if "trend" in df.columns:
        for gw_idx, gateway_name in enumerate(instruments["security"]):
            for sec_idx, security in enumerate(
                    instruments["security"][gateway_name]):
                tmp = df.copy()
                tmp["trend"] = tmp["trend"].apply(
                    lambda x: ast.literal_eval(x)[gw_idx][sec_idx])
                close_ups[security] = go.Scatter(
                    mode="markers",
                    x=tmp[tmp.trend == "UP"]['datetime'],
                    y=tmp[tmp.trend == "UP"]['close'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    name=f"CLOSE_UP_{gateway_name}_{security}",
                    fillcolor="red")
                close_downs[security] = go.Scatter(
                    mode="markers",
                    x=tmp[tmp.trend == "DOWN"]['datetime'],
                    y=tmp[tmp.trend == "DOWN"]['close'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    name=f"ClOSE_DOWN_{gateway_name}_{security}",
                    fillcolor="green")
                close_line[security] = go.Scatter(
                    mode="lines",
                    x=tmp['datetime'],
                    y=tmp['close'].apply(
                        lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                    name=f"CLOSE_{gateway_name}_{security}",
                    marker_color="rgba(165,165,165,0.5)")
        has_trend = True

    volumes = {gw: {sec: {} for sec in instruments["security"][gw]}
               for gw in instruments["security"]}
    for gw_idx, gateway_name in enumerate(instruments["security"]):
        for sec_idx, security in enumerate(
                instruments["security"][gateway_name]):
            volumes[gateway_name][security] = go.Bar(
                x=df['datetime'],
                y=df['volume'].apply(
                    lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                name=f"Volume_{gateway_name}_{security}",
                marker=dict(
                    color="blue"))

    pnl = go.Scatter(
        mode="lines",
        x=df['datetime'],
        y=df['strategy_portfolio_value'],
        marker=dict(color="orange"),
        name="pnl",
    )

    closes = {gw: {sec: {} for sec in instruments["security"][gw]}
              for gw in instruments["security"]}
    for gw_idx, gateway_name in enumerate(instruments["security"]):
        for sec_idx, security in enumerate(
                instruments["security"][gateway_name]):
            closes[gateway_name][security] = go.Scatter(
                mode="lines",
                x=df['datetime'],
                y=df['close'].apply(
                    lambda x: ast.literal_eval(x)[gw_idx][sec_idx]),
                marker=dict(color="green"),
                name=f"close_{gateway_name}_{security}"
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
        for gw_idx, gateway_name in enumerate(instruments["security"]):
            for sec_idx, security in enumerate(
                    instruments["security"][gateway_name]):
                fig.add_trace(
                    candlesticks[gateway_name][security],
                    row=1,
                    col=1,
                    secondary_y=True)
    elif not has_trend:
        for gw_idx, gateway_name in enumerate(instruments["security"]):
            for sec_idx, security in enumerate(
                    instruments["security"][gateway_name]):
                fig.add_trace(
                    closes[gateway_name][security],
                    row=1,
                    col=1,
                    secondary_y=True)

    if has_trend:
        for gw_idx, gateway_name in enumerate(instruments["security"]):
            for sec_idx, security in enumerate(
                    instruments["security"][gateway_name]):
                fig.add_trace(
                    close_ups[gateway_name][security],
                    row=1,
                    col=1,
                    secondary_y=True)
                fig.add_trace(
                    close_downs[gateway_name][security],
                    row=1,
                    col=1,
                    secondary_y=True)
                fig.add_trace(
                    close_line[gateway_name][security],
                    row=1,
                    col=1,
                    secondary_y=True)

    fig.add_trace(
        pnl,
        row=1,
        col=1,
        secondary_y=False
    )

    # include a go.Bar trace for volumes
    for gw_idx, gateway_name in enumerate(instruments["security"]):
        for sec_idx, security in enumerate(
                instruments["security"][gateway_name]):
            fig.add_trace(
                volumes[gateway_name][security],
                row=2,
                col=1
            )

    # mark the actions/categories
    for idx, cat in enumerate(df_category["signal"].unique()):
        categories = [c for c in cat.split("|") if c != ""]
        for category in categories:
            gw_idx = int(category.split("_")[0].replace("gw", "")) - 1
            gateway_name = list(instruments["security"].keys())[gw_idx]
            security = category.split("_")[1]
            sec_idx = instruments["security"][gateway_name].index(security)
            fig.add_trace(
                go.Scatter(
                    mode='markers',
                    x=df_category.query(f'signal=="{cat}"')['datetime'].tolist(),
                    y=[ast.literal_eval(p)[gw_idx][sec_idx]
                       for p in df_category.query(f'signal=="{cat}"')['close'].tolist()],
                    marker=dict(symbol=idx, size=10, color=idx, line=dict(width=2)),
                    name=f'{gateway_name}_{security}: {cat}'),
                row=1,
                col=1,
                secondary_y=True
            )

    fig.layout.update(
        title=(f"Strategy {start.strftime('%Y-%m-%d %H:%M:%S')}"
               f"-{end.strftime('%Y-%m-%d %H:%M:%S')}"),
    )

    fig.update(layout_xaxis_rangeslider_visible=False)

    fig.layout.yaxis2.showgrid = False

    # fig.show()
    offline.plot(
        fig,
        filename=f"{str(Path(result_path).parent)}/pnl_with_category.html")
    print(f"Saved to {str(Path(result_path).parent)}/pnl_with_category.html")


if __name__ == "__main__":
    # Information provided must be consistent with the corresponding strategy
    instruments = {
        "security": {
            # ["FUT.GC", "FUT.SI", "FUT.CO"], ["HK.MHImain", "HK.HHImain"]
            "Backtest": ["FUT.GC", "FUT.SI", "FUT.CO"],
        },
        "lot": {
            "Backtest": [100, 5000, 1000],  # [100, 5000, 1000], [10, 50]
        },
        "commission": {
            "Backtest": [1.92, 1.92, 1.92],  # [1.92, 1.92, 1.92]  [10.1, 10.1]
        },
        "slippage": {
            "Backtest": [0.0, 0.0, 0.0],    # [0.0, 0.0, 0.0], [0.0, 0.0]
        }
    }

    result_path = (
        Path(os.path.abspath(__file__)).parent.parent.parent.parent.joinpath(
            "results/2022-06-01 09-48-35.546702/result.csv")
    )
    stats_path = result_path.parent.joinpath("stats.xlsx")

    perf_cta = PerformanceCTA(
        instruments=instruments,
        result_path=result_path,
    )
    perf_cta.calc_statistics()
    perf_cta.save()

    plot_monthly_pnl(stats_path=stats_path, target="total_trades")

    plot_pnl(result_path=result_path, freq="daily")

    plot_pnl_with_category(
        instruments=instruments,
        result_path=result_path,
        category="action",
        start=datetime(2022, 3, 4, 9, 30, 0),
        end=datetime(2022, 3, 11, 23, 0, 0)
    )
