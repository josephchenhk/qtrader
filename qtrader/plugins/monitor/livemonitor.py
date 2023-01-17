# -*- coding: utf-8 -*-
# @Time    : 5/5/2022 2:52 pm
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
import sys
import os

import pyautogui
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash.dependencies import Input, Output
import dash
from pathlib import Path
import pickle
import ast

from qtrader_config import TIME_STEP
from qtrader_config import LOCAL_PACKAGE_PATHS
from qtrader_config import ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH
if ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH:
    for pth in LOCAL_PACKAGE_PATHS:
        if pth not in sys.path:
            sys.path.insert(0, pth)
from qtrader.core.utility import try_parsing_datetime



if len(sys.argv) > 2:
    print(sys.argv[1])
    print(sys.argv[2])
    monitor_config = {}
    livemonitor_name = sys.argv[1]
    strategies = ast.literal_eval(sys.argv[2])
    for strategy_name, securities in strategies.items():
        monitor_config[strategy_name] = {
            "livemonitor_name": livemonitor_name,
            "instruments": {}
        }
        for gateway_name, security_codes in securities.items():
            monitor_config[strategy_name]["instruments"][gateway_name] = security_codes
else:
    # Just give a sample of monitor config
    monitor_config = {
        "strategy1_name": {
            "instruments": {
                "Backtest": ["security.code1", "security.code2"]
            },
            "livemonitor_name": "20220701"
        },
        "strategy2_name": {
            "instruments": {
                "Backtest": ["security.code3"]
            },
            "livemonitor_name": "20220701"
        }
    }

app = dash.Dash(__name__)
app.layout = dash.html.Div(
    dash.html.Div([
        dash.html.H4('Live Strategy Monitor'),
        dash.dcc.Dropdown(
            id='strategy_name',
            options=[{'label': k, 'value': k} for k in monitor_config],
            value=list(monitor_config.keys())[0]
        ),
        dash.dcc.Graph(id='live-update-graph'),
        dash.dcc.Interval(
            id='interval-component',
            interval=1 * TIME_STEP,  # in milliseconds
            n_intervals=0
        )
    ])
)


@app.callback(Output('live-update-graph', 'figure'),
              [Input('interval-component', 'n_intervals'),
               Input('strategy_name', 'value')])
def update_graph_live(n, strategy_name):
    instruments = monitor_config.get(strategy_name).get("instruments")
    livemonitor_name = monitor_config.get(
        strategy_name).get("livemonitor_name")

    home_dir = Path(os.getcwd())
    data_path = home_dir.joinpath(
        f".qtrader_cache/livemonitor/{strategy_name}/{livemonitor_name}")
    with open(data_path, "rb") as f:
        data = pickle.load(f)

        # get latest timestamp
        datetime_ts = [
            try_parsing_datetime(max(dt)) for dt in data["datetime"]]
        # sum over gateways
        portfolio_value_ts = [
            sum(spv) for spv in data["strategy_portfolio_value"]]
        portfolio_value = go.Scatter(
            x=datetime_ts,
            y=portfolio_value_ts,
            mode='lines',
            name='Portfolio Value')

        candlesticks = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        volumes = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        trend_ups = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        trend_downs = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        signals = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        actions = {
            gw: {
                sec: None for sec in instruments[gw]} for gw in instruments}
        for gw_idx, gateway_name in enumerate(instruments):
            for idx, security in enumerate(instruments[gateway_name]):
                open_ts = []
                high_ts = []
                low_ts = []
                close_ts = []
                volume_ts = []
                trend_ts = []
                trend_up_ts = []
                trend_down_ts = []
                signals[gateway_name][security] = []
                actions[gateway_name][security] = []
                for i in range(len(data["datetime"])):
                    if (
                        data["datetime"][i][gw_idx] is not None
                        and data["open"][i][gw_idx][idx] is not None
                        and data["high"][i][gw_idx][idx] is not None
                        and data["low"][i][gw_idx][idx] is not None
                        and data["close"][i][gw_idx][idx] is not None
                        and data["volume"][i][gw_idx][idx] is not None
                    ):
                        open_ts.append(data["open"][i][gw_idx][idx])
                        high_ts.append(data["high"][i][gw_idx][idx])
                        low_ts.append(data["low"][i][gw_idx][idx])
                        close_ts.append(data["close"][i][gw_idx][idx])
                        volume_ts.append(data["volume"][i][gw_idx][idx])
                        if data.get("trend"):
                            trend_ts.append(data["trend"][i][gw_idx][idx])
                        if data.get("trend_up"):
                            trend_up_ts.append(
                                data["trend_up"][i][gw_idx][idx])
                        if data.get("trend_down"):
                            trend_down_ts.append(
                                data["trend_down"][i][gw_idx][idx])

                        if (
                            data.get("signal")
                            and data["signal"][i][gw_idx][idx] == 1
                        ):
                            x = data["datetime"][i][gw_idx]
                            y = data["close"][i][gw_idx][idx]
                            arrowhead = 1
                            arrowsize = 1
                            arrowwidth = 2
                            arrowcolor = 'green'
                            ax = 0
                            ay = 30
                            yanchor = 'top'
                            text = "Entry Long"
                        elif (
                            data.get("signal")
                            and data["signal"][i][gw_idx][idx] == -1
                        ):
                            x = data["datetime"][i][gw_idx]
                            y = data["close"][i][gw_idx][idx]
                            arrowhead = 1
                            arrowsize = 1
                            arrowwidth = 2
                            arrowcolor = 'red'
                            ax = 0
                            ay = -30
                            yanchor = 'bottom'
                            text = "Entry Short"
                        elif (
                            data.get("signal")
                            and data["signal"][i][gw_idx][idx] == 10
                        ):
                            x = data["datetime"][i][gw_idx]
                            y = data["close"][i][gw_idx][idx]
                            arrowhead = 1
                            arrowsize = 1
                            arrowwidth = 2
                            arrowcolor = "green"
                            ax = 0
                            ay = 30
                            yanchor = 'top'
                            text = "Exit Short"
                        elif (
                            data.get("signal")
                            and data["signal"][i][gw_idx][idx] == -10
                        ):
                            x = data["datetime"][i][gw_idx]
                            y = data["close"][i][gw_idx][idx]
                            arrowhead = 1
                            arrowsize = 1
                            arrowwidth = 2
                            arrowcolor = "red"
                            ax = 0
                            ay = -30
                            yanchor = 'bottom'
                            text = "Exit Long"

                        if (
                            data.get("signal")
                            and data["signal"][i][gw_idx][idx] in (1, -1, 10, -10)
                        ):
                            annotation_params = dict(
                                x=x,
                                y=y,
                                hovertext=text,
                                yanchor=yanchor,
                                showarrow=True,
                                arrowhead=arrowhead,
                                arrowsize=arrowsize,
                                arrowwidth=arrowwidth,
                                arrowcolor=arrowcolor,
                                ax=ax,
                                ay=ay,
                                align="left",
                                borderwidth=2,
                                opacity=1.0,
                            )
                            signals[gateway_name][security].append(
                                annotation_params)

                        if (
                            data.get("action")
                            and security in data["action"][i][gw_idx]
                        ):
                            action_str_list = data["action"][i][gw_idx].split(
                                "|")
                            action_str_list = [
                                a for a in action_str_list if a != ""]
                            action_list = [
                                ast.literal_eval(a) for a in action_str_list]
                            for action in action_list:
                                if action["sec"] != security:
                                    continue
                                if (
                                    action["side"] == "LONG"
                                    and action["offset"] == "OPEN"
                                ):
                                    x = data["datetime"][i][gw_idx]
                                    y = data["close"][i][gw_idx][idx]
                                    bgcolor = "#CFECEC"
                                    bordercolor = 'green'
                                    arrowhead = 1
                                    arrowsize = 1
                                    arrowwidth = 2
                                    ax = -20
                                    ay = 30
                                    yanchor = 'top'
                                    text = "Entry Long"
                                elif (
                                    action["side"] == "SHORT"
                                    and action["offset"] == "OPEN"
                                ):
                                    x = data["datetime"][i][gw_idx]
                                    y = data["close"][i][gw_idx][idx]
                                    bgcolor = "#ffb3b3"
                                    bordercolor = 'red'
                                    arrowhead = 1
                                    arrowsize = 1
                                    arrowwidth = 2
                                    ax = 20
                                    ay = -30
                                    yanchor = 'bottom'
                                    text = "Entry Short"
                                elif (
                                    action["side"] == "LONG"
                                    and action["offset"] == "CLOSE"
                                ):
                                    x = data["datetime"][i][gw_idx]
                                    y = data["close"][i][gw_idx][idx]
                                    bgcolor = "#CFECEC"
                                    bordercolor = 'green'
                                    arrowhead = 1
                                    arrowsize = 1
                                    arrowwidth = 2
                                    ax = -20
                                    ay = 30
                                    yanchor = 'top'
                                    text = "Exit Short"
                                elif (
                                    action["side"] == "SHORT"
                                    and action["offset"] == "CLOSE"
                                ):
                                    x = data["datetime"][i][gw_idx]
                                    y = data["close"][i][gw_idx][idx]
                                    bgcolor = "#ffb3b3"
                                    bordercolor = 'red'
                                    arrowhead = 1
                                    arrowsize = 1
                                    arrowwidth = 2
                                    ax = 20
                                    ay = -30
                                    yanchor = 'bottom'
                                    text = "Exit Long"

                                action_annotation_params = dict(
                                    x=x,
                                    y=y,
                                    text=text,
                                    yanchor=yanchor,
                                    showarrow=True,
                                    arrowhead=arrowhead,
                                    arrowsize=arrowsize,
                                    arrowwidth=arrowwidth,
                                    arrowcolor="#636363",
                                    ax=ax,
                                    ay=ay,
                                    font=dict(
                                        size=15,
                                        color=bordercolor,
                                        family="Courier New, monospace"),
                                    align="left",
                                    bordercolor=bordercolor,
                                    borderwidth=2,
                                    bgcolor=bgcolor,
                                    opacity=1.0,
                                )
                                actions[gateway_name][security].append(
                                    action_annotation_params)

                candlesticks[gateway_name][security] = go.Candlestick(
                    x=datetime_ts,
                    open=open_ts,
                    high=high_ts,
                    low=low_ts,
                    close=close_ts,
                    name=f"OHLC_{security}"
                )
                volumes[gateway_name][security] = go.Bar(
                    x=datetime_ts,
                    y=volume_ts,
                    name=f"Volume_{security}",
                    marker=dict(color="blue")
                )

                if len(trend_ts) > 0:
                    up_color = [
                        'rgba(206,72,45, 1.0)' if t == 1.0
                        else 'rgba(206,72,45, 0.0)' for t in trend_ts]
                    trend_ups[gateway_name][security] = go.Scatter(
                        x=datetime_ts,
                        y=trend_up_ts,
                        mode='markers+lines',
                        name="trend_up",
                        line=dict(color="rgba(206,72,45, 0.3)"),
                        marker_color=up_color,
                    )

                    down_color = [
                        'rgba(60,179,113, 1.0)' if t == -1.0
                        else 'rgba(60,179,113, 0.0)' for t in trend_ts]
                    trend_downs[gateway_name][security] = go.Scatter(
                        x=datetime_ts,
                        y=trend_down_ts,
                        mode='markers+lines',
                        name="trend_down",
                        line=dict(color="rgba(60,179,113, 0.3)"),
                        marker_color=down_color
                    )

        vertical_spacing = 0.05
        num_plots = 1  # 1 for portfolio value
        for k, v in instruments.items():
            num_plots += len(v)   # k for gateway, v for list of securities
        row_height = (1 - vertical_spacing * num_plots) / num_plots
        row_heights = []
        row_titles = []
        row_heights.append(row_height)
        row_titles.append("Portfolio Value")
        for gateway_name in instruments:
            for security in instruments[gateway_name]:
                ohlc_height = row_height * 0.7
                v_height = row_height * 0.3
                row_heights.extend([ohlc_height, v_height])
                row_titles.extend(
                    [f"OHLC_{gateway_name}_{security}",
                     f"Volume_{gateway_name}_{security}"])
        fig = make_subplots(
            rows=(num_plots - 1) * 2 + 1,
            # 1 for portfolio_value; 2 for ohlc and volume respectively
            cols=1,
            shared_xaxes=False,
            vertical_spacing=vertical_spacing,
            subplot_titles=row_titles,
            row_heights=row_heights,
        )

        fig.add_trace(
            portfolio_value,
            row=1,
            col=1
        )
        fig.update_xaxes(row=1, col=1, rangeslider_visible=False)

        has_ohlcv = True
        if has_ohlcv:
            # gw_idx: 0, 1
            # idx: 0, 1, 2
            # gw_idx=0, idx=0: row=2=idx*2+2
            # gw_idx=0, idx=1: row=4=idx*2+2
            # gw_idx=0, idx=2: row=6=idx*2+2
            # gw_idx=1, idx=0: row=8=3*2+idx*2+2=len(instruments[list(instruments.keys())[gw_idx-1]])*2+idx*2+2
            # gw_idx=1, idx=1:
            # row=10=3*2+idx*2+2=len(instruments[list(instruments.keys())[gw_idx-1]])*2+idx*2+2
            for gw_idx, gateway_name in enumerate(instruments):
                for idx, security in enumerate(instruments[gateway_name]):
                    if gw_idx == 0:
                        row = idx * 2 + 2
                    else:
                        row = len(instruments[list(instruments.keys())[
                                  gw_idx - 1]]) * 2 + idx * 2 + 2
                    fig.add_trace(
                        candlesticks[gateway_name][security],
                        row=row,
                        col=1,
                    )
                    if trend_ups[gateway_name][security] is not None:
                        fig.add_trace(
                            trend_ups[gateway_name][security],
                            row=row,
                            col=1,
                        )
                    if trend_downs[gateway_name][security] is not None:
                        fig.add_trace(
                            trend_downs[gateway_name][security],
                            row=row,
                            col=1,
                        )
                    if signals[gateway_name][security] is not None:
                        for annotation_param in signals[gateway_name][security]:
                            fig.add_annotation(
                                row=row,
                                col=1,
                                **annotation_param
                            )
                    if actions[gateway_name][security] is not None:
                        for act_annotation_param in actions[gateway_name][security]:
                            fig.add_annotation(
                                row=row,
                                col=1,
                                **act_annotation_param
                            )
                    fig.update_xaxes(row=row, col=1, rangeslider_visible=False)
                    fig.add_trace(
                        volumes[gateway_name][security],
                        row=row + 1,
                        col=1
                    )
                    fig.update_xaxes(
                        row=row + 1, col=1, rangeslider_visible=False)

        fig.layout.update(
            title="Live trade monitor",
        )

        width, height = pyautogui.size()
        fig.layout.height = (height // 3) * num_plots
        fig.layout.width = width
        return fig


if __name__ == "__main__":
    app.run_server(debug=True)
