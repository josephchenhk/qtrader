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
from qtrader.plugins.analysis.performance import plot_signals



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
                "Backtest": {
                    "security": ["HK.MHImain", "HK.HHImain"],
                    "lot": [10, 50],
                    "commission": [10.1, 10.1],
                    "slippage": [0.0, 0.0],
                }
            },
            "show_fields": {
                "Backtest": {
                    "tsv_t": [
                        dict(func=go.Bar,
                             style=dict(marker=dict(color="grey")), pos=2),
                        dict(func=go.Bar,
                             style=dict(marker=dict(color="grey")), pos=2),
                    ],
                    "tsv_m": [
                        dict(func=go.Line,
                             style=dict(marker=dict(color="blue")), pos=2),
                        dict(func=go.Line,
                             style=dict(marker=dict(color="blue")), pos=2),
                    ],
                }
            },
            "livemonitor_name": "20220701"
        },
        "strategy2_name": {
            "instruments": {
                "Backtest": {
                    "security": ["HK.HSImain"],
                    "lot": [50],
                    "commission": [10.1],
                    "slippage": [0.0],
                }
            },
            "show_fields": {
                "Backtest": {
                    "tsv_t": [
                        dict(func=go.Bar,
                             style=dict(marker=dict(color="grey")), pos=2),
                    ],
                }
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
    show_fields = monitor_config.get(strategy_name).get('show_fields')
    livemonitor_name = monitor_config.get(
        strategy_name).get("livemonitor_name")

    home_dir = Path(os.getcwd())
    data_path = home_dir.joinpath(
        f".qtrader_cache/livemonitor/{strategy_name}/{livemonitor_name}")
    with open(data_path, "rb") as f:
        data = pickle.load(f)
        fig = plot_signals(
            data=data,
            instruments=instruments,
            show_fields=show_fields
        )
        return fig


if __name__ == "__main__":
    app.run_server(debug=True)
