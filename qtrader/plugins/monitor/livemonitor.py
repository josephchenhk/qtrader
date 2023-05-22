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
from datetime import datetime

import pandas as pd
from dash.dependencies import Input, Output
import dash
from pathlib import Path
import pickle

from monitor_config import instruments
from qtrader_config import TIME_STEP
from qtrader_config import LOCAL_PACKAGE_PATHS
from qtrader_config import ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH
if ADD_LOCAL_PACKAGE_PATHS_TO_SYSPATH:
    for pth in LOCAL_PACKAGE_PATHS:
        if pth not in sys.path:
            sys.path.insert(0, pth)
from qtrader.plugins.analysis.performance import plot_signals

if len(sys.argv) > 1:
    monitor_name = sys.argv[1]
else:
    monitor_name = datetime.now().strftime("%Y%m%d")


app = dash.Dash(__name__)
app.layout = dash.html.Div(
    dash.html.Div([
        dash.html.H4('Live Strategy Monitor'),
        dash.dcc.Dropdown(
            id='strategy_name',
            options=[{'label': k, 'value': k} for k in instruments],
            value=list(instruments.keys())[0]
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
    home_dir = Path(os.getcwd())
    data_path = home_dir.joinpath(
        f".qtrader_cache/livemonitor/{strategy_name}/{monitor_name}")
    with open(data_path, "rb") as f:
        data = pd.DataFrame(pickle.load(f))
        fig = plot_signals(
            data=data,
            instruments=instruments.get(f'{strategy_name}'),
        )
        return fig


if __name__ == "__main__":
    app.run_server(debug=True)
