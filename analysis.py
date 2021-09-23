# -*- coding: utf-8 -*-
# @Time    : 9/21/2021 2:21 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: analysis_temp.py
# @Software: PyCharm

from datetime import datetime

import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.io as pio
import plotly.offline as offline

pio.renderers.default = "browser"

result_1m = pd.read_csv("result.csv")
# result_1m = pd.read_csv("result_1M.csv")
# result_4m = pd.read_csv("result_4M.csv")
#
# result_1m_action = result_1m[result_1m["action"].notna()]
# result_4m_action = result_4m[result_4m["action"].notna()]
#
# print(result_1m_action.shape, result_4m_action.shape)
# print()

result_1m["datetime"] = result_1m["datetime"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
df = result_1m[(datetime(2021, 1, 1, 6, 0, 0, 0)<=result_1m["datetime"]) & (result_1m["datetime"]<=datetime(2021, 1, 31, 5, 15, 0, 0))]
df_action = df[df["action"].notna()]
df_action["signal"] = df_action["action"].apply(lambda x: "Buy" if "Buy" in x else "Sell")
candlestick = go.Candlestick(
    x=df['datetime'],
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close'],
    name="GC"
)

# ohlc = go.Ohlc(
#     x=df['datetime'],
#     open=df['open'],
#     high=df['high'],
#     low=df['low'],
#     close=df['close'],
#     name="GC"
# )

volume = go.Bar(
    x=df['datetime'],
    y=df['volume'],
    name="Volume",
    marker=dict(color="yellow")
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
    subplot_titles=('GC', 'Volume'),
    row_width=[0.2, 0.7],
    specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
)

fig.add_trace(
    candlestick,
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



# 买点
fig.add_trace(
    go.Scatter(
        mode='markers',
        x=df_action.query('action=="Buy: open long"')['datetime'].tolist(),
        y=[p-5 for p in df_action.query('action=="Buy: open long"')['low'].tolist()],
        marker=dict(symbol="triangle-up-open", size=10, color="Crimson", line=dict(width=2)),
        name='Open long'),
    row=1,
    col=1,
    secondary_y=True
)

fig.add_trace(
    go.Scatter(
        mode='markers',
        x=df_action.query('action=="Buy: close short"')['datetime'].tolist(),
        y=[p-5 for p in df_action.query('action=="Buy: close short"')['low'].tolist()],
        marker=dict(symbol="triangle-up", size=10, color="Crimson", line=dict(width=2)),
        name='Close short'),
    row=1,
    col=1,
    secondary_y=True
)

# 賣点
fig.add_trace(
    go.Scatter(
        mode='markers',
        x=df_action.query('action=="Sell: open short"')['datetime'].tolist(),
        y=[p+5 for p in df_action.query('action=="Sell: open short"')['high'].tolist()],
        marker=dict(symbol="triangle-down", size=10, color="ForestGreen", line=dict(width=2)),
        name='Open short'),
    row=1,
    col=1,
    secondary_y=True
)

fig.add_trace(
    go.Scatter(
        mode='markers',
        x=df_action.query('action=="Sell: close long"')['datetime'].tolist(),
        y=[p+5 for p in df_action.query('action=="Sell: close long"')['high'].tolist()],
        marker=dict(symbol="triangle-down-open", size=10, color="ForestGreen", line=dict(width=2)),
        name='Close long'),
    row=1,
    col=1,
    secondary_y=True
)

fig.layout.update(
    title="Scalper Strategy",
)

fig.update(layout_xaxis_rangeslider_visible=False)

fig.layout.yaxis2.showgrid=False

# fig.show()
offline.plot(fig)
