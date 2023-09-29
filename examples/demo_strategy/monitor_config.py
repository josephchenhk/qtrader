# -*- coding: utf-8 -*-
# @Time    : 27/4/2023 4:31 pm
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: monitor_config.py

"""
Copyright (C) 2022 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the terms of the JXW license, 
which unfortunately won't be written for another century.

You should have received a copy of the JXW license with this file. If not, 
please write to: josephchenhk@gmail.com
"""
from typing import List
from functools import partial

import pandas as pd
import plotly.graph_objects as go


def up_color(
        data: pd.DataFrame,
        gw_idx: int,
        sec_idx: int,
) -> List[str]:
    return [
        'rgba(206,72,45, 1.0)' if t == 1.0
        else 'rgba(206,72,45, 0.0)' for t in
        [t[gw_idx][sec_idx] for t in data["trend"]]
    ]

def down_color(
        data: pd.DataFrame,
        gw_idx: int,
        sec_idx: int,
) -> List[str]:
    return [
        'rgba(60,179,113, 1.0)' if t == -1.0
        else 'rgba(60,179,113, 0.0)' for t in
        [t[gw_idx][sec_idx] for t in data["trend"]]
    ]

instruments = {
    "CTA_HKFE_Strategy": {
        "Futufutures": {
                "security": ["HK.MHImain", "HK.HHImain"],
                "lot": [10, 50],
                "commission": [10.1, 10.1],
                "slippage": [0.0, 0.0],
                # "show_fields": {
                #     "tsv_t": [
                #         dict(func=go.Bar, style=dict(marker=dict(color="grey")), pos=2),
                #         dict(func=go.Bar, style=dict(marker=dict(color="grey")), pos=2),
                #     ],
                #     "tsv_m": [
                #         dict(func=go.Line, style=dict(marker=dict(color="blue")), pos=2),
                #         dict(func=go.Line, style=dict(marker=dict(color="blue")), pos=2),
                #     ],
                #     "tsv_avg_inflow": [
                #         dict(func=go.Line, style=dict(marker=dict(color="green")), pos=2),
                #         dict(func=go.Line, style=dict(marker=dict(color="green")), pos=2),
                #     ],
                #     "tsv_avg_outflow": [
                #         dict(func=go.Line, style=dict(marker=dict(color="red")), pos=2),
                #         dict(func=go.Line, style=dict(marker=dict(color="red")), pos=2),
                #     ],
                #     "trend_up": [
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(206,72,45, 0.3)")
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(up_color, gw_idx=0, sec_idx=0),
                #             ),
                #             pos=1
                #         ),
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(206,72,45, 0.3)"),
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(up_color, gw_idx=0, sec_idx=1),
                #             ),
                #             pos=1
                #         ),
                #     ],
                #     "trend_down": [
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(60,179,113, 0.3)"),
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(down_color, gw_idx=0, sec_idx=0),
                #             ),
                #             pos=1
                #         ),
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(60,179,113, 0.3)"),
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(down_color, gw_idx=0, sec_idx=1),
                #             ),
                #             pos=1
                #         ),
                #     ]
                # },
                "show_fields": {}
            }
    },

    "CTA_CME_Strategy": {
        "Ib": {
                "security": ["FUT.GC"],
                "lot": [100],
                "commission": [10.1],
                "slippage": [0.0],
                # "show_fields": {
                #     "tsv_t": [
                #         dict(func=go.Bar, style=dict(marker=dict(color="grey")), pos=2),
                #     ],
                #     "tsv_m": [
                #         dict(func=go.Line, style=dict(marker=dict(color="blue")), pos=2),
                #     ],
                #     "tsv_avg_inflow": [
                #         dict(func=go.Line, style=dict(marker=dict(color="green")), pos=2),
                #     ],
                #     "tsv_avg_outflow": [
                #         dict(func=go.Line, style=dict(marker=dict(color="red")), pos=2),
                #     ],
                #     "trend_up": [
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(206,72,45, 0.3)")
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(up_color, gw_idx=0, sec_idx=0),
                #             ),
                #             pos=1
                #         ),
                #     ],
                #     "trend_down": [
                #         dict(
                #             func=go.Scatter,
                #             style=dict(
                #                 mode='markers+lines',
                #                 line=dict(color="rgba(60,179,113, 0.3)"),
                #             ),
                #             style_func=dict(
                #                 marker_color=partial(down_color, gw_idx=0, sec_idx=0),
                #             ),
                #             pos=1
                #         ),
                #     ]
                # },
                "show_fields": {}
            }
    }
}