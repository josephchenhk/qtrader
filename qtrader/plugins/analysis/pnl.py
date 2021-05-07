# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 12:50 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: pnl.py
# @Software: PyCharm

import os
from datetime import datetime, time
from typing import List
import matplotlib.pyplot as plt
from matplotlib.ticker import Formatter


class DatetimeFormatter(Formatter):
    """时间格式化"""
    def __init__(self, X_dates):
        self.X_dates = X_dates

    def __call__(self, x, pos=0):
        'Return the label for time x at position pos'
        ind = int(x)
        if ind >= len(self.X_dates) or ind < 0:
            return ''
        return self.X_dates[ind]


def get_xticks(X:datetime):
    """刻画港股非均匀时间刻度，选取开盘和收盘时间作为横坐标刻度 """
    X_ticks = []
    for xt, x in enumerate(X):
        if len(X_ticks)==0:
            X_ticks.append(xt)
            continue
        if len(X_ticks)>0 and (x.time()==time(9,30) and xt-X_ticks[-1]==1):
            X_ticks.pop(-1)
        if x.time() in [time(9,30), time(12,0), time(16,0)]:
            X_ticks.append(xt)
    return X_ticks


def plot_pnl(datetime:List[datetime], portfolio_value:List[float], *args:List, **kwargs):
    """Plot and show profit and loss curve and other curves as given"""
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    formatter = DatetimeFormatter(datetime)
    ax1.xaxis.set_major_formatter(formatter)
    ax1.plot(range(len(datetime)), portfolio_value, color='b')
    X_ticks = get_xticks(datetime)
    plt.xticks(X_ticks)
    plt.grid()
    fig.autofmt_xdate()
    if len(args)>0:
        ax2 = ax1.twinx()
        for arg in args:
            ax2.plot(range(len(datetime)), arg)
    if "path" not in kwargs:
        path = "results"
    else:
        path = kwargs["path"]
    now = sorted(next(os.walk('results'))[1])[-1]
    plt.savefig(f"{path}/{now}/pnl.png")
    plt.show()