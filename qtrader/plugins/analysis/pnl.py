# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 12:50 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: pnl.py
# @Software: PyCharm

import os
from datetime import datetime
from typing import List
import matplotlib.pyplot as plt


def plot_pnl(datetime:List[datetime], portfolio_value:List[float], *args:List, **kwargs):
    """Plot and show profit and loss curve and other curves as given"""
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.plot(datetime, portfolio_value, color='b')

    if len(args)>0:
        ax2 = ax1.twinx()
        for arg in args:
            ax2.plot(datetime, arg)
    
    if "path" not in kwargs:
        path = "results"
    else:
        path = kwargs["path"]
    now = sorted(os.listdir(path))[-1]
    plt.savefig(f"{path}/{now}/pnl.png")

    plt.show()