# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:57 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: __init__.py.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from .base_gateway import BaseGateway
from .backtest import BacktestGateway

try:
    from .futu import FutuGateway, FutuFuturesGateway
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")

try:
    from .ib import IbGateway
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")

try:
    from .cqg import CqgGateway
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")
