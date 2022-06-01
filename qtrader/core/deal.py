# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: deal.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from dataclasses import dataclass
from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType
from qtrader.core.security import Security


@dataclass
class Deal:
    """Done deal/execution"""
    security: Security
    direction: Direction
    offset: Offset
    order_type: OrderType
    updated_time: datetime = None
    filled_avg_price: float = 0
    filled_quantity: int = 0
    dealid: str = ""
    orderid: str = ""
