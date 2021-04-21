# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: deal.py
# @Software: PyCharm
from dataclasses import dataclass
from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType
from qtrader.core.security import Stock

@dataclass
class Deal:
    """
    成交
    """
    security: Stock
    direction: Direction
    offset: Offset
    order_type: OrderType
    updated_time: datetime = None
    filled_avg_price: float = 0
    filled_quantity: int = 0
    dealid: str = ""
    orderid: str = ""
