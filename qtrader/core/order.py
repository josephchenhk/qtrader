# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: order.py
# @Software: PyCharm

from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType
from qtrader.core.security import Stock

class Order:
    """订单"""

    def __init__(self,
                 security: Stock,
                 price: float,
                 volume: float,
                 direction: Direction,
                 offset: Offset,
                 order_type: OrderType,
                 place_time: datetime,
                 filled_time: datetime=None
    ):
        self.security = security
        self.price = price
        self.volume = volume
        self.direction = direction
        self.offset = offset
        self.order_type = order_type
        self.place_time = place_time
        self.filled_time = filled_time

    def __str__(self):
        return f"Order[{self.security}, price={self.price}, volume={self.volume}, direction={self.direction}, " + \
               f"offset={self.offset}, ordertype={self.order_type}, place_time={self.place_time}, filled_time={self.filled_time}]"
    __repr__ = __str__