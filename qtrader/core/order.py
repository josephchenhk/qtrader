# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: order.py
# @Software: PyCharm

from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType, OrderStatus
from qtrader.core.security import Stock

class Order:
    """
    订单
    """

    def __init__(self,
                 security: Stock,
                 price: float,
                 quantity: float,
                 direction: Direction,
                 offset: Offset,
                 order_type: OrderType,
                 create_time: datetime,
                 updated_time: datetime=None,
                 filled_avg_price: float=0,
                 filled_quantity:int=0,
                 status: OrderStatus=OrderStatus.UNKNOWN
    ):
        self.security = security
        self.price = price
        self.quantity = quantity
        self.direction = direction
        self.offset = offset
        self.order_type = order_type
        self.create_time = create_time
        self.updated_time = updated_time
        self.filled_avg_price = filled_avg_price
        self.filled_quantity = filled_quantity
        self.status = status

    def __str__(self):
        return f"Order[{self.security}, price={self.price}, quantity={self.quantity}, direction={self.direction}, " + \
               f"offset={self.offset}, ordertype={self.order_type}, create_time={self.create_time}, updated_time={self.updated_time}, " +\
               f"filled_avg_price={self.filled_avg_price}, filled_quantity={self.filled_quantity}, status={self.status}]"
    __repr__ = __str__