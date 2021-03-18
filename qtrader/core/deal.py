# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: deal.py
# @Software: PyCharm
from dataclasses import dataclass
from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType, OrderStatus, Exchange
from qtrader.core.security import Stock


class Deal:
    """
    成交
    Deal data contains information of a fill of an order. One order
    can have several trade fills.
    """

    def __init__(self,
                 security: Stock,
                 direction: Direction,
                 offset: Offset,
                 order_type: OrderType,
                 updated_time: datetime=None,
                 filled_avg_price: float=0,
                 filled_quantity:int=0,
                 dealid:str="",
                 orderid:str=""
    ):
        self.security = security
        self.direction = direction
        self.offset = offset
        self.order_type = order_type
        self.updated_time = updated_time
        self.filled_avg_price = filled_avg_price
        self.filled_quantity = filled_quantity
        self.dealid = dealid
        self.orderid = orderid


    def __str__(self):
        return f"Deal[{self.security}, direction={self.direction}, " + \
               f"offset={self.offset}, ordertype={self.order_type}, updated_time={self.updated_time}, " +\
               f"filled_avg_price={self.filled_avg_price}, filled_quantity={self.filled_quantity}" +\
               f"dealid={self.dealid}, orderid={self.orderid}]"

    __repr__ = __str__
