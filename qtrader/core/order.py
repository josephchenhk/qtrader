# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: order.py
# @Software: PyCharm
from dataclasses import dataclass
from datetime import datetime

from qtrader.core.constants import Direction, Offset, OrderType, OrderStatus
from qtrader.core.security import Stock


@dataclass
class Order:
    """
    订单
    """
    security: Stock
    price: float
    quantity: float
    direction: Direction
    offset: Offset
    order_type: OrderType
    create_time: datetime
    updated_time: datetime = None
    stop_price: float = None
    filled_avg_price: float = 0
    filled_quantity: int = 0
    status: OrderStatus = OrderStatus.UNKNOWN
    orderid: str = ""