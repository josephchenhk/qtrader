# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: engine.py
# @Software: PyCharm

from qtrader.core.constants import Direction, Offset, OrderType
from qtrader.core.order import Order
from qtrader.core.portfolio import Portfolio
from qtrader.core.security import Stock
from qtrader.core.logger import logger

class Engine:

    """Execution engine"""

    def __init__(self, portfolio:Portfolio):
        self.portfolio = portfolio
        self.position = portfolio.position
        self.account_balance = portfolio.account_balance
        self.market = portfolio.market
        self.log = logger

    def send_order(self,
        security:Stock,
        price:float,
        volume:float,
        direction:Direction,
        offset:Offset,
        order_type:OrderType
    ):
        place_time = self.market.datetime
        order = Order(
            security = security,
            price = price,
            volume = volume,
            direction = direction,
            offset = offset,
            order_type = order_type,
            place_time = place_time
        )
        filled_order = self.market.process_order(order)
        return filled_order