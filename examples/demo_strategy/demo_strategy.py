# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:56 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: demo_strategy.py
# @Software: PyCharm
from time import sleep
from typing import Dict, List

from qtrader.core.constants import Direction, Offset, OrderType
from qtrader.core.data import Bar
from qtrader.core.engine import Engine
from qtrader.core.security import Stock
from qtrader.core.strategy import BaseStrategy

class DemoStrategy(BaseStrategy):

    def __init__(self, securities:List[Stock], engine:Engine):
        super().__init__(engine)
        # 股票
        self.securities = securities
        # 执行引擎
        self.engine = engine
        # 投资组合管理
        self.portfolio = engine.portfolio

    def init_strategy(self):
        pass

    def on_bar(self, cur_data:Dict[Stock, Bar]):
        self.engine.log.info(f"当前bar: {cur_data}")

        # check balance
        balance = self.engine.get_balance()
        broker_balance = self.engine.get_broker_balance()
        print(balance, broker_balance)

        # check position
        positions = self.engine.get_all_positions()
        broker_positions = self.engine.get_all_broker_positions()
        print(positions, broker_positions)

        # send orders
        for security in cur_data:
            bar = cur_data[security]
            orderid = self.engine.send_order(
                security=security,
                price=bar.close,
                quantity=security.lot_size,
                direction=Direction.LONG,
                offset=Offset.OPEN,
                order_type=OrderType.LIMIT
            )

            sleep(1)
            order = self.engine.get_order(orderid)
            self.engine.log.info(f"发出订单{order}")

            deals = self.engine.find_deals_with_orderid(orderid)
            for deal in deals:
                self.portfolio.update(deal)

            if orderid:
                self.engine.cancel_order(orderid)
                self.engine.log.info(f"取消订单{order}")

