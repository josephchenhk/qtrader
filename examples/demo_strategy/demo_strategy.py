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

    def __init__(self, securities:List[Stock], strategy_account:str, strategy_version:str, engine:Engine):
        super().__init__(engine=engine, strategy_account=strategy_account, strategy_version=strategy_version)
        # 股票
        self.securities = securities
        # 执行引擎
        self.engine = engine
        # 投资组合管理
        self.portfolio = engine.portfolio

    def init_strategy(self):
        pass

    def on_bar(self, cur_data:Dict[Stock, Bar]):

        self.engine.log.info("-"*30 + "进入on_bar" + "-"*30)

        # check balance
        balance = self.engine.get_balance()
        self.engine.log.info(f"{balance}")

        # check position
        positions = self.engine.get_all_positions()
        self.engine.log.info(f"{positions}")

        # send orders
        for security in cur_data:

            orderbook = self.engine.get_orderbook(security)
            quote = self.engine.get_quote(security)
            bar = cur_data[security]
            self.engine.log.info(quote)
            self.engine.log.info(orderbook)
            self.engine.log.info(bar)

            orderid = self.engine.send_order(
                security=security,
                price=bar.close,
                quantity=security.lot_size,
                direction=Direction.LONG,
                offset=Offset.OPEN,
                order_type=OrderType.LIMIT
            )

            # sleep(1)
            order = self.engine.get_order(orderid)
            self.engine.log.info(f"发出订单{order}")

            deals = self.engine.find_deals_with_orderid(orderid)
            for deal in deals:
                self.portfolio.update(deal)

            if orderid:
                self.engine.cancel_order(orderid)
                self.engine.log.info(f"取消订单{order}")

