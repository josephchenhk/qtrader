# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:37 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: strategy.py
# @Software: PyCharm

from qtrader.core.engine import Engine
from qtrader.core.portfolio import Portfolio


class BaseStrategy:
    """
    策略基类
    To write a strategy, override init_strategy and on_bar methods
    """

    securities = []

    def __init__(self, engine:Engine):
        # 引擎
        self.engine = engine

    def init_strategy(self):
        self.engine.log.info("完成策略初始化")

    def on_bar(self):
        raise NotImplementedError("on_bar 方法需要被覆写")

    def on_tick(self):
        raise NotImplementedError("on_tick 方法需要被覆写")

    def get_datetime(self):
        return self.engine.market.market_datetime

    def get_portfolio_value(self):
        return self.engine.portfolio.value
