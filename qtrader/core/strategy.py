# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:37 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: strategy.py
# @Software: PyCharm
from typing import List, Dict, Any

from qtrader.core.engine import Engine
from qtrader.core.portfolio import Portfolio
from qtrader.core.security import Stock


class BaseStrategy:
    """
    策略基类
    To write a strategy, override init_strategy and on_bar methods
    """
    strategy_account:str = ""
    strategy_version:str = ""
    securities:List[Stock] = list()

    def __init__(self, engine:Engine, strategy_account:str, strategy_version:str, init_strategy_cash:Dict[str,float]):
        # 引擎
        self.engine = engine
        self.strategy_account = strategy_account
        self.strategy_version = strategy_version
        self.engine.init_portfolio(
            strategy_account=strategy_account,
            strategy_version=strategy_version,
            init_strategy_cash=init_strategy_cash,
        )
        # 记录策略每一个时间点执行的操作
        self._actions = {gateway_name:"" for gateway_name in self.engine.gateways}

    def init_strategy(self):
        self.engine.log.info("完成策略初始化")

    def on_bar(self):
        raise NotImplementedError("on_bar 方法需要被覆写")

    def on_tick(self):
        raise NotImplementedError("on_tick 方法需要被覆写")

    def get_datetime(self, gateway_name:str):
        return self.engine.gateways[gateway_name].market_datetime

    def get_portfolio_value(self, gateway_name:str):
        return self.engine.portfolios[gateway_name].value

    def get_action(self, gateway_name:str):
        return self._actions[gateway_name]

    def reset_action(self, gateway_name:str):
        self._actions[gateway_name] = ""

    def update_action(self, gateway_name:str, action:Dict[str,Any]):
        self._actions[gateway_name] += str(action)
        self._actions[gateway_name] += "|"