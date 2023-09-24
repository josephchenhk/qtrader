# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:37 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: strategy.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from typing import List, Dict, Any
from datetime import datetime
from functools import wraps

from qtrader.core.balance import AccountBalance
from qtrader.core.data import Bar
from qtrader.core.engine import Engine
from qtrader.core.portfolio import Portfolio
from qtrader.core.position import Position
from qtrader.core.security import Security


def init_portfolio_and_params(init_strategy):
    @wraps(init_strategy)
    def wrapper(self, *args, **kwargs):
        # initialize portfolio (account balance and position) and params
        self._init_strategy()
        # custom strategy initialization function
        result = init_strategy(self, *args, **kwargs)
        return result
    return wrapper

class BaseStrategy:
    """Base class for strategy

    To write a strategy, override init_strategy and on_bar methods
    """

    def __init__(
            self,
            securities: Dict[str, List[Security]],
            strategy_account: str,
            strategy_version: str,
            engine: Engine,
            strategy_trading_sessions: List[List[datetime]] = None,
            init_strategy_portfolios: Dict[str, List[Portfolio]] = None,
            init_strategy_params: Dict[str, Dict[str, Dict]] = None
    ):
        self.securities = securities
        self.engine = engine
        self.strategy_account = strategy_account
        self.strategy_version = strategy_version
        self.strategy_trading_sessions = strategy_trading_sessions
        # portfolios
        if init_strategy_portfolios is None:
            init_strategy_portfolios = {}
            for gateway_name in securities:
                init_strategy_portfolios[gateway_name] = Portfolio(
                    account_balance=AccountBalance(cash=0),
                    position=Position(),
                    market=engine.gateways[gateway_name]
                )
        self.portfolios = init_strategy_portfolios
        # parameters
        if init_strategy_params is None:
            init_strategy_params = {gw: {} for gw in securities}
        self._init_strategy_params = init_strategy_params
        # Record the action at each time step
        self._actions = {gateway_name: "" for gateway_name in engine.gateways}
        # Record bar data at each time step
        self._data = {
            gateway_name: {
                security: None for security in securities.get(gateway_name, [])
            } for gateway_name in engine.gateways
        }

    def _init_strategy(self):
        """Initialize portfolio information for a specific strategy"""
        init_strategy_params = self._init_strategy_params
        self.params = {}
        for gateway_name in self.securities:
            self.params[gateway_name] = {}
            for sec_code in init_strategy_params[gateway_name]:
                self.params[gateway_name][sec_code] = {}
                for param in init_strategy_params[gateway_name][sec_code]:
                    self.params[gateway_name][sec_code][param] = \
                        init_strategy_params[gateway_name][sec_code][param]

    @init_portfolio_and_params
    def init_strategy(self, *args, **kwargs):
        raise NotImplementedError(
            "init_strategy has not been implemented yet.")

    def update_bar(self, gateway_name: str, security: Security, data: Bar):
        self._data[gateway_name][security] = data

    def on_bar(self, cur_data: Dict[str, Dict[Security, Bar]]):
        raise NotImplementedError("on_bar has not been implemented yet.")

    def on_tick(self):
        raise NotImplementedError("on_tick has not been implemented yet.")

    def get_datetime(self, gateway_name: str) -> datetime:
        return self.engine.gateways[gateway_name].market_datetime

    def get_strategy_portfolio_value(self, gateway_name: str) -> float:
        return self.portfolios[gateway_name].value

    def get_strategy_account_balance(self, gateway_name: str) -> AccountBalance:
        return self.portfolios[gateway_name].account_balance

    def get_strategy_position(self, gateway_name: str) -> Position:
        return self.portfolios[gateway_name].position

    def get_action(self, gateway_name: str) -> str:
        return self._actions[gateway_name]

    def get_open(self, gateway_name: str) -> List[float]:
        opens = []
        for g in self.engine.gateways:
            if g == gateway_name:
                for security in self.securities[gateway_name]:
                    opens.append(self._data[gateway_name][security].open)
        return opens

    def get_high(self, gateway_name: str) -> List[float]:
        highs = []
        for g in self.engine.gateways:
            if g == gateway_name:
                for security in self.securities[gateway_name]:
                    highs.append(self._data[gateway_name][security].high)
        return highs

    def get_low(self, gateway_name: str) -> List[float]:
        lows = []
        for g in self.engine.gateways:
            if g == gateway_name:
                for security in self.securities[gateway_name]:
                    lows.append(self._data[gateway_name][security].low)
        return lows

    def get_close(self, gateway_name: str) -> List[float]:
        closes = []
        for g in self.engine.gateways:
            if g == gateway_name:
                for security in self.securities[gateway_name]:
                    closes.append(self._data[gateway_name][security].close)
        return closes

    def get_volume(self, gateway_name: str) -> List[float]:
        volumes = []
        for g in self.engine.gateways:
            if g == gateway_name:
                for security in self.securities[gateway_name]:
                    volumes.append(self._data[gateway_name][security].volume)
        return volumes

    def reset_action(self, gateway_name: str):
        self._actions[gateway_name] = ""

    def update_action(self, gateway_name: str, action: Dict[str, Any]):
        self._actions[gateway_name] += str(action)
        self._actions[gateway_name] += "|"
