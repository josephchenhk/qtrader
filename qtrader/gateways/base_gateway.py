# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 1:24 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: base_gateway.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import os
from pathlib import Path
from datetime import datetime
from datetime import time as Time
from abc import ABC
from typing import List, Dict

import yaml

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode
from qtrader.core.data import Bar, Quote, OrderBook
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Security
from qtrader.core.utility import BlockingDict
from qtrader.core.utility import is_trading_time
from qtrader_config import GATEWAYS


class BaseGateway(ABC):
    """
    Abstract gateway class for creating gateways connection
    to different trading systems.
    """
    broker_name = ""
    broker_account = ""
    trading_sessions = {}

    def __init__(
            self,
            securities: List[Security],
            gateway_name: str = "Backtest",
            **kwargs
    ):
        """
        Base gateway
        :param securities: securities
        :param gateway_name: name of the gateway
        :param kwargs:
        """
        self._market_datetime = None
        self._trade_mode = None
        self.securities = securities
        assert gateway_name in GATEWAYS, (
            f"{gateway_name} is NOT in GATEWAYS, please check your config file!"
        )
        self.broker_account = GATEWAYS[gateway_name]["broker_name"]
        self.broker_account = GATEWAYS[gateway_name]["broker_account"]
        self.orders = BlockingDict()
        self.deals = BlockingDict()
        self.quote = BlockingDict()
        self.orderbook = BlockingDict()

        # If trading sessions are not specified explicitly, we load them from
        # yaml file
        if 'trading_sessions' in kwargs:
            trading_sessions = kwargs.get('trading_sessions')
            assert type(trading_sessions) == dict, (
                "trading_sessions should be a dict: Dict[str, List].")
            self.trading_sessions = trading_sessions
        else:
            gateway_path = os.path.dirname(os.path.realpath(__file__))
            if "instrument_cfg.yaml" not in os.listdir(Path(gateway_path)):
                raise ValueError(
                    "trading_sessions are NOT specified in gateway, and "
                    "instrument_cfg.yaml can NOT be found in "
                    f"{os.listdir(Path(gateway_path))} either!")
            with open(Path(gateway_path).joinpath("instrument_cfg.yaml"),
                      'r', encoding='utf-8') as f:
                instrument_cfg = f.read()
                instrument_cfg = yaml.load(
                    instrument_cfg, Loader=yaml.FullLoader)
                # check if info of all securities is available
                for security in securities:
                    assert security.code in instrument_cfg, (
                        f"{security.code} is NOT available in "
                        f"{Path(gateway_path).joinpath('instrument_cfg.yaml')}"
                    )
                    self.trading_sessions[security.code] = instrument_cfg[
                        security.code]["sessions"]

    def close(self):
        """In backtest, no need to do anything"""
        raise NotImplementedError("[close] is not implemented yet.")

    @property
    def market_datetime(self):
        """Current Market time"""
        return self._market_datetime

    def is_security_trading_time(
            self,
            security: Security,
            cur_time: Time
    ) -> bool:
        """whether the security is whitin trading time"""
        trading_sessions = self.trading_sessions[security.code]
        return is_trading_time(cur_time, trading_sessions)

    def is_trading_time(self, cur_datetime: datetime) -> bool:
        """Whether the gateway is within trading time (a gateway
        might have different securities that are of different
        trading hours)
        """
        # Any security is in trading session, we return True
        _is_trading_time = False
        for security in self.securities:
            _is_trading_time = self.is_security_trading_time(
                security, cur_datetime.time())
            if _is_trading_time:
                break
        return _is_trading_time

    @market_datetime.setter
    def market_datetime(self, value):
        """Set market time"""
        self._market_datetime = value

    def get_order(self, orderid):
        """Get order"""
        return self.orders.get(orderid)

    def find_deals_with_orderid(self, orderid: str) -> List[Deal]:
        """Find deals based on orderid"""
        found_deals = []
        for dealid in self.deals:
            deal = self.deals.get(dealid)
            if deal.orderid == orderid:
                found_deals.append(deal)
        return found_deals

    def place_order(self, order: Order):
        """Place order"""
        raise NotImplementedError("[place_order] has not been implemented")

    def cancel_order(self, orderid):
        """Cancel order"""
        raise NotImplementedError("[cancel_order] has not been implemented")

    def get_broker_balance(self) -> AccountBalance:
        """Get broker balance"""
        raise NotImplementedError(
            "[get_broker_balance] has not been implemented")

    def get_broker_position(
            self,
            security: Security,
            direction: Direction
    ) -> PositionData:
        """Get broker position"""
        raise NotImplementedError(
            "[get_broker_position] has not been implemented")

    def get_all_broker_positions(self) -> List[PositionData]:
        """Get all broker positions"""
        raise NotImplementedError(
            "[get_all_broker_positions] has not been implemented")

    def get_all_orders(self) -> List[Order]:
        """Get all orders (sent by current algo)"""
        all_orders = []
        for orderid, order in self.orders.queue.items():
            order.orderid = orderid
            all_orders.append(order)
        return all_orders

    def get_all_deals(self) -> List[Deal]:
        """Get all deals (sent by current algo and got executed)"""
        all_deals = []
        for dealid, deal in self.deals.queue.items():
            deal.dealid = dealid
            all_deals.append(deal)
        return all_deals

    @property
    def trade_mode(self):
        return self._trade_mode

    @trade_mode.setter
    def trade_mode(self, trade_mode: TradeMode):
        self._trade_mode = trade_mode

    def get_quote(self, security: Security) -> Quote:
        """Get quote"""
        raise NotImplementedError("[get_quote] has not been implemented")

    def get_orderbook(self, security: Security) -> OrderBook:
        """Get orderbook"""
        raise NotImplementedError("[get_orderbook] has not been implemented")

    def req_historical_bars(
            self,
            security: Security,
            periods: int,
            freq: str,
            cur_datetime: datetime = None,
            daily_open_time: Time = None,
            daily_close_time: Time = None,
    ) -> List[Bar]:
        """request historical bar data."""
        raise NotImplementedError(
            "[req_historical_bars] has not been implemented")

    def subscribe(self):
        """Subscribe market data (quote and orderbook, and ohlcv)"""
        raise NotImplementedError("[subscribe] has not been implemented")

    def unsubscribe(self):
        """Unsubscribe market data (quote and orderbook, and ohlcv)"""
        raise NotImplementedError("[unsubscribe] has not been implemented")


class BaseFees:
    """Base class for fees"""
    commissions: float = 0              # Broker fee
    platform_fees: float = 0            # Broker fee
    system_fees: float = 0              # Exchange fee
    settlement_fees: float = 0          # Clearing fee
    stamp_fees: float = 0               # Government Stamp Duty
    trade_fees: float = 0               # Exchange Fee
    transaction_fees: float = 0         # (SFC) transaction levy
    total_fees: float = 0
    total_trade_amount: float = 0
    total_number_of_trades: float = 0
