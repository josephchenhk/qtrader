# -*- coding: utf-8 -*-
# @Time    : 11/22/2021 11:33 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: cqg_gateway.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import time
from typing import Dict, List, Any
from datetime import datetime
from datetime import timedelta
from time import sleep

from qtrader.gateways.cqg.wrapper.cqg_api import CQGAPI
from qtrader.gateways.cqg.wrapper.CELEnvironment import CELEnvironment
from qtrader.gateways.base_gateway import BaseFees
from qtrader.gateways import BaseGateway
from qtrader.config import GATEWAYS, DATA_PATH, TIME_STEP
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.security import Stock, Security
from qtrader.core.position import PositionData
from qtrader.core.order import Order
from qtrader.core.deal import Deal
from qtrader.core.constants import OrderStatus
from qtrader.core.constants import Direction, TradeMode, Exchange
from qtrader.core.balance import AccountBalance


"""
IMPORTANT
Please install CQG Integrated Client (IC) on your machine first
Ref: https://partners.cqg.com/api-resources/cqg-data-and-trading-apis

Install pywin32
> pip install pywin32==225
"""


CQG = GATEWAYS.get("Cqg")


class CqgGateway(BaseGateway):
    """CQG Gateway"""

    # Minimal time step, which was read from config
    TIME_STEP = TIME_STEP

    # Short interest rate, e.g., 0.0098 for HK stock
    SHORT_INTEREST_RATE = 0.0

    # Name of the gateway
    NAME = "CQG"

    def __init__(
            self,
            securities: List[Security],
            gateway_name: str,
            start: datetime = None,
            end: datetime = None,
            fees: BaseFees = BaseFees,
            **kwargs
    ):
        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        if "num_of_1min_bar" in kwargs:
            self.num_of_1min_bar = kwargs["num_of_1min_bar"]
        else:
            self.num_of_1min_bar = 30
        if "trading_sessions" in kwargs:
            self.trading_sessions = kwargs.get("trading_sessions")

        # Convert symbols
        self.cqg_symbol_to_qt_security_map = {}
        self.qt_security_to_cqg_symbol_map = {}
        for security in securities:
            cqg_symbol = get_cqg_symbol(security)
            self.cqg_symbol_to_qt_security_map[cqg_symbol] = security
            self.qt_security_to_cqg_symbol_map[security] = cqg_symbol

        # key:Security, value:Quote
        self.cqg_quotes = {s: None for s in securities}
        # key:Security, value:Orderbook
        self.cqg_orderbooks = {s: None for s in securities}

        # Logon CQG API
        self.connect_trade()

        # Subscribe data:
        # Wait until the end of the whole minute and start to request,
        # so that we can minimize the delay of the real-time bars.
        cur_time = datetime.now()
        if cur_time.second >= 1:
            req_time = datetime(
                year=cur_time.year,
                month=cur_time.month,
                day=cur_time.day,
                hour=cur_time.hour,
                minute=cur_time.minute) + timedelta(minutes=1)
            # +1 to ensure it is already next minute
            wait_time = (req_time - cur_time).seconds + 1
            print(f"Wait for {wait_time} seconds to subscribe data ...")
            time.sleep(wait_time)
        self.subscribe()

    def close(self):
        # Unsubscribe account
        self.api.unsubscribe_account_and_positions(
            GWAccountID=CQG["GWAccountID"])
        # Unsubscribe market data
        self.unsubscribe()
        # Logoff CQG API
        self.api.Logoff()
        # Shutdown CQG CEL
        self.celEnvironment.Shutdown()

    def connect_quote(self):
        pass

    def connect_trade(self):
        """Connect to CQG trade API"""
        self.celEnvironment = CELEnvironment()
        self.api = self.celEnvironment.Init(CQGAPI, None)  # start CEL
        if not self.celEnvironment.errorHappened:
            self.api.Init(self)
            self.api.Logon(
                user_name=CQG["broker_account"],
                password=CQG["password"])
            self.api.subscribe_account_and_positions(
                GWAccountID=CQG["GWAccountID"])
        print("Successfully connected to CQG API.")

    def process_quote(self, quote: Quote):
        """Quote"""
        security = quote.security
        self.quote.put(security, quote)

    def process_orderbook(self, orderbook: OrderBook):
        """Orderbook"""
        security = orderbook.security
        self.orderbook.put(security, orderbook)

    def process_order(self, content: Dict[str, Any]):
        """Order"""
        print("[process_order]")
        orderid = content["order_id"]
        # blocking, wait for 0.001 second
        order = self.orders.get(orderid, 0.001, None)
        if order is None:
            print(f"{content['updated_time']}: orderid {orderid} not found!")
            return
        order.orderid = orderid
        order.updated_time = content["updated_time"]
        order.filled_avg_price = content["filled_avg_price"]
        order.filled_quantity = content["filled_quantity"]
        order.status = content["status"]
        self.orders.put(orderid, order)
        print(f"orderid {orderid} has been put")

    def process_deal(self, content: Dict[str, Any]):
        """Deal"""
        print("[process_deal]")
        orderid = content["order_id"]
        # blocking, wait for 0.001 second
        order = self.orders.get(orderid, 0.001, None)
        if order is None:
            print(f"{content['updated_time']}: orderid {orderid} not found!")
            return
        dealid = content["deal_id"]
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=content["updated_time"],
            filled_avg_price=content["filled_avg_price"],
            filled_quantity=content["filled_quantity"],
            dealid=dealid,
            orderid=orderid)
        self.deals.put(dealid, deal)
        print(f"dealid {dealid} has been put")

    @property
    def market_datetime(self):
        return datetime.now()

    def set_trade_mode(self, trade_mode: TradeMode):
        if trade_mode not in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            raise ValueError(
                f"CqgGateway only supports `SIMULATE` or `LIVETRADE` mode, "
                f"{trade_mode} is invalid.")
        self._trade_mode = trade_mode

    def subscribe(self):
        # Subscribe market data
        for security in self.securities:
            cqg_symbol = get_cqg_symbol(security)
            # Quote
            self.api.subscribe_quote(cqg_symbol)
            # Orderbook
            self.api.subscribe_orderbook(cqg_symbol)
            # Bar
            self.api.subscribe_bar(cqg_symbol, self.num_of_1min_bar, "1Min")
            # Do not request data too frequently
            time.sleep(0.2)

    def unsubscribe(self):
        # Unsubscribe market data
        for security in self.securities:
            cqg_symbol = get_cqg_symbol(security)
            # Quote
            self.api.unsubscribe_quote(cqg_symbol)
            # Orderbook
            self.api.unsubscribe_orderbook(cqg_symbol)
            # Bar
            self.api.unsubscribe_bar(cqg_symbol)
            # Do not request data too frequently
            time.sleep(0.1)

    def get_recent_bars(self, security: Security, periods: int) -> List[Bar]:
        """Get recent OHLCV (with given periods)"""
        bars = self.api.get_subscribe_bars_data(security)
        if bars:
            assert len(bars) >= periods, (
                "There is not sufficient number of bars, "
                f"{periods} was requested, but only {len(bars)} is available"
            )
            return bars[-periods:]
        return None

    def get_recent_bar(self, security: Security) -> Bar:
        """Get recent OHLCV"""
        bars = self.get_recent_bars(security=security, periods=1)
        if bars:
            return bars[0]
        return None

    def get_recent_capital_distribution(
            self, security: Stock) -> CapitalDistribution:
        """capital distribution"""
        raise NotImplementedError(
            "get_recent_capital_distribution method is not yet implemented in "
            "CQG gateway!")

    def get_recent_data(
            self,
            security: Stock,
            **kwargs
    ) -> Dict or Bar or CapitalDistribution:
        """Get recent data (OHLCV or CapitalDistribution)"""
        if kwargs:
            assert "dfield" in kwargs, (
                f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
            )
            dfields = [kwargs["dfield"]]
        else:
            dfields = DATA_PATH
        data = dict()
        for dfield in dfields:
            if dfield == "kline":
                data[dfield] = self.get_recent_bar(security)
            elif dfield == "capdist":
                data[dfield] = self.get_recent_capital_distribution(security)
        if len(dfields) == 1:
            return data[dfield]
        return data

    def get_security(self, code: str) -> Security:
        """Get security with security code"""
        for security in self.securities:
            if security.code == code:
                return security
        return None

    def place_order(self, order: Order) -> str:
        """Place an order"""
        # if order submitted, must return orderid
        orderid = self.api.place_order(order)
        # change status to submitted
        order.status = OrderStatus.SUBMITTED
        # update order status later via callbacks
        self.orders.put(orderid, order)
        return orderid

    def cancel_order(self, orderid):
        """Cancel order by orderid"""
        self.api.cancel_order(orderid)

    def get_broker_balance(self) -> AccountBalance:
        """Get broker balance"""
        self.api.update_account_balance()
        return self.api.account_balance

    def get_broker_position(
            self, security: Stock,
            direction: Direction
    ) -> PositionData:
        """Get broker position"""
        self.api.update_positions()
        positions = self.api.positions
        for position_data in positions:
            if (
                position_data.security == security
                and position_data.direction == direction
            ):
                return position_data
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """Get all broker positions"""
        self.api.update_positions()
        return self.api.positions

    def get_quote(self, security: Stock) -> Quote:
        """Get quote"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """Get orderbook"""
        return self.orderbook.get(security)

    def get_qt_security_from_cqg_symbol(self, cqg_symbol: str) -> Security:
        """Get security with cqg symbol"""
        if cqg_symbol in self.cqg_symbol_to_qt_security_map:
            return self.cqg_symbol_to_qt_security_map[cqg_symbol]

    def get_cqg_symbol_from_qt_security(self, security: Security) -> str:
        """Get cqg symbol with security"""
        if security in self.qt_security_to_cqg_symbol_map:
            return self.qt_security_to_cqg_symbol_map[security]

    def req_historical_bars(
            self,
            security: Security,
            periods: int,
            freq: str,
            cur_datetime: datetime,
            trading_sessions: List[datetime] = None,
            **kwargs
    ) -> List[Bar]:
        """request historical bar data."""
        # Check params
        if (
            freq == "1Day"
            and (trading_sessions is None or len(trading_sessions) == 0)
        ):
            raise ValueError(
                f"Parameters trading_sessions is mandatory if freq={freq}.")

        # return historical bar data
        if freq == "1Min":
            return _req_historical_bars_cqg_1min(
                security=security,
                periods=periods,
                gateway=self)
        elif freq == "1Day":
            return _req_historical_bars_cqg_1day(
                security=security,
                periods=periods,
                gateway=self)

        # freq is not valid
        FREQ_ALLOWED = ("1Day", "1Min")
        raise ValueError(
            f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} are "
            "allowed.")


def _req_historical_bars_cqg_1min(
        security: Security,
        periods: int,
        gateway: BaseGateway,
        **kwargs
) -> List[Bar]:
    """Request 1min bars (by default we have already subscribed 1min bars)"""
    bars = gateway.get_recent_bars(security, periods)
    return bars


def _req_historical_bars_cqg_1day(
        security: Security,
        periods: int,
        gateway: BaseGateway,
        **kwargs
) -> List[Bar]:
    """Request 1day bars"""
    cqg_symbol = get_cqg_symbol(security)

    gateway.api.unsubscribe_bar(cqg_symbol)
    sleep(3)
    freq = "1Day"
    gateway.api.subscribe_bar(cqg_symbol, periods, freq)
    gateway.api.eventBarDone.wait(60)
    bars = gateway.get_recent_bars(security, periods)
    gateway.api.unsubscribe_bar(cqg_symbol)

    gateway.api.subscribe_bar(cqg_symbol, gateway.num_of_1min_bar, "1Min")
    return bars


def get_cqg_symbol(security: Security) -> str:
    """Convert the security to CQG symbol
    """
    if security.exchange == Exchange.SGX:
        symbol = security.code.split(".")[1]
        year = security.expiry_date[0:4]
        month = int(security.expiry_date[4:6])
        cqg_symbol = f"{symbol}{cme_contract_month_codes(month)}{year[2:]}"
        return cqg_symbol
    elif security.exchange == Exchange.NYMEX:
        symbol = security.code.split(".")[1]
        year = security.expiry_date[0:4]
        month = int(security.expiry_date[4:6])
        cqg_symbol = f"{symbol}E{cme_contract_month_codes(month)}{year[2:]}"
        return cqg_symbol
    elif security.exchange == Exchange.ICE:
        symbol = security.code.split(".")[1]
        # special handling for Brent Oil (CO -> QO)
        if symbol == "CO":
            symbol = "QO"
        year = security.expiry_date[0:4]
        month = int(security.expiry_date[4:6])
        cqg_symbol = f"{symbol}{cme_contract_month_codes(month)}{year[2:]}"
        return cqg_symbol
    raise ValueError(f"{security} is not valid security in CQG!")


def cme_contract_month_codes(month: int) -> str:
    """ CME month codes
    Ref: https://www.cmegroup.com/month-codes.html
    """
    assert month in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
                     12], f"Month must be between 1 to 12, got {month} instead!"
    return {1: "F",
            2: "G",
            3: "H",
            4: "J",
            5: "K",
            6: "M",
            7: "N",
            8: "Q",
            9: "U",
            10: "V",
            11: "X",
            12: "Z"}.get(month)
