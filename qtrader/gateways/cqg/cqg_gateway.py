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

from typing import Dict, List, Union, Any
from datetime import datetime

from qtrader.gateways.cqg.wrapper.cqg_api import CQGAPI
from qtrader.gateways.cqg.wrapper.CELEnvironment import CELEnvironment
from qtrader.gateways.base_gateway import BaseFees
from qtrader.gateways import BaseGateway
from qtrader.config import GATEWAYS, DATA_PATH
from qtrader.core.utility import Time
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.security import Stock, Security
from qtrader.core.position import PositionData
from qtrader.core.order import Order
from qtrader.core.deal import Deal
from qtrader.core.constants import OrderStatus
from qtrader.core.constants import Direction, TradeMode, Exchange
from qtrader.core.balance import AccountBalance

"""
Please install CQG Integrated Client (IC) on your machine first
Ref: https://partners.cqg.com/api-resources/cqg-data-and-trading-apis

Install pywin32
> pip install pywin32==225
"""


CQG = GATEWAYS.get("Cqg")


class CqgGateway(BaseGateway):

    # Assuming 24 hours
    TRADING_HOURS_AM = [Time(0, 0, 0), Time(12, 0, 0)]
    TRADING_HOURS_PM = [Time(12, 0, 0), Time(23, 59, 59)]

    # Minimal time step in seconds
    TIME_STEP = 60

    # Short interest rate (might not be used at the moment)
    SHORT_INTEREST_RATE = 0

    # Name of the gateway
    NAME = "CQG"

    def __init__(self,
                 securities: List[Security],
                 gateway_name: str,
                 start: datetime = None,
                 end: datetime = None,
                 fees: BaseFees = BaseFees,
                 ):
        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        self.trade_mode = None

        # convert symbols
        self.cqg_symbol_to_qt_security_map = {}
        self.qt_security_to_cqg_symbol_map = {}
        for security in securities:
            cqg_symbol = get_cqg_symbol(security)
            self.cqg_symbol_to_qt_security_map[cqg_symbol] = security
            self.qt_security_to_cqg_symbol_map[security] = cqg_symbol

        self.cqg_quotes = {s: None for s in securities}      # key:Security, value:Quote
        self.cqg_orderbooks = {s: None for s in securities}  # key:Security, value:Orderbook

        # Logon CQG API
        self.celEnvironment = CELEnvironment()
        self.api = self.celEnvironment.Init(CQGAPI, None)
        if not self.celEnvironment.errorHappened:
            self.api.Init(self)
            self.api.Logon(user_name=CQG["broker_account"], password=CQG["password"])
            self.api.subscribe_account_and_positions(GWAccountID=CQG["GWAccountID"])
        # subscribe data
        self.subscribe()

    def close(self):
        # Unsubscribe market data
        for security in self.securities:
            cqg_symbol = get_cqg_symbol(security)
            # Quote
            self.api.unsubscribe_quote(cqg_symbol)
            # Orderbook
            self.api.unsubscribe_orderbook(cqg_symbol)
            # Bar
            self.api.unsubscribe_bar(cqg_symbol)
        # Logoff CQG API
        self.api.Logoff()
        self.celEnvironment.Shutdown()

    def connect_quote(self):
        """
        行情需要处理报价和订单簿
        """
        print("行情接口连接成功")

    def connect_trade(self):
        """
        交易需要处理订单和成交
        """
        print("交易接口连接成功")

    def process_quote(self, quote:Quote):
        """更新报价的状态"""
        security = quote.security
        self.quote.put(security, quote)

    def process_orderbook(self, orderbook:OrderBook):
        """更新订单簿的状态"""
        security = orderbook.security
        self.orderbook.put(security, orderbook)

    def process_order(self, content: Dict[str, Any]):
        """更新订单的状态"""
        orderid = content["order_id"]
        order = self.orders.get(orderid)  # blocking
        order.orderid = orderid
        order.updated_time = content["updated_time"]
        order.filled_avg_price = content["filled_avg_price"]
        order.filled_quantity = content["filled_quantity"]
        order.status = content["status"]
        self.orders.put(orderid, order)

    def process_deal(self, content: Dict[str, Any]):
        """更新成交的信息"""
        orderid = content["order_id"]
        dealid = content["deal_id"]
        order = self.orders.get(orderid)  # blocking
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=content["updated_time"],
            filled_avg_price=content["filled_avg_price"],
            filled_quantity=content["filled_quantity"],
            dealid=dealid,
            orderid=orderid
        )
        self.deals.put(dealid, deal)

    @property
    def market_datetime(self):
        return datetime.now()

    def set_trade_mode(self, trade_mode: TradeMode):
        if trade_mode not in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            raise ValueError(f"CqgGateway only supports `SIMULATE` or `LIVETRADE` mode, {trade_mode} was passed in instead.")
        self.trade_mode = trade_mode

    def subscribe(self):

        for security in self.securities:
            cqg_symbol = get_cqg_symbol(security)
            # Quote
            self.api.subscribe_quote(cqg_symbol)
            # Orderbook
            self.api.subscribe_orderbook(cqg_symbol)
            # Bar
            # TODO: set the number of bars to be configurable
            self.api.subscribe_bar(cqg_symbol, 20, "1Min")

    def is_trading_time(self, cur_datetime: datetime) -> bool:
        """
        判断当前时间是否属于交易时间段

        :param cur_datetime:
        :return:
        """
        # TODO: 先判断是否交易日
        cur_time = Time(
            hour=cur_datetime.hour,
            minute=cur_datetime.minute,
            second=cur_datetime.second)
        return (self.TRADING_HOURS_AM[0] <= cur_time <= self.TRADING_HOURS_AM[1]) or (
            self.TRADING_HOURS_PM[0] <= cur_time <= self.TRADING_HOURS_PM[1])

    def get_recent_bars(self, security: Security, periods: int) -> List[Bar]:
        """
        获取最接近当前时间的若干条bar
,
        :param security:
        :param cur_time:
        :return:
        """
        bars = self.api.get_subscribe_bars_data(security)
        if bars:
            assert len(bars) >= periods, f"There is not sufficient number of bars, {periods} was requested, " \
                                         f"but only {len(bars)} is available"

            return bars[-periods:]
        return None

    def get_recent_bar(self, security: Security) -> Bar:
        """
        获取最接近当前时间的最新bar
,
        :param security:
        :param cur_time:
        :return:
        """
        bars = self.get_recent_bars(security=security, periods=1)
        if bars:
            return bars[0]
        return None

    def get_recent_capital_distribution(
            self, security: Stock) -> CapitalDistribution:
        """capital distribution"""
        raise NotImplementedError("get_recent_capital_distribution method is not yet implemented in CQG gateway!")

    def get_recent_data(self, security: Stock, **
                        kwargs) -> Dict[str, Union[Bar, CapitalDistribution]] or Union[Bar, CapitalDistribution]:
        """
        获取最接近当前时间的数据点
        """
        if kwargs:
            assert "dfield" in kwargs, f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
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

    def get_stock(self, code: str) -> Stock:
        """根据股票代号，找到对应的股票"""
        for stock in self.securities:
            if stock.code == code:
                return stock
        return None

    def place_order(self, order: Order) -> str:
        """提交订单"""
        orderid = self.api.place_order(order)  # if order submitted, must return orderid
        order.status = OrderStatus.SUBMITTED   # change status to submitted
        self.orders.put(orderid, order)        # update order status later via callbacks
        return orderid

    def cancel_order(self, orderid):
        """取消订单"""
        self.api.cancel_order(orderid)

    def get_broker_balance(self) -> AccountBalance:
        """获取券商资金"""
        self.api.update_account_balance()
        return self.api.account_balance

    def get_broker_position(self, security: Stock,
                            direction: Direction) -> PositionData:
        """获取券商持仓"""
        self.api.update_positions()
        positions = self.api.positions
        for position_data in positions:
            if position_data.security == security and position_data.direction == direction:
                return position_data
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """获取券商所有持仓"""
        self.api.update_positions()
        return self.api.positions

    def get_quote(self, security: Stock) -> Quote:
        """获取报价"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """获取订单簿"""
        return self.orderbook.get(security)

    def get_qt_security_from_cqg_symbol(self, cqg_symbol:str)->Security:
        """"""
        return self.cqg_symbol_to_qt_security_map[cqg_symbol]

    def get_cqg_symbol_from_qt_security(self, security:Security)->str:
        """"""
        return self.qt_security_to_cqg_symbol_map[security]


def get_cqg_symbol(security: Security) -> str:
    """Convert the security to CQG symbol
    """
    if security.exchange == Exchange.SGX:
        symbol = security.code.split(".")[1]
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
