# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 1:24 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: base_gateway.py
# @Software: PyCharm
from abc import ABC
from typing import List

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction
from qtrader.core.data import Quote, OrderBook
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Stock
from qtrader.core.utility import BlockingDict
from qtrader.config import GATEWAYS


class BaseGateway(ABC):
    """
    交易通道基类
    Abstract gateway class for creating gateways connection
    to different trading systems.
    """
    broker_name = ""
    broker_account = ""
    def __init__(self, securities:List[Stock], gateway_name="Backtest"):
        self._market_datetime = None
        self.securities = securities
        assert gateway_name not in GATEWAYS, f"{gateway_name} is NOT in GATEWAYS, please check your config file!"
        self.broker_account = GATEWAYS[gateway_name]["broker_name"]
        self.broker_account = GATEWAYS[gateway_name]["broker_account"]
        self.orders = BlockingDict()
        self.deals = BlockingDict()
        self.quote = BlockingDict()
        self.orderbook = BlockingDict()

    def close(self):
        """与实盘对应的功能，在回测gateway里无需实现任何功能"""
        pass

    @property
    def market_datetime(self):
        """市场当前时间"""
        return self._market_datetime

    @market_datetime.setter
    def market_datetime(self, value):
        """设置市场当前时间"""
        self._market_datetime = value

    def get_order(self, orderid):
        """获取订单的状态"""
        return self.orders.get(orderid)

    def find_deals_with_orderid(self, orderid:str)->List[Deal]:
        """根据orderid找出成交的deal"""
        found_deals = []
        for dealid in self.deals:
            deal = self.deals.get(dealid)
            if deal.orderid==orderid:
                found_deals.append(deal)
        return found_deals

    def place_order(self, order:Order):
        """下单"""
        raise NotImplementedError("place_order has not been implemented")

    def cancel_order(self, orderid):
        """取消下单"""
        raise NotImplementedError("cancel_order has not been implemented")

    def get_broker_balance(self)->AccountBalance:
        """获取券商资金"""
        raise NotImplementedError("get_broker_balance has not been implemented")

    def get_broker_position(self, security:Stock, direction:Direction)->PositionData:
        """获取券商持仓"""
        raise NotImplementedError("get_broker_position has not been implemented")

    def get_all_broker_positions(self)->List[PositionData]:
        """获取券商所有持仓"""
        raise NotImplementedError("get_all_broker_positions has not been implemented")

    def get_all_orders(self) -> List[Order]:
        """获取当前运行策略的所有订单"""
        all_orders = []
        for orderid, order in self.orders.queue.items():
            order.orderid = orderid
            all_orders.append(order)
        return all_orders

    def get_all_deals(self) -> List[Deal]:
        """获取当前运行策略的所有成交"""
        all_deals = []
        for dealid, deal in self.deals.queue.items():
            deal.dealid = dealid
            all_deals.append(deal)
        return all_deals

    def get_quote(self, security:Stock)->Quote:
        """获取报价"""
        raise NotImplementedError("get_quote has not been implemented")

    def get_orderbook(self, security:Stock)->OrderBook:
        """获取订单簿"""
        raise NotImplementedError("get_orderbook has not been implemented")


class BaseFees:
    """收费明细，作为基类被覆写"""
    commissions: float = 0
    platform_fees: float = 0
    system_fees: float = 0
    settlement_fees: float = 0
    stamp_fees: float = 0
    trade_fees: float = 0
    transaction_fees: float = 0
    total_fees: float = 0