# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 1:24 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: base_gateway.py
# @Software: PyCharm
from abc import ABC
from typing import List

from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.security import Stock
from qtrader.core.utility import BlockingDict


class BaseGateway(ABC):
    """
    交易通道基类
    Abstract gateway class for creating gateways connection
    to different trading systems.
    """
    def __init__(self, securities:List[Stock]):
        self.securities = securities
        self.orders = BlockingDict()
        self.deals = BlockingDict()

    def close(self):
        """与实盘对应的功能，在回测gateway里无需实现任何功能"""
        pass

    @property
    def market_datetime(self):
        """市场当前时间"""
        raise NotImplementedError("market_datetime has not been implemented")

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
        """place order"""
        raise NotImplementedError("place_order has not been implemented")

    def cancel_order(self, orderid):
        """cancel order"""
        raise NotImplementedError("cancel_order has not been implemented")

    def get_account_balance(self):
        """Account balance"""
        raise NotImplementedError("get_account_balance has not been implemented")

    def get_position(self):
        """Position"""
        raise NotImplementedError("get_position has not been implemented")

