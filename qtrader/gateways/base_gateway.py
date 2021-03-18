# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 1:24 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: base_gateway.py
# @Software: PyCharm
from abc import ABC
from typing import List

from qtrader.core.deal import Deal


class BaseGateway(ABC):
    """
    交易通道基类
    Abstract gateway class for creating gateways connection
    to different trading systems.
    """
    def __init__(self):
        self.orders = dict()
        self.deals = dict()

    def close(self):
        """与实盘对应的功能，在回测gateway里无需实现任何功能"""
        pass

    def get_order(self, orderid):
        """获取订单的状态"""
        return self.orders.get(orderid)

    def find_deals_with_orderid(self, orderid:str)->List[Deal]:
        """根据orderid找出成交的deal"""
        found_deals = []
        for dealid in self.deals:
            deal = self.deals[dealid]
            if deal.orderid==orderid:
                found_deals.append(deal)
        return found_deals

