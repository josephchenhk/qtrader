# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:22 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: portfolio.py
# @Software: PyCharm

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, Offset
from qtrader.core.deal import Deal
from qtrader.core.position import Position, PositionData
from qtrader.gateways import BaseGateway


class Portfolio:
    """
    投资组合
    """

    def __init__(self, account_balance:AccountBalance, position:Position, market:BaseGateway):
        self.account_balance = account_balance
        self.position = position
        self.market = market

    def update(self, deal:Deal):
        security = deal.security
        price = deal.filled_avg_price
        quantity = deal.filled_quantity
        direction = deal.direction
        offset = deal.offset
        filled_time = deal.updated_time
        # fee = self.market.fees({"price": price, "size": quantity}).total_fees
        fee = self.market.fees(deal).total_fees
        # update balance
        self.account_balance.cash -= fee
        if direction==Direction.LONG:
            self.account_balance.cash -= price*quantity
            if offset==Offset.CLOSE: # close a short position, need to pay short interest
                short_position = self.position.holdings[security][Direction.SHORT]
                short_interest = short_position.holding_price * short_position.quantity * (
                    filled_time - short_position.update_time).days / 365 * self.market.SHORT_INTEREST_RATE
                self.account_balance.cash -= short_interest
        elif direction==Direction.SHORT:
            self.account_balance.cash += price * quantity
        # update position
        position_data = PositionData(
            security=security,
            direction=direction,
            holding_price=price,
            quantity=quantity,
            update_time=deal.updated_time
        )
        self.position.update(
            position_data=position_data,
            offset=offset
        )

    @property
    def value(self):
        v = self.account_balance.cash
        for security in self.position.holdings:
            cur_price = self.market.get_recent_data(security=security, cur_datetime=self.market.market_datetime, dfield="kline").close
            for direction in self.position.holdings[security]:
                position_data = self.position.holdings[security][direction]
                if direction==Direction.LONG:
                    v += cur_price * position_data.quantity
                elif direction==Direction.SHORT:
                    v -= cur_price * position_data.quantity
        return v