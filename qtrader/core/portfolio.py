# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:22 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: portfolio.py
# @Software: PyCharm

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, Offset
from qtrader.core.order import Order
from qtrader.core.position import Position, PositionData
from qtrader.gateways import BacktestGateway
from qtrader.gateways.backtest import fees


class Portfolio:

    """投资组合"""

    def __init__(self, account_balance:AccountBalance, position:Position, market:BacktestGateway):
        self.account_balance = account_balance
        self.position = position
        self.market = market

    def update(self, filled_order:Order):
        security = filled_order.security
        price = filled_order.price
        volume = filled_order.volume
        direction = filled_order.direction
        offset = filled_order.offset
        filled_time = filled_order.filled_time
        fee = fees({"price": price, "size": volume})[-1]
        # update balance
        self.account_balance.total -= fee
        if direction==Direction.LONG:
            self.account_balance.total -= price*volume
            if offset==Offset.CLOSE: # close a short position, need to pay short interest
                short_position = self.position.holdings[security][Direction.SHORT]
                short_interest = short_position.holding_price * short_position.quantity * (
                    filled_time - short_position.update_time).days / 365 * BacktestGateway.SHORT_INTEREST_RATE
                self.account_balance.total -= short_interest
        elif direction==Direction.SHORT:
            self.account_balance.total += price * volume
        # update position
        position_data = PositionData(
            security=security,
            direction=direction,
            holding_price=price,
            quantity=volume,
            update_time=filled_order.filled_time
        )
        self.position.update(
            position_data=position_data,
            offset=offset
        )

    @property
    def value(self):
        v = self.account_balance.total
        for security in self.position.holdings:
            cur_price = self.market.prev_cache[security].close
            for direction in self.position.holdings[security]:
                position_data = self.position.holdings[security][direction]
                if direction==Direction.LONG:
                    v += cur_price * position_data.quantity
                elif direction==Direction.SHORT:
                    v -= cur_price * position_data.quantity
        return v