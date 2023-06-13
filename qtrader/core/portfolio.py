# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:22 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: portfolio.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
import threading

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, Offset
from qtrader.core.deal import Deal
from qtrader.core.position import Position, PositionData
from qtrader.gateways import BaseGateway

lock = threading.Lock()

class Portfolio:
    """Portfolio is bind to a specific gateway, it includes:
    1. Account Balance
    2. Position
    3. Market (Gateway)
    """

    def __init__(
            self,
            account_balance: AccountBalance,
            position: Position,
            market: BaseGateway):
        self.account_balance = account_balance
        self.position = position
        self.market = market

    def update(self, deal: Deal):
        with lock:
            security = deal.security
            lot_size = security.lot_size
            price = deal.filled_avg_price
            quantity = deal.filled_quantity
            direction = deal.direction
            offset = deal.offset
            filled_time = deal.updated_time
            fee = self.market.fees(deal).total_fees
            # update balance
            self.account_balance.cash -= fee
            if direction == Direction.LONG:
                self.account_balance.cash -= price * quantity * lot_size
                if offset == Offset.CLOSE:  # pay interest when closing short
                    short_position = self.position.holdings[security][Direction.SHORT]
                    short_interest = (
                        short_position.holding_price
                        * short_position.quantity
                        * (filled_time - short_position.update_time).days / 365
                        * self.market.SHORT_INTEREST_RATE
                    )
                    self.account_balance.cash -= short_interest
            elif direction == Direction.SHORT:
                self.account_balance.cash += price * quantity * lot_size
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
        with lock:
            v = self.account_balance.cash
            for security in self.position.holdings:
                recent_data = self.market.get_recent_data(
                    security=security,
                    cur_datetime=self.market.market_datetime,
                    dfield="kline"
                )
                if recent_data is not None:
                    cur_price = recent_data.close
                else:
                    # 2022.02.23 (Joseph): If bar data is not available, we will not
                    # be able to get the updated portfolio value; We circumvent this
                    # by using the holding prices of the securities (Be alerted that
                    # this is an estimation of the portfolio value, it is NOT
                    # accurate).
                    cur_price = 0
                    for i, pos in enumerate(self.position.holdings[security]):
                        cur_price += self.position.holdings[security][pos].holding_price
                    cur_price /= (i + 1)
                for direction in self.position.holdings[security]:
                    position_data = self.position.holdings[security][direction]
                    if direction == Direction.LONG:
                        v += cur_price * position_data.quantity * security.lot_size
                    elif direction == Direction.SHORT:
                        v -= cur_price * position_data.quantity * security.lot_size
            return v
