# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:18 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: position.py
# @Software: PyCharm
from datetime import datetime

from qtrader.core.constants import Direction, Offset
from qtrader.core.security import Stock


class PositionData:

    """Position record"""

    def __init__(self, security:Stock, direction:Direction, holding_price:float, quantity:int, update_time:datetime):
        self.security = security
        self.direction = direction
        self.holding_price = holding_price
        self.quantity = quantity
        self.update_time = update_time

    def __str__(self):
        return f"PositionData[{self.security}, {self.direction}, {self.holding_price}, {self.quantity}, {self.update_time}]"
    __repr__=__str__


class Position:

    """记录持仓"""

    def __init__(self, holdings={}):
        self.holdings = holdings

    def update(self, position_data:PositionData, offset:Offset):
        security = position_data.security
        direction = position_data.direction
        holding_price = position_data.holding_price
        quantity = position_data.quantity
        update_time = position_data.update_time
        if offset==Offset.OPEN:
            if security not in self.holdings:
                self.holdings[security] = {}
                self.holdings[security][direction] = position_data
            elif direction not in self.holdings[security]:
                self.holdings[security][direction] = position_data
            else:
                old_position_data = self.holdings[security][direction]
                new_quantity = old_position_data.quantity + quantity
                new_holding_price = (holding_price*quantity + old_position_data.holding_price*old_position_data.quantity) / new_quantity
                self.holdings[security][direction] = PositionData(
                    security = security,
                    direction = direction,
                    holding_price = new_holding_price,
                    quantity = new_quantity,
                    update_time = update_time
                )
        elif offset==offset.CLOSE:
            offset_direction = Direction.SHORT if direction==Direction.LONG else Direction.LONG
            old_position_data = self.holdings[security][offset_direction]
            new_quantity = old_position_data.quantity - quantity
            if new_quantity>0:
                new_holding_price = (old_position_data.holding_price*old_position_data.quantity - holding_price*quantity) / new_quantity
                self.holdings[security][offset_direction] = PositionData(
                    security = security,
                    direction = offset_direction,
                    holding_price = new_holding_price,
                    quantity = new_quantity,
                    update_time = update_time
                )
            else:
                self.holdings[security].pop(offset_direction, None)
        if len(self.holdings[security])==0:
            self.holdings.pop(security, None)

    def get_position(self, security:Stock, direction:Direction)->PositionData:
        return self.holdings[security][direction]