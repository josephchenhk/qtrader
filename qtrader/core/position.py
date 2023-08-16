# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:18 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: position.py
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

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

from qtrader.core.constants import Direction, Offset
from qtrader.core.security import Stock

lock = threading.Lock()

@dataclass
class PositionData:
    """Position information of a specific security"""
    security: Stock
    direction: Direction
    holding_price: float
    quantity: int
    update_time: datetime


class Position:
    """Position information of a specific gateway (may include multiple
    securities)"""

    def __init__(self, holdings: Dict = None):
        if holdings is None:
            holdings = dict()
        self.holdings = holdings

    def update(self, position_data: PositionData, offset: Offset):
        with lock:
            security = position_data.security
            direction = position_data.direction
            holding_price = position_data.holding_price
            quantity = position_data.quantity
            update_time = position_data.update_time
            if offset == Offset.OPEN:
                if security not in self.holdings:
                    self.holdings[security] = {}
                    self.holdings[security][direction] = position_data
                elif direction not in self.holdings[security]:
                    self.holdings[security][direction] = position_data
                else:
                    old_position_data = self.holdings[security][direction]
                    new_quantity = old_position_data.quantity + quantity
                    new_total_value = (
                        old_position_data.holding_price * old_position_data.quantity
                        + holding_price * quantity
                    )
                    new_holding_price = new_total_value / new_quantity
                    self.holdings[security][direction] = PositionData(
                        security=security,
                        direction=direction,
                        holding_price=new_holding_price,
                        quantity=new_quantity,
                        update_time=update_time
                    )
            elif offset == offset.CLOSE:
                offset_direction = (
                    Direction.SHORT
                    if direction == Direction.LONG
                    else Direction.LONG)
                old_position_data = self.holdings[security][offset_direction]
                new_quantity = old_position_data.quantity - quantity
                if new_quantity > 0:
                    new_total_value = (
                        old_position_data.holding_price * old_position_data.quantity
                        - holding_price * quantity
                    )
                    new_holding_price = new_total_value / new_quantity
                    self.holdings[security][offset_direction] = PositionData(
                        security=security,
                        direction=offset_direction,
                        holding_price=new_holding_price,
                        quantity=new_quantity,
                        update_time=update_time
                    )
                else:
                    self.holdings[security].pop(offset_direction, None)
            if len(self.holdings[security]) == 0:
                self.holdings.pop(security, None)

    def get_position(
            self,
            security: Stock,
            direction: Direction
    ) -> PositionData:
        if security not in self.holdings:
            return None
        elif direction not in self.holdings[security]:
            return None
        return self.holdings[security][direction]

    def get_all_positions(self) -> List[PositionData]:
        positions = []
        for security in self.holdings:
            if Direction.LONG in self.holdings[security]:
                positions.append(self.holdings[security][Direction.LONG])
            if Direction.SHORT in self.holdings[security]:
                positions.append(self.holdings[security][Direction.SHORT])
        return positions

    def __str__(self):
        position_str = "Position(\n"
        for security in self.holdings:
            for direction in [Direction.LONG, Direction.SHORT]:
                if self.holdings[security].get(direction) is not None:
                    position_str += str(self.holdings[security][direction])
                    position_str += "\n"
        position_str += ")"
        return position_str
    __repr__ = __str__
