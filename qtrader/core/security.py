# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:05 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: security.py
# @Software: PyCharm

from dataclasses import dataclass

from qtrader.core.constants import Exchange


@dataclass(frozen=True)
class Security:
    """
    证券的基本属性
    """

    code:str
    security_name:str
    lot_size:int = None
    exchange:Exchange = None

    def __eq__(self, other):
        return (self.code==other.code) and (self.security_name==other.security_name)

    def __hash__(self):
        return hash(f"{self.security_name}|{self.code}")


class Stock(Security):
    """
    股票的基本属性
    """
    stock_name:str

    def __post_init__(self):
        self.stock_name = self.security_name
        if self.lot_size is None: self.lot_size = 1 # 默认1手
        if self.exchange is None: self.exchange = Exchange.SEHK # 默认香港股票



