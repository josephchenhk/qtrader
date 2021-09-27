# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:05 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: security.py
# @Software: PyCharm

from dataclasses import dataclass, replace

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
        return hash(f"{self.security_name}|{self.code}|{self.exchange.value}")


@dataclass(frozen=True)
class Stock(Security):
    """
    股票的基本属性
    """

    code:str
    security_name:str
    lot_size:int = 1                   # 默认1手
    exchange:Exchange = Exchange.SEHK  # 默认香港股票

    def __post_init__(self):
        pass


@dataclass(frozen=True)
class Currency(Security):
    """
    外汇的基本属性
    """

    code:str
    security_name:str
    lot_size:int = 1000                    # 默认1000
    exchange:Exchange = Exchange.IDEALPRO  # 默认IDEALPRO

    def __post_init__(self):
        pass


