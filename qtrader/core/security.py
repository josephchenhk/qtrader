# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:05 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: security.py
# @Software: PyCharm

from dataclasses import dataclass

from qtrader.core.constants import Exchange


@dataclass(frozen=True)
class Stock:
    """
    股票的基本属性
    """

    code:str
    stock_name:str
    lot_size:int = 1
    exchange:Exchange = Exchange.SEHK # 默认港股

    def __eq__(self, other):
        return (self.code==other.code) and (self.stock_name==other.stock_name)

