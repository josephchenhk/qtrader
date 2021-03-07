# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:05 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: security.py
# @Software: PyCharm

class Stock:
    """股票的基本属性"""
    def __init__(self, code:str, lot_size:int, stock_name:str):
        self.code = code
        self.lot_size = lot_size
        self.stock_name = stock_name

    def __str__(self):
        return f"Stock[{self.code}, {self.stock_name}, {self.lot_size}]"
    __repr__=__str__