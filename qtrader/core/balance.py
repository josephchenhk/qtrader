# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:20 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: balance.py
# @Software: PyCharm

from dataclasses import dataclass


@dataclass
class AccountBalance:
    """
    账户资金信息
    """
    cash: float = 0.0                     # 初始现金
    power: float = None                   # 最大购买力
    max_power_short: float = None         # 卖空购买力
    net_cash_power: float = None          # 现金购买力
