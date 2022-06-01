# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:20 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: balance.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class AccountBalance:
    """Account Balance Information"""
    cash: float = 0.0                          # CashBalance(BASE)
    cash_by_currency: Dict[str, float] = None  # CashBlance(HKD, USD, GBP)
    available_cash: float = 0.0                # AvailableFunds(HKD)
    max_power_short: float = None              # Cash Power for Short
    net_cash_power: float = None               # BuyingPower(HKD)
    maintenance_margin: float = None           # MaintMarginReq(HKD)
    unrealized_pnl: float = 0.0                # UnrealizedPnL(HKD)
    realized_pnl: float = 0.0                  # RealizedPnL(HKD)
