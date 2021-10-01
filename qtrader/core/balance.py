# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:20 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: balance.py
# @Software: PyCharm

from dataclasses import dataclass
from typing import Dict


@dataclass
class AccountBalance:
    """
    账户资金信息
    """
    cash:float = 0.0                       # 现金                   CashBalance(BASE)
    cash_by_currency:Dict[str,float]=None  # 现金按货币分类           CashBlance(HKD, USD, GBP)
    available_cash:float = 0.0             # 购买力 = 现金 - 冻结现金  AvailableFunds(HKD)
    max_power_short:float = None           # 卖空购买力
    net_cash_power:float = None            # 现金购买力              BuyingPower(HKD)
    maintenance_margin:float = None        # 维持保证金              MaintMarginReq(HKD)
    unrealized_pnl:float = 0.0             # 为实现盈亏              UnrealizedPnL(HKD)
    realized_pnl:float = 0.0               # 已实现盈亏              RealizedPnL(HKD)

