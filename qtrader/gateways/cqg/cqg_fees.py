# -*- coding: utf-8 -*-
# @Time    : 9/15/2021 3:11 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: cqg_fees.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from qtrader.core.deal import Deal
from qtrader.gateways.base_gateway import BaseFees


class CQGFees(BaseFees):
    """
    """

    def __init__(self, *deals:Deal):
        # 平台收费
        commissions = 0       # 佣金
        platform_fees = 0     # 平台使用费
        # 平台代收费
        system_fees = 0       # 交易系统使用费
        settlement_fees = 0   # 交收费
        stamp_fees = 0        # 印花税
        trade_fees = 0        # 交易费
        transaction_fees = 0  # 交易征费

        for deal in deals:
            security = deal.security
            # price = deal.filled_avg_price
            quantity = deal.filled_quantity
            # direction = deal.direction
            commissions += 1.92 * quantity

        # 总费用
        total_fees = commissions + platform_fees + system_fees + settlement_fees + stamp_fees + trade_fees + transaction_fees

        self.commissions = commissions
        self.platform_fees = platform_fees
        self.system_fees = system_fees
        self.settlement_fees = settlement_fees
        self.stamp_fees = stamp_fees
        self.trade_fees = trade_fees
        self.transaction_fees = transaction_fees
        self.total_fees = total_fees
