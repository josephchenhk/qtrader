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
    CQG fee model
    """

    def __init__(self, *deals: Deal):
        # Platform fees (to the platform)
        commissions = 0
        platform_fees = 0
        # Agency fees (to other parties such as exchange, tax authorities)
        system_fees = 0
        settlement_fees = 0
        stamp_fees = 0
        trade_fees = 0
        transaction_fees = 0

        for deal in deals:
            # price = deal.filled_avg_price
            quantity = deal.filled_quantity
            commissions += 1.92 * quantity  # 1.92 per contract

        # Total fees
        total_fees = (
            commissions
            + platform_fees
            + system_fees
            + settlement_fees
            + stamp_fees
            + trade_fees
            + transaction_fees
        )

        self.commissions = commissions
        self.platform_fees = platform_fees
        self.system_fees = system_fees
        self.settlement_fees = settlement_fees
        self.stamp_fees = stamp_fees
        self.trade_fees = trade_fees
        self.transaction_fees = transaction_fees
        self.total_fees = total_fees
