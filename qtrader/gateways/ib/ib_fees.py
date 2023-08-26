# -*- coding: utf-8 -*-
# @Time    : 16/10/2021 10:25 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: ib_fees.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import math

from qtrader.core.deal import Deal
from qtrader.gateways.base_gateway import BaseFees


class IbHKEquityFees(BaseFees):
    """
    https://www.interactivebrokers.com.hk/en/index.php?f=1590

    Tier breaks and the applicable commission rates are provided on the IBKR website. In general, volume/value tiers are
    calculated *once daily*, not at the time of the trade. As such, execution reductions will start the trading day after
    the threshold has been exceeded. （https://ibkr.info/article/1197）

    - Government Stamp Duty and SFC Transaction Levy (https://ibkr.info/article/4017)
    - Exchange Fee and Clearing Fee (https://www.interactivebrokers.com.hk/en/index.php?f=1315&nhf=T)
    - Hong Kong Tier Commission And Fixed Commission (https://www.interactivebrokers.com.hk/en/index.php?f=49708)
    """

    def __init__(self, *deals: Deal):
        for deal in deals:
            price = deal.filled_avg_price
            size = deal.filled_quantity
            trade_amount = price * size
            self.total_number_of_trades += 1
            self.total_trade_amount += trade_amount

            # Exchange Fee
            system_fee = round(0.50, 2)
            self.system_fees += system_fee

            # CLearing Fee
            settlement_fee = 0.00002 * trade_amount
            if settlement_fee < 2.0:
                settlement_fee = 2.0
            elif settlement_fee > 100.0:
                settlement_fee = 100.0
            settlement_fee = round(settlement_fee, 2)
            self.settlement_fees += settlement_fee

            # Government Stamp Duty, applies only to stocks
            stamp_fee = math.ceil(0.0013 * trade_amount)
            self.stamp_fees += stamp_fee

            # Exchange Fee
            trade_fee = max(0.00005 * trade_amount, 0.01)
            trade_fee = round(trade_fee, 2)
            self.trade_fees += trade_fee

            # SFC transaction levy, applies to stocks and warrrants
            transaction_fee = max(0.000027 * trade_amount, 0.01)
            transaction_fee = round(transaction_fee, 2)
            self.transaction_fees += transaction_fee

        # Hong Kong Fixed Commissions
        self.commissions += max(0.0008 * self.total_trade_amount, 18)
        self.commissions = round(self.commissions, 2)

        # Platform fee
        self.platform_fees = 0

        # Total fee
        self.total_fees = (
            self.commissions
            + self.platform_fees
            + self.system_fees
            + self.settlement_fees
            + self.stamp_fees
            + self.trade_fees
            + self.transaction_fees)


class IbSHSZHKConnectEquityFees(BaseFees):
    """
    https://www.interactivebrokers.com.hk/en/index.php?f=1590

    Tier breaks and the applicable commission rates are provided on the IBKR website. In general, volume/value tiers are
    calculated *once daily*, not at the time of the trade. As such, execution reductions will start the trading day after
    the threshold has been exceeded. （https://ibkr.info/article/1197）

    - Government Stamp Duty and SFC Transaction Levy (https://ibkr.info/article/4017)
    - Exchange Fee and Clearing Fee (https://www.interactivebrokers.com.hk/en/index.php?f=11719&nhf=T)
    - Hong Kong Tier Commission And Fixed Commission (https://www.interactivebrokers.com.hk/en/index.php?f=49708)
    """

    def __init__(self, *deals: Deal):
        for deal in deals:
            price = deal.filled_avg_price
            size = deal.filled_quantity
            trade_amount = price * size
            self.total_number_of_trades += 1
            self.total_trade_amount += trade_amount

            # Exchange Fee, security management
            system_fee = round(0.00002 * trade_amount, 2)
            self.system_fees += system_fee

            # CLearing Fee
            settlement_fee = round(0.00004 * trade_amount, 2)
            self.settlement_fees += settlement_fee

            # Sale proceeds Stamp Duty, applies only to stocks
            stamp_fee = round(0.001 * trade_amount, 2)
            self.stamp_fees += stamp_fee

            # Exchange Fee, handling fee
            trade_fee = round(0.0000487 * trade_amount, 2)
            self.trade_fees += trade_fee

            # SFC transaction levy, applies to stocks and warrrants
            transaction_fee = 0
            self.transaction_fees += transaction_fee

        # Hong Kong Fixed Commissions
        self.commissions += max(0.0008 * self.total_trade_amount, 18)
        self.commissions = round(self.commissions, 2)

        # Platform fee
        self.platform_fees = 0

        # Total fee
        self.total_fees = (
            self.commissions
            + self.platform_fees
            + self.system_fees
            + self.settlement_fees
            + self.stamp_fees
            + self.trade_fees
            + self.transaction_fees)


class IbUSFuturesFees(BaseFees):
    """
    https://www.interactivebrokers.com.hk/en/index.php?f=1590&p=futures

    For simplicity, use 0.85 USD/contract
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
            commissions += 0.85 * quantity  # 1.92 per contract

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
