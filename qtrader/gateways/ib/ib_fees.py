# -*- coding: utf-8 -*-
# @Time    : 16/10/2021 10:25 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: ib_fees.py
# @Software: PyCharm

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

    def __init__(self, *deals:Deal):
        for deal in deals:
            price = deal.filled_avg_price
            size = deal.filled_quantity
            trade_amount = price * size
            self.total_number_of_trades += 1
            self.total_trade_amount += trade_amount

            # 交易系统使用费（Exchange Fee）
            system_fee = round(0.50, 2)
            self.system_fees += system_fee

            # 交收费（CLearing Fee）
            settlement_fee = 0.00002 * trade_amount
            if settlement_fee < 2.0:
                settlement_fee = 2.0
            elif settlement_fee > 100.0:
                settlement_fee = 100.0
            settlement_fee = round(settlement_fee, 2)
            self.settlement_fees += settlement_fee

            # 印花税（Government Stamp Duty, applies only to stocks）
            stamp_fee = math.ceil(0.0013 * trade_amount)
            self.stamp_fees += stamp_fee

            # 交易费（Exchange Fee）
            trade_fee = max(0.00005 * trade_amount, 0.01)
            trade_fee = round(trade_fee, 2)
            self.trade_fees += trade_fee

            # 交易征费（SFC transaction levy, applies to stocks and warrrants）
            transaction_fee = max(0.000027 * trade_amount, 0.01)
            transaction_fee = round(transaction_fee, 2)
            self.transaction_fees += transaction_fee

        # 佣金(Hong Kong Fixed Commissions)
        self.commissions += max(0.0008 * self.total_trade_amount, 18)
        self.commissions = round(self.commissions, 2)

        # 平台使用费
        self.platform_fees = 0

        # 总费用
        self.total_fees = (
            self.commissions +
            self.platform_fees +
            self.system_fees +
            self.settlement_fees +
            self.stamp_fees +
            self.trade_fees +
            self.transaction_fees
        )


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

    def __init__(self, *deals:Deal):
        for deal in deals:
            price = deal.filled_avg_price
            size = deal.filled_quantity
            trade_amount = price * size
            self.total_number_of_trades += 1
            self.total_trade_amount += trade_amount

            # 交易系统使用费（Exchange Fee, security management）
            system_fee = round(0.00002 * trade_amount, 2)
            self.system_fees += system_fee

            # 交收费（CLearing Fee）
            settlement_fee = round(0.00004 * trade_amount, 2)
            self.settlement_fees += settlement_fee

            # 印花税（Sale proceeds Stamp Duty, applies only to stocks）
            stamp_fee = round(0.001 * trade_amount, 2)
            self.stamp_fees += stamp_fee

            # 交易费（Exchange Fee, handling fee）
            trade_fee = round(0.0000487 * trade_amount, 2)
            self.trade_fees += trade_fee

            # 交易征费（SFC transaction levy, applies to stocks and warrrants）
            transaction_fee = 0
            self.transaction_fees += transaction_fee

        # 佣金(Hong Kong Fixed Commissions)
        self.commissions += max(0.0008 * self.total_trade_amount, 18)
        self.commissions = round(self.commissions, 2)

        # 平台使用费
        self.platform_fees = 0

        # 总费用
        self.total_fees = (
            self.commissions +
            self.platform_fees +
            self.system_fees +
            self.settlement_fees +
            self.stamp_fees +
            self.trade_fees +
            self.transaction_fees
        )