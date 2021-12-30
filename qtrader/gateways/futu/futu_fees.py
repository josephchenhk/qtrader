# -*- coding: utf-8 -*-
# @Time    : 16/10/2021 11:18 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: futu_fees.py
# @Software: PyCharm
import math

from qtrader.core.deal import Deal
from qtrader.gateways.base_gateway import BaseFees


class FutuHKEquityFees(BaseFees):
    """
    港股融资融券（8332）套餐一（适合一般交易者）
    融资利率: 年利率6.8%

    佣金: 0.03%， 最低3港元
    平台使用费: 15港元/笔

    交易系统使用费（香港交易所）: 每笔成交0.50港元
    交收费（香港结算所）: 0.002%， 最低2港元，最高100港元
    印花税（香港政府）: 0.13%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取 (原来0.10%, 新制0.13%)
    交易费（香港交易所）: 0.005%*成交金额，最低0.01港元
    交易征费（香港证监会）: 0.0027*成交金额，最低0.01港元
    -----------------------
    港股融资融券（8332）套餐二（适合高频交易者）
    融资利率: 年利率6.8%

    佣金: 0.03%， 最低3港元
    平台使用费: 阶梯式收费（以自然月计算）
              每月累计订单           费用（港币/每笔订单）
              ---------            ----------------
              1-5                  30
              6-20                 15
              21-50                10
              51-100               9
              101-500              8
              501-1000             7
              1001-2000            6
              2001-3000            5
              3001-4000            4
              4001-5000            3
              5001-6000            2
              6001及以上            1

    交易系统使用费（香港交易所）: 每笔成交0.50港元
    交收费（香港结算所）: 0.002%， 最低2港元，最高100港元
    印花税（香港政府）: 0.13%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取 (原来0.10%, 新制0.13%)
    交易费（香港交易所）: 0.005%*成交金额，最低0.01港元
    交易征费（香港证监会）: 0.0027*成交金额，最低0.01港元
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
        self.commissions += max(0.0003 * self.total_trade_amount, 3)
        self.commissions = round(self.commissions, 2)

        # 平台使用费
        self.platform_fees = 15

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