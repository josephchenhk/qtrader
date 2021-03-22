# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:58 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: backtest_gateway.py
# @Software: PyCharm

import uuid
import math
from datetime import datetime
from typing import List, Dict, Iterator
from dateutil.relativedelta import relativedelta

from qtrader.core.constants import TradeMode, OrderStatus
from qtrader.core.data import _get_full_data, _get_data_iterator
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.security import Stock
from qtrader.core.utility import Time
from qtrader.gateways import BaseGateway


class BacktestGateway(BaseGateway):

    # 定义交易时间 (港股)
    TRADING_HOURS_AM = [Time(9,30,0), Time(12,0,0)]
    TRADING_HOURS_PM = [Time(13,0,0), Time(16,0,0)]

    # 定义最小时间单位 (秒)
    TIME_STEP = 60

    # 参数设定
    SHORT_INTEREST_RATE = 0.0098  # 融券利息

    def __init__(self,
                 securities:List[Stock],
                 start:datetime,
                 end:datetime,
                 dtype:List[str]=["open", "high", "low", "close", "volume"]
        )->Dict[Stock, Iterator]:
        """
        历史数据分派器

        :param securities:
        :param start:
        :param end:
        :param dtype:
        :return:
        """
        super().__init__()
        data_iterators = {}
        trading_days = {}
        for security in securities:
            full_data = _get_full_data(security=security, start=start, end=end, dtype=dtype)
            data_it = _get_data_iterator(security=security, full_data=full_data)
            data_iterators[security] = data_it
            trading_days[security] = sorted(set(t.split(" ")[0] for t in full_data["time_key"].values))
        self.data_iterators = data_iterators
        self.trading_days = trading_days
        trading_days_list = set()
        for k,v in self.trading_days.items():
            trading_days_list.update(v)
        self.trading_days_list = sorted(trading_days_list)
        self.prev_cache = {s:None for s in securities}
        self.next_cache = {s:None for s in securities}
        self.start = start
        self.end = end
        self.market_datetime = start

    def set_trade_mode(self, trade_mode:TradeMode):
        """设置交易模式"""
        self.trade_mode = trade_mode

    def is_trading_time(self, cur_datetime:datetime)->bool:
        """
        判断当前时间是否属于交易时间段

        :param cur_datetime:
        :return:
        """
        is_trading_day = cur_datetime.strftime("%Y-%m-%d") in self.trading_days_list
        if not is_trading_day:
            return False
        cur_time = Time(hour=cur_datetime.hour, minute=cur_datetime.minute, second=cur_datetime.second)
        return (self.TRADING_HOURS_AM[0]<=cur_time<=self.TRADING_HOURS_AM[1]) or (self.TRADING_HOURS_PM[0]<=cur_time<=self.TRADING_HOURS_PM[1])

    def next_trading_datetime(self, cur_datetime:datetime, security:Stock)->datetime:
        """
        根据已有数据寻找下一个属于交易时间的时间点，如果找不到，返回None

        :param cur_datetime:
        :param security:
        :return:
        """
        # 移动一个时间单位，看是否属于交易时间
        next_datetime = cur_datetime + relativedelta(seconds=self.TIME_STEP)
        next_time = Time(hour=next_datetime.hour, minute=next_datetime.minute, second=next_datetime.second)
        next_trading_daytime = None

        # 如果下一个时间点，不属于交易日；或者已经超出pm交易时间，找到并返回下一个交易日的开盘时间
        if (next_datetime.strftime("%Y-%m-%d") not in self.trading_days[security]) or (next_time>self.TRADING_HOURS_PM[1]):
            for trading_day in self.trading_days[security]:
                year, month, day = trading_day.split("-")
                trade_datetime = datetime(
                    int(year),
                    int(month),
                    int(day),
                    self.TRADING_HOURS_AM[0].hour,
                    self.TRADING_HOURS_AM[0].minute,
                    self.TRADING_HOURS_AM[0].second
                )
                if trade_datetime>=next_datetime:
                    next_trading_daytime = trade_datetime
                    break
        # 如果下一个时间点属于交易日，并且没有超出pm交易时间，则找到并返回上午或者下午的开盘时间
        elif (not self.is_trading_time(next_datetime)):
            if next_time < self.TRADING_HOURS_AM[0]:
                next_trading_daytime = datetime(
                    next_datetime.year,
                    next_datetime.month,
                    next_datetime.day,
                    self.TRADING_HOURS_AM[0].hour,
                    self.TRADING_HOURS_AM[0].minute,
                    self.TRADING_HOURS_AM[0].second
                )
            elif next_time < self.TRADING_HOURS_PM[0]:
                next_trading_daytime = datetime(
                    next_datetime.year,
                    next_datetime.month,
                    next_datetime.day,
                    self.TRADING_HOURS_PM[0].hour,
                    self.TRADING_HOURS_PM[0].minute,
                    self.TRADING_HOURS_PM[0].second
                )
        # 如果下一个时间点属于交易日，并且属于交易时间段内，则直接返回下一个时间点
        else:
            next_trading_daytime = next_time
        return next_trading_daytime

    def get_recent_bar(self, security:Stock, cur_datetime:datetime):
        """
        获取最接近当前时间的数据点

        :param security:
        :param cur_time:
        :return:
        """
        assert cur_datetime>=self.market_datetime, f"历史不能回头，当前时间{cur_datetime}在dispatcher的系统时间{self.market_datetime}之前了"
        data_it = self.data_iterators[security]
        data_prev = self.prev_cache[security]
        data_next = self.next_cache[security]

        if cur_datetime>self.end:
            pass

        elif (data_prev is None) and (data_next is None):
            bar = next(data_it)
            if bar.datetime > cur_datetime:
                self.next_cache[security] = bar
            else:
                while bar.datetime <= cur_datetime:
                    self.prev_cache[security] = bar
                    bar = next(data_it)
                self.next_cache[security] = bar

        else:
            if self.next_cache[security].datetime <= cur_datetime:
                self.prev_cache[security] = self.next_cache[security]
                try:
                    bar = next(data_it)
                    while bar.datetime <= cur_datetime:
                        self.prev_cache[security] = bar
                        bar = next(data_it)
                    self.next_cache[security] = bar
                except StopIteration:
                    pass

        self.market_datetime = cur_datetime
        return self.prev_cache[security]

    def place_order(self, order:Order):
        """最简单的处理，假设全部成交"""
        order.filled_time = self.market_datetime
        order.filled_quantity = order.quantity
        order.filled_avg_price = order.price
        order.status = OrderStatus.FILLED
        orderid = "bt-order-" + str(uuid.uuid4())
        dealid = "bt-deal-" + str(uuid.uuid4())
        self.orders.put(orderid, order)

        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=self.market_datetime,
            filled_avg_price=order.price,
            filled_quantity=order.quantity,
            dealid=dealid,
            orderid=orderid
        )
        self.deals.put(dealid, deal)

        return orderid

    def cancel_order(self, orderid):
        """取消订单"""
        order = self.orders.get(orderid)
        if order.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.FAILED):
            print(f"不能取消订单{orderid}，因为订单状态已经为{order.status}")
            return
        order.status = OrderStatus.CANCELLED
        self.orders.put(orderid, order)


def fees(*trades:Dict)->float:
    """
    港股融资融券（8332）套餐一（适合一般交易者）
    融资利率: 年利率6.8%

    佣金: 0.03%， 最低3港元
    平台使用费: 15港元/笔

    交易系统使用费（香港交易所）: 每笔成交0.50港元
    交收费（香港结算所）: 0.002%， 最低2港元，最高100港元
    印花税（香港政府）: 0.1%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取
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
    印花税（香港政府）: 0.1%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取
    交易费（香港交易所）: 0.005%*成交金额，最低0.01港元
    交易征费（香港证监会）: 0.0027*成交金额，最低0.01港元
    """
    # 富途收费
    commissions = 0       # 佣金
    platform_fees = 0     # 平台使用费
    # 富途代收费
    system_fees = 0       # 交易系统使用费
    settlement_fees = 0   # 交收费
    stamp_fees = 0        # 印花税
    trade_fees = 0        # 交易费
    transaction_fees = 0  # 交易征费

    total_trade_amount = 0
    total_number_of_trades = 0
    for trade in trades:
        price = trade.get("price")
        size = trade.get("size")
        trade_amount = price * size
        total_number_of_trades += 1
        total_trade_amount += trade_amount

        # 交易系统使用费
        system_fee = round(0.50, 2)
        system_fees += system_fee

        # 交收费
        settlement_fee = 0.00002*trade_amount
        if settlement_fee<2.0:
            settlement_fee = 2.0
        elif settlement_fee>100.0:
            settlement_fee = 100.0
        settlement_fee = round(settlement_fee, 2)
        settlement_fees += settlement_fee

        # 印花税
        stamp_fee = math.ceil(0.001*trade_amount)
        stamp_fees += stamp_fee

        # 交易费
        trade_fee = max(0.00005*trade_amount, 0.01)
        trade_fee = round(trade_fee, 2)
        trade_fees += trade_fee

        # 交易征费
        transaction_fee = max(0.000027*trade_amount, 0.01)
        transaction_fee = round(transaction_fee, 2)
        transaction_fees += transaction_fee

    # 佣金
    commissions = max(0.0003*total_trade_amount, 3)
    commissions = round(commissions, 2)

    # 平台使用费
    platform_fees = 15

    # 总费用
    total_fees = commissions + platform_fees + system_fees + settlement_fees + stamp_fees + trade_fees + transaction_fees
    return commissions, platform_fees, system_fees, settlement_fees, stamp_fees, trade_fees, transaction_fees, total_fees