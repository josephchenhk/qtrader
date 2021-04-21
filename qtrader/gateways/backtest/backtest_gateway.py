# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:58 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: backtest_gateway.py
# @Software: PyCharm

import uuid
from datetime import datetime
from typing import List, Dict, Iterator, Union
from dateutil.relativedelta import relativedelta

from qtrader.config import DATA_PATH, DATA_MODEL
from qtrader.core.balance import AccountBalance
from qtrader.core.constants import TradeMode, OrderStatus, Direction
from qtrader.core.data import Quote, OrderBook, Bar, CapitalDistribution
from qtrader.core.data import _get_data, _get_data_iterator
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Stock
from qtrader.core.utility import Time
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees
from qtrader.gateways.futu.futu_gateway import FutuHKEquityFees


assert set(DATA_PATH.keys())==set(DATA_MODEL.keys()), "`DATA_PATH` and `DATA_MODEL` keys are not aligned! Please check qtrader.config.config.py"

class BacktestGateway(BaseGateway):

    # 定义交易时间 (港股)
    TRADING_HOURS_AM = [Time(9,30,0), Time(12,0,0)]
    TRADING_HOURS_PM = [Time(13,0,0), Time(16,0,0)]

    # 定义最小时间单位 (秒)
    TIME_STEP = 60

    # 参数设定
    SHORT_INTEREST_RATE = 0.0098  # 融券利息

    # 名字
    NAME = "BACKTEST"

    def __init__(self,
                 securities:List[Stock],
                 start:datetime,
                 end:datetime,
                 dtypes:Dict[str, List[str]]=dict(k1m=["time_key", "open", "high", "low", "close", "volume"]),
                 fees:BaseFees=FutuHKEquityFees, # 默认是港股富途收费
        )->Dict[Stock, Iterator]:
        """
        历史数据分派器

        :param securities:
        :param start:
        :param end:
        :param dtypes: key与DATA_PATH对应，e.g. {"k1m": ["open", "high", "low", "close", "volume"]}
        :param fees:
        :return:
        """
        assert set(dtypes.keys())==set(DATA_PATH.keys()), (
            f"在{self.__class__.__name__}的__init__函数里，"
            f"输入参数dtypes的键值必须与DATA_PATH里的设定一致，dtypes需输入以下数据：{','.join(DATA_PATH.keys())}，"
            f"但目前只有：{','.join(dtypes.keys())}"
        )
        super().__init__(securities)
        self.fees = fees
        data_iterators = dict()
        prev_cache = dict()
        next_cache = dict()
        trading_days = dict()
        for security in securities:
            data_iterators[security] = dict()
            prev_cache[security]  = dict()
            next_cache[security]  = dict()
            for dfield in DATA_PATH.keys(): # k线数据 | 资金分布数据
                # 存储进生成器字典data_iterators
                data = _get_data(security=security, start=start, end=end, dfield=dfield, dtype=dtypes[dfield])
                data_it = _get_data_iterator(security=security, full_data=data, class_name=DATA_MODEL[dfield])
                data_iterators[security][dfield] = data_it
                # 初始化数据缓存
                prev_cache[security][dfield] = None
                next_cache[security][dfield] = None
                # 记录回测交易日(以分钟k线日历为准)
                if dfield=="k1m":
                    trading_days[security] = sorted(set(t.split(" ")[0] for t in data["time_key"].values))
        self.data_iterators = data_iterators
        self.prev_cache = prev_cache
        self.next_cache = next_cache
        self.trading_days = trading_days
        trading_days_list = set()
        for k,v in self.trading_days.items():
            trading_days_list.update(v)
        self.trading_days_list = sorted(trading_days_list)

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

    def get_recent_data(self, security:Stock, cur_datetime:datetime, **kwargs)->Dict[str, Union[Bar, CapitalDistribution]] or Union[Bar, CapitalDistribution]:
        """
        获取最接近当前时间的数据点

        :param security:
        :param cur_time:
        :return:
        """
        assert cur_datetime>=self.market_datetime, f"历史不能回头，当前时间{cur_datetime}在dispatcher的系统时间{self.market_datetime}之前了"
        if kwargs:
            assert "dfield" in kwargs, f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
            dfields = [kwargs["dfield"]]
        else:
            dfields = DATA_PATH
        data_it = dict()
        data_prev = dict()
        data_next = dict()
        for dfield in dfields:
            data_it[dfield] = self.data_iterators[security][dfield]
            data_prev[dfield] = self.prev_cache[security][dfield]
            data_next[dfield] = self.next_cache[security][dfield]

            if cur_datetime>self.end:
                pass

            elif (data_prev[dfield] is None) and (data_next[dfield] is None):
                data = next(data_it[dfield])
                if data.datetime > cur_datetime:
                    self.next_cache[security][dfield] = data
                else:
                    while data.datetime <= cur_datetime:
                        self.prev_cache[security][dfield] = data
                        data = next(data_it[dfield])
                    self.next_cache[security][dfield] = data

            else:
                if self.next_cache[security][dfield].datetime <= cur_datetime:
                    self.prev_cache[security][dfield] = self.next_cache[security][dfield]
                    try:
                        data = next(data_it[dfield])
                        while data.datetime <= cur_datetime:
                            self.prev_cache[security][dfield] = data
                            data = next(data_it[dfield])
                        self.next_cache[security][dfield] = data
                    except StopIteration:
                        pass

        self.market_datetime = cur_datetime
        if len(dfields)==1:
            return self.prev_cache[security][dfield]
        return self.prev_cache[security]

    def place_order(self, order:Order)->str:
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

    def get_broker_balance(self)->AccountBalance:
        """获取券商资金 (回测此接口不可用)"""
        return None

    def get_broker_position(self, security:Stock, direction:Direction)->PositionData:
        """获取券商持仓 (回测此接口不可用)"""
        return None

    def get_all_broker_positions(self)->List[PositionData]:
        """获取券商所有持仓 (回测此接口不可用)"""
        return None

    def get_quote(self, security: Stock)->Quote:
        """获取报价 (回测此接口不可用)"""
        return None

    def get_orderbook(self, security: Stock)->OrderBook:
        """获取订单簿 (回测此接口不可用)"""
        return None