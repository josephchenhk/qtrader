# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 9:47 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: event_engine.py
# @Software: PyCharm

from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta

from qtrader.core.constants import TradeMode
from qtrader.core.strategy import BaseStrategy
from qtrader.core.utility import timeit



class BarEventEngineRecorder:
    """记录bar事件过程的变量"""

    def __init__(self, **kwargs):
        self.recorded_fields = ["datetime", "portfolio_value"]
        self.datetime = []
        self.portfolio_value = []
        for k,v in kwargs:
            setattr(self, k, [])
            self.recorded_fields.append(str(k))

    def get_recorded_fields(self):
        return self.recorded_fields

    def write_record(self, field, value):
        record = getattr(self, field, None)
        if record is not None:
            record.append(value)


class BarEventEngine:
    """Bar事件框架"""

    def __init__(self,
                 strategy:BaseStrategy,
                 recorder:BarEventEngineRecorder,
                 start:datetime=None,
                 end:datetime=None,
                 trade_mode=TradeMode.BACKTEST):
        self.strategy = strategy
        self.recorder = recorder

        # mode用于判断是回测模式还是实盘模式
        self.trade_mode = trade_mode
        strategy.engine.market.set_trade_mode(trade_mode)

        if start is None:
            self.start = strategy.engine.market.start
        if end is None:
            self.end = strategy.engine.market.end

    @timeit
    def run(self):
        engine = self.strategy.engine
        market = engine.market
        securities = self.strategy.securities

        # 开始事件循环（若为回测，则回放历史数据）
        time_step = market.TIME_STEP
        cur_datetime = datetime.now() if self.start is None else self.start
        while cur_datetime<=self.end:
            if not market.is_trading_time(cur_datetime):
                # 回测和实盘做不同的处理
                if self.trade_mode==TradeMode.BACKTEST:
                    next_trading_datetime = {}
                    for security in securities:
                        next_trading_dt = market.next_trading_datetime(cur_datetime, security)
                        if next_trading_dt is not None:
                            next_trading_datetime[security] = next_trading_dt
                    if len(next_trading_datetime)==0:
                        break
                    sorted_next_trading_datetime = sorted(next_trading_datetime.items(), key=lambda item: item[1])
                    engine.log.info(f"当前时间{cur_datetime}非交易时间，跳到{sorted_next_trading_datetime[0][1]}")
                    cur_datetime = sorted_next_trading_datetime[0][1]
                    continue
                elif self.trade_mode==TradeMode.LIVETRADE:
                    sleep(60)
                    continue

            # 获取每只股票的最新bar数据
            cur_data = {}
            for security in securities:
                if self.trade_mode == TradeMode.BACKTEST:
                    data = market.get_recent_bar(security, cur_datetime)
                else:
                    data = market.get_recent_bar(security)
                # if data is None:
                #     engine.log.info.info(f"{cur_datetime} {security} 没有数据！")
                cur_data[security] = data

            # 运行策略
            self.strategy.on_bar(cur_data)

            for field in self.recorder.get_recorded_fields():
                value = getattr(self.strategy, f"get_{field}")()
                self.recorder.write_record(field, value)

            # 更新事件循环时间戳
            cur_datetime += relativedelta(seconds=time_step)

        market.close()
        engine.log.info("到达预期结束时间，策略停止")
