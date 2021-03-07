# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:27 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: backtest.py
# @Software: PyCharm

from datetime import datetime
from dateutil.relativedelta import relativedelta

from qtrader.core.strategy import BaseStrategy
from qtrader.core.utility import timeit

class BacktestRecorder:
    """记录回测过程的变量"""

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


class Backtest:
    """回测框架"""

    def __init__(self, strategy:BaseStrategy, recorder:BacktestRecorder, start:datetime=None, end:datetime=None):
        self.strategy = strategy
        self.recorder = recorder
        if start is None:
            self.start = strategy.engine.market.start
        if end is None:
            self.end = strategy.engine.market.end

    @timeit
    def run(self):
        engine = self.strategy.engine
        market = engine.market
        securities = self.strategy.securities

        # 回放历史数据
        cur_datetime = self.start
        time_step = market.TIME_STEP
        while cur_datetime<=self.end:
            if not market.is_trading_time(cur_datetime):
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

            # 获取每只股票的最新bar数据
            cur_data = {}
            for security in securities:
                data = market.get_recent_data(cur_datetime, security)
                # if data is None:
                #     engine.log.info.info(f"{cur_datetime} {security} 没有数据！")
                cur_data[security] = data

            # 运行策略
            self.strategy.on_bar(cur_data)

            engine.log.info(f"{cur_datetime}, {self.strategy.portfolio_pnl[-2:]}, {self.strategy.portfolio.value}")

            for field in self.recorder.get_recorded_fields():
                value = getattr(self.strategy, f"get_{field}")()
                self.recorder.write_record(field, value)

            # 更新事件循环时间戳
            cur_datetime += relativedelta(seconds=time_step)

        engine.log.info("所有历史数据已经回放完毕，回测停止")
