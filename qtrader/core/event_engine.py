# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 9:47 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: event_engine.py
# @Software: PyCharm

import os
from time import sleep
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

from qtrader.core.constants import TradeMode
from qtrader.core.strategy import BaseStrategy
from qtrader.core.utility import timeit



class BarEventEngineRecorder:
    """记录bar事件过程的变量"""

    def __init__(self, **kwargs):
        self.recorded_methods = {"datetime": "append", "portfolio_value": "append"}
        self.datetime = []
        self.portfolio_value = []
        for k,v in kwargs.items():
            if v is None:
                self.recorded_methods[str(k)] = "override"
            elif isinstance(v, list) and len(v)==0:
                self.recorded_methods[str(k)] = "append"
            else:
                raise ValueError(f"BarEventEngineRecorder 的输入参数{k}的类型为{type(v)}, 只有[]或None是合法的输入")
            setattr(self, k, v)

    def get_recorded_fields(self):
        return list(self.recorded_methods.keys())

    def write_record(self, field, value):
        record = getattr(self, field, None)
        if self.recorded_methods[field]=="append":
            record.append(value)
        elif self.recorded_methods[field]=="override":
            setattr(self, field, value)

    def save_csv(self, path=None):
        """保存所有记录变量至csv"""
        vars = [attr for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__")]
        assert "datetime" in vars, "`datetime` is not in the recorder!"
        assert "portfolio_value" in vars, "`portfolio_value` is not in the recorder!"
        if path is None:
            path = "results"
        if path not in os.listdir():
            os.mkdir(path)
        now = datetime.now()
        os.mkdir(f"{path}/{now}")

        dt = getattr(self, "datetime")
        pv = getattr(self, "portfolio_value")
        df = pd.DataFrame([dt, pv], index=["datetime", "portfolio_value"]).T
        for var in vars:
            if var in ("datetime", "portfolio_value", "recorded_methods"):
                continue
            v = getattr(self, var)
            if self.recorded_methods[var]=="append":
                df[var] = v
            elif self.recorded_methods[var]=="override":
                df[var] = None
                df.iloc[len(dt)-1, df.columns.get_loc(var)] = str(v)
        df.to_csv(f"{path}/{now}/result.csv")



class BarEventEngine:
    """
    Bar事件框架
    """

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

        # 确定模式之后，尝试同步券商的资金和持仓信息（回测模式下不会有任何变化）
        strategy.engine.sync_broker_balance()
        strategy.engine.sync_broker_position()
        # 输出初始账户资金和持仓
        strategy.engine.log.info(strategy.engine.get_balance())
        strategy.engine.log.info(strategy.engine.get_all_positions())

        if start is None:
            self.start = strategy.engine.market.start
        if end is None:
            self.end = strategy.engine.market.end

    @timeit
    def run(self):
        engine = self.strategy.engine
        market = self.strategy.engine.market
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
                elif self.trade_mode in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
                    sleep(time_step)
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
            if self.trade_mode == TradeMode.BACKTEST:
                cur_datetime += relativedelta(seconds=time_step)
            elif self.trade_mode in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
                sleep(time_step)
                cur_datetime = datetime.now()

        market.close()
        engine.log.info("到达预期结束时间，策略停止")
