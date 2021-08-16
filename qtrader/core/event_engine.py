# -*- coding: utf-8 -*-
# @Time    : 18/3/2021 9:47 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: event_engine.py
# @Software: PyCharm

import os
from time import sleep
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

from qtrader.core.constants import TradeMode
from qtrader.core.strategy import BaseStrategy
from qtrader.core.utility import timeit, get_kline_dfield_from_seconds
from qtrader.config import TIME_STEP


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

                if isinstance(v, list) and len(v)>0 and isinstance(v[0][0], datetime) and isinstance(v[0][1], str):
                    for i in range(len(v)):
                        date_time = v[i][0]
                        idx = df[df["datetime"] == date_time].index[0]
                        if df.loc[idx, var] is None:
                            df.loc[idx, var] = v[i][1]
                        elif isinstance(df.loc[idx, var], str):
                            df.loc[idx, var] = df.loc[idx, var] + "; " + v[i][1]
                else:
                    df.iloc[len(dt)-1, df.columns.get_loc(var)] = str(v)
        df.to_csv(f"{path}/{now}/result.csv", index=False)



class BarEventEngine:
    """
    Bar事件框架
    """

    def __init__(self,
            strategy:BaseStrategy,
            recorder:BarEventEngineRecorder,
            start:datetime=None,
            end:datetime=None,
        ):
        self.strategy = strategy
        self.recorder = recorder
        self.trade_modes = {}
        starts = {}
        ends = {}
        for gateway_name in strategy.engine.gateways:
            # mode用于判断是回测模式还是实盘模式
            self.trade_modes[gateway_name] = strategy.engine.gateways[gateway_name].trade_mode
            # 确定模式之后，尝试同步券商的资金和持仓信息（回测模式下不会有任何变化）
            strategy.engine.sync_broker_balance(gateway_name=gateway_name)
            strategy.engine.sync_broker_position(gateway_name=gateway_name)
            # 输出初始账户资金和持仓
            strategy.engine.log.info(strategy.engine.get_balance(gateway_name=gateway_name))
            strategy.engine.log.info(strategy.engine.get_all_positions(gateway_name=gateway_name))
            # 确定起始和截止时间
            starts[gateway_name] = strategy.engine.gateways[gateway_name].start
            ends[gateway_name] = strategy.engine.gateways[gateway_name].end
        starts = [starts[gn] for gn in starts if isinstance(starts[gn], datetime)]
        ends = [ends[gn] for gn in ends if isinstance(ends[gn], datetime)]
        self.start = min(starts) if start is None else start
        self.end = max(ends) if end is None else end

    @timeit
    def run(self):
        engine = self.strategy.engine
        gateways = self.strategy.engine.gateways
        securities = self.strategy.securities

        engine.start()

        # 开始事件循环（若为回测，则回放历史数据）
        # kline_dfield = get_kline_dfield_from_seconds(time_step)
        cur_datetime = datetime.now() if self.start is None else self.start
        while cur_datetime<=self.end:
            # 检查每个gateway的交易时间
            jump_to_datetime = {} # 如果当前时间不属于交易时间，就记录下需要跳转到的时间
            for gateway_name in gateways:
                gateway = gateways[gateway_name]
                trade_mode = self.trade_modes[gateway_name]
                if not gateway.is_trading_time(cur_datetime):
                    if trade_mode == TradeMode.BACKTEST:
                        next_trading_datetime = {}
                        for security in securities[gateway_name]:
                            next_trading_dt = gateway.next_trading_datetime(cur_datetime, security)
                            if next_trading_dt is not None:
                                next_trading_datetime[security] = next_trading_dt
                        if len(next_trading_datetime) == 0:
                            break
                        sorted_next_trading_datetime = sorted(next_trading_datetime.items(), key=lambda item: item[1])
                        engine.log.info(f"当前时间{cur_datetime}非交易时间，跳到{sorted_next_trading_datetime[0][1]}")
                        jump_to_datetime[gateway_name] = sorted_next_trading_datetime[0][1]
                    elif trade_mode in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
                        jump_to_datetime[gateway_name] = cur_datetime + timedelta(milliseconds=TIME_STEP)

            # 全部gateways都不在交易时间
            if len(jump_to_datetime)==len(gateways):
                cur_datetime = min(jump_to_datetime.values())
                continue

            # 至少有一个gateway在交易时间
            active_gateways = [gateway_name for gateway_name in gateways if gateway_name not in jump_to_datetime]
            assert len(active_gateways)>=1, f"Active gateways is: {len(active_gateways)}. We expect at least 1."

            # 获取每只股票的最新bar数据(按 gateway_name 进行划分)
            cur_data = {}
            for gateway_name in active_gateways:
                if gateway_name not in securities:
                    continue
                gateway = gateways[gateway_name]
                cur_gateway_data = {}
                for security in securities[gateway_name]:
                    if self.trade_modes[gateway_name] == TradeMode.BACKTEST:
                        data = gateway.get_recent_data(security, cur_datetime)
                    else:
                        data = gateway.get_recent_data(security)
                    cur_gateway_data[security] = data
                cur_data[gateway_name] = cur_gateway_data

            # 运行策略
            self.strategy.on_bar(cur_data)

            for gateway_name in active_gateways:
                for field in self.recorder.get_recorded_fields():
                    value = getattr(self.strategy, f"get_{field}")(gateway_name)
                    self.recorder.write_record(field, value)

            # 通过控件来中断程序运行：
            if False:
                for gateway_name in gateways:
                    gateway = gateways[gateway_name]
                    gateway.close()
                engine.stop()
                engine.log.info("程序被人手终止")
                return

            # 更新事件循环时间戳
            if self.trade_modes[gateway_name] == TradeMode.BACKTEST:
                cur_datetime += timedelta(milliseconds=TIME_STEP)
            elif self.trade_modes[gateway_name] in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
                sleep(TIME_STEP/1000.)
                cur_datetime = datetime.now()

        for gateway_name in gateways:
            gateway = gateways[gateway_name]
            gateway.close()
        engine.stop()
        engine.log.info("到达预期结束时间，策略停止（其他工作任务线程将会在1分钟内停止）")



        #     if not market.is_trading_time(cur_datetime):
        #         # 回测和实盘做不同的处理
        #         if self.trade_mode==TradeMode.BACKTEST:
        #             next_trading_datetime = {}
        #             for security in securities:
        #                 next_trading_dt = market.next_trading_datetime(cur_datetime, security)
        #                 if next_trading_dt is not None:
        #                     next_trading_datetime[security] = next_trading_dt
        #             if len(next_trading_datetime)==0:
        #                 break
        #             sorted_next_trading_datetime = sorted(next_trading_datetime.items(), key=lambda item: item[1])
        #             engine.log.info(f"当前时间{cur_datetime}非交易时间，跳到{sorted_next_trading_datetime[0][1]}")
        #             cur_datetime = sorted_next_trading_datetime[0][1]
        #             continue
        #         elif self.trade_mode in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
        #             sleep(time_step)
        #             continue
        #
        #     # 获取每只股票的最新bar数据
        #     cur_data = {}
        #     for security in securities:
        #         if self.trade_mode == TradeMode.BACKTEST:
        #             data = market.get_recent_data(security, cur_datetime)
        #         else:
        #             data = market.get_recent_data(security)
        #         cur_data[security] = data
        #
        #     # 运行策略
        #     self.strategy.on_bar(cur_data)
        #
        #     for field in self.recorder.get_recorded_fields():
        #         value = getattr(self.strategy, f"get_{field}")()
        #         self.recorder.write_record(field, value)
        #
        #     # 通过控件来中断程序运行：
        #     if False:
        #         market.close()
        #         engine.stop()
        #         engine.log.info("程序被人手终止")
        #         return
        #
        #     # 更新事件循环时间戳
        #     if self.trade_mode == TradeMode.BACKTEST:
        #         cur_datetime += relativedelta(seconds=time_step)
        #     elif self.trade_mode in (TradeMode.LIVETRADE, TradeMode.SIMULATE):
        #         sleep(time_step)
        #         cur_datetime = datetime.now()
        #
        # market.close()
        # engine.stop()
        # engine.log.info("到达预期结束时间，策略停止（其他工作任务线程将会在1分钟内停止）")
