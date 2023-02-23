# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:56 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: demo_strategy.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from time import sleep
from typing import Dict, List

import pandas as pd
from finta import TA

from qtrader.core.balance import AccountBalance
from qtrader.core.position import Position
from qtrader.core.constants import Direction, Offset, OrderType, TradeMode, OrderStatus
from qtrader.core.data import Bar
from qtrader.core.engine import Engine
from qtrader.core.security import Stock, Security
from qtrader.core.strategy import BaseStrategy


class DemoStrategy(BaseStrategy):
    """Demo strategy"""

    def __init__(self,
                 securities: Dict[str, List[Stock]],
                 strategy_account: str,
                 strategy_version: str,
                 init_strategy_account_balance: Dict[str, AccountBalance],
                 init_strategy_position: Dict[str, Position],
                 engine: Engine,
                 **kwargs
                 ):
        super().__init__(
            securities=securities,
            strategy_account=strategy_account,
            strategy_version=strategy_version,
            init_strategy_account_balance=init_strategy_account_balance,
            init_strategy_position=init_strategy_position,
            engine=engine,
            **kwargs
        )
        # security list
        self.securities = securities
        # execution engine
        self.engine = engine
        # portfolios
        self.portfolios = engine.portfolios
        # For simulation/live trading, set the waiting time > 0
        self.sleep_time = 0
        for gateway_name in engine.gateways:
            if engine.gateways[gateway_name].trade_mode != TradeMode.BACKTEST:
                self.sleep_time = 5

    def init_strategy(self):
        self.ohlcv = {}
        for gateway_name in self.engine.gateways:
            self.ohlcv[gateway_name] = {}
            for security in self.engine.gateways[gateway_name].securities:
                self.ohlcv[gateway_name][security] = []

    def on_bar(self, cur_data: Dict[str, Dict[Security, Bar]]):

        self.engine.log.info("-" * 30 + "Enter on_bar" + "-" * 30)
        self.engine.log.info(cur_data)

        for gateway_name in self.engine.gateways:

            if gateway_name not in cur_data:
                continue

            # check balance
            balance = self.engine.get_balance(gateway_name=gateway_name)
            self.engine.log.info(f"{balance}")
            broker_balance = self.engine.get_broker_balance(
                gateway_name=gateway_name)
            self.engine.log.info(f"{broker_balance}")

            # check position
            positions = self.engine.get_all_positions(
                gateway_name=gateway_name)
            self.engine.log.info(f"{positions}")
            broker_positions = self.engine.get_all_broker_positions(
                gateway_name=gateway_name)
            self.engine.log.info(f"{broker_positions}")

            # send orders
            for security in cur_data[gateway_name]:

                if security not in self.securities[gateway_name]:
                    continue

                bar = cur_data[gateway_name][security]

                # Collect bar data (only keep latest 20 records)
                self.ohlcv[gateway_name][security].append(bar)
                if len(self.ohlcv[gateway_name][security]) > 20:
                    self.ohlcv[gateway_name][security].pop(0)

                open_ts = [b.open for b in
                           self.ohlcv[gateway_name][security]]
                high_ts = [b.high for b in
                           self.ohlcv[gateway_name][security]]
                low_ts = [b.low for b in
                          self.ohlcv[gateway_name][security]]
                close_ts = [b.close for b in
                            self.ohlcv[gateway_name][security]]

                ohlc = pd.DataFrame({
                    "open": open_ts,
                    "high": high_ts,
                    "low": low_ts,
                    "close": close_ts
                })
                macd = TA.MACD(
                    ohlc,
                    period_fast=12,
                    period_slow=26,
                    signal=9)

                if len(macd) < 2:
                    continue

                prev_macd = macd.iloc[-2]["MACD"]
                cur_macd = macd.iloc[-1]["MACD"]
                cur_signal = macd.iloc[-1]["SIGNAL"]
                signal = None
                if prev_macd > cur_signal > cur_macd > 0:
                    signal = "SELL"
                elif prev_macd < cur_signal < cur_macd < 0:
                    signal = "BUY"

                long_position = self.engine.get_position(
                    security=security, direction=Direction.LONG, gateway_name=gateway_name)
                short_position = self.engine.get_position(
                    security=security, direction=Direction.SHORT, gateway_name=gateway_name)

                if short_position and signal == "SELL":
                    continue
                elif long_position and signal == "BUY":
                    continue
                elif long_position and signal == "SELL":
                    order_instruct = dict(
                        security=security,
                        quantity=long_position.quantity,
                        direction=Direction.SHORT,
                        offset=Offset.CLOSE,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                elif signal == "SELL":
                    order_instruct = dict(
                        security=security,
                        quantity=1,
                        direction=Direction.SHORT,
                        offset=Offset.OPEN,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                elif short_position and signal == "BUY":
                    order_instruct = dict(
                        security=security,
                        quantity=short_position.quantity,
                        direction=Direction.LONG,
                        offset=Offset.CLOSE,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                elif signal == "BUY":
                    order_instruct = dict(
                        security=security,
                        quantity=1,
                        direction=Direction.LONG,
                        offset=Offset.OPEN,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                else:
                    continue

                self.engine.log.info(f"Submit order: \n{order_instruct}")
                orderid = self.engine.send_order(**order_instruct)
                if orderid == "":
                    self.engine.log.info("Fail to submit order")
                    return

                sleep(self.sleep_time)
                order = self.engine.get_order(
                    orderid=orderid, gateway_name=gateway_name)
                self.engine.log.info(
                    f"Order ({orderid}) has been sent: {order}")

                deals = self.engine.find_deals_with_orderid(
                    orderid, gateway_name=gateway_name)
                for deal in deals:
                    self.portfolios[gateway_name].update(deal)

                if order.status == OrderStatus.FILLED:
                    self.engine.log.info(f"Order ({orderid}) has been filled.")
                else:
                    err = self.engine.cancel_order(
                        orderid=orderid, gateway_name=gateway_name)
                    if err:
                        self.engine.log.info(
                            f"Can't cancel order ({orderid}). Error: {err}")
                    else:
                        self.engine.log.info(
                            f"Successfully cancel order ({orderid}).")
