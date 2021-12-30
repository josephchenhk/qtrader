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
from random import random

from qtrader.core.constants import Direction, Offset, OrderType, TradeMode, OrderStatus
from qtrader.core.data import Bar
from qtrader.core.engine import Engine
from qtrader.core.security import Stock, Security
from qtrader.core.strategy import BaseStrategy

class DemoStrategy(BaseStrategy):

    def __init__(self,
            securities:Dict[str,List[Stock]],
            strategy_account:str,
            strategy_version:str,
            init_strategy_cash:Dict[str,float],
            engine:Engine
        ):
        super().__init__(
            engine=engine,
            strategy_account=strategy_account,
            strategy_version=strategy_version,
            init_strategy_cash=init_strategy_cash,
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
            if engine.gateways[gateway_name].trade_mode!=TradeMode.BACKTEST:
                self.sleep_time = 5

    def init_strategy(self):
        pass

    def on_bar(self, cur_data:Dict[str,Dict[Security, Bar]]):

        self.engine.log.info("-"*30 + "Enter on_bar" + "-"*30)
        self.engine.log.info(cur_data)

        for gateway_name in self.engine.gateways:

            if gateway_name not in cur_data:
                continue

            # check balance
            balance = self.engine.get_balance(gateway_name=gateway_name)
            self.engine.log.info(f"{balance}")
            broker_balance = self.engine.get_broker_balance(gateway_name=gateway_name)
            self.engine.log.info(f"{broker_balance}")

            # check position
            positions = self.engine.get_all_positions(gateway_name=gateway_name)
            self.engine.log.info(f"{positions}")
            broker_positions = self.engine.get_all_broker_positions(gateway_name=gateway_name)
            self.engine.log.info(f"{broker_positions}")

            # check orders
            all_orders = self.engine.get_all_orders(gateway_name=gateway_name)

            # check deals
            all_deals = self.engine.get_all_deals(gateway_name=gateway_name)

            # send orders
            for security in cur_data[gateway_name]:

                if security not in self.securities[gateway_name]:
                    continue

                bar = cur_data[gateway_name][security]
                orderbook = self.engine.get_orderbook(security=security, gateway_name=gateway_name )
                quote = self.engine.get_quote(security=security, gateway_name=gateway_name )
                self.engine.log.info(quote)
                self.engine.log.info(orderbook)

                long_position = self.engine.get_position(security=security, direction=Direction.LONG,
                                                         gateway_name=gateway_name)
                short_position = self.engine.get_position(security=security, direction=Direction.SHORT,
                                                         gateway_name=gateway_name)

                # Randomly open long or short positions, and
                if long_position:
                    order_instruct = dict(
                        security=security,
                        quantity=long_position.quantity,
                        direction=Direction.SHORT,
                        offset=Offset.CLOSE,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                elif short_position:
                    order_instruct = dict(
                        security=security,
                        quantity=short_position.quantity,
                        direction=Direction.LONG,
                        offset=Offset.CLOSE,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                else:
                    direction = Direction.LONG if random() > 0.5 else Direction.SHORT
                    order_instruct = dict(
                        security=security,
                        quantity=1,
                        direction=direction,
                        offset=Offset.OPEN,
                        order_type=OrderType.MARKET,
                        gateway_name=gateway_name,
                    )
                self.engine.log.info(f"提交订单:\n{order_instruct}")
                orderid = self.engine.send_order(**order_instruct)
                if orderid=="":
                    self.engine.log.info("提交订单失败")
                    return

                sleep(self.sleep_time)
                order = self.engine.get_order(orderid=orderid, gateway_name=gateway_name)
                self.engine.log.info(f"订单{orderid}已发出:{order}")

                deals = self.engine.find_deals_with_orderid(orderid, gateway_name=gateway_name)
                for deal in deals:
                    self.portfolios[gateway_name].update(deal)

                if order.status==OrderStatus.FILLED:
                    self.engine.log.info(f"订单已成交{orderid}")
                else:
                    err = self.engine.cancel_order(orderid=orderid, gateway_name=gateway_name)
                    if err:
                        self.engine.log.info(f"不能取消订单{orderid},因爲{err}")
                    else:
                        self.engine.log.info(f"已經取消订单{orderid}")

