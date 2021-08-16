# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:56 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: demo_strategy.py
# @Software: PyCharm
from time import sleep
from typing import Dict, List

from qtrader.core.constants import Direction, Offset, OrderType, TradeMode
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
        # 股票
        self.securities = securities
        # 执行引擎
        self.engine = engine
        # 投资组合管理
        self.portfolios = engine.portfolios
        # 等待执行时间
        self.sleep_time = 0
        for gateway_name in engine.gateways:
            if engine.gateways[gateway_name].trade_mode!=TradeMode.BACKTEST:
                self.sleep_time = 15

    def init_strategy(self):
        pass

    def on_bar(self, cur_data:Dict[str,Dict[Security, Bar]]):

        self.engine.log.info("-"*30 + "进入on_bar" + "-"*30)

        for gateway_name in self.engine.gateways:

            if gateway_name not in cur_data:
                continue

            # check balance
            balance = self.engine.get_balance(gateway_name=gateway_name )
            self.engine.log.info(f"{balance}")

            # check position
            positions = self.engine.get_all_positions(gateway_name=gateway_name )
            self.engine.log.info(f"{positions}")

            # check orders
            all_orders = self.engine.get_all_orders(gateway_name=gateway_name )

            # check deals
            all_deals = self.engine.get_all_deals(gateway_name=gateway_name )

            # send orders
            for security in cur_data[gateway_name]:

                if security not in self.securities[gateway_name]:
                    continue

                bar = cur_data[gateway_name][security]
                orderbook = self.engine.get_orderbook(security=security, gateway_name=gateway_name )
                quote = self.engine.get_quote(security=security, gateway_name=gateway_name )
                data = cur_data[gateway_name][security]
                self.engine.log.info(quote)
                self.engine.log.info(orderbook)
                self.engine.log.info(data)

                # if isinstance(data, dict): price = data["k1m"].close
                # elif isinstance(data, Bar): price = data.close
                # else: raise ValueError(f"data不是合法的格式！")

                order_instruct = dict(
                    security=security,
                    price=bar.close if quote is None else quote.last_price,
                    quantity=security.lot_size,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT,
                    gateway_name=gateway_name ,
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

                if orderid:
                    self.engine.cancel_order(orderid=orderid, gateway_name=gateway_name)
                    self.engine.log.info(f"取消订单{order}")

