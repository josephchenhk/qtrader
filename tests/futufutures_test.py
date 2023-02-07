# -*- coding: utf-8 -*-
# @Time    : 11/2/2022 1:46 pm
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: futufutures_test.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the 
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
import sys
import os
import time
from pathlib import Path
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.modules.pop("qtrader", None)
sys.path.insert(0, str(Path(SCRIPT_PATH).parent.parent.joinpath("qtalib")))
sys.path.insert(0, str(Path(SCRIPT_PATH).parent.parent.joinpath("qtrader")))
sys.path.insert(0, str(Path(SCRIPT_PATH)))


from datetime import datetime, timedelta
from datetime import time as Time

import pytest

from qtrader.core.security import Security
from qtrader.core.engine import Engine
from qtrader.core.constants import TradeMode, Exchange
from qtrader.core.security import Futures, Currency, Stock
from qtrader.gateways.futu import FutuFeesHKFE, FutuFeesSEHK
from qtrader.gateways import FutuGateway, FutuFuturesGateway
from qtrader.core.constants import (
    Direction, Offset, OrderType, TradeMode, OrderStatus
)

# pytest fixture ussage
# https://iter01.com/578851.html


class TestFutuGateway:

    def setup_class(self):
        stock_list = [
            Futures(
                code="HK.MCHmain",
                lot_size=10,
                security_name="HK.MCHmain",
                exchange=Exchange.HKFE,
                expiry_date="20230228"),
        ]
        gateway_name = "Futufutures"
        gateway = FutuFuturesGateway(
            securities=stock_list,
            end=datetime.now() + timedelta(minutes=2),
            gateway_name=gateway_name,
            fees=FutuFeesHKFE,
            trading_sessions={'HK.MCHmain': [
                [datetime(1970, 1, 1, 9, 15, 0),
                 datetime(1970, 1, 1, 12, 0, 0)],
                [datetime(1970, 1, 1, 13, 0, 0),
                 datetime(1970, 1, 1, 16, 0, 0)],
                [datetime(1970, 1, 1, 17, 15, 0),
                 datetime(1970, 1, 1, 3, 0, 0)]]
            }
        )

        gateway.SHORT_INTEREST_RATE = 0.0
        gateway.trade_mode = TradeMode.LIVETRADE
        if gateway.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            assert datetime.now() < gateway.end, (
                "Gateway end time must be later than current datetime!")
        engine = Engine(gateways={gateway_name: gateway})
        self.gateway_name = gateway_name
        self.gateway = gateway
        self.engine = engine
        self.sleep_time = 5

    def teardown_class(self):
        self.gateway.close()

    # @pytest.mark.skip("Already tested")
    def test_get_recent_bar(self):
        for _ in range(len(self.gateway.securities)):
            for security in self.gateway.securities:
                bar = self.gateway.get_recent_bar(security)
                print(f"Bar data: {bar}")
                assert bar.datetime.second == 0
                assert isinstance(bar.close, float)
            time.sleep(5)

    # @pytest.mark.skip("Already tested")
    def test_get_broker_balance(self):
        balance = self.gateway.get_broker_balance()
        assert balance.cash > 0

    # @pytest.mark.skip("Already tested")
    def test_get_all_broker_positions(self):
        positions = self.gateway.get_all_broker_positions()
        if positions:
            position = positions[0]
            assert position.quantity != 0

    def send_order(self, security: Security,
                   quantity: int,
                   direction: Direction,
                   offset: Offset,
                   order_type: OrderType,
                   gateway_name: str
                   ) -> bool:
        """return True if order is successfully fully filled, else False"""
        order_instruct = dict(
            security=security,
            quantity=quantity,
            direction=direction,
            offset=offset,
            order_type=order_type,
            gateway_name=gateway_name,
        )

        self.engine.log.info(f"Submit order:\n{order_instruct}")
        orderid = self.engine.send_order(**order_instruct)
        # TODO: sometimes timeout here.
        if orderid == "":
            self.engine.log.info("Fail to submit order")
            return False
        self.engine.log.info(f"Order {orderid} has been submitted")
        time.sleep(self.sleep_time)
        order = self.engine.get_order(
            orderid=orderid, gateway_name=gateway_name)
        self.engine.log.info(f"Order {orderid} details: {order}")

        deals = self.engine.find_deals_with_orderid(
            orderid, gateway_name=gateway_name)
        self.engine.log.info(f"\tDeals: {deals}")
        self.engine.log.info(f"\tBefore portfolio update: "
                             f"{self.engine.portfolios[gateway_name].value}")
        for deal in deals:
            self.engine.portfolios[gateway_name].update(deal)
            # self.portfolios[gateway_name].update(deal)
        self.engine.log.info(f"\tAfter portfolio update: "
                             f"{self.engine.portfolios[gateway_name].value}")

        if order.status == OrderStatus.FILLED:
            self.engine.log.info(f"Order {orderid} has been filled.")
            return True
        else:
            err = self.engine.cancel_order(
                orderid=orderid, gateway_name=gateway_name)
            if err:
                self.engine.log.info(
                    f"Fail to cancel order {orderid} for reason: {err}")
            else:
                self.engine.log.info(f"Successfully cancelled order {orderid}")
            return False

    def test_send_order(self):
        security = Futures(
            code="HK.MCHmain",
            lot_size=10,
            security_name="HK.MCHmain",
            exchange=Exchange.HKFE,
            expiry_date="20230228"
        )
        quantity = 1
        direction = Direction.SHORT
        offset = Offset.OPEN
        order_type = OrderType.MARKET
        gateway_name = self.gateway_name
        filled = self.send_order(
            security,
            quantity,
            direction,
            offset,
            order_type,
            gateway_name
        )
        assert filled

if __name__ == "__main__":
    test = TestFutuGateway()
    test.setup_class()
    test.test_send_order()
    test.teardown_class()
    print("Done.")