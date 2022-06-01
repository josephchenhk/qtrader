# -*- coding: utf-8 -*-
# @Time    : 3/2/2022 9:00 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: cqg_test.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
import time
from datetime import datetime, timedelta
from datetime import time as Time

import pytest

from qtrader.core.constants import TradeMode, Exchange, Direction, Offset, OrderType
from qtrader.core.order import Order
from qtrader.core.security import Futures, Currency
from qtrader.gateways.cqg import CQGFees
from qtrader.gateways import CqgGateway

# pytest fixture ussage
# https://iter01.com/578851.html


class TestCqgGateway:

    def setup_class(self):
        # Must provide full list of exsiting positions
        stock_list = [
            # Futures(code="FUT.GC", lot_size=100, security_name="GCJ2", exchange=Exchange.NYMEX,
            #         expiry_date="20220427"),
            # Futures(code="FUT.SI", lot_size=5000, security_name="SIK2", exchange=Exchange.NYMEX,
            #         expiry_date="20220529"),
            Futures(code="FUT.CO", lot_size=1000, security_name="QON2", exchange=Exchange.ICE,
                    expiry_date="20220727")
        ]
        gateway_name = "Cqg"
        gateway = CqgGateway(
            securities=stock_list,
            end=datetime.now() + timedelta(hours=1),
            gateway_name=gateway_name,
            fees=CQGFees
        )
        time.sleep(10)

        gateway.SHORT_INTEREST_RATE = 0.0
        gateway.trade_mode = TradeMode.SIMULATE
        if gateway.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            assert datetime.now() < gateway.end, "Gateway end time must be later than current datetime!"
        self.gateway = gateway

    def teardown_class(self):
        self.gateway.close()

    # @pytest.mark.skip("Already tested")
    def test_send_order(self):
        orderids = []
        for security in self.gateway.securities:
            print(security)
            quote = self.gateway.get_quote(security)
            create_time = self.gateway.market_datetime
            order = Order(
                security=security,
                price=quote.ask_price,
                stop_price=None,
                quantity=1,
                direction=Direction.LONG,
                offset=Offset.OPEN,
                order_type=OrderType.LIMIT,
                create_time=create_time
            )
            orderid = self.gateway.place_order(order)
            orderids.append(orderid)
            time.sleep(2)
        time.sleep(3)
        assert all(len(oid) > 0 for oid in orderids)
