# -*- coding: utf-8 -*-
# @Time    : 1/16/2022 10:07 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: ib_test.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the 
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
# import sys
# import os
# import time
# from pathlib import Path
# SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
# sys.modules.pop("qtrader", None)
# sys.path.insert(0, str(Path(SCRIPT_PATH).parent.parent.joinpath("qtalib")))
# sys.path.insert(0, str(Path(SCRIPT_PATH).parent.parent.joinpath("qtrader")))
# sys.path.insert(0, str(Path(SCRIPT_PATH)))


from datetime import datetime, timedelta
from datetime import time as Time

import pytest

from qtrader.core.constants import TradeMode, Exchange
from qtrader.core.security import Futures, Currency
from qtrader.gateways.cqg import CQGFees
from qtrader.gateways import IbGateway

# pytest fixture ussage
# https://iter01.com/578851.html


class TestIbGateway:

    def setup_class(self):
        stock_list = [
            # Currency(code="EUR.USD", lot_size=1000, security_name="EUR.USD", exchange=Exchange.IDEALPRO),
            Futures(code="FUT.GC", lot_size=100, security_name="GCZ2",
                    exchange=Exchange.NYMEX, expiry_date="20221228"),
            Futures(code="FUT.SI", lot_size=5000, security_name="SIZ2",
                    exchange=Exchange.NYMEX, expiry_date="20221228"),
        ]
        gateway_name = "Ib"
        gateway = IbGateway(
            securities=stock_list,
            end=datetime.now() + timedelta(hours=1),
            gateway_name=gateway_name,
            fees=CQGFees
        )

        gateway.SHORT_INTEREST_RATE = 0.0
        gateway.trade_mode = TradeMode.SIMULATE
        if gateway.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            assert datetime.now() < gateway.end, "Gateway end time must be later than current datetime!"
        # Asia time
        gateway.TRADING_HOURS_AM = [Time(9, 0, 0), Time(10, 0, 0)]
        gateway.TRADING_HOURS_PM = [Time(10, 0, 0), Time(23, 30, 0)]
        self.gateway = gateway

    def teardown_class(self):
        self.gateway.close()

    # @pytest.mark.skip("Already tested")
    def test_get_recent_bars(self):
        for security in self.gateway.securities:
            bars = self.gateway.get_recent_bars(security, "2min")
            print(f"Number of bars: {len(bars)}")
            assert len(bars) == 60

    # @pytest.mark.skip("Already tested")
    def test_get_recent_bar(self):
        for _ in range(2):
            for security in self.gateway.securities:
                bar = self.gateway.get_recent_bar(security, "2min")
                print(f"Bar data: {bar}")
                assert bar.datetime.second == 0
                assert isinstance(bar.close, float)
            time.sleep(130)

    @pytest.mark.skip("Already tested")
    def test_get_broker_balance(self):
        balance = self.gateway.get_broker_balance()
        assert balance.available_cash > 0

    @pytest.mark.skip("Already tested")
    def test_get_all_broker_positions(self):
        positions = self.gateway.get_all_broker_positions()
        if positions:
            position = positions[0]
            assert position.quantity != 0