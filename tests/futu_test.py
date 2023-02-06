# -*- coding: utf-8 -*-
# @Time    : 11/2/2022 1:46 pm
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: futu_test.py

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

from qtrader.core.constants import TradeMode, Exchange
from qtrader.core.security import Futures, Currency
from qtrader.gateways.futu import FutuFeesHKFE
from qtrader.gateways import FutuGateway

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
        gateway = FutuGateway(
            securities=stock_list,
            end=datetime.now() + timedelta(minutes=2),
            gateway_name=gateway_name,
            fees=FutuFeesHKFE
        )

        gateway.SHORT_INTEREST_RATE = 0.0
        gateway.trade_mode = TradeMode.LIVETRADE
        if gateway.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            assert datetime.now() < gateway.end, (
                "Gateway end time must be later than current datetime!")
        self.gateway = gateway

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