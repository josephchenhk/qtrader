# -*- coding: utf-8 -*-
# @Time    : 1/6/2022 9:53 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: gateway_test.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
from datetime import datetime
from datetime import timedelta
from datetime import time as Time
from sys import platform

import pytest

from qtrader.config import GATEWAYS
from qtrader.core.constants import Exchange, TradeMode
from qtrader.core.engine import Engine
from qtrader.core.security import Futures
from qtrader.gateways import CqgGateway
from qtrader.gateways.cqg import CQGFees
from qtrader.gateways import IbGateway
from qtrader.gateways.ib import IbHKEquityFees


def check_sys_platform():
    if platform == "linux" or platform =="linux2":
        return "Linux"
    elif platform == "darwin":
        return "OS X"
    elif platform == "win32":
        return "Windows"
    else:
        raise ValueError(f"Platform {platform} is not recognized.")

class GatewayEngines:

    @classmethod
    def cqg(self):
        stock_list = [
            # Futures(code="FUT.GC", lot_size=100, security_name="GCZ1", exchange=Exchange.NYMEX, expiry_date="20211229"),
            Futures(code="FUT.ZUC", lot_size=100, security_name="ZUCF22", exchange=Exchange.SGX, expiry_date="20220131"),
        ]

        gateway_name = "Cqg"
        init_capital = 100000
        gateway = CqgGateway(
            securities=stock_list,
            end=datetime.now() + timedelta(hours=1),
            gateway_name=gateway_name,
            fees=CQGFees
        )
        gateway.SHORT_INTEREST_RATE = 0.0
        gateway.set_trade_mode(TradeMode.SIMULATE)
        gateway.TRADING_HOURS_AM = [Time(9, 0, 0), Time(10, 0, 0)]
        gateway.TRADING_HOURS_PM = [Time(10, 0, 0), Time(16, 0, 0)]

        engine = Engine(gateways={gateway_name: gateway})
        return engine

cqg_engine = GatewayEngines.cqg()

class TestCQG:

    def test_sys_platform(self):
        """ CQG only works in Windows platform
        """
        if "Cqg" in GATEWAYS:
            assert check_sys_platform() == "Windows"
        else:
            assert 1

    def test_wincom32_installed(self):
        """ Install pywin32
        > pip install pywin32==225
        """
        if "Cqg" in GATEWAYS:
            installed = 0
            try:
                import win32com.client
                installed = 1
            except ImportError:
                pass
            assert installed
        else:
            assert 1

if __name__ == "__main__":
    print()