# -*- coding: utf-8 -*-
# @Time    : 6/9/2021 3:53 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: ib_gateway.py
# @Software: PyCharm

"""
Please install ibapi first:
1. Go to https://interactivebrokers.github.io/#
2. Download twsapi_macunix.976.01.zip
3. unzip the file
4. cd Downloads/twsapi_macunix.976.01/IBJts/source/pythonclient
5. > python setup.py install
"""

import math
import uuid
from typing import Dict, List, Union

from ibapi.client import EClient
from ibapi.wrapper import EWrapper

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode
from qtrader.core.constants import OrderStatus as QTOrderStatus
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import Position, PositionData
from qtrader.core.security import Stock
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.utility import Time, try_parsing_datetime, BlockingDict
from qtrader.config import GATEWAYS, DATA_PATH
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees

IB = GATEWAYS.get("Ib")

class IbFees(BaseFees):
    """

    """

    def __init__(self, *trades:Dict):
        # IB收费
        commissions = 0       # 佣金
        platform_fees = 0     # 平台使用费
        # IB代收费
        system_fees = 0       # 交易系统使用费
        settlement_fees = 0   # 交收费
        stamp_fees = 0        # 印花税
        trade_fees = 0        # 交易费
        transaction_fees = 0  # 交易征费

        # 总费用
        total_fees = commissions + platform_fees + system_fees + settlement_fees + stamp_fees + trade_fees + transaction_fees

        # TODO：未写IB收费细则
        self.commissions = commissions
        self.platform_fees = platform_fees
        self.system_fees = system_fees
        self.settlement_fees = settlement_fees
        self.stamp_fees = stamp_fees
        self.trade_fees = trade_fees
        self.transaction_fees = transaction_fees
        self.total_fees = total_fees


class IbGateway(BaseGateway):

    def __init__(self,
            securities:List[Stock],
            gateway_name:str,
            start:datetime=None,
            end:datetime=None,
            fees:BaseFees=FutuHKEquityFees,
        ):
        super().__init__(securities, gateway_name)

        self.fees = fees
        self.start = start
        self.end = end

        self.trade_mode = None

        self.quote_ctx = OpenQuoteContext(host=FUTU["host"], port=FUTU["port"])
        self.connect_quote()
        self.subscribe()

        self.trd_ctx = OpenHKTradeContext(host=FUTU["host"], port=FUTU["port"])
        self.connect_trade()