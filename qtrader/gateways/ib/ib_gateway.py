# -*- coding: utf-8 -*-
# @Time    : 6/9/2021 3:53 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: ib_gateway.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import pickle
import threading
from dataclasses import replace
from datetime import datetime
from typing import Dict, List, Any
from threading import Thread
from pathlib import Path

from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import OrderId, TickAttrib, TickerId, MarketDataTypeEnum, BarData
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order as IbOrder
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode, Exchange, OrderType
from qtrader.core.constants import OrderStatus as QTOrderStatus
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Stock, Security, Currency, Futures
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.utility import Time, try_parsing_datetime, BlockingDict
from qtrader_config import GATEWAYS, DATA_PATH, TIME_STEP
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees

"""
IMPORTANT
---------
Please install ibapi first:
1. Go to https://interactivebrokers.github.io/#
2. Download twsapi_macunix.1019.01.zip
3. > unzip twsapi_macunix.1019.01.zip  # unzip the file
4. cd ~Downloads/IBJts/source/pythonclient
5. > python setup.py install
"""

IB = GATEWAYS.get("Ib")


class IbWrapper(EWrapper):
    pass


class IbClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


class IbAPI(IbWrapper, IbClient):
    def __init__(self, gateway: BaseGateway):
        IbWrapper.__init__(self)
        IbClient.__init__(self, wrapper=self)
        self.gateway = gateway
        self.next_valid_id = 0
        self.connect(IB["host"], IB["port"], IB["clientid"])
        # EReader Thread
        self.thread = Thread(target=self.run)
        self.thread.start()

    def close(self):
        self.disconnect()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        contract = contractDetails.contract
        # Attached contracts to IB gateway
        for security in self.gateway.securities:
            if self.gateway.ib_contractdetails[security] is not None:
                continue
            if (
                get_ib_symbol(security) == contract.symbol
                and get_ib_security_type(security) == contract.secType
                and get_ib_exchange(security) == contract.exchange
                and get_ib_currency(security) == contract.currency
            ):
                self.gateway.ib_contractdetails[security] = contractDetails
                break

    def contractDetailsEnd(self, reqId: int):
        security = self.gateway.get_security_from_ib_contractdetails_reqid(
            reqId)
        # Notify threads that are waiting for ib_contractdetails_done
        self.gateway.ib_contractdetails_done[security].set()

    def historicalData(self, reqId: int, bar: BarData):
        bar_interval = self.gateway.get_bar_interval_from_ib_hist_bars_reqid(
            reqId)
        security = self.gateway.get_security_from_ib_hist_bars_reqid(reqId)
        if ":" in bar.date:
            if "Asia/Hong_Kong" in bar.date:
                bar_time = datetime.strptime(bar.date,
                                             "%Y%m%d  %H:%M:%S Asia/Hong_Kong")
            else:
                bar_time = datetime.strptime(bar.date, "%Y%m%d  %H:%M:%S")
        else:
            if "Asia/Hong_Kong" in bar.date:
                bar_time = datetime.strptime(bar.date, "%Y%m%d Asia/Hong_Kong")
            else:
                bar_time = datetime.strptime(bar.date, "%Y%m%d")
        qt_bar = Bar(
            datetime=bar_time,
            security=security,
            open=float(bar.open),
            high=float(bar.high),
            low=float(bar.low),
            close=float(bar.close),
            volume=int(bar.volume)
        )
        self.gateway.ib_hist_bars[bar_interval][security].append(qt_bar)

        # Notify threads that are waiting for ib_hist_bars_done
        # num_bars = len(self.gateway.ib_hist_bars[bar_interval][security])
        # if num_bars == self.gateway.ib_hist_bars_num[bar_interval][security]:
        #     self.gateway.ib_hist_bars_done[bar_interval][security].set()

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """The last bar is usually not completed, and shall be updated in
        historicalDataUpdate. Therefore we should abandon the last bar and only
        extract self.gateway.ib_bars[:-1]"""
        bar_interval = self.gateway.get_bar_interval_from_ib_hist_bars_reqid(
            reqId)
        security = self.gateway.get_security_from_ib_hist_bars_reqid(reqId)
        # Notify threads that are waiting for ib_hist_bars_done
        self.gateway.ib_hist_bars_done[bar_interval][security].set()

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        pass

    def realtimeBar(
            self,
            reqId: TickerId,
            ib_time: int,
            open_: float,
            high: float,
            low: float,
            close: float,
            volume: int,
            wap: float,
            count: int
    ):
        bar_interval = self.gateway.get_bar_interval_from_ib_bars_reqid(reqId)
        if "min" not in bar_interval:
            raise ValueError(f"bar_interval={bar_interval} is NOT valid!")
        security = self.gateway.get_security_from_ib_bars_reqid(reqId)
        # Once a 5s bar is received, the request can be considered successful
        # and we don't need to wait for all the data to be downloaded. We can
        # notify the threads that are waiting for the result of this request.
        self.gateway.ib_bars_req_done[bar_interval][security].set()

        bar_time = datetime.fromtimestamp(ib_time)
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=int(volume)
        )
        # print(bar)
        self.gateway.ib_bars["5sec"][security].append(bar)
        self.gateway.ib_bars_num["5sec"][security] += 1

        bar_interval_mins = int(bar_interval.replace("min", ""))
        num_5s_bars = bar_interval_mins * 12
        if (
                bar_time.second == 0
                and bar_time.minute % bar_interval_mins == 0
                # and self.gateway.ib_bars_num["5sec"][security] >= num_5s_bars
        ):
            bars = self.gateway.ib_bars["5sec"][security][-num_5s_bars:]

            # [Comment out the assertation]:
            # We give up a rigorous check for the bars, as we think an update
            # bar with less accuracy (for example, a 1min consolidated bar
            # aggregated from only 10, instead of 12, 5-seconds bars) is
            # more important than legacy/outdated bars.
            # assert validate_bar_interval(bars, 1), (
            #     f"Failed to validate 5s bars:\n{bars}"
            # )

            consolidated_bar = Bar(
                datetime=get_time_key(bars),
                security=security,
                open=get_open(bars),
                high=get_high(bars),
                low=get_low(bars),
                close=get_close(bars),
                volume=get_volume(bars))
            self.gateway.ib_bars[bar_interval][security].append(
                consolidated_bar)
            self.gateway.ib_bars["5sec"][security] = []
            self.gateway.ib_bars_num["5sec"][security] = 0
            # Notify the threads that are waiting for ib_bars_done
            self.gateway.ib_bars_done[bar_interval][security].set()

    def tickPrice(
            self,
            reqId: TickerId,
            tickType: TickType,
            price: float,
            attrib: TickAttrib
    ):
        # process quote
        security = self.gateway.get_security_from_ib_quotes_reqid(reqId)
        if self.gateway.ib_quotes[security] is None:
            quote = Quote(
                security=security,
                exchange=security.exchange,
                datetime=datetime.now())
        else:
            quote = self.gateway.ib_quotes[security]
        if tickType == TickTypeEnum.LAST:
            quote = replace(quote, last_price=price)
        elif tickType == TickTypeEnum.BID:
            quote = replace(quote, bid_price=price)
        elif tickType == TickTypeEnum.ASK:
            quote = replace(quote, ask_price=price)
        self.gateway.ib_quotes[security] = quote
        if (
                quote.last_price != 0
                or (quote.bid_price != 0 and quote.ask_price != 0)
        ):
            # Notify the threads that are waiting for ib_quotes_done
            self.gateway.ib_quotes_done[security].set()
            self.gateway.process_quote(quote)

        # process orderbook
        if self.gateway.ib_orderbooks[security] is None:
            orderbook = OrderBook(
                security=security,
                exchange=security.exchange,
                datetime=datetime.now())
        else:
            orderbook = self.gateway.ib_orderbooks[security]
        if tickType == TickTypeEnum.BID:
            orderbook = replace(orderbook, bid_price_1=price)
        elif tickType == TickTypeEnum.ASK:
            orderbook = replace(orderbook, ask_price_1=price)
        self.gateway.ib_orderbooks[security] = orderbook
        if (
                orderbook.bid_price_1 != 0
                and orderbook.ask_price_1 != 0
                and orderbook.bid_volume_1 != 0
                and orderbook.ask_volume_1 != 0
        ):
            # Notify the threads that are waiting for ib_orderbooks_done
            self.gateway.ib_orderbooks_done[security].set()
            self.gateway.process_orderbook(orderbook)

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        # process orderbook
        security = self.gateway.get_security_from_ib_quotes_reqid(reqId)
        if self.gateway.ib_orderbooks[security] is None:
            orderbook = OrderBook(
                security=security,
                exchange=security.exchange,
                datetime=datetime.now())
        else:
            orderbook = self.gateway.ib_orderbooks[security]
        if tickType == TickTypeEnum.BID_SIZE:
            orderbook = replace(orderbook, bid_volume_1=size)
        elif tickType == TickTypeEnum.ASK_SIZE:
            orderbook = replace(orderbook, ask_volume_1=size)
        else:
            # print("TickSize. TickerId:", reqId, "TickType:", tickType, "Size:", size)
            pass
        self.gateway.ib_orderbooks[security] = orderbook
        if (
                orderbook.bid_price_1 != 0
                and orderbook.ask_price_1 != 0
                and orderbook.bid_volume_1 != 0
                and orderbook.ask_volume_1 != 0
        ):
            # Notify the threads that are waiting for ib_orderbooks_done
            self.gateway.ib_orderbooks_done[security].set()
            self.gateway.process_orderbook(orderbook)

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        pass

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        pass

    def managedAccounts(self, accountsList: str):
        pass

    def updateAccountValue(
            self,
            key: str,
            val: str,
            currency: str,
            accountName: str
    ):
        if accountName != IB["broker_account"]:
            return
        account = self.gateway.balance
        if key == "CashBalance" and currency == "BASE":
            account.cash = float(val)
        elif key == "CashBalance":
            account.cash_by_currency[currency] = float(val)
        elif key == "AvailableFunds":
            account.available_cash = float(val)
        elif key == "BuyingPower":
            account.net_cash_power = float(val)
        elif key == "MaintMarginReq":
            account.maintenance_margin = float(val)
        elif key == "UnrealizedPnL":
            account.unrealized_pnl = float(val)
        elif key == "RealizedPnL":
            account.realized_pnl = float(val)

    def updatePortfolio(
            self,
            contract: Contract,
            position: float,
            marketPrice: float,
            marketValue: float,
            averageCost: float,
            unrealizedPNL: float,
            realizedPNL: float,
            accountName: str
    ):
        # Only update those securities listed in gateway initialization
        security = self.gateway.get_security_from_ib_contract(contract)
        if security is None:
            return
        if hasattr(self.gateway, "positions"):
            position_data = PositionData(
                security=security,
                direction=Direction.LONG if position > 0 else Direction.SHORT,
                holding_price=float(averageCost),
                quantity=abs(int(position)),
                update_time=datetime.now())
            self.gateway.positions.append(position_data)

    def updateAccountTime(self, timeStamp: str):
        pass

    def accountDownloadEnd(self, accountName: str):
        if hasattr(self.gateway, "balance"):
            self.gateway.ib_accounts.put(
                IB["broker_account"], self.gateway.balance)
        if hasattr(self.gateway, "positions"):
            self.gateway.ib_positions.put(
                IB["broker_account"], self.gateway.positions)

    def accountSummary(
            self,
            reqId: int,
            account: str,
            tag: str,
            value: str,
            currency: str
    ):
        pass

    def accountSummaryEnd(self, reqId: int):
        pass

    def error(
            self,
            reqId: TickerId,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str = ""
    ):
        """Ref: https://interactivebrokers.github.io/tws-api/message_codes.html#system_codes"""
        if errorCode in (502, 110):
            raise ConnectionError(
                f"reqId:{reqId} ErrorCode:{errorCode} ErrorMsg:{errorString} "
                f"advancedOrderRejectJson:{advancedOrderRejectJson}")
        else:
            print(
                f"reqId:{reqId} ErrorCode:{errorCode} ErrorMsg:{errorString} "
                f"advancedOrderRejectJson:{advancedOrderRejectJson}")

    def nextValidId(self, orderId: int):
        print(f"nextValidId={orderId}")
        self.next_valid_id = orderId
        # Notify the threads that are waiting for next_valid_id_event
        self.gateway.next_valid_id_event.set()

    def openOrder(
            self,
            orderId: OrderId,
            contract: Contract,
            order: IbOrder,
            orderState: OrderState):
        # get external order id
        self.gateway.ib_orderids.put(
            order.orderId, order.permId)

    def orderStatus(
            self,
            orderId: OrderId,
            status: str,
            filled: float,
            remaining: float,
            avgFillPrice: float,
            permId: int,
            parentId: int,
            lastFillPrice: float,
            clientId: int,
            whyHeld: str,
            mktCapPrice: float
    ):
        order_status = dict(
            orderId=orderId,
            status=status,
            filled=filled,
            remaining=remaining,
            avgFillPrice=avgFillPrice,
            permId=permId,
            parentId=parentId,
            lastFillPrice=lastFillPrice,
            clientId=clientId,
            whyHeld=whyHeld,
            mktCapPrice=mktCapPrice
        )
        self.gateway.process_order(order_status)

    def execDetails(
            self,
            reqId: int,
            contract: Contract,
            execution: Execution
    ):
        # get external deal id
        self.gateway.ib_dealids.put(
            execution.orderId, execution.execId)
        deal_status = dict(
            reqId=reqId,
            contract=contract,
            execution=execution
        )
        self.gateway.process_deal(deal_status)

    def commissionReport(self, commissionReport: CommissionReport):
        self.gateway.ib_commissions.put(
            commissionReport.execId,
            commissionReport.commission
        )


class IbGateway(BaseGateway):

    # Short interest rate, e.g., 0.0098 for HK stock
    SHORT_INTEREST_RATE = 0.0098

    # Name of the gateway
    NAME = "IB"

    def __init__(
            self,
            securities: List[Security],
            gateway_name: str,
            start: datetime = None,
            end: datetime = None,
            fees: BaseFees = BaseFees,
            **kwargs
    ):
        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        if "num_of_min_bar" in kwargs:
            self.num_of_min_bar = kwargs["num_of_min_bar"]
        else:
            self.num_of_min_bar = 60

        self.next_valid_id_event = threading.Event()
        self.local_valid_id = 1  # Local track of nextValidId/reqId

        # key: Security, value: threading.Event()
        self.ib_contractdetails_done = {
            s: threading.Event() for s in securities}
        # key:Security, value:IB.Contract
        self.ib_contractdetails = {s: None for s in securities}
        # key:Security, value:int (reqId)
        self.ib_contractdetails_reqid = {s: None for s in securities}

        bar_interval = f"{round(TIME_STEP / 60. / 1000.)}min"
        bar_req = ["5sec", bar_interval, "1day"]
        self.ib_bars_done = {f: {s: threading.Event()
                                 for s in self.securities} for f in bar_req}
        self.ib_bars_req_done = {f: {s: threading.Event()
                                     for s in self.securities} for f in bar_req}
        self.ib_bars = {f: {s: list() for s in self.securities}
                        for f in bar_req}
        self.ib_bars_reqid = {
            f: {s: None for s in self.securities} for f in bar_req}
        self.ib_bars_num = {f: {s: 0 for s in self.securities}
                            for f in bar_req}

        # for hist requests
        self.ib_hist_bars_done = {f: {s: threading.Event()
                                      for s in self.securities} for f in bar_req}
        self.ib_hist_bars = {f: {s: list() for s in self.securities}
                             for f in bar_req}
        self.ib_hist_bars_reqid = {
            f: {s: None for s in self.securities} for f in bar_req}
        self.ib_hist_bars_num = {f: {s: 0 for s in self.securities}
                                 for f in bar_req}

        self.ib_quotes_done = {
            s: threading.Event() for s in securities}
        # key:Security, value:Quote
        self.ib_quotes = {s: None for s in securities}
        # key:Security, value:int (reqId)
        self.ib_quotes_reqid = {s: None for s in securities}

        self.ib_orderbooks_done = {
            s: threading.Event() for s in securities}
        # key:Security, value:Orderbook
        self.ib_orderbooks = {s: None for s in securities}
        # key:Security, value:int (reqId)
        self.ib_orderbooks_reqid = {s: None for s in securities}

        self.ib_accounts = BlockingDict()
        self.ib_positions = BlockingDict()

        self.ib_orderids = BlockingDict()  # key:reqId, value:IbOrder.permId
        self.ib_dealids = BlockingDict()   # key:reqId, value:Execution.execId

        self.ib_commissions = BlockingDict()  # Key:Execution.execId, value:float

        self.api = IbAPI(self)
        self.subscribe()

    def close(self):
        # Unsubscribe data
        self.unsubscribe()
        # Disconnect API
        self.api.disconnect()

    def connect_quote(self):
        """"""
        raise NotImplementedError("connect_quote")

    def connect_trade(self):
        """"""
        raise NotImplementedError("connect_trade")

    def process_quote(self, quote: Quote):
        """Quote"""
        security = quote.security
        self.quote.put(security, quote)

    def process_orderbook(self, orderbook: OrderBook):
        """Orderbook"""
        security = orderbook.security
        self.orderbook.put(security, orderbook)

    def process_order(self, content: Dict[str, Any]):
        """Order

        content = order_status = dict(
            orderId=orderId,
            status=status,
            filled=filled,
            remaining=remaining,
            avgFillPrice=avgFillPrice,
            permId=permId,
            parentId=parentId,
            lastFillPrice=lastFillPrice,
            clientId=clientId,
            whyHeld=whyHeld,
            mktCapPrice=mktCapPrice
        )
        """
        orderid = str(content.get("permId"))
        order = self.orders.get(orderid, timeout=2)  # blocking
        if order is None:
            raise TimeoutError(
                f"Failed to get order from orderid {orderid} within the time limit.")
        order.updated_time = datetime.now()
        order.filled_avg_price = content.get("avgFillPrice")
        order.filled_quantity = int(content.get("filled"))
        order.status = convert_orderstatus_ib2qt(content.get("status"))
        self.orders.put(orderid, order)

    def process_deal(self, content: Dict[str, Any]):
        """Deal

        content = deal_status = dict(
            reqId=reqId,
            contract=contract,
            execution=execution
        )
        """
        execution = content.get("execution")
        order_reqId = execution.orderId
        dealid = str(execution.execId)
        assert self.ib_dealids.get(order_reqId, timeout=2) == execution.execId, (
            "execId does not match in self.ib_dealids!")
        orderid = self.ib_orderids.get(order_reqId, timeout=2)
        # IB returns an integer, so we need to convert it to a string
        orderid = str(orderid)
        order = self.orders.get(orderid, timeout=2)  # blocking
        if order is None:
            raise TimeoutError(
                f"Failed to get order from orderid {orderid} within the time limit.")
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=try_parsing_datetime(execution.time),
            filled_avg_price=float(execution.avgPrice),
            filled_quantity=int(execution.cumQty),
            dealid=dealid,
            orderid=orderid
        )
        self.deals.put(dealid, deal)

    @property
    def market_datetime(self):
        return datetime.now()

    def set_trade_mode(self, trade_mode: TradeMode):
        if trade_mode not in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            raise ValueError(
                "IbGateway only supports `SIMULATE` or `LIVETRADE` mode, "
                f"{trade_mode} was passed in instead.")
        self._trade_mode = trade_mode

    def gen_valid_id(self) -> int:
        """Maintain a local valid id, and compare it with nextValidId
        from IB server each time."""
        # Send request to IB and get response in api.nextValidId()
        self.api.reqIds(-1)
        if self.next_valid_id_event.wait():
            # Increase 1
            reqId = max(self.api.next_valid_id, self.local_valid_id) + 1
            # Keep track of reqId locally
            self.local_valid_id = reqId
        return reqId

    def req_hist_bars(
            self,
            ib_contract: Contract,
            security: Security,
            bar_interval: str,
            bar_num: int,
            update: bool = False
    ) -> int:
        """Request historical bars"""
        queryTime = "" if update else datetime.now().strftime(
            "%Y%m%d %H:%M:%S Asia/Hong_Kong")
        if "min" in bar_interval:
            bar_interval_num = int(bar_interval.replace("min", ""))
            assert bar_interval_num in (1, 2, 3, 5, 10, 15, 20, 30), (
                f"{bar_interval} is NOT a valid bar size!"
            )
            bar_size = f"{bar_interval_num} mins" if bar_interval_num > 1 else f"{bar_interval_num} min"
            durationStr = f"{bar_num * bar_interval_num * 60} S"

            # Blocking here
            reqId = self.gen_valid_id()

            self.ib_hist_bars_reqid[bar_interval][security] = reqId
            self.ib_hist_bars_num[bar_interval][security] = bar_num
            self.ib_hist_bars[bar_interval][security] = []
            # request historical data
            self.api.reqHistoricalData(
                reqId=reqId,
                contract=ib_contract,
                endDateTime=queryTime,
                durationStr=durationStr,
                barSizeSetting=bar_size,
                whatToShow=get_what_to_show(security),
                useRTH=0,
                formatDate=1,
                keepUpToDate=update,  # if True, endDateTime can not be specified
                chartOptions=[])
            return reqId
        elif "day" in bar_interval:
            bar_interval_num = int(bar_interval.replace("day", ""))
            assert bar_interval_num in (1, ), (
                f"{bar_interval} is NOT a valid bar size!"
            )
            durationStr = f"{bar_num} D"  # bar_interval_num == 1

            # Blocking here
            reqId = self.gen_valid_id()

            self.ib_hist_bars_reqid[bar_interval][security] = reqId
            self.ib_hist_bars_num[bar_interval][security] = bar_num
            self.ib_hist_bars[bar_interval][security] = []
            # request historical data
            self.api.reqHistoricalData(
                reqId=reqId,
                contract=ib_contract,
                endDateTime=queryTime,
                durationStr=durationStr,
                barSizeSetting="1 day",
                whatToShow=get_what_to_show(security),
                useRTH=0,
                formatDate=1,
                keepUpToDate=update,  # if True, endDateTime can not be specified
                chartOptions=[])
            return reqId
        else:
            raise ValueError(
                "bar_interval can only be '{int}min' or '1day', but "
                f"'{bar_interval}' was passed in.")

    def req_realtime_bars(
            self,
            ib_contract: Contract,
            security: Security,
            bar_interval: str,
    ) -> int:
        """Request 5-seconds realtime bar"""
        # Blocking here
        reqId = self.gen_valid_id()

        self.ib_bars_reqid[bar_interval][security] = reqId
        self.api.reqRealTimeBars(
            reqId,
            ib_contract,
            5,
            get_what_to_show(security),
            False,
            []
        )
        return reqId

    def req_market_data(
            self,
            ib_contract: Contract,
            security: Security
    ) -> int:
        """Request market data (Quotes and Orderbooks)"""
        # Blocking here
        reqId = self.gen_valid_id()

        self.ib_quotes_reqid[security] = reqId
        self.ib_orderbooks_reqid[security] = reqId
        self.api.reqMktData(
            reqId, ib_contract, "", False, False, [])
        return reqId

    def req_contract_details(
            self,
            ib_contract: Contract,
            security: Security
    ) -> int:
        """Request contract details"""
        # Blocking here
        reqId = self.gen_valid_id()

        self.ib_contractdetails_reqid[security] = reqId
        # Get accurate contract
        self.api.reqContractDetails(
            reqId=reqId, contract=ib_contract)
        return reqId

    def unsubscribe(self):
        # Unsubscribe data
        for security in self.securities:
            if self.ib_quotes_reqid[security]:
                self.api.cancelMktData(reqId=self.ib_quotes_reqid[security])
            for bar_interval in self.ib_bars_reqid:
                reqId = self.ib_bars_reqid[bar_interval][security]
                if reqId is None:
                    continue
                if bar_interval == "5sec":
                    self.api.cancelRealTimeBars(reqId=reqId)
                else:
                    self.api.cancelHistoricalData(reqId=reqId)

    def subscribe(self):
        # "REALTIME", "FROZEN", "DELAYED", "DELAYED_FROZEN"
        self.api.reqMarketDataType(MarketDataTypeEnum.REALTIME)
        for security in self.securities:
            if self.ib_contractdetails[security] is None:
                try:
                    # Try to load contract details from pickle file
                    with open(f".qtrader_cache/ib/contractdetails/{security.code}", "rb") as f:
                        contractdetails = pickle.load(f)
                        self.ib_contractdetails[security] = contractdetails
                except FileNotFoundError:
                    # Construct vague contract
                    ib_contract = generate_ib_contract(security)
                    # Get accurate contract
                    reqId = self.req_contract_details(
                        ib_contract=ib_contract,
                        security=security
                    )
                    # blocking here
                    if self.ib_contractdetails_done[security].wait():
                        print(f"[{reqId}]Obtained contract for {security.code}")

                    # pickle contractdetails for use next time to avoid
                    # requesting too frequently
                    Path(
                        ".qtrader_cache/ib/contractdetails").mkdir(parents=True, exist_ok=True)
                    with open(f".qtrader_cache/ib/contractdetails/{security.code}", "wb") as f:
                        contractdetails = self.ib_contractdetails.get(security)
                        pickle.dump(contractdetails, f)

            if self.ib_contractdetails[security] is None:
                raise ConnectionError(
                    "Connection to IB server is probably blocked temporally "
                    "due to requesting contract details too frequently."
                )

            # Prepare accurate IB contract
            ib_contract = self.get_ib_contract_from_security(security)

            # Request market data (quote and orderbook)
            reqId = self.req_market_data(
                ib_contract=ib_contract,
                security=security
            )
            # Blocking here
            if self.ib_quotes_done[security].wait():
                print(f"[{reqId}]Subscribed market data for {security.code}")

            # Read bar interval from config file
            bar_interval = f"{round(TIME_STEP / 60. / 1000.)}min"

            # Request hist bar data
            reqId = self.req_hist_bars(
                ib_contract=ib_contract,
                security=security,
                bar_interval=bar_interval,
                bar_num=self.num_of_min_bar,
                update=False
            )
            if self.ib_hist_bars_done[bar_interval][security].wait():
                print(f"[{reqId}]Subscribed hist bars for {security.code}")

            self.api.cancelHistoricalData(reqId=reqId)
            self.ib_hist_bars_reqid[bar_interval][security] = None
            self.ib_hist_bars_num[bar_interval][security] = 0
            self.ib_bars[bar_interval][security] = self.ib_hist_bars[
                bar_interval][security][:]
            print(f"[{reqId}]Cancelled hist bars for {security.code}")

            # Request realtime bar data
            reqId = self.req_realtime_bars(
                ib_contract=ib_contract,
                security=security,
                bar_interval=bar_interval,
            )
            if self.ib_bars_req_done[bar_interval][security].wait():
                print(f"[{reqId}]Subscribed realtime bars for {security.code}")

    def get_recent_bars(
            self,
            security: Security,
            bar_interval: str = None
    ) -> List[Bar]:
        """Get recent historical OHLCVs"""
        if bar_interval is None:
            bar_interval = f"{round(TIME_STEP / 60. / 1000.)}min"
        num_bars = self.num_of_min_bar
        recent_bars = self.ib_bars[bar_interval][security][-num_bars:]
        return recent_bars

    def get_recent_bar(
            self,
            security: Security,
            bar_interval: str = None
    ) -> Bar:
        """Get most recent historical OHLCV"""
        if bar_interval is None:
            bar_interval = f"{round(TIME_STEP / 60. / 1000.)}min"
        num_bars = self.num_of_min_bar
        recent_bars = self.ib_bars[bar_interval][security][-num_bars:]
        return recent_bars[-1]

    def get_recent_capital_distribution(
            self,
            security: Stock
    ) -> CapitalDistribution:
        """capital distribution"""
        raise NotImplementedError(
            "[get_recent_capital_distribution] not yet implemented in IB gateway!")

    def get_recent_data(
            self,
            security: Security,
            **kwargs
    ) -> Dict or Bar or CapitalDistribution:
        """Get recent data (OHLCV or CapitalDistributions)"""
        if kwargs:
            assert "dfield" in kwargs, (
                f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
            )
            dfields = [kwargs["dfield"]]
        else:
            dfields = DATA_PATH
        data = dict()
        for dfield in dfields:
            if dfield == "kline":
                data[dfield] = self.get_recent_bar(security)
            elif dfield == "capdist":
                data[dfield] = self.get_recent_capital_distribution(security)
        if len(dfields) == 1:
            return data[dfield]
        return data

    def get_security(self, code: str) -> Security:
        """Get security with security code"""
        for security in self.securities:
            if security.code == code:
                return security
        return None

    def place_order(self, order: Order) -> str:
        """Place order"""
        # Obtain Contract
        contract_details = self.ib_contractdetails.get(order.security)
        contract = contract_details.contract
        contract.lastTradeDateOrContractMonth = order.security.expiry_date
        min_tick = contract_details.minTick

        # Generate Order
        ib_order = IbOrder()
        ib_order.action = order_direction_qt2ib(order.direction)
        ib_order.orderType = order_type_qt2ib(order.order_type)
        ib_order.totalQuantity = order.quantity
        if order.order_type == OrderType.LIMIT:
            # adjust price precision
            if order.direction == Direction.LONG:
                limit_price = (order.price // min_tick) * min_tick
            elif order.direction == Direction.SHORT:
                limit_price = (order.price // min_tick) * min_tick + min_tick
            ib_order.lmtPrice = limit_price

        # generate next valid id
        order_reqId = self.gen_valid_id()

        self.api.placeOrder(order_reqId, contract, ib_order)
        # blocking here to obtain order id (suppose to be very fast here)
        orderid = self.ib_orderids.get(order_reqId, timeout=2, default_item="")
        if orderid == "":
            raise ConnectionError(
                f"Failed to get orderid. {order_reqId}: {contract}: {ib_order}")
        # IB returns integer, we need to convert it to str
        orderid = str(orderid)
        order.status = QTOrderStatus.SUBMITTED
        self.orders.put(orderid, order)
        return orderid

    def cancel_order(self, orderid):
        """Cancel order"""
        order_reqId = self.get_order_reqId_from_orderid(orderid)
        if order_reqId is None:
            print(f"[cancel_order] failed: {orderid}")
            return
        self.api.cancelOrder(order_reqId)

    def get_broker_balance(self) -> AccountBalance:
        """Broker balance"""
        # Initialize an empty account balance
        self.balance = AccountBalance(cash_by_currency={})
        # request account updates from IB server
        self.api.reqAccountUpdates(True, IB["broker_account"])
        # Wait for results
        balance = self.ib_accounts.get(IB["broker_account"], timeout=2)
        # Close request
        self.api.reqAccountUpdates(False, IB["broker_account"])
        return balance

    def get_broker_position(
            self,
            security: Stock,
            direction: Direction
    ) -> PositionData:
        """Broker position"""
        positions = self.get_all_broker_positions()
        for position_data in positions:
            if position_data.security == security and position_data.direction == direction:
                return position_data
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """All broker positions"""
        # Create an empty list to store positions
        self.positions = []
        # Request account updates from IB server
        self.api.reqAccountUpdates(True, IB["broker_account"])
        # Wait for results
        positions = self.ib_positions.get(IB["broker_account"], timeout=2)
        # Close request
        self.api.reqAccountUpdates(False, IB["broker_account"])
        return positions

    def get_quote(self, security: Stock) -> Quote:
        """Quote"""
        return self.quote.get(security, timeout=2)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """Orderbook"""
        return self.orderbook.get(security, timeout=2)

    def get_ib_contract_from_security(self, security: Security) -> Contract:
        contract_details = self.ib_contractdetails.get(security)
        return contract_details.contract

    def get_security_from_ib_contract(self, contract: Contract) -> Security:
        for security in self.ib_contractdetails:
            ib_contract = self.get_ib_contract_from_security(security)
            if ib_contract.conId == contract.conId:
                return security

    def get_security_from_ib_contractdetails_reqid(
            self, reqId: int) -> Security:
        for security in self.ib_contractdetails_reqid:
            if self.ib_contractdetails_reqid[security] == reqId:
                return security

    def get_security_from_ib_bars_reqid(self, reqId: int) -> Security:
        for bar_interval in self.ib_bars_reqid:
            for security in self.ib_bars_reqid[bar_interval]:
                if self.ib_bars_reqid[bar_interval][security] == reqId:
                    return security

    def get_security_from_ib_hist_bars_reqid(self, reqId: int) -> Security:
        for bar_interval in self.ib_hist_bars_reqid:
            for security in self.ib_hist_bars_reqid[bar_interval]:
                if self.ib_hist_bars_reqid[bar_interval][security] == reqId:
                    return security

    def get_bar_interval_from_ib_bars_reqid(self, reqId: int) -> str:
        for bar_interval in self.ib_bars_reqid:
            for security in self.ib_bars_reqid[bar_interval]:
                if self.ib_bars_reqid[bar_interval][security] == reqId:
                    return bar_interval

    def get_bar_interval_from_ib_hist_bars_reqid(self, reqId: int) -> str:
        for bar_interval in self.ib_hist_bars_reqid:
            for security in self.ib_hist_bars_reqid[bar_interval]:
                if self.ib_hist_bars_reqid[bar_interval][security] == reqId:
                    return bar_interval

    def get_security_from_ib_quotes_reqid(self, reqId: int) -> Security:
        for security in self.ib_quotes_reqid:
            if self.ib_quotes_reqid[security] == reqId:
                return security

    def get_order_reqId_from_orderid(self, orderid: str) -> int:
        for reqId in self.ib_orderids:
            if self.ib_orderids.get(reqId, timeout=2) == orderid:
                return reqId

    def req_historical_bars(
            self,
            security: Security,
            periods: int,
            freq: str,
            cur_datetime: datetime,
            daily_open_time: Time = None,
            daily_close_time: Time = None,
    ) -> List[Bar]:
        """request historical bar data."""
        # Check params
        if freq == "1Day" and (
                daily_open_time is None or daily_close_time is None):
            raise ValueError(
                "Parameters daily_open_time and daily_close_time are "
                f"mandatory if freq={freq}.")

        # return historical bar data
        if freq == "1Min":
            return _req_historical_bars_ib_1min(
                security=security,
                periods=periods,
                gateway=self
            )
        elif freq == "1Day":
            return _req_historical_bars_ib_1day(
                security=security,
                periods=periods,
                gateway=self
            )

        # freq is not valid
        FREQ_ALLOWED = ("1Day", "1Min")
        raise ValueError(
            f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} "
            "are allowed.")


def _req_historical_bars_ib_1min(
    security: Security,
    periods: int,
    gateway: BaseGateway
) -> List[Bar]:
    if gateway.ib_bars_num["1min"][security] is None:
        ib_contract = gateway.get_ib_contract_from_security(
            security)
        gateway.req_bar(
            ib_contract,
            security,
            "1min",
            periods,
            False  # if set True, will block receiving data in reqAccountUpdates
        )
    return gateway.get_recent_bars(security, "1min")


def _req_historical_bars_ib_1day(
    security: Security,
    periods: int,
    gateway: BaseGateway,
) -> List[Bar]:
    if gateway.ib_bars_num["1day"][security] is None:
        ib_contract = gateway.get_ib_contract_from_security(
            security)
        gateway.req_bar(
            ib_contract,
            security,
            "1day",
            periods,
            False
        )
    return gateway.get_recent_bars(security, "1day")


def get_ib_security_type(security: Security) -> str:
    if isinstance(security, Stock):
        return "STK"
    elif isinstance(security, Currency):
        return "CASH"
    elif isinstance(security, Futures):
        return "FUT"
    else:
        raise ValueError(f"Type {security} not supported in IB yet!")


def get_ib_currency(security: Security) -> str:
    if security.exchange == Exchange.SEHK:
        return "HKD"
    elif security.exchange == Exchange.IDEALPRO:
        base_currency, quote_currency = security.code.split(".")
        return quote_currency
    elif security.exchange == Exchange.NYMEX:
        return "USD"
    elif security.exchange == Exchange.SMART:
        return "USD"
    else:
        raise ValueError(f"Currency of {security} not specified yet!")


def get_ib_exchange(security: Security) -> str:
    if security.exchange == Exchange.SEHK:
        return "SEHK"
    elif security.exchange == Exchange.IDEALPRO:
        return "IDEALPRO"
    elif security.exchange == Exchange.SMART:
        return "SMART"
    elif security.exchange == Exchange.NYMEX:
        return "NYMEX"
    else:
        raise ValueError(f"Exchange of {security} not supported in IB yet!")


def get_ib_symbol(security: Security) -> str:
    if isinstance(security, Stock):
        # TODO: maybe keep 4 digits?
        market, symbol = security.code.split(".")
        return symbol.lstrip("0")
    elif isinstance(security, Currency):
        base_currency, quote_currency = security.code.split(".")
        return base_currency
    elif isinstance(security, Futures):
        market, symbol = security.code.split(".")
        return symbol
    else:
        raise ValueError(f"Can not find IB symbol for {security}!")


def generate_ib_contract(security: Security) -> Contract:
    """
    Example:
    ib_contract.exchange = "SEHK"
    ib_contract.secType = "STK"
    ib_contract.currency = "HKD"
    ib_contract.symbol = "1157"
    :param security:
    :return:
    """
    ib_contract = Contract()
    ib_contract.exchange = get_ib_exchange(security)
    ib_contract.secType = get_ib_security_type(security)
    ib_contract.currency = get_ib_currency(security)
    ib_contract.symbol = get_ib_symbol(security)
    if isinstance(security, Futures):
        ib_contract.lastTradeDateOrContractMonth = security.expiry_date
    return ib_contract


def order_direction_qt2ib(direction: Direction):
    if direction == Direction.LONG:
        return "BUY"
    elif direction == Direction.SHORT:
        return "SELL"
    else:
        raise ValueError(f"Direction {direction} is not supported in IB!")


def order_type_qt2ib(order_type: OrderType):
    if order_type == OrderType.MARKET:
        return "MKT"
    elif order_type == OrderType.LIMIT:
        return "LMT"
    elif order_type == OrderType.STOP:
        return "STP"
    else:
        raise ValueError(f"OrderType {order_type} is not supported in IB!")


def convert_orderstatus_ib2qt(status: str) -> QTOrderStatus:
    """Convert IB order status to QT"""
    # No partial filled in IB
    # Ref: https://interactivebrokers.github.io/tws-api/order_submission.html
    if status in ("PendingCancel", "ApiCancelled"):
        return QTOrderStatus.UNKNOWN
    if status in ("ApiPending", "PendingSubmit", "PreSubmitted"):
        return QTOrderStatus.SUBMITTING
    elif status in ("Submitted"):
        return QTOrderStatus.SUBMITTED
    elif status in ("Cancelled"):
        return QTOrderStatus.CANCELLED
    elif status in ("Filled"):
        return QTOrderStatus.FILLED
    elif status in ("Inactive"):
        return QTOrderStatus.FAILED
    else:
        raise ValueError(f"IB Order status {status} can not be recognized.")


def get_what_to_show(security: Security) -> str:
    if isinstance(security, Currency):
        what_to_show = "MIDPOINT"
    else:
        what_to_show = "TRADES"
    return what_to_show


def get_default_bar_num_from_bar_interval(bar_interval: str) -> int:
    if bar_interval == "1min":
        bar_num = 60 * 8
    elif bar_interval == "1day":
        bar_num = 60
    else:
        raise ValueError(f"bar_interval = {bar_interval} is not supported!")
    return bar_num


def validate_bar_interval(bars: List[Bar], bar_interval: int):
    """Validate the aggregated bar interval is as expected, bar_interval is
    measured with minutes"""
    time_key1 = bars[0].datetime
    time_key2 = bars[-1].datetime
    time_key_diff = (time_key2 - time_key1).total_seconds()
    return time_key_diff == bar_interval * 60 - 5


def get_time_key(bars: List[Bar]):
    return bars[-1].datetime


def get_open(bars: List[Bar]):
    return bars[0].open


def get_high(bars: List[Bar]):
    high = -float("Inf")
    for bar in bars:
        if bar.high > high:
            high = bar.high
    return high


def get_low(bars: List[Bar]):
    low = float("Inf")
    for bar in bars:
        if bar.low < low:
            low = bar.low
    return low


def get_close(bars: List[Bar]):
    return bars[-1].close


def get_volume(bars: List[Bar]):
    volume = 0
    for bar in bars:
        volume += bar.volume
    return volume
