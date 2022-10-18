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
from qtrader.core.utility import Time, try_parsing_datetime, BlockingDict, DefaultQueue
from qtrader_config import GATEWAYS, DATA_PATH, TIME_STEP
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees

NUM_BARS = 10
"""
IMPORTANT
---------
Please install ibapi first:
1. Go to https://interactivebrokers.github.io/#
2. Download twsapi_macunix.976.01.zip
3. unzip the file
4. cd Downloads/twsapi_macunix.976.01/IBJts/source/pythonclient
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
        self.reqId = 0
        self.nextValidIdQueue = DefaultQueue(maxsize=1)
        self.connect(IB["host"], IB["port"], IB["clientid"])
        # EReader Thread
        self.thread = Thread(target=self.run)
        self.thread.start()

    def close(self):
        self.disconnect()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        # super().contractDetails(reqId, contractDetails)
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
        # super().contractDetailsEnd(reqId)
        # print("ContractDetailsEnd. ReqId:", reqId)
        security = self.gateway.get_security_from_ib_contractdetails_reqid(
            reqId)
        # Notify threads that are waiting for ib_contractdetails_done
        self.gateway.ib_contractdetails_done[security].put(reqId)

    def historicalData(self, reqId: int, bar: BarData):
        # print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        bar_interval_num = self.gateway.get_bar_interval_from_ib_bars_reqid(reqId)
        security = self.gateway.get_security_from_ib_bars_reqid(reqId)
        if ":" in bar.date:
            bar_interval = f"{bar_interval_num}min"
            bar_time = datetime.strptime(bar.date, "%Y%m%d  %H:%M:%S")
        else:
            bar_interval = f"{bar_interval_num}day"
            bar_time = datetime.strptime(bar.date, "%Y%m%d")
        qt_bar = Bar(
            datetime=bar_time,
            security=security,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume)
        self.gateway.ib_bars[bar_interval][security].append(qt_bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """The last bar is usually not completed, and shall be updated in
        historicalDataUpdate. Therefore we should abandon the last bar and only
        extract self.gateway.ib_bars[:-1]"""
        # super().historicalDataEnd(reqId, start, end)
        # print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        bar_interval = self.gateway.get_bar_interval_from_ib_bars_reqid(reqId)
        security = self.gateway.get_security_from_ib_bars_reqid(reqId)
        bar_num_diff = len(self.gateway.ib_bars[bar_interval][security]) - \
            self.gateway.ib_bars_num[bar_interval][security]
        if "min" in bar_interval:
            if not (bar_num_diff == 0 or bar_num_diff == 1):
                raise ValueError(
                    "Received "
                    f"{len(self.gateway.ib_bars[bar_interval][security])} "
                    f"{bar_interval} bars, expect to get "
                    f"{self.gateway.ib_bars_num[bar_interval][security]} (last "
                    f"1min bar should be updating). Check {security.code} and "
                    f"{bar_interval}")
        elif "day" in bar_interval:
            if not (bar_num_diff == 0 or bar_num_diff == 1):
                raise ValueError(
                    "Received "
                    f"{len(self.gateway.ib_bars[bar_interval][security])} "
                    f"{bar_interval} bars, expect to get "
                    f"{self.gateway.ib_bars_num[bar_interval][security]}. "
                    f"Check {security.code} and {bar_interval}")
        # Notify threads that are waiting for ib_bars_done
        if self.gateway.ib_bars_done[bar_interval][security].qsize() == 0:
            self.gateway.ib_bars_done[bar_interval][security].put(reqId)

    def historicalDataUpdate(self, reqId: int, bar: BarData):
        # print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)
        security = self.gateway.get_security_from_ib_bars_reqid(reqId)
        if ":" in bar.date:
            bar_interval = f"{self.gateway.time_step_in_mins}min"
            bar_time = datetime.strptime(bar.date, "%Y%m%d  %H:%M:%S")
        else:
            bar_interval = "1day"
            bar_time = datetime.strptime(bar.date, "%Y%m%d")
        update_bar = Bar(
            datetime=bar_time,
            security=security,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume)
        last_bar = self.gateway.ib_bars[bar_interval][security][-1]
        bar_reqId = self.gateway.ib_bars_done[bar_interval][security].get()
        assert bar_reqId == reqId, (
            f"reqId does not match! bar_reqId = {bar_reqId}, but reqId = "
            f"{reqId}."
        )
        if update_bar.datetime == last_bar.datetime:
            self.gateway.ib_bars[bar_interval][security][-1] = update_bar
        elif update_bar.datetime > last_bar.datetime:
            self.gateway.ib_bars[bar_interval][security].append(update_bar)
            self.gateway.ib_bars[bar_interval][security].pop(0)
            # Notify threads that are waiting for ib_bars_done
            if self.gateway.ib_bars_done[bar_interval][security].qsize() == 0:
                self.gateway.ib_bars_done[bar_interval][security].put(reqId)
        else:
            raise ValueError("Update bar datetime is earlier than last bar!")

    def realtimeBar(
            self,
            reqId: TickerId,
            time: int,
            open_: float,
            high: float,
            low: float,
            close: float,
            volume: int,
            wap: float,
            count: int
    ):
        # super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        # print("RealTimeBar. TickerId:", reqId, time, open_, high, low, close, volume, wap, count)
        security = self.gateway.get_security_from_ib_5s_bars_reqid(reqId)
        bar_time = datetime.fromtimestamp(time)
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume)

        if (
            security in self.gateway.ib_5s_bars
            and len(self.gateway.ib_5s_bars[security]) == self.gateway.ib_5s_bars_max_no
        ):
            self.gateway.ib_5s_bars[security].pop(0)
        self.gateway.ib_5s_bars[security].append(bar)
        assert len(self.gateway.ib_5s_bars[security]) <= self.gateway.ib_5s_bars_max_no, (
            f"Error: There can not be more than "
            f"{self.gateway.ib_5s_bars_max_no} bars!")

        # Notify threads that are waiting for ib_consolidated_bars_done
        if (
            len(self.gateway.ib_5s_bars[security]) == self.gateway.ib_5s_bars_max_no
            and self.gateway.ib_5s_bars[security][-1].datetime.second == 0
            and self.gateway.ib_consolidated_bars_done[security].qsize() == 0
        ):
            bars = self.gateway.ib_5s_bars[security][:]
            assert validate_bar_interval(bars, 1), (
                f"Failed to validate 5s bars:\n{bars}"
            )
            consolidated_bar = Bar(
                datetime=get_time_key(bars),
                security=security,
                open=get_open(bars),
                high=get_high(bars),
                low=get_low(bars),
                close=get_close(bars),
                volume=get_volume(bars))
            self.gateway.ib_consolidated_1m_bar[security] = consolidated_bar
            self.gateway.ib_consolidated_bars_done[security].put(reqId)
        elif (
            self.gateway.ib_consolidated_1m_bar[security] is None
            and self.gateway.ib_consolidated_bars_done[security].qsize() == 1
        ):
            self.gateway.ib_consolidated_bars_done[security].get()

    def tickPrice(
            self,
            reqId: TickerId,
            tickType: TickType,
            price: float,
            attrib: TickAttrib
    ):
        # super().tickPrice(reqId, tickType, price, attrib)
        # print("TickPrice. TickerId:", reqId, "tickType:", tickType,
        #     "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
        #     "PastLimit:", attrib.pastLimit, end = ' '
        # )

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
        if quote.last_price != 0 or (
                quote.bid_price != 0 and quote.ask_price != 0):
            if self.gateway.ib_quotes_done[security].qsize() == 1:
                self.gateway.ib_quotes_done[security].get()
            self.gateway.ib_quotes_done[security].put(reqId)
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
            if self.gateway.ib_orderbooks_done[security].qsize() == 1:
                self.gateway.ib_orderbooks_done[security].get()
            self.gateway.ib_orderbooks_done[security].put(reqId)
            self.gateway.process_orderbook(orderbook)

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        # super().tickSize(reqId, tickType, size)
        # print("TickSize. TickerId:", reqId, "TickType:", tickType, "Size:", size)

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
            if self.gateway.ib_orderbooks_done[security].qsize() == 1:
                self.gateway.ib_orderbooks_done[security].get()
            self.gateway.ib_orderbooks_done[security].put(reqId)
            self.gateway.process_orderbook(orderbook)

    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        # super().tickString(reqId, tickType, value)
        # print("TickString. TickerId:", reqId, "Type:", tickType, "Value:", value)
        pass

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        # super().tickGeneric(reqId, tickType, value)
        # print("TickGeneric. TickerId:", reqId, "TickType:", tickType, "Value:", value)
        pass

    def managedAccounts(self, accountsList: str):
        # super().managedAccounts(accountsList)
        # print("Account list:", accountsList)
        pass

    def updateAccountValue(
            self,
            key: str,
            val: str,
            currency: str,
            accountName: str
    ):
        # super().updateAccountValue(key, val, currency, accountName)
        # print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)
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
        # super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName)
        # print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType,
        #       "Exchange:", contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
        #       "MarketValue:", marketValue, "AverageCost:", averageCost,
        #       "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
        #       "AccountName:", accountName
        # )

        # Only update those securities listed in gateway initialization
        security = self.gateway.get_security_from_ib_contract(contract)
        if security is None:
            return
        if hasattr(self.gateway, "positions"):
            position_data = PositionData(
                security=security,
                direction=Direction.LONG if position > 0 else Direction.SHORT,
                holding_price=averageCost,
                quantity=abs(position),
                update_time=datetime.now())
            self.gateway.positions.append(position_data)

    def updateAccountTime(self, timeStamp: str):
        # super().updateAccountTime(timeStamp)
        # print("UpdateAccountTime. Time:", timeStamp)
        pass

    def accountDownloadEnd(self, accountName: str):
        # super().accountDownloadEnd(accountName)
        # print("AccountDownloadEnd. Account:", accountName)
        # Update finished, insert into blocking dict
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
        # super().accountSummary(reqId, account, tag, value, currency)
        # print("AccountSummary. ReqId:", reqId, "Account:", account, "Tag: ", tag, "Value:", value, "Currency:", currency)
        pass

    def accountSummaryEnd(self, reqId: int):
        # super().accountSummaryEnd(reqId)
        # print("AccountSummaryEnd. ReqId:", reqId)
        pass

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        """Ref: https://interactivebrokers.github.io/tws-api/message_codes.html#system_codes"""
        # super().error(reqId, errorCode, errorString)
        # print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)

        if errorCode in (502, 110):
            raise ConnectionError(
                f"ErrorCode:{errorCode} ErrorMsg:{errorString}")

    def nextValidId(self, orderId: int):
        # super().nextValidId(orderId)
        # print("setting nextValidOrderId: %d", orderId)
        # self.nextValidOrderId = orderId
        # print("NextValidId:", orderId)
        self.nextValidIdQueue.put(orderId)

    def openOrder(
            self,
            orderId: OrderId,
            contract: Contract,
            order: IbOrder,
            orderState: OrderState):
        # super().openOrder(orderId, contract, order, orderState)
        # print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId,
        #     "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
        #     "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
        #     "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty,
        #     "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status
        # )

        # get external order id
        self.gateway.ib_orderids.put(order.orderId, order.permId)

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
        # super().orderStatus(
        #     orderId, status, filled, remaining,
        #     avgFillPrice, permId, parentId,
        #     lastFillPrice, clientId, whyHeld, mktCapPrice
        # )
        # print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        #     "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        #     "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        #     lastFillPrice, "ClientId:", clientId, "WhyHeld:",
        #     whyHeld, "MktCapPrice:", mktCapPrice
        # )

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
        # super().execDetails(reqId, contract, execution)
        # print("ExecDetails. ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType,
        #       "Currency:", contract.currency, execution
        # )

        # get external deal id
        self.gateway.ib_dealids.put(execution.orderId, execution.execId)

        deal_status = dict(
            reqId=reqId,
            contract=contract,
            execution=execution
        )
        self.gateway.process_deal(deal_status)

    def commissionReport(self, commissionReport: CommissionReport):
        # super().commissionReport(commissionReport)
        # print("CommissionReport.", commissionReport)
        self.gateway.ib_commissions.put(
            commissionReport.execId,
            commissionReport.commission)


class IbGateway(BaseGateway):

    # Minimal time step, which was read from config
    TIME_STEP = TIME_STEP

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
            fees: BaseFees = BaseFees
    ):
        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end

        # key: Security, value: quque
        self.ib_contractdetails_done = {
            s: DefaultQueue(maxsize=1) for s in securities}
        # key:Security, value:IB.Contract
        self.ib_contractdetails = {s: None for s in securities}
        # key:Security, value:int (reqId)
        self.ib_contractdetails_reqid = {s: None for s in securities}

        # RealtimeBars subscription only supports 5 seconds bar (we use them to
        # aggregate to 1 minute bars)
        self.ib_consolidated_bars_done = {
            s: DefaultQueue(maxsize=1) for s in securities}
        # key:Security, value:List[Bar] (store up to 12, i.e., 1 min bar)
        self.ib_5s_bars = {s: list() for s in securities}
        # key:Security, value:int (reqId)
        self.ib_5s_bars_reqid = {s: None for s in securities}
        self.ib_5s_bars_max_no = 12
        # to be consumed in get_recent_bar
        self.ib_consolidated_1m_bar = {s: None for s in securities}

        self.time_step_in_mins = round(TIME_STEP / (60. * 1000))
        bar_req = [f"{self.time_step_in_mins}min", "1day"]
        self.ib_bars_done = {f: {s: DefaultQueue(
            maxsize=1) for s in self.securities} for f in bar_req}
        self.ib_bars = {f: {s: list() for s in self.securities}
                        for f in bar_req}
        self.ib_bars_reqid = {
            f: {s: None for s in self.securities} for f in bar_req}
        self.ib_bars_num = {f: {s: None for s in self.securities}
                            for f in bar_req}

        self.ib_quotes_done = {s: DefaultQueue(maxsize=1) for s in securities}
        # key:Security, value:Quote
        self.ib_quotes = {s: None for s in securities}
        # key:Security, value:int (reqId)
        self.ib_quotes_reqid = {s: None for s in securities}

        self.ib_orderbooks_done = {
            s: DefaultQueue(
                maxsize=1) for s in securities}
        # key:Security, value:Quote
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
        for security in self.securities:
            if self.ib_5s_bars_reqid[security]:
                self.api.cancelRealTimeBars(
                    reqId=self.ib_5s_bars_reqid[security])
            if self.ib_quotes_reqid[security]:
                self.api.cancelMktData(reqId=self.ib_quotes_reqid[security])

            for bar_interval in self.ib_bars_reqid:
                if self.ib_bars_reqid[bar_interval][security]:
                    self.api.cancelHistoricalData(
                        reqId=self.ib_bars_reqid[bar_interval][security])

            # if self.ib_bars_reqid["1min"][security]:
            #     self.api.cancelHistoricalData(
            #         reqId=self.ib_bars_reqid["1min"][security])
            # if self.ib_bars_reqid["1day"][security]:
            #     self.api.cancelHistoricalData(
            #         reqId=self.ib_bars_reqid["1day"][security])

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
        orderid = content.get("permId")
        order = self.orders.get(orderid, timeout=5)  # blocking
        if order is None:
            raise TimeoutError(
                f"Failed to get order from orderid {orderid} within the time limit.")
        order.updated_time = datetime.now()
        order.filled_avg_price = content.get("avgFillPrice")
        order.filled_quantity = content.get("filled")
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
        dealid = execution.execId
        assert self.ib_dealids.get(
            order_reqId) == execution.execId, "execId does not match in self.ib_dealids!"
        orderid = self.ib_orderids.get(order_reqId)
        order = self.orders.get(orderid, timeout=5)  # blocking
        if order is None:
            raise TimeoutError(
                f"Failed to get order from orderid {orderid} within the time limit.")
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=try_parsing_datetime(execution.time),
            filled_avg_price=execution.avgPrice,
            filled_quantity=execution.cumQty,
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

    def req_bar(
            self,
            ib_contract: Contract,
            security: Security,
            bar_interval: str,
            bar_num: int,
            update: bool = False
    ) -> int:
        queryTime = "" if update else datetime.now().strftime("%Y%m%d %H:%M:%S")
        if "min" in bar_interval:
            durationStr = f"{bar_num * 60} S"
            self.api.reqId += 1
            self.ib_bars_reqid[bar_interval][security] = self.api.reqId
            self.ib_bars_num[bar_interval][security] = bar_num
            # request historical data
            self.api.reqHistoricalData(
                reqId=self.api.reqId,
                contract=ib_contract,
                endDateTime=queryTime,
                durationStr=durationStr,
                barSizeSetting=f"{bar_interval.replace('min', '')} min",
                whatToShow=get_what_to_show(security),
                useRTH=0,
                formatDate=1,
                keepUpToDate=update,  # if True, endDateTime can not be specified
                chartOptions=[])
            return self.api.reqId
        elif "day" in bar_interval:
            durationStr = f"{bar_num} D"
            self.api.reqId += 1
            self.ib_bars_reqid[bar_interval][security] = self.api.reqId
            self.ib_bars_num[bar_interval][security] = bar_num
            # request historical data
            self.api.reqHistoricalData(
                reqId=self.api.reqId,
                contract=ib_contract,
                endDateTime=queryTime,
                durationStr=durationStr,
                barSizeSetting=f"{bar_interval.replace('day', '')} day",
                whatToShow=get_what_to_show(security),
                useRTH=0,
                formatDate=1,
                keepUpToDate=update,  # if True, endDateTime can not be specified
                chartOptions=[])
            return self.api.reqId
        else:
            raise ValueError(
                "bar_interval can only be '{int}min' or '{int}day', but "
                f"'{bar_interval}' was passed in.")

    def subscribe(self):
        # "REALTIME", "FROZEN", "DELAYED", "DELAYED_FROZEN"
        self.api.reqMarketDataType(MarketDataTypeEnum.REALTIME)
        for security in self.securities:
            if self.ib_contractdetails[security] is None:
                # construct vague contract
                ib_contract = generate_ib_contract(security)
                # Always remember reqId before request contracts
                self.api.reqId += 1
                self.ib_contractdetails_reqid[security] = self.api.reqId
                self.api.reqContractDetails(
                    reqId=self.api.reqId, contract=ib_contract)
            # blocking here: get accurate contract from IB (at most wait for 5
            # seconds)
            reqId = self.ib_contractdetails_done[security].get(timeout=5)
            if reqId is None:
                try:
                    with open(f".qtrader_cache/ib/contractdetails/{security.code}", "rb") as f:
                        contractdetails = pickle.load(f)
                        self.ib_contractdetails[security] = contractdetails
                        reqId = self.api.reqId
                except BaseException:
                    raise FileNotFoundError(
                        "Failed to get contract details within the time limit, "
                        "and no .qtrader_cache either.")
            if reqId == self.api.reqId:
                # pickle contractdetails for later use to avoid requesting too
                # frequently
                Path(
                    ".qtrader_cache/ib/contractdetails").mkdir(parents=True, exist_ok=True)
                with open(f".qtrader_cache/ib/contractdetails/{security.code}", "wb") as f:
                    contractdetails = self.ib_contractdetails.get(security)
                    pickle.dump(contractdetails, f)

                ib_contract = self.get_ib_contract_from_security(security)
                # request market data (quotes and orderbook)
                self.api.reqId += 1
                self.ib_quotes_reqid[security] = self.api.reqId
                self.api.reqMktData(
                    self.api.reqId, ib_contract, "", False, False, [])
                print(f"Subscribed market data (quote and orderbook) of {security}")

                # # request bar data
                # bar_interval = f"{self.time_step_in_mins}min"
                # reqId = self.req_bar(
                #     ib_contract,
                #     security,
                #     bar_interval,
                #     NUM_BARS,
                #     False
                # )
                # # Blocking here to wait for the bars request done
                # bars_done_reqId = self.ib_bars_done[bar_interval][security].get(
                #     timeout=30)
                # if bars_done_reqId is None:
                #     raise TimeoutError("Failed to get bars within the time limit.")
                # if bars_done_reqId != reqId:
                #     raise ValueError(f'reqId mismatch: {bars_done_reqId} != {reqId}')
                # print(f"Subscribed bar data of {security}")

            else:
                raise ValueError(
                    f"reqId mismatch: {reqId} != {self.api.reqId}")

    def unsubscribe(self):
        pass

    def get_recent_bars(
            self,
            security: Security,
            bar_interval: str
    ) -> List[Bar]:
        """Get recent historical OHLCV"""

        # num_bars = self.ib_bars_num[bar_interval][security]
        # recent_bars = self.ib_bars[bar_interval][security][:num_bars]
        # return recent_bars

        reqId = self.ib_bars_reqid[bar_interval][security]
        if reqId is None:
            ib_contract = self.get_ib_contract_from_security(security)
            reqId = self.req_bar(
                ib_contract,
                security,
                bar_interval,
                NUM_BARS,
                False
            )
        # Blocking here to wait for the bars request done
        bars_done_reqId = self.ib_bars_done[bar_interval][security].get(
            timeout=30)
        if bars_done_reqId is None:
            raise TimeoutError("Failed to get bars within the time limit.")
        if bars_done_reqId == reqId:
            num_bars = self.ib_bars_num[bar_interval][security]
            recent_bars = self.ib_bars[bar_interval][security][:num_bars]

            # Cancel request immediately
            # Unlike get_recent_bar, this function (get_recent_bars) will send
            # request to IB each time. So every time we have received the data,
            # we must reset all related params, so that next time we can make
            # a clean request
            self.api.cancelHistoricalData(reqId=reqId)
            self.ib_bars_reqid[bar_interval][security] = None
            self.ib_bars_num[bar_interval][security] = None

            # return the requested data
            return recent_bars
        else:
            raise ValueError(f"reqId mismatch: {bars_done_reqId} != {reqId}")

    def get_recent_bar(
            self,
            security: Security,
    ) -> Bar:
        """Get most recent OHLCV
        """
        # Note (joseph 2022-01-13):
        #
        # reqHistoricalData can get streaming bar data (by setting keepUpToDate=True),
        # [see https://interactivebrokers.github.io/tws-api/historical_bars.html]
        # But if we do so, reqAccountUpdates will fail to receive data. Therefore
        # we still use reqRealTimeBars to fetch 5sec bars, and then aggregate 12
        # bars to a 1min bar.
        #
        # Below is the code that initially intend for receiving realtime 1min bar
        # via reqHistoricalData:

        bar_interval = f"{self.time_step_in_mins}min"
        bars = self.get_recent_bars(security, bar_interval)
        assert (
            self.ib_bars_done[bar_interval][security].get()
            == self.ib_bars_reqid[bar_interval][security]), (
            "reqId does not match in ib bars!")
        return bars[-1]

        # reqId = self.ib_5s_bars_reqid[security]
        # if reqId is None:
        #     # request 5sec bar data (to be aggregated to 1min bars)
        #     # no more than 60 *new* requests for real time bars can be made in
        #     # 10 minutes
        #     # Ref:
        #     # https://interactivebrokers.github.io/tws-api/realtime_bars.html
        #     ib_contract = self.get_ib_contract_from_security(security)
        #     self.api.reqId += 1
        #     self.ib_5s_bars_reqid[security] = self.api.reqId
        #     self.api.reqRealTimeBars(
        #         self.api.reqId,
        #         ib_contract,
        #         5,
        #         get_what_to_show(security),
        #         False,
        #         []
        #     )
        #     reqId = self.api.reqId
        # # Blocking here, until 1 min bar is available by aggregating all 5s bars
        # # If 1min bar is not available after 120 seconds, raise error
        # print("Aggregating 5sec bars to make 1min bar ...")
        # consolidated_bars_done_reqId = self.ib_consolidated_bars_done[security].get(
        #     timeout=120)
        # if consolidated_bars_done_reqId is None:
        #     raise TimeoutError(
        #         "Failed to get consolidated bars within the time limit.")
        # if consolidated_bars_done_reqId == reqId:
        #     consolidated_bar = self.ib_consolidated_1m_bar[security]
        #     # bar has been consumed, reset to None
        #     self.ib_consolidated_1m_bar[security] = None
        #     return consolidated_bar
        # else:
        #     raise ValueError(
        #         f"reqId mismatch: {consolidated_bars_done_reqId} != {reqId}")

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

        # invoke next valid id
        if self.api.nextValidIdQueue.qsize() == 0:
            self.api.reqIds(-1)
        order_reqId = self.api.nextValidIdQueue.get()

        self.api.placeOrder(order_reqId, contract, ib_order)
        # blocking here to obtain order id (suppose to be very fast here)
        orderid = self.ib_orderids.get(order_reqId, timeout=5, default_item="")
        if orderid == "":
            raise ConnectionError(
                f"Failed to get orderid. {order_reqId}: {contract}: {ib_order}")
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
        balance = self.ib_accounts.get(IB["broker_account"])
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
        positions = self.ib_positions.get(IB["broker_account"])
        # Close request
        self.api.reqAccountUpdates(False, IB["broker_account"])
        return positions

    def get_quote(self, security: Stock) -> Quote:
        """Quote"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """Orderbook"""
        return self.orderbook.get(security)

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

    def get_security_from_ib_5s_bars_reqid(self, reqId: int) -> Security:
        for security in self.ib_5s_bars_reqid:
            if self.ib_5s_bars_reqid[security] == reqId:
                return security

    def get_security_from_ib_bars_reqid(self, reqId: int) -> Security:
        for bar_interval in self.ib_bars_reqid:
            for security in self.ib_bars_reqid[bar_interval]:
                if self.ib_bars_reqid[bar_interval][security] == reqId:
                    return security

    def get_bar_interval_from_ib_bars_reqid(self, reqId: int) -> str:
        for bar_interval in self.ib_bars_reqid:
            for security in self.ib_bars_reqid[bar_interval]:
                if self.ib_bars_reqid[bar_interval][security] == reqId:
                    return bar_interval

    def get_security_from_ib_quotes_reqid(self, reqId: int) -> Security:
        for security in self.ib_quotes_reqid:
            if self.ib_quotes_reqid[security] == reqId:
                return security

    def get_order_reqId_from_orderid(self, orderid: str) -> int:
        for reqId in self.ib_orderids:
            if self.ib_orderids.get(reqId) == orderid:
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
        if "Min" in freq:
            return _req_historical_bars_ib_min(
                security=security,
                periods=periods,
                gateway=self
            )
        elif "Day" in freq:
            return _req_historical_bars_ib_day(
                security=security,
                periods=periods,
                gateway=self
            )

        # freq is not valid
        FREQ_ALLOWED = ("1Day", "1Min")
        raise ValueError(
            f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} "
            "are allowed.")


def _req_historical_bars_ib_min(
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


def _req_historical_bars_ib_day(
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


# def get_default_bar_num_from_bar_interval(bar_interval: str) -> int:
#     if bar_interval == "1min":
#         bar_num = 60 * 8
#     elif bar_interval == "1day":
#         bar_num = 60
#     else:
#         raise ValueError(f"bar_interval = {bar_interval} is not supported!")
#     return bar_num


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
