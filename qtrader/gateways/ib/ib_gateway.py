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
import time
import queue
from dataclasses import replace
from datetime import datetime
from typing import Dict, List, Union, Any
from threading import Thread, Event

import pandas as pd
from ibapi.account_summary_tags import AccountSummaryTags

from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
from ibapi.common import OrderId, TickAttrib, TickerId, MarketDataTypeEnum
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order as IbOrder
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.common import BarData as IbBarData

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode, Exchange, OrderType
from qtrader.core.constants import OrderStatus as QTOrderStatus
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import Position, PositionData
from qtrader.core.security import Stock, Security, Currency, Futures
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.utility import Time, try_parsing_datetime, BlockingDict
from qtrader.config import GATEWAYS, DATA_PATH
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees

IB = GATEWAYS.get("Ib")


class IbWrapper(EWrapper):
    pass


class IbClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class IbAPI(IbWrapper, IbClient):
    def __init__(self, gateway:BaseGateway):
        IbWrapper.__init__(self)
        IbClient.__init__(self, wrapper=self)
        self.gateway = gateway
        self.reqId = 0
        self.nextValidIdQueue = queue.Queue(maxsize=1)
        self.connect(IB["host"], IB["port"], IB["clientid"])
        # EReader Thread
        self.thread = Thread(target=self.run)
        self.thread.start()

    def close(self):
        self.disconnect()

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        # super().contractDetails(reqId, contractDetails)
        contract = contractDetails.contract
        # Attached contracts to IB gateway
        for security in self.gateway.securities:
            if self.gateway.ib_contractdetails[security] is not None:
                continue
            if (get_ib_symbol(security)==contract.symbol and
                get_ib_security_type(security)==contract.secType and
                get_ib_exchange(security)==contract.exchange and
                get_ib_currency(security)==contract.currency
            ):
                self.gateway.ib_contractdetails[security] = contractDetails
                break

    def contractDetailsEnd(self, reqId: int):
        # super().contractDetailsEnd(reqId)
        # print("ContractDetailsEnd. ReqId:", reqId)
        security = self.gateway.get_security_from_ib_contractdetails_reqid(reqId)
        # Notify threads that are waiting for ib_contractdetails_done
        self.gateway.ib_contractdetails_done[security].put(reqId)

    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float,
            volume: int, wap: float, count: int
        ):
        # super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        # print("RealTimeBar. TickerId:", reqId, RealTimeBar(time, -1, open_, high, low, close, volume, wap, count))
        security = self.gateway.get_security_from_ib_5s_bars_reqid(reqId)
        bar_time = datetime.fromtimestamp(time)
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume
        )

        if security in self.gateway.ib_5s_bars and len(self.gateway.ib_5s_bars[security])==self.gateway.ib_5s_bars_max_no:
            self.gateway.ib_5s_bars[security].pop(0)
        self.gateway.ib_5s_bars[security].append(bar)
        assert len(self.gateway.ib_5s_bars[security])<=self.gateway.ib_5s_bars_max_no, (
            f"Error: There can not be more than {self.gateway.ib_5s_bars_max_no} bars!"
        )

        # Notify threads that are waiting for ib_consolidated_bars_done
        if (len(self.gateway.ib_5s_bars[security])==self.gateway.ib_5s_bars_max_no and
            self.gateway.ib_consolidated_bars_done[security].qsize()==0
        ):
            self.gateway.ib_consolidated_bars_done[security].put(reqId)
        elif (len(self.gateway.ib_5s_bars[security])<self.gateway.ib_5s_bars_max_no and
            self.gateway.ib_consolidated_bars_done[security].qsize()==1
        ):
            self.gateway.ib_consolidated_bars_done[security].get()

    def tickPrice(self, reqId:TickerId, tickType:TickType, price:float, attrib:TickAttrib):
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
                datetime=datetime.now()
            )
        else:
            quote = self.gateway.ib_quotes[security]
        if tickType==TickTypeEnum.LAST:
            quote = replace(quote, last_price=price)
        elif tickType==TickTypeEnum.BID:
            quote = replace(quote, bid_price=price)
        elif tickType==TickTypeEnum.ASK:
            quote = replace(quote, ask_price=price)
        self.gateway.ib_quotes[security] = quote
        if quote.last_price!=0 or (quote.bid_price!=0 and quote.ask_price!=0):
            if self.gateway.ib_quotes_done[security].qsize()==1:
                self.gateway.ib_quotes_done[security].get()
            self.gateway.ib_quotes_done[security].put(reqId)
            self.gateway.process_quote(quote)

        # process orderbook
        if self.gateway.ib_orderbooks[security] is None:
            orderbook = OrderBook(
                security=security,
                exchange=security.exchange,
                datetime=datetime.now()
            )
        else:
            orderbook = self.gateway.ib_orderbooks[security]
        if tickType==TickTypeEnum.BID:
            orderbook = replace(orderbook, bid_price_1=price)
        elif tickType==TickTypeEnum.ASK:
            orderbook = replace(orderbook, ask_price_1=price)
        self.gateway.ib_orderbooks[security] = orderbook
        if orderbook.bid_price_1!=0 and orderbook.ask_price_1!=0 and orderbook.bid_volume_1!=0 and orderbook.ask_volume_1!=0:
            if self.gateway.ib_orderbooks_done[security].qsize()==1:
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
                datetime=datetime.now()
            )
        else:
            orderbook = self.gateway.ib_orderbooks[security]
        if tickType==TickTypeEnum.BID_SIZE:
            orderbook = replace(orderbook, bid_volume_1=size)
        elif tickType==TickTypeEnum.ASK_SIZE:
            orderbook = replace(orderbook, ask_volume_1=size)
        else:
            print("TickSize. TickerId:", reqId, "TickType:", tickType, "Size:", size)
        self.gateway.ib_orderbooks[security] = orderbook
        if orderbook.bid_price_1!=0 and orderbook.ask_price_1!=0 and orderbook.bid_volume_1!=0 and orderbook.ask_volume_1!=0:
            if self.gateway.ib_orderbooks_done[security].qsize()==1:
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

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        # super().updateAccountValue(key, val, currency, accountName)
        # print("UpdateAccountValue. Key:", key, "Value:", val, "Currency:", currency, "AccountName:", accountName)
        if accountName!=IB["broker_account"]:
            return
        account = self.gateway.balance
        if key=="CashBalance" and currency=="BASE":
            account.cash = float(val)
        elif key=="CashBalance":
            account.cash_by_currency[currency] = float(val)
        elif key=="AvailableFunds":
            account.available_cash = float(val)
        elif key=="BuyingPower":
            account.net_cash_power = float(val)
        elif key=="MaintMarginReq":
            account.maintenance_margin = float(val)
        elif key=="UnrealizedPnL":
            account.unrealized_pnl = float(val)
        elif key=="RealizedPnL":
            account.realized_pnl = float(val)

    def updatePortfolio(self, contract: Contract, position: float, marketPrice: float, marketValue: float,
            averageCost: float, unrealizedPNL: float, realizedPNL: float, accountName: str
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
                update_time=datetime.now(),
            )
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
            self.gateway.ib_accounts.put(IB["broker_account"], self.gateway.balance)
        if hasattr(self.gateway, "positions"):
            self.gateway.ib_positions.put(IB["broker_account"], self.gateway.positions)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
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
            raise ConnectionError(f"ErrorCode:{errorCode} ErrorMsg:{errorString}")

    def nextValidId(self, orderId: int):
        # super().nextValidId(orderId)
        # print("setting nextValidOrderId: %d", orderId)
        # self.nextValidOrderId = orderId
        # print("NextValidId:", orderId)
        self.nextValidIdQueue.put(orderId)

    def openOrder(self, orderId: OrderId, contract: Contract, order: IbOrder, orderState: OrderState):
        # super().openOrder(orderId, contract, order, orderState)
        # print("OpenOrder. PermId: ", order.permId, "ClientId:", order.clientId, " OrderId:", orderId,
        #     "Account:", order.account, "Symbol:", contract.symbol, "SecType:", contract.secType,
        #     "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
        #     "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty,
        #     "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status
        # )

        # get external order id
        self.gateway.ib_orderids.put(order.orderId, order.permId)

    def orderStatus(self, orderId: OrderId, status: str, filled: float,
            remaining: float, avgFillPrice: float, permId: int,
            parentId: int, lastFillPrice: float, clientId: int,
            whyHeld: str, mktCapPrice: float
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

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
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
        self.gateway.ib_commissions.put(commissionReport.execId, commissionReport.commission)


class IbGateway(BaseGateway):

    # 定义交易时间 (港股)
    TRADING_HOURS_AM = [Time(0,0,0), Time(12,0,0)]
    TRADING_HOURS_PM = [Time(12,0,0), Time(23,59,59)]

    # 定义最小时间单位 (秒)
    TIME_STEP = 60

    # 参数设定
    SHORT_INTEREST_RATE = 0.0098  # 融券利息

    # 名字
    NAME = "IB"

    def __init__(self,
            securities:List[Stock],
            gateway_name:str,
            start:datetime=None,
            end:datetime=None,
            fees:BaseFees=BaseFees,
        ):

        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        self.trade_mode = None

        self.ib_contractdetails_done = {s:queue.Queue(maxsize=1) for s in securities}
        self.ib_contractdetails = {s:None for s in securities}               # key:Security, value:IB.Contract
        self.ib_contractdetails_reqid = {s: None for s in securities}        # key:Security, value:int (reqId)

        self.ib_consolidated_bars_done = {s:queue.Queue(maxsize=1) for s in securities}
        self.ib_5s_bars = {s:list() for s in securities}               # key:Security, value:List[Bar] (store up to 12, i.e., 1 min bar)
        self.ib_5s_bars_reqid = {s:None for s in securities}           # key:Security, value:int (reqId)
        self.ib_5s_bars_max_no = 12

        self.ib_quotes_done = {s:queue.Queue(maxsize=1) for s in securities}
        self.ib_quotes = {s:None for s in securities}                  # key:Security, value:Quote
        self.ib_quotes_reqid = {s:None for s in securities}            # key:Security, value:int (reqId)

        self.ib_orderbooks_done = {s:queue.Queue(maxsize=1) for s in securities}
        self.ib_orderbooks = {s:None for s in securities}              # key:Security, value:Quote
        self.ib_orderbooks_reqid = {s:None for s in securities}        # key:Security, value:int (reqId)

        self.ib_accounts = BlockingDict()
        self.ib_positions = BlockingDict()

        self.ib_orderids = BlockingDict()  # key:reqId, value:IbOrder.permId
        self.ib_dealids = BlockingDict()   # key:reqId, value:Execution.execId

        self.ib_commissions = BlockingDict() # Key:Execution.execId, value:float

        self.api = IbAPI(self)
        self.connect_quote()
        self.subscribe()
        self.connect_trade()

    def close(self):
        self.api.disconnect()

    def connect_quote(self):
        """
        行情需要处理报价和订单簿（在tickPrice和tickSize中进行回调处理）
        """
        print("行情接口连接成功")

    def connect_trade(self):
        """
        交易需要处理订单和成交（在tickPrice和tickSize中进行回调处理）
        """
        print("交易接口连接成功")

    def process_quote(self, quote:Quote):
        """更新报价的状态"""
        security = quote.security
        self.quote.put(security, quote)

    def process_orderbook(self, orderbook:OrderBook):
        """更新订单簿的状态"""
        security = orderbook.security
        self.orderbook.put(security, orderbook)

    def process_order(self, content: Dict[str, Any]):
        """更新订单的状态
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
        print("process_order ", content.get("status"))
        orderid = content.get("permId")
        order = self.orders.get(orderid)  # blocking
        order.updated_time = datetime.now()
        order.filled_avg_price = content.get("avgFillPrice")
        order.filled_quantity = content.get("filled")
        order.status = convert_orderstatus_ib2qt(content.get("status"))
        self.orders.put(orderid, order)

    def process_deal(self, content: Dict[str,Any]):
        """更新成交的信息
        content = deal_status = dict(
            reqId=reqId,
            contract=contract,
            execution=execution
        )
        """
        execution = content.get("execution")
        order_reqId = execution.orderId
        dealid = execution.execId
        assert self.ib_dealids.get(order_reqId)==execution.execId, "execId does not match in self.ib_dealids!"
        orderid = self.ib_orderids.get(order_reqId)
        order = self.orders.get(orderid)  # blocking
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
            raise ValueError(f"IbGateway only supports `SIMULATE` or `LIVETRADE` mode, {trade_mode} was passed in instead.")
        self.trade_mode = trade_mode

    def subscribe(self):
        self.api.reqMarketDataType(MarketDataTypeEnum.REALTIME) # "REALTIME", "FROZEN", "DELAYED", "DELAYED_FROZEN"
        for security in self.securities:
            if self.ib_contractdetails[security] is None:
                # construct vague contract
                ib_contract = generate_ib_contract(security)
                # Always remember reqId before request contracts
                self.api.reqId += 1
                self.ib_contractdetails_reqid[security] = self.api.reqId
                self.api.reqContractDetails(reqId=self.api.reqId, contract=ib_contract)
            # blocking here: get accurate contract from IB
            if self.ib_contractdetails_done[security].get()==self.api.reqId:
                ib_contract = self.get_ib_contract_from_security(security)

                # request bar data
                # no more than 60 *new* requests for real time bars can be made in 10 minutes
                # Ref: https://interactivebrokers.github.io/tws-api/realtime_bars.html
                self.api.reqId += 1
                self.ib_5s_bars_reqid[security] = self.api.reqId
                if isinstance(security, Currency):
                    what_to_show = "MIDPOINT"
                else:
                    what_to_show = "TRADES"
                self.api.reqRealTimeBars(self.api.reqId, ib_contract, 5, what_to_show, False, []) # MIDPOINT/TRADES/BID/ASK

                # request market data (quotes and orderbook)
                self.api.reqId += 1
                self.ib_quotes_reqid[security] = self.api.reqId
                self.api.reqMktData(self.api.reqId, ib_contract, "", False, False, [])
            print(f"Subscribed {security}")

    def is_trading_time(self, cur_datetime: datetime) -> bool:
        """
        判断当前时间是否属于交易时间段

        :param cur_datetime:
        :return:
        """
        # TODO: 先判断是否交易日
        cur_time = Time(hour=cur_datetime.hour, minute=cur_datetime.minute, second=cur_datetime.second)
        return (self.TRADING_HOURS_AM[0] <= cur_time <= self.TRADING_HOURS_AM[1]) or (
                    self.TRADING_HOURS_PM[0] <= cur_time <= self.TRADING_HOURS_PM[1])

    def get_recent_bar(self, security: Security) -> Bar:
        """
        获取最接近当前时间的数据点
,
        :param security:
        :param cur_time:
        :return:
        """

        print("Waiting for 1 min bar ...")
        reqId = self.ib_5s_bars_reqid[security]
        # Blocking here, until 1 min bar is available by aggregating all 5s bars
        if self.ib_consolidated_bars_done[security].get()==reqId:
            bars = self.ib_5s_bars[security][:]
            if not validate_bar_interval(bars, 1):
                print(f"获取最近bar数据失败：{bars}")
                return
            consolidated_bar = Bar(
                datetime=get_time_key(bars),
                security=security,
                open=get_open(bars),
                high=get_high(bars),
                low=get_low(bars),
                close=get_close(bars),
                volume=get_volume(bars)
            )
            return consolidated_bar

    def get_recent_capital_distribution(self, security: Stock) -> CapitalDistribution:
        """capital distribution"""
        raise NotImplementedError("get_recent_capital_distribution method is not yet implemented in IB gateway!")

    def get_recent_data(self, security: Security, **kwargs) -> Dict[str, Union[Bar, CapitalDistribution]] or Union[
        Bar, CapitalDistribution]:
        """
        获取最接近当前时间的数据点
,
        :param security:
        :param cur_time:
        :return:
        """
        if kwargs:
            assert "dfield" in kwargs, f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
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

    def get_stock(self, code: str) -> Stock:
        """根据股票代号，找到对应的股票"""
        for stock in self.securities:
            if stock.code == code:
                return stock
        return None

    def place_order(self, order: Order) -> str:
        """提交订单"""
        # Obtain Contract
        contract_details = self.ib_contractdetails.get(order.security)
        contract = contract_details.contract
        min_tick = contract_details.minTick

        # Generate Order
        ib_order = IbOrder()
        ib_order.action = order_direction_qt2ib(order.direction)
        ib_order.orderType = order_type_qt2ib(order.order_type)
        ib_order.totalQuantity = order.quantity
        if order.order_type==OrderType.LIMIT:
            # adjust price precision
            if order.direction==Direction.LONG:
                limit_price = (order.price // min_tick) * min_tick
            elif order.direction==Direction.SHORT:
                limit_price = (order.price // min_tick) * min_tick + min_tick
            ib_order.lmtPrice = limit_price

        # invoke next valid id
        if self.api.nextValidIdQueue.qsize()==0:
            self.api.reqIds(-1)
        order_reqId = self.api.nextValidIdQueue.get()

        self.api.placeOrder(order_reqId , contract, ib_order)
        # blocking here to obtain order id (suppose to be very fast here)
        orderid = self.ib_orderids.get(order_reqId)
        order.status = QTOrderStatus.SUBMITTED  # 修改状态为已提交
        self.orders.put(orderid, order)         # 稍后通过callback更新order状态
        return orderid

    def cancel_order(self, orderid):
        """取消订单"""
        order_reqId = self.get_order_reqId_from_orderid(orderid)
        if order_reqId is None:
            print(f"撤单失败：{data}")
            return
        self.api.cancelOrder(order_reqId)


    def get_broker_balance(self) -> AccountBalance:
        """获取券商资金"""
        # 先初始化一个空的account balance
        self.balance = AccountBalance(cash_by_currency={})
        # 获取account updates
        self.api.reqAccountUpdates(True, IB["broker_account"])
        # 等待获取结果，然后关闭request
        balance = self.ib_accounts.get(IB["broker_account"])
        self.api.reqAccountUpdates(False, IB["broker_account"])
        return balance

    def get_broker_position(self, security: Stock, direction: Direction) -> PositionData:
        """获取券商持仓"""
        positions = self.get_all_broker_positions()
        for position_data in positions:
            if position_data.security == security and position_data.direction == direction:
                return position_data
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """获取券商所有持仓"""
        # 先初始化一个空的positions
        self.positions = []
        # 获取account updates
        self.api.reqAccountUpdates(True, IB["broker_account"])
        # 等待获取结果，然后关闭request
        positions = self.ib_positions.get(IB["broker_account"])
        self.api.reqAccountUpdates(False, IB["broker_account"])
        return positions

    def get_quote(self, security: Stock) -> Quote:
        """获取报价"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """获取订单簿"""
        return self.orderbook.get(security)

    def get_ib_contract_from_security(self, security:Security):
        # blocking here
        contract_details = self.ib_contractdetails.get(security)
        return contract_details.contract

    def get_security_from_ib_contract(self, contract:Contract):
        for security in self.ib_contractdetails:
            ib_contract = self.get_ib_contract_from_security(security)
            if ib_contract.conId==contract.conId:
                return security

    def get_security_from_ib_contractdetails_reqid(self, reqId:int):
        for security in self.ib_contractdetails_reqid:
            if self.ib_contractdetails_reqid[security]==reqId:
                return security

    def get_security_from_ib_5s_bars_reqid(self, reqId:int):
        for security in self.ib_5s_bars_reqid:
            if self.ib_5s_bars_reqid[security]==reqId:
                return security

    def get_security_from_ib_quotes_reqid(self, reqId:int):
        for security in self.ib_quotes_reqid:
            if self.ib_quotes_reqid[security]==reqId:
                return security

    def get_order_reqId_from_orderid(self, orderid:str):
        for reqId in self.ib_orderids:
            if self.ib_orderids.get(reqId)==orderid:
                return reqId


def get_ib_security_type(security:Security):
    if isinstance(security, Stock):
        return "STK"
    elif isinstance(security, Currency):
        return "CASH"
    elif isinstance(security, Futures):
        return "FUT"
    else:
        raise ValueError(f"Type {security} not supported in IB yet!")

def get_ib_currency(security:Security):
    if security.exchange==Exchange.SEHK:
        return "HKD"
    elif security.exchange==Exchange.IDEALPRO:
        base_currency, quote_currency = security.code.split(".")
        return quote_currency
    elif security.exchange==Exchange.NYMEX:
        return "USD"
    elif security.exchange==Exchange.SMART:
        return "USD"
    else:
        raise ValueError(f"Currency of {security} not specified yet!")

def get_ib_exchange(security:Security):
    if security.exchange==Exchange.SEHK:
        return "SEHK"
    elif security.exchange==Exchange.IDEALPRO:
        return "IDEALPRO"
    elif security.exchange==Exchange.SMART:
        return "SMART"
    elif security.exchange==Exchange.NYMEX:
        return "NYMEX"
    else:
        raise ValueError(f"Exchange of {security} not supported in IB yet!")

def get_ib_symbol(security:Security):
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

def generate_ib_contract(security:Security):
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

def order_direction_qt2ib(direction:Direction):
    if direction==Direction.LONG:
        return "BUY"
    elif direction==Direction.SHORT:
        return "SELL"
    else:
        raise ValueError(f"Direction {direction} is not supported in IB!")

def order_type_qt2ib(order_type:OrderType):
    if order_type==OrderType.MARKET:
        return "MKT"
    elif order_type==OrderType.LIMIT:
        return "LMT"
    elif order_type==OrderType.STOP:
        return "STP"
    else:
        raise ValueError(f"OrderType {order_type} is not supported in IB!")

def convert_orderstatus_ib2qt(status:str)->QTOrderStatus:
    """状态转换"""
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
        raise ValueError(f"订单状态{status}不在程序处理范围内")


def validate_bar_interval(bars:List[Bar], bar_interval:int):
    """Validate the aggregated bar interval is as expected, bar_interval is measured with minutes"""
    time_key1 = bars[0].datetime
    time_key2 = bars[-1].datetime
    time_key_diff = (time_key2 - time_key1).total_seconds()
    return time_key_diff==bar_interval * 60 - 5

def get_time_key(bars:List[Bar]):
    return bars[-1].datetime

def get_open(bars:List[Bar]):
    return bars[0].open

def get_high(bars:List[Bar]):
    high = -float("Inf")
    for bar in bars:
        if bar.high>high:
            high = bar.high
    return high

def get_low(bars:List[Bar]):
    low = float("Inf")
    for bar in bars:
        if bar.low<low:
            low = bar.low
    return low

def get_close(bars:List[Bar]):
    return bars[-1].close

def get_volume(bars:List[Bar]):
    volume = 0
    for bar in bars:
        volume += bar.volume
    return volume





