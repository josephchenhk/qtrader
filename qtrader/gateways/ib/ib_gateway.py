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
from typing import Dict, List, Union
from threading import Thread, Event

import pandas as pd
from ibapi.account_summary_tags import AccountSummaryTags

from ibapi.client import EClient
from ibapi.common import OrderId, TickAttrib, TickerId, MarketDataTypeEnum
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.common import BarData as IbBarData

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode, Exchange
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
        self.connect(IB["host"], IB["port"], IB["clientid"])
        # EReader Thread
        self.thread = Thread(target=self.run)
        self.thread.start()

    def close(self):
        self.disconnect()

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        super().contractDetails(reqId, contractDetails)
        # print(contractDetails)
        contract = contractDetails.contract
        # Attached contracts to IB gateway
        for security in self.gateway.securities:
            if self.gateway.ib_contracts[security] is not None:
                continue
            if (get_ib_symbol(security)==contract.symbol and
                get_ib_security_type(security)==contract.secType and
                get_ib_exchange(security)==contract.exchange and
                get_ib_currency(security)==contract.currency
            ):
                self.gateway.ib_contracts[security] = contract
                break

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        print("ContractDetailsEnd. ReqId:", reqId)
        security = self.gateway.get_security_from_ib_contracts_reqid(reqId)
        # Notify threads that are waiting for ib_contracts_done
        self.gateway.ib_contracts_done[security].put(reqId)

    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float,
            volume: int, wap: float, count: int
        ):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
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
        super().tickPrice(reqId, tickType, price, attrib)
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
        super().tickSize(reqId, tickType, size)
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
        super().tickString(reqId, tickType, value)
        # print("TickString. TickerId:", reqId, "Type:", tickType, "Value:", value)

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
        # print("TickGeneric. TickerId:", reqId, "TickType:", tickType, "Value:", value)

    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        # print("Account list:", accountsList)

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
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
        super().updatePortfolio(contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, accountName)
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType,
              "Exchange:", contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
              "MarketValue:", marketValue, "AverageCost:", averageCost,
              "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
              "AccountName:", accountName
        )
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
        super().updateAccountTime(timeStamp)
        print("UpdateAccountTime. Time:", timeStamp)

    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        print("AccountDownloadEnd. Account:", accountName)
        # Update finished, insert into blocking dict
        if hasattr(self.gateway, "balance"):
            self.gateway.ib_accounts.put(IB["broker_account"], self.gateway.balance)
        if hasattr(self.gateway, "positions"):
            self.gateway.ib_positions.put(IB["broker_account"], self.gateway.positions)

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        """
        AccountSummary. ReqId: 4 Account: DU4267228 Tag:  AccountType Value: INDIVIDUAL Currency:
        :param reqId:
        :param account:
        :param tag:
        :param value:
        :param currency:
        :return:
        """
        super().accountSummary(reqId, account, tag, value, currency)
        print("AccountSummary. ReqId:", reqId, "Account:", account, "Tag: ", tag, "Value:", value, "Currency:", currency)

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print("AccountSummaryEnd. ReqId:", reqId)

    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)
        if errorCode==502:
            raise ConnectionError(f"ErrorCode:{errorCode} ErrorMsg:{errorString}")

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
            fees:BaseFees=IbFees,
        ):

        super().__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        self.trade_mode = None

        self.ib_contracts_done = {s:queue.Queue(maxsize=1) for s in securities}
        self.ib_contracts = {s:None for s in securities}               # key:Security, value:IB.Contract
        self.ib_contracts_reqid = {s: None for s in securities}        # key:Security, value:int (reqId)

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

    def process_order(self, content: pd.DataFrame):
        """更新订单的状态"""
        orderid = content["order_id"].values[0]
        order = self.orders.get(orderid)  # blocking
        order.updated_time = try_parsing_datetime(content["updated_time"].values[0])
        order.filled_avg_price = content["dealt_avg_price"].values[0]
        order.filled_quantity = content["dealt_qty"].values[0]
        order.status = convert_orderstatus_futu2qt(content["order_status"].values[0])
        # 富途的仿真环境不推送deal，需要在这里进行模拟处理
        if self.trade_mode == TradeMode.SIMULATE and order.status in (
        QTOrderStatus.FILLED, QTOrderStatus.PART_FILLED):
            dealid = "futu-sim-deal-" + str(uuid.uuid4())
            deal = Deal(
                security=order.security,
                direction=order.direction,
                offset=order.offset,
                order_type=order.order_type,
                updated_time=order.updated_time,
                filled_avg_price=order.filled_avg_price,
                filled_quantity=order.filled_quantity,
                dealid=dealid,
                orderid=orderid
            )
            self.deals.put(dealid, deal)
        self.orders.put(orderid, order)

    def process_deal(self, content: pd.DataFrame):
        """更新成交的信息"""
        orderid = content["order_id"].values[0]
        dealid = content["deal_id"].values[0]
        order = self.orders.get(orderid)  # blocking
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=try_parsing_datetime(content["create_time"].values[0]),
            filled_avg_price=content["price"].values[0],
            filled_quantity=content["qty"].values[0],
            dealid=dealid,
            orderid=orderid
        )
        self.deals.put(dealid, deal)

    @property
    def market_datetime(self):
        return datetime.now()

    def set_trade_mode(self, trade_mode: TradeMode):
        self.trade_mode = trade_mode

    def subscribe(self):
        self.api.reqMarketDataType(MarketDataTypeEnum.REALTIME) # "REALTIME", "FROZEN", "DELAYED", "DELAYED_FROZEN"
        for security in self.securities:
            if self.ib_contracts[security] is None:
                # construct vague contract
                ib_contract = generate_ib_contract(security)
                # Always remember reqId before request contracts
                self.api.reqId += 1
                self.ib_contracts_reqid[security] = self.api.reqId
                self.api.reqContractDetails(reqId=self.api.reqId, contract=ib_contract)
            # blocking here: get accurate contract from IB
            if self.ib_contracts_done[security].get()==self.api.reqId:
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
                self.api.reqRealTimeBars(self.api.reqId, ib_contract, 5, what_to_show, True, []) # MIDPOINT/TRADES/BID/ASK

                # # request market data (quote)
                # self.api.reqId += 1
                # self.ib_quotes_reqid[security] = self.api.reqId
                # self.api.reqMktData(self.api.reqId, ib_contract, "", False, False, [])
            print(f"Subscribed {security}")

        # ret_sub, err_message = self.quote_ctx.subscribe(codes, [SubType.K_1M, SubType.QUOTE, SubType.ORDER_BOOK],
        #                                                 subscribe_push=True)
        # # 订阅成功后FutuOpenD将持续收到服务器的推送，False代表暂时不需要推送给脚本
        # if ret_sub == RET_OK:  # 订阅成功
        #     print(f"成功订阅1min K线、报价和订单簿: {self.securities}")
        # else:
        #     raise ValueError(f"订阅失败: {err_message}")

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
                print(f"获取最近bar数据失败：{data}")
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
        ret_code, data = self.quote_ctx.get_capital_distribution(security.code)
        if ret_code:
            print(f"获取资金分布失败：{data}")
            return
        cap_dist = CapitalDistribution(
            datetime=datetime.strptime(data["update_time"].values[0], "%Y-%m-%d %H:%M:%S"),
            security=security,
            capital_in_big=data["capital_in_big"].values[0],
            capital_in_mid=data["capital_in_mid"].values[0],
            capital_in_small=data["capital_in_small"].values[0],
            capital_out_big=data["capital_out_big"].values[0],
            capital_out_mid=data["capital_out_mid"].values[0],
            capital_out_small=data["capital_out_small"].values[0]
        )
        return cap_dist

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
        ret_code, data = self.trd_ctx.place_order(
            price=order.price,
            qty=order.quantity,
            code=order.security.code,
            trd_side=convert_direction_qt2futu(order.direction),
            trd_env=self.futu_trd_env
        )
        if ret_code:
            print(f"提交订单失败：{data}")
            return ""
        orderid = data["order_id"].values[0]  # 如果成功提交订单，一定会返回一个orderid
        order.status = QTOrderStatus.SUBMITTED  # 修改状态为已提交
        self.orders.put(orderid, order)  # 稍后通过callback更新order状态
        return orderid

    def cancel_order(self, orderid):
        """取消订单"""
        ret_code, data = self.trd_ctx.modify_order(
            ModifyOrderOp.CANCEL,
            orderid,
            0,
            0,
            trd_env=self.futu_trd_env
        )
        if ret_code:
            print(f"撤单失败：{data}")

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
        return self.ib_contracts.get(security)

    def get_security_from_ib_contract(self, contract: Contract):
        for security in self.ib_contracts:
            ib_contract = self.get_ib_contract_from_security(security)
            if ib_contract.conId==contract.conId:
                return security

    def get_security_from_ib_contracts_reqid(self, reqId: int):
        for security in self.ib_contracts_reqid:
            if self.ib_contracts_reqid[security]==reqId:
                return security

    def get_security_from_ib_5s_bars_reqid(self, reqId: int):
        for security in self.ib_5s_bars_reqid:
            if self.ib_5s_bars_reqid[security]==reqId:
                return security

    def get_security_from_ib_quotes_reqid(self, reqId: int):
        for security in self.ib_quotes_reqid:
            if self.ib_quotes_reqid[security]==reqId:
                return security


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





