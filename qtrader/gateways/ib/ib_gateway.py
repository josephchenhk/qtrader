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
from datetime import datetime
from typing import Dict, List, Union
from threading import Thread

import pandas as pd

from ibapi.client import EClient
from ibapi.common import OrderId, TickAttrib, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.execution import Execution
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.ticktype import TickType, TickTypeEnum
from ibapi.wrapper import EWrapper
from ibapi.common import BarData as IbBarData

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


class IbWrapper(EWrapper):
    pass


class IbClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

class IbAPI(IbWrapper, IbClient):
    def __init__(self):
        IbWrapper.__init__(self)
        IbClient.__init__(self, wrapper=self)
        self.connect(IB["host"], IB["port"], IB["clientid"])
        # EReader Thread
        self.thread = Thread(target=self.run)
        self.thread.start()

    def close(self):
        self.disconnect()

class IbGateway(BaseGateway):

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

        self.api = IbAPI()

        self.connect_quote()
        self.subscribe()
        self.connect_trade()

    def close(self):
        self.api.disconnect()

    def connect_quote(self):
        """
        行情需要处理报价和订单簿
        """
        print("行情接口连接成功")

    def connect_trade(self):
        """
        交易需要处理订单和成交
        """
        print("交易接口连接成功")

    def process_quote(self, content: pd.DataFrame):
        """更新报价的状态"""
        stock = self.get_stock(code=content['code'].values[0])
        if stock is None:
            return
        svr_datetime_str = content["data_date"].values[0] + " " + content["data_time"].values[0]
        svr_datetime = try_parsing_datetime(svr_datetime_str)
        quote = Quote(
            security=stock,
            exchange=stock.exchange,
            datetime=svr_datetime,
            last_price=content['last_price'].values[0],
            open_price=content['open_price'].values[0],
            high_price=content['high_price'].values[0],
            low_price=content['last_price'].values[0],
            prev_close_price=content['prev_close_price'].values[0],
            volume=content['volume'].values[0],
            turnover=content['turnover'].values[0],
            turnover_rate=content['turnover_rate'].values[0],
            amplitude=content['amplitude'].values[0],
            suspension=content['suspension'].values[0],
            price_spread=content['price_spread'].values[0],
            sec_status=content['sec_status'].values[0],
        )
        self.quote.put(stock, quote)

    def process_orderbook(self, content: Dict):
        """更新订单簿的状态"""
        stock = self.get_stock(code=content['code'])
        if stock is None:
            return
        svr_datetime = max(
            try_parsing_datetime(content['svr_recv_time_bid']),
            try_parsing_datetime(content['svr_recv_time_ask']),
        )
        orderbook = OrderBook(
            security=stock,
            exchange=stock.exchange,
            datetime=svr_datetime
        )
        for i, bid in enumerate(content['Bid']):
            setattr(orderbook, f"bid_price_{i + 1}", bid[0])
            setattr(orderbook, f"bid_volume_{i + 1}", bid[1])
            setattr(orderbook, f"bid_num_{i + 1}", bid[2])
        for i, ask in enumerate(content['Ask']):
            setattr(orderbook, f"ask_price_{i + 1}", ask[0])
            setattr(orderbook, f"ask_volume_{i + 1}", ask[1])
            setattr(orderbook, f"ask_num_{i + 1}", ask[2])
        self.orderbook.put(stock, orderbook)

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
        self.futu_trd_env = convert_trade_mode_qt2futu(trade_mode)

    def subscribe(self):
        # TODO: 暂时写死了订阅1分钟k线
        codes = [s.code for s in self.securities]
        ret_sub, err_message = self.quote_ctx.subscribe(codes, [SubType.K_1M, SubType.QUOTE, SubType.ORDER_BOOK],
                                                        subscribe_push=True)
        # 订阅成功后FutuOpenD将持续收到服务器的推送，False代表暂时不需要推送给脚本
        if ret_sub == RET_OK:  # 订阅成功
            print(f"成功订阅1min K线、报价和订单簿: {self.securities}")
        else:
            raise ValueError(f"订阅失败: {err_message}")

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

    def get_recent_bar(self, security: Stock) -> Bar:
        """
        获取最接近当前时间的数据点
,
        :param security:
        :param cur_time:
        :return:
        """
        # TODO：暂时写定是1分钟bar
        ret_code, data = self.quote_ctx.get_cur_kline(security.code, 1, SubType.K_1M,
                                                      AuType.QFQ)  # 获取港股00700最近2个K线数据
        if ret_code:
            print(f"获取最近bar数据失败：{data}")
            return
        bars = []
        for i in range(data.shape[0]):
            bar_time = datetime.strptime(data.loc[i, "time_key"], "%Y-%m-%d %H:%M:%S")
            bar = Bar(
                datetime=bar_time,
                security=security,
                open=data.loc[i, "open"],
                high=data.loc[i, "high"],
                low=data.loc[i, "low"],
                close=data.loc[i, "close"],
                volume=data.loc[i, "volume"]
            )
            bars.append(bar)
        assert len(bars) == 1, f"We only get 1 kline, but received {len(bars)} rows."
        return bars[0]

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

    def get_recent_data(self, security: Stock, **kwargs) -> Dict[str, Union[Bar, CapitalDistribution]] or Union[
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
            if dfield == "k1m":
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
        ret_code, data = self.trd_ctx.accinfo_query(trd_env=self.futu_trd_env)
        if ret_code:
            print(f"获取券商资金失败：{data}")
            return
        balance = AccountBalance()
        balance.cash = data["cash"].values[0]
        balance.power = data["power"].values[0]
        balance.max_power_short = data["max_power_short"].values[0]
        balance.net_cash_power = data["net_cash_power"].values[0]
        if not isinstance(balance.max_power_short, float): balance.max_power_short = -1
        if not isinstance(balance.net_cash_power, float): balance.net_cash_power = -1
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
        ret_code, data = self.trd_ctx.position_list_query(trd_env=self.futu_trd_env)
        if ret_code:
            print(f"获取券商所有持仓失败：{data}")
            return
        positions = []
        for idx, row in data.iterrows():
            security = self.get_stock(code=row["code"])
            if security is None:
                security = Stock(code=row["code"], security_name=row["stock_name"])
            position_data = PositionData(
                security=security,
                direction=Direction.LONG if row["position_side"] == "LONG" else Direction.SHORT,
                holding_price=row["cost_price"],
                quantity=row["qty"],
                update_time=datetime.now(),
            )
            positions.append(position_data)
        return positions

    def get_quote(self, security: Stock) -> Quote:
        """获取报价"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """获取订单簿"""
        return self.orderbook.get(security)






