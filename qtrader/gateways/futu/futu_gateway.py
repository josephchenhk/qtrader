# -*- coding: utf-8 -*-
# @Time    : 15/3/2021 4:55 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: futu_gateway.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import uuid
import re
from typing import Dict, List
from datetime import datetime
from datetime import timedelta

import pandas as pd
from futu import (
    RET_OK,
    RET_ERROR,
    OrderBookHandlerBase,
    TradeOrderHandlerBase,
    StockQuoteHandlerBase,
    TradeDealHandlerBase,
    OpenFutureTradeContext,
    SubType,
    OrderType,
    ModifyOrderOp,
    KLType,
    AuType,
    KL_FIELD,
    TrdSide,
    TrdEnv,
    OrderStatus,
    OpenQuoteContext,
    OpenSecTradeContext
)

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, TradeMode
from qtrader.core.constants import OrderStatus as QTOrderStatus
from qtrader.core.constants import OrderType as QTOrderType
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Stock, Security, Futures
from qtrader.core.data import Bar, OrderBook, Quote, CapitalDistribution
from qtrader.core.utility import try_parsing_datetime
from qtrader.core.utility import get_kline_dfield_from_seconds
from qtrader_config import GATEWAYS, DATA_PATH, TIME_STEP
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees

"""
IMPORTANT
---------
Please install futu-api first:
> $ pip install futu-api
"""

FUTU = GATEWAYS.get("Futu")
FUTUFUTURES = GATEWAYS.get("Futufutures")


class FutuGateway(BaseGateway):
    """Futu Gateway"""

    # Minimal time step, which was read from config
    TIME_STEP = TIME_STEP

    # Short interest rate, e.g., 0.0098 for HK stock
    SHORT_INTEREST_RATE = 0.0098

    # Name of the gateway
    NAME = "FUTU"

    # kline type supported
    KL_ALLOWED = ("1Day", "1Min")

    def __init__(
            self,
            securities: List[Stock],
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
        if "trading_sessions" in kwargs:
            self.trading_sessions = kwargs.get("trading_sessions")

        self.pwd_unlock = FUTU["pwd_unlock"]

        self.quote_ctx = OpenQuoteContext(host=FUTU["host"], port=FUTU["port"])
        self.connect_quote()
        self.subscribe()

        self.trd_ctx = OpenSecTradeContext(
            host=FUTU["host"], port=FUTU["port"])
        self.connect_trade()

    def close(self):
        # FutuOpenD will terminate price subscriptions after 1 min
        self.quote_ctx.close()
        # Close trading channel
        self.trd_ctx.close()

    def connect_quote(self):
        """Price subscription, including Quote and Orderbook"""

        class QuoteHandler(StockQuoteHandlerBase):
            gateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(QuoteHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_quote(content)
                return RET_OK, content

        class OrderBookHandler(OrderBookHandlerBase):
            gateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(OrderBookHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_orderbook(content)
                return RET_OK, content

        self.quote_ctx.set_handler(QuoteHandler())
        self.quote_ctx.set_handler(OrderBookHandler())
        self.quote_ctx.start()
        print(f"{self.NAME} successfully connected to Quote and Orderbook.")

    def connect_trade(self):
        """Trade subscription, including Order and Deal"""

        class TradeOrderHandler(TradeOrderHandlerBase):
            gateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(TradeOrderHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_order(content)
                return RET_OK, content

        class TradeDealHandler(TradeDealHandlerBase):
            gateway = self

            def on_recv_rsp(self, rsp_str):
                ret_code, content = super(TradeDealHandler, self).on_recv_rsp(
                    rsp_str
                )
                if ret_code != RET_OK:
                    return RET_ERROR, content
                self.gateway.process_deal(content)
                return RET_OK, content

        self.trd_ctx.set_handler(TradeOrderHandler())
        self.trd_ctx.set_handler(TradeDealHandler())
        print(self.trd_ctx.unlock_trade(self.pwd_unlock))
        self.trd_ctx.start()
        print(f"{self.NAME} successfully connected to Order and Deal.")

    def process_quote(self, content: pd.DataFrame):
        """Callback of Quote"""
        security = self.get_security(code=content['code'].values[0])
        if security is None:
            return
        svr_datetime_str = (
            content["data_date"].values[0]
            + " "
            + content["data_time"].values[0]
        )
        svr_datetime = try_parsing_datetime(svr_datetime_str)
        quote = Quote(
            security=security,
            exchange=security.exchange,
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
        self.quote.put(security, quote)

    def process_orderbook(self, content: Dict):
        """Callback of Orderbook"""
        security = self.get_security(code=content['code'])
        if security is None:
            return
        svr_datetime = max(
            try_parsing_datetime(content['svr_recv_time_bid']),
            try_parsing_datetime(content['svr_recv_time_ask']),
        )
        orderbook = OrderBook(
            security=security,
            exchange=security.exchange,
            datetime=svr_datetime
        )
        for i, bid in enumerate(content['Bid']):
            setattr(orderbook, f"bid_price_{i+1}", bid[0])
            setattr(orderbook, f"bid_volume_{i+1}", bid[1])
            setattr(orderbook, f"bid_num_{i+1}", bid[2])
        for i, ask in enumerate(content['Ask']):
            setattr(orderbook, f"ask_price_{i+1}", ask[0])
            setattr(orderbook, f"ask_volume_{i+1}", ask[1])
            setattr(orderbook, f"ask_num_{i+1}", ask[2])
        self.orderbook.put(security, orderbook)

    def process_order(self, content: pd.DataFrame):
        """Callback of Order"""
        orderid = content["order_id"].values[0]
        order = self.orders.get(orderid)  # blocking
        order.updated_time = try_parsing_datetime(
            content["updated_time"].values[0])
        order.filled_avg_price = content["dealt_avg_price"].values[0]
        order.filled_quantity = content["dealt_qty"].values[0]
        order.status = convert_orderstatus_futu2qt(
            content["order_status"].values[0])
        # In simulate env, deal is not pushed; we handle it here
        if (self.trade_mode == TradeMode.SIMULATE and order.status in [
                QTOrderStatus.FILLED, QTOrderStatus.PART_FILLED]):
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
                orderid=orderid)
            self.deals.put(dealid, deal)
        self.orders.put(orderid, order)

    def process_deal(self, content: pd.DataFrame):
        """Callback of Deal"""
        orderid = content["order_id"].values[0]
        dealid = content["deal_id"].values[0]
        order = self.orders.get(orderid)  # blocking
        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=try_parsing_datetime(
                content["create_time"].values[0]),
            filled_avg_price=content["price"].values[0],
            filled_quantity=content["qty"].values[0],
            dealid=dealid,
            orderid=orderid)
        self.deals.put(dealid, deal)

    @property
    def market_datetime(self):
        return datetime.now()

    @property
    def trade_mode(self):
        return self._trade_mode

    @trade_mode.setter
    def trade_mode(self, trade_mode: TradeMode):
        if trade_mode not in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
            raise ValueError(
                "FutuGateway only supports `SIMULATE` or `LIVETRADE` mode, "
                f"{trade_mode} was passed in instead.")
        self._trade_mode = trade_mode
        self.futu_trd_env = convert_trade_mode_qt2futu(trade_mode)

    def subscribe(self):
        # Subscribed kline is set in config
        kline = get_kline_dfield_from_seconds(TIME_STEP // 1000)
        codes = [s.code for s in self.securities]
        ret_sub, err_message = self.quote_ctx.subscribe(
            codes,
            [getattr(SubType, kline), SubType.QUOTE, SubType.ORDER_BOOK],
            subscribe_push=True)
        if ret_sub == RET_OK:
            print(f"Successfully subscribed {kline}, QUOTE, "
                  f"and ORDER_BOOK for {self.securities}")
        else:
            raise ValueError(f"Subscription Error: {err_message}")

    def unsubscribe(self):
        pass

    def get_recent_bar(self, security: Stock) -> Bar:
        """Get recent OHLCV"""
        # Subscribed kline is set in config
        kline = get_kline_dfield_from_seconds(TIME_STEP // 1000)
        ret_code, data = self.quote_ctx.get_cur_kline(
            security.code, 1, getattr(SubType, kline), AuType.QFQ)
        if ret_code:
            print(f"[get_recent_bar]({security.code}) failed: {data}")
            return
        bars = []
        for i in range(data.shape[0]):
            bar_time = datetime.strptime(
                data.loc[i, "time_key"], "%Y-%m-%d %H:%M:%S")
            bar = Bar(
                datetime=bar_time,
                security=security,
                open=data.loc[i, "open"],
                high=data.loc[i, "high"],
                low=data.loc[i, "low"],
                close=data.loc[i, "close"],
                volume=data.loc[i, "volume"])
            bars.append(bar)
        assert len(bars) == 1, (
            f"We only get 1 kline, but received {len(bars)} rows."
        )
        return bars[0]

    def get_recent_capital_distribution(
            self,
            security: Stock
    ) -> CapitalDistribution:
        """capital distribution"""
        ret_code, data = self.quote_ctx.get_capital_distribution(security.code)
        if ret_code:
            print(f"[get_recent_capital_distribution]({security.code})"
                  f" failed: {data}")
            return
        cap_dist = CapitalDistribution(
            datetime=datetime.strptime(
                data["update_time"].values[0],
                "%Y-%m-%d %H:%M:%S"),
            security=security,
            capital_in_big=data["capital_in_big"].values[0],
            capital_in_mid=data["capital_in_mid"].values[0],
            capital_in_small=data["capital_in_small"].values[0],
            capital_out_big=data["capital_out_big"].values[0],
            capital_out_mid=data["capital_out_mid"].values[0],
            capital_out_small=data["capital_out_small"].values[0])
        return cap_dist

    def get_recent_data(
            self,
            security: Stock,
            **kwargs
    ) -> Dict or Bar or CapitalDistribution:
        """Get recent data (OHLCV or CapitalDistribution)"""
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
        if order.order_type == QTOrderType.MARKET:
            price = 0.01  # pass in any positive float
            order_type = OrderType.MARKET
        elif order.order_type == QTOrderType.LIMIT:
            price = order.price
            order_type = OrderType.NORMAL
        else:
            raise ValueError(
                f"Order type {order.order_type} is not supported in Futu "
                "Gateway.")

        code = order.security.code
        # Special treatment for HK futures
        if (
            isinstance(order.security, Futures)
            and "HK." in order.security.code
            and "main" in order.security.code
        ):
            code = get_hk_futures_code(security=order.security)

        ret_code, data = self.trd_ctx.place_order(
            price=price,
            qty=order.quantity,
            code=code,
            trd_side=convert_direction_qt2futu(order.direction),
            order_type=order_type,
            trd_env=self.futu_trd_env
        )
        if ret_code:
            print(f"[place_order]({order}) failed: {data}")
            return ""
        # valid orderid must be returned by server
        orderid = data["order_id"].values[0]
        # change order status
        order.status = QTOrderStatus.SUBMITTED
        # order will be updated later by process_order method
        self.orders.put(orderid, order)
        return orderid

    def cancel_order(self, orderid):
        """Cancel order"""
        ret_code, data = self.trd_ctx.modify_order(
            ModifyOrderOp.CANCEL,
            orderid,
            0,
            0,
            trd_env=self.futu_trd_env)
        if ret_code:
            print(f"[cancel_order]({orderid}) failed: {data}")

    def get_broker_balance(self) -> AccountBalance:
        """Broker balance"""
        ret_code, data = self.trd_ctx.accinfo_query(trd_env=self.futu_trd_env)
        if ret_code:
            print(f"[get_broker_balanc] failed: {data}")
            return
        balance = AccountBalance()
        balance.cash = data["power"].values[0]
        balance.available_cash = data["available_funds"].values[0]
        balance.maintenance_margin = data["maintenance_margin"].values[0]
        balance.unrealized_pnl = data["unrealized_pl"].values[0]
        balance.max_power_short = data["max_power_short"].values[0]
        balance.net_cash_power = data["net_cash_power"].values[0]
        if not isinstance(balance.max_power_short, float):
            balance.max_power_short = -1
        if not isinstance(balance.net_cash_power, float):
            balance.net_cash_power = -1
        return balance

    def get_broker_position(
            self,
            security: Stock,
            direction: Direction
    ) -> PositionData:
        """Broker position"""
        positions = self.get_all_broker_positions()
        for position_data in positions:
            if (
                    position_data.security == security
                    and position_data.direction == direction
            ):
                return position_data
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """All broker positions"""
        ret_code, data = self.trd_ctx.position_list_query(
            trd_env=self.futu_trd_env)
        if ret_code:
            print(f"[get_all_broker_positions] failed: {data}")
            return
        positions = []

        # Handle HK contracts with specified month
        p = re.compile("HK\.[A-Z]{3,5}[0-9]{4}")
        for idx, row in data.iterrows():
            code = row["code"]
            if p.match(row["code"]):
                code = row["code"][:-4] + "main"
            security = self.get_security(code=code)
            if security is None:
                security = Security(
                    code=row["code"],
                    security_name=row["stock_name"])
            position_data = PositionData(
                security=security,
                direction=(Direction.LONG
                           if row["position_side"] == "LONG"
                           else Direction.SHORT),
                holding_price=row["cost_price"],
                quantity=row["qty"],
                update_time=datetime.now())
            positions.append(position_data)
        return positions

    def get_quote(self, security: Stock) -> Quote:
        """Quote"""
        return self.quote.get(security)

    def get_orderbook(self, security: Stock) -> OrderBook:
        """Orderbook"""
        return self.orderbook.get(security)

    def req_historical_bars(
            self,
            security: Security,
            periods: int,
            freq: str,
            cur_datetime: datetime,
            trading_sessions: List[List[datetime]] = None,
            mode: str = "direct"
    ) -> List[Bar]:
        """request historical bar data."""
        # Check whether freq is valid
        assert freq in self.KL_ALLOWED, (
            f"Parameter freq={freq} is Not supported. "
            f"Only {self.KL_ALLOWED} are allowed."
        )

        if freq == "1Day" and trading_sessions is None:
            raise ValueError(
                f"Parameters trading_sessions is mandatory if freq={freq}.")

        # return historical bar data
        data_df = self._api_get_historical_bar(
            instrument=security.code,
            asset_type=f"{security.__class__.__name__}_{security.exchange.value}",
            periods=periods,
            freq=freq)

        hist_bars = []
        for _, row in data_df.iterrows():
            bar = Bar(
                datetime=try_parsing_datetime(row["time_key"]),
                security=security,
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=row["volume"]
            )
            hist_bars.append(bar)
        return hist_bars

    def _api_req_historical_bar(
            self,
            code: str,
            start: str,
            end: str,
            ktype: KLType = KLType.K_DAY,
            autype: AuType = AuType.QFQ,
            fields: List[KL_FIELD] = [KL_FIELD.ALL],
            max_count: int = 500,
            extended_time: bool = False
    ):
        """
        FUTU API: 获取历史 K 线

        接口限制
        -------
        我们会根据您账户的资产和交易的情况，下发历史 K 线额度。因此，30 天内您只能获取有限只股票的历史 K 线数据。具体规则参见 API 用户额度 。您
        当日消耗的历史 K 线额度，会在 30 天后自动释放。
        每 30 秒内最多请求 60 次历史 K 线接口。注意：如果您是分页获取数据，此限频规则仅适用于每只股票的首页，后续页请求不受限频规则的限制。
        分 K 提供最近 2 年数据，日 K 及以上提供最近 10 年的数据。
        美股盘前和盘后 K 线仅支持 60 分钟及以下级别。由于美股盘前和盘后时段为非常规交易时段，此时段的 K 线数据可能不足 2 年。
        https://openapi.futunn.com/futu-api-doc/quote/request-history-kline.html
        :param code: 'HK.00700'
        :param start: '2019-09-11'
        :param end: '2019-09-18'
        :param ktype: KLType.K_DAY,
        :param autype: AuType.QFQ,
        :param fields: [KL_FIELD.ALL],
        :param max_count: 500,
        :param extended_time: False
        :return:
        """
        ret, data, page_req_key = self.quote_ctx.request_history_kline(
            code=code,
            start=start,
            end=end,
            ktype=ktype,
            autype=autype,
            fields=fields,
            max_count=max_count,
            extended_time=extended_time,
        )  # 每页max_count个，请求第一页
        if ret == RET_OK:
            yield data
        else:
            print('error:', data)
            return
        while page_req_key is not None:  # 请求后面的所有结果
            # print('*************************************')
            ret, data, page_req_key = self.quote_ctx.request_history_kline(
                code=code,
                start=start,
                end=end,
                ktype=ktype,
                autype=autype,
                fields=fields,
                max_count=max_count,
                extended_time=extended_time,
                page_req_key=page_req_key
            )  # 请求翻页后的数据
            if ret == RET_OK:
                yield data
            else:
                print('error:', data)
                return

    def _api_get_historical_bar(
        self,
        instrument: str,
        asset_type: str,
        end_date: str = 'today',
        start_date: str = None,
        periods: int = 1,
        **kwargs
    ):
        """Use FUTU API to get historical Data"""
        assert asset_type in ("Equity_SEHK", "Futures_HKFE"), (
            f"Asset type {asset_type} is not supported."
        )
        if "freq" in kwargs:
            freq = kwargs.get("freq")
        else:
            freq = "1Min"  # default getting 1 min bar
        if freq == "1Min":
            ktype = KLType.K_1M
        elif freq == "1Day":
            ktype = KLType.K_DAY
        else:
            raise ValueError(f"Freq {freq} is not supported.")

        if end_date == "today":
            now = datetime.now()
            end_date = now.strftime("%Y-%m-%d")
            if freq == "1Min":
                num_days = int(periods / 5. / 60.) + 5
                start_date = (now - timedelta(days=num_days)
                              ).strftime("%Y-%m-%d")
            elif freq == "1Day":
                num_days = int(periods * 7. / 5.) + 60
                start_date = (now - timedelta(days=num_days)
                              ).strftime("%Y-%m-%d")
        history_kline = self._api_req_historical_bar(
            code=instrument,
            start=start_date,
            end=end_date,
            ktype=ktype,
        )

        klines = []
        for kl in history_kline:
            klines.append(kl)
        if len(klines) > 0:
            data = pd.concat(klines)
            assert data.shape[0] >= periods, (
                "Data received is not sufficient to requested"
                f" ({data.shape[0]} < {periods})."
            )
            return data[["time_key", "open", "high",
                         "low", "close", "volume"]].tail(periods)


class FutuFuturesGateway(FutuGateway):
    """Futu futures gateway"""

    # Minimal time step, which was read from config
    TIME_STEP = TIME_STEP

    # Short interest rate, 0.0 for HK futures
    SHORT_INTEREST_RATE = 0.0

    # Name of the gateway
    NAME = "FUTUFUTURES"

    def __init__(
            self,
            securities: List[Stock],
            gateway_name: str,
            start: datetime = None,
            end: datetime = None,
            fees: BaseFees = BaseFees,
            **kwargs
    ):
        super(FutuGateway, self).__init__(securities, gateway_name)
        self.fees = fees
        self.start = start
        self.end = end
        if "trading_sessions" in kwargs:
            self.trading_sessions = kwargs.get("trading_sessions")

        self.pwd_unlock = FUTUFUTURES["pwd_unlock"]

        self.quote_ctx = OpenQuoteContext(
            host=FUTUFUTURES["host"],
            port=FUTUFUTURES["port"])
        self.connect_quote()
        self.subscribe()

        self.trd_ctx = OpenFutureTradeContext(
            host=FUTUFUTURES["host"], port=FUTUFUTURES["port"])
        self.connect_trade()


def convert_direction_qt2futu(direction: Direction) -> TrdSide:
    """Convert QT direction to Futu"""
    if direction == Direction.SHORT:
        return TrdSide.SELL
    elif direction == Direction.LONG:
        return TrdSide.BUY
    else:
        raise ValueError(f"Direction {direction} is not supported.")


def convert_trade_mode_qt2futu(trade_mode: TradeMode) -> TrdEnv:
    """Convert trade mode to Futu"""
    if trade_mode == TradeMode.SIMULATE:
        return TrdEnv.SIMULATE
    elif trade_mode == TradeMode.LIVETRADE:
        return TrdEnv.REAL
    else:
        raise ValueError(f"TradeMode {trade_mode} is not supported.")


def convert_orderstatus_futu2qt(status: OrderStatus) -> QTOrderStatus:
    """Convert order status to Futu"""
    if status in (
            OrderStatus.NONE,
            OrderStatus.UNSUBMITTED,
            OrderStatus.WAITING_SUBMIT,
            OrderStatus.SUBMITTING,
            OrderStatus.DISABLED,
            OrderStatus.DELETED):
        return QTOrderStatus.UNKNOWN
    elif status in (OrderStatus.SUBMITTED):
        return QTOrderStatus.SUBMITTED
    elif status in (OrderStatus.FILLED_ALL):
        return QTOrderStatus.FILLED
    elif status in (OrderStatus.FILLED_PART):
        return QTOrderStatus.PART_FILLED
    elif status in (
            OrderStatus.CANCELLED_ALL,
            OrderStatus.CANCELLED_PART,
            OrderStatus.CANCELLING_PART):
        return QTOrderStatus.CANCELLED
    elif status in (
            OrderStatus.SUBMIT_FAILED,
            OrderStatus.TIMEOUT,
            OrderStatus.FAILED):
        return QTOrderStatus.FAILED
    else:
        raise ValueError(f"Order status {status} is not recognized.")

def get_hk_futures_code(security: Futures) -> str:
    """Use security code and expiry date to determine exact futures code"""
    return security.code.replace("main", security.expiry_date[2:6])