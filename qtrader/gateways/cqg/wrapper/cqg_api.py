# -*- coding: utf-8 -*-
# @Time    : 11/25/2021 11:33 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: cqg_api.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import threading
from datetime import datetime
from dataclasses import replace
from typing import Dict
import pytz
import queue

import win32com.client

from qtrader.core.order import Order
from qtrader.core.constants import Direction, OrderType, OrderStatus
from qtrader.core.position import PositionData
from qtrader.core.balance import AccountBalance
from qtrader.core.data import Quote, OrderBook, Bar
from qtrader.core.utility import safe_call
from qtrader.gateways import BaseGateway
from qtrader.gateways.cqg.wrapper.CELEnvironment import CELSinkBase
from qtrader.gateways.cqg.wrapper.CELEnvironment import Trace
from qtrader.gateways.cqg.wrapper.CELEnvironment import AssertMessage

TIMEOUT = 10                                   # Events waiting time
TIMEZONE = pytz.timezone("Asia/Hong_Kong")     # Default timezone we are working on
INVALID_VALUE = -2147483648                    # CQG default invalid values to be ignored
lock = threading.Lock()                        # Lock to update account_balance and positions safely


class CQGAPI(CELSinkBase):
    def __init__(self):
        self.eventGatewayIsUp = threading.Event()
        self.eventGatewayIsDown = threading.Event()
        self.eventAccountIsReady = threading.Event()
        self.eventInstrumentIsReady = threading.Event()
        self.eventOrderPlaced = threading.Event()
        self.eventQuoteDone = threading.Event()      # subscribe to real time quote data
        self.eventOrderbookDone = threading.Event()  # subscribe to real time orderbook data
        self.eventBarDone = threading.Event()        # subscribe to real time bar data (only set after meaningful value)

        self.subscribe_bars_map = {}
        self.subscribe_bars_data = {}

        self.subscribe_quotes_map = {} # we can fetch cqgInstrument via this map
        self.quote_instrument_fullname = queue.Queue(maxsize=1)  # make sure instrument is subscribed

        self.subscribe_orderbooks_map = {}

        # Use threading lock to ensure these two values are only modified in safe mode.
        self.account_balance = AccountBalance()
        self.positions = []

        self.order_id = queue.Queue(maxsize=1)                   # make sure order is placed

    def Init(self, gateway: BaseGateway):
        self.celEnvironment = gateway.celEnvironment
        self.gateway = gateway

    def Logon(self, user_name: str, password: str):
        Trace("Connecting to GW")
        self.celEnvironment.cqgCEL.GWLogon(user_name, password)

        Trace("Waiting for GW connection...")
        AssertMessage(
            self.eventGatewayIsUp.wait(TIMEOUT),
            "GW connection timeout!")

    def Logoff(self):
        Trace("Logoff from GW")
        self.eventGatewayIsDown.clear()
        self.celEnvironment.cqgCEL.GWLogoff()
        AssertMessage(
            self.eventGatewayIsDown.wait(TIMEOUT),
            "GW disconnection timeout!")
        Trace(f"Successfully logoff account {self.cqg_account.GWAccountName}")

    def subscribe_quote(self, cqg_symbol: str):
        self.celEnvironment.cqgCEL.NewInstrument(cqg_symbol)
        # blocking here, wait for the instrument to be resolved in OnInstrumentResolved
        instrument_fullname = self.quote_instrument_fullname.get()
        self.subscribe_quotes_map[cqg_symbol] = self.celEnvironment.cqgCEL.Instruments.Item(
            instrument_fullname)
        self.eventQuoteDone.wait(TIMEOUT)

        # request = win32com.client.Dispatch(self.celEnvironment.cqgCEL.CreateInstrumentRequest())
        # request.Symbol = cqg_symbol
        # request.QuoteLevel = win32com.client.constants.qsQuotes
        # cqgQuote = self.celEnvironment.cqgCEL.SubscribeNewInstrument(request)
        # self.subscribe_quotes_map[cqg_symbol] = cqgQuote

    def unsubscribe_quote(self, cqg_symbol: str):
        cqgQuote = self.subscribe_quotes_map.pop(cqg_symbol)
        self.celEnvironment.cqgCEL.RemoveInstrument(cqgQuote)
        Trace(f"Successfully unsubscribe quote for {cqg_symbol}")

    def subscribe_orderbook(self, cqg_symbol: str):
        request = win32com.client.Dispatch(
            self.celEnvironment.cqgCEL.CreateTicksRequest())
        request.Symbol = cqg_symbol
        request.Type = win32com.client.constants.trtCurrentNotify
        request.TickFilter = win32com.client.constants.tfAll
        request.SessionsFilter = 31
        request.Limit = 10
        Trace("Limit: {}".format(request.Limit))
        cqgOrderbooks = self.celEnvironment.cqgCEL.RequestTicks(request)
        self.subscribe_orderbooks_map[cqg_symbol] = cqgOrderbooks
        self.eventOrderbookDone.wait(TIMEOUT)

    def unsubscribe_orderbook(self, cqg_symbol: str):
        cqgOrderbooks = self.subscribe_orderbooks_map.pop(cqg_symbol)
        self.celEnvironment.cqgCEL.RemoveTicks(cqgOrderbooks)
        Trace(f"Successfully unsubscribe orderbook for {cqg_symbol}")

    def subscribe_bar(self, cqg_symbol: str, lookback_period: int, freq: str="1Min"):
        FREQ_ALLOWED = ("1Day", "1Min")
        if freq not in FREQ_ALLOWED:
            raise ValueError(f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} are allowed.")
        Trace("Create timed bars request")
        request = win32com.client.Dispatch(
            self.celEnvironment.cqgCEL.CreateTimedBarsRequest())
        request.Symbol = cqg_symbol
        request.RangeStart = 0
        request.RangeEnd = -lookback_period
        if freq == "1Day":
            request.HistoricalPeriod = win32com.client.constants.hpDaily
        elif freq == "1Min":
            request.IntradayPeriod = win32com.client.constants.pfds1Min
        request.DailyBarClose = win32com.client.constants.dbcLastQuoteOrSettlement
        request.UpdatesEnabled = True
        request.IncludeOutput(
            win32com.client.constants.tbrContractVolume, True)
        request.IncludeOutput(
            win32com.client.constants.tbrCommodityVolume, True)
        request.SessionsFilter = "all"
        cqgTimedBar = self.celEnvironment.cqgCEL.RequestTimedBars(request)
        self.subscribe_bars_map[cqg_symbol] = cqgTimedBar
        self.eventBarDone.wait(TIMEOUT)

    @safe_call
    def unsubscribe_bar(self, cqg_symbol: str):
        cqgTimedBar = self.subscribe_bars_map.pop(cqg_symbol)
        self.celEnvironment.cqgCEL.RemoveTimedBars(cqgTimedBar)
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        self.eventBarDone.clear()
        self.subscribe_bars_data.pop(security, None)
        Trace(f"Successfully unsubscribe timedbar for {cqg_symbol}")

    def subscribe_account_and_positions(self, GWAccountID: int):
        self.celEnvironment.cqgCEL.AccountSubscriptionLevel = win32com.client.constants.aslAccountUpdatesAndOrders
        Trace("Waiting for accounts coming...")
        AssertMessage(self.eventAccountIsReady.wait(TIMEOUT), "Accounts coming timeout!")

        Trace(f"Select account {GWAccountID}, subscribe to open positions.")
        accounts = win32com.client.Dispatch(self.celEnvironment.cqgCEL.Accounts)
        for account in accounts:
            if account.GWAccountID == GWAccountID:
                account.AutoSubscribeInstruments = True
                account.PositionSubcriptionLevel = win32com.client.constants.pslSnapshotAndUpdates
                Trace("GW Account id: {} name: {}".format(account.GWAccountID, account.GWAccountName))
                break
        self.cqg_account = win32com.client.Dispatch(accounts.Item(GWAccountID))
        self.update_account_balance()
        self.update_positions()

    def update_account_balance(self):
        with lock:
            # update account balance
            self.account_balance = AccountBalance()
            self.account_balance.cash: float = self.cqg_account.Summary.Balance(0)
            self.account_balance.cash_by_currency: Dict[str, float] = None
            self.account_balance.available_cash: float = self.cqg_account.Summary.Balance(0) - self.cqg_account.Summary.Collaterals(1)
            self.account_balance.max_power_short: float = None
            self.account_balance.net_cash_power: float = self.cqg_account.Summary.NLV(0)
            self.account_balance.maintenance_margin: float = self.cqg_account.Summary.MaintenanceMargin(1)
            self.account_balance.realized_pnl: float = self.cqg_account.Summary.ProfitLoss(0)
            self.account_balance.unrealized_pnl: float = self.cqg_account.Summary.UPL(0)

    def update_positions(self):
        with lock:
            # update positions
            self.positions = []
            num_pos = self.cqg_account.Positions.Count
            for n in range(num_pos):
                cqg_pos = self.cqg_account.Positions.ItemByIndex(n)
                if cqg_pos.Quantity:
                    cqg_symbol = cqg_pos.InstrumentName.split(".")[-1]
                    security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
                    if cqg_pos.Side==win32com.client.constants.osdBuy:
                        direction = Direction.LONG
                    elif cqg_pos.Side==win32com.client.constants.osdSell:
                        direction = Direction.SHORT
                    else:
                        raise ValueError(f"Return CQG Position Side {cqg_pos.Side} is invalid!")
                    position_data = PositionData(
                        security=security,
                        direction=direction,
                        holding_price=cqg_pos.AveragePrice,
                        quantity=abs(cqg_pos.Quantity),
                        update_time=datetime.now(),
                    )
                    self.positions.append(position_data)

    def get_subscribe_bars_data(self, security):
        self.eventBarDone.wait(TIMEOUT)
        bars = self.subscribe_bars_data.get(security)

        # backfill the bar if necessary
        for i in list(range(len(bars)-2, -1, -1)):
            bar = bars[i]
            next_bar = bars[i+1]
            bar.open = next_bar.open if (bar.open == INVALID_VALUE and next_bar.open != INVALID_VALUE) else bar.open
            bar.high = next_bar.high if (bar.high == INVALID_VALUE and next_bar.high != INVALID_VALUE) else bar.high
            bar.low = next_bar.low if (bar.low == INVALID_VALUE and next_bar.low != INVALID_VALUE) else bar.low
            bar.close = next_bar.close if (bar.close == INVALID_VALUE and next_bar.close != INVALID_VALUE) else bar.close
            bar.volume = next_bar.volume if (bar.volume == INVALID_VALUE and next_bar.volume != INVALID_VALUE) else bar.volume
            bars[i] = bar

        # forward fill the bar if necessary
        for i in list(range(1, len(bars), 1)):
            bar = bars[i]
            prev_bar = bars[i-1]
            bar.open = prev_bar.open if (bar.open == INVALID_VALUE and prev_bar.open != INVALID_VALUE) else bar.open
            bar.high = prev_bar.high if (bar.high == INVALID_VALUE and prev_bar.high != INVALID_VALUE) else bar.high
            bar.low = prev_bar.low if (bar.low == INVALID_VALUE and prev_bar.low != INVALID_VALUE) else bar.low
            bar.close = prev_bar.close if (bar.close == INVALID_VALUE and prev_bar.close != INVALID_VALUE) else bar.close
            bar.volume = prev_bar.volume if (bar.volume == INVALID_VALUE and prev_bar.volume != INVALID_VALUE) else bar.volume
            bars[i] = bar

        # The last bar must be valid
        last_bar = bars[-1]
        if (last_bar.open == INVALID_VALUE or
            last_bar.high == INVALID_VALUE or
            last_bar.low == INVALID_VALUE or
            last_bar.close == INVALID_VALUE or
            last_bar.volume == INVALID_VALUE
        ):
            raise ValueError(f"The latest bar is INVALID: {last_bar}.")

        return bars


    ####################################################################################################################
    #                                                                                                                  #
    #                                       RealTimeMarketDataReceiving.py                                             #
    ####################################################################################################################
    def OnDataError(self, cqgError, errorDescription):
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("OnDataError: Code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                 dispatchedCQGError.Description))
        # TODO: Handle different errors separately

    def OnInstrumentResolved(self, symbol, cqgInstrument, cqgError):
        if cqgError is not None:
            self.eventQuoteDone.set()
            return
        instrument = win32com.client.Dispatch(cqgInstrument)
        instrument.DataSubscriptionLevel = win32com.client.constants.dsQuotesAndBBA
        # Notify the threads that are waiting for quote_insturment_fullname
        self.quote_instrument_fullname.put(instrument.FullName)
        self.eventQuoteDone.set()

    def OnInstrumentChanged(self, cqgInstrument, cqgQuotes, cqgInstrumentProperties):
        instrument = win32com.client.Dispatch(cqgInstrument)
        quotes = win32com.client.Dispatch(cqgQuotes)
        for quote in quotes:
            if (quote.IsValid):
                cqg_symbol = instrument.FullName.split(".")[-1]
                security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
                if self.gateway.cqg_quotes[security] is None:
                    qt_quote = Quote(
                        security=security,
                        exchange=security.exchange,
                        datetime=datetime.now()
                    )
                else:
                    qt_quote = self.gateway.cqg_quotes[security]

                if QuoteType2String(quote.Type) == "Last trade price":
                    qt_quote = replace(qt_quote, last_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Best bid":
                    qt_quote = replace(qt_quote, bid_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Best ask":
                    qt_quote = replace(qt_quote, ask_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Day open price":
                    qt_quote = replace(qt_quote, open_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Current day high price":
                    qt_quote = replace(qt_quote, high_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Current day low price":
                    qt_quote = replace(qt_quote, low_price=quote.price, datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Yesterday's settlement price":
                    qt_quote = replace(qt_quote, prev_close_price=quote.price, datetime=datetime.now())
                # else:
                #     print("QuoteType:", QuoteType2String(quote.Type))
                self.gateway.cqg_quotes[security] = qt_quote
                if qt_quote.last_price != 0 or (
                        qt_quote.bid_price != 0 and qt_quote.ask_price != 0):
                    self.gateway.process_quote(qt_quote)

    ####################################################################################################################
    #                                                                                                                  #
    #                                          TicksRequestRealtime.py                                                 #
    ####################################################################################################################

    def OnTicksResolved(self, cqgTicks, cqgError):
        if cqgError is not None:
            self.eventOrderbookDone.set()
            return
        self.eventOrderbookDone.set()

    def OnTicksAdded(self, cqgTicks, addedTicksCount):
        dispatchedCQGTicks = win32com.client.Dispatch(cqgTicks)
        for i in range(dispatchedCQGTicks.Count - addedTicksCount, dispatchedCQGTicks.Count):
            tick = dispatchedCQGTicks.Item(i)
            # process orderbook
            cqg_symbol = dispatchedCQGTicks.Request.Symbol
            security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
            if self.gateway.cqg_orderbooks[security] is None:
                orderbook = OrderBook(
                    security=security,
                    exchange=security.exchange,
                    datetime=datetime.now()
                )
            else:
                orderbook = self.gateway.cqg_orderbooks[security]
            if TickType2String(tick.PriceType) == "Ask price":
                orderbook = replace(
                    orderbook,
                    ask_volume_1=tick.Volume,
                    ask_price_1=tick.Price,
                    datetime=datetime.now()
                )
            elif TickType2String(tick.PriceType) == "Bid price":
                orderbook = replace(
                    orderbook,
                    bid_volume_1=tick.Volume,
                    bid_price_1=tick.Price,
                    datetime=datetime.now()
                )
            # else:
            #     print("TickType:", TickType2String(tick.PriceType))
            self.gateway.cqg_orderbooks[security] = orderbook
            if orderbook.bid_price_1 != 0 and orderbook.ask_price_1 != 0 and orderbook.bid_volume_1 != 0 and orderbook.ask_volume_1 != 0:
                self.gateway.process_orderbook(orderbook)

    def OnTicksRemoved(self, cqgTicks, removedTickIndex):
        pass

    ####################################################################################################################
    #                                                                                                                  #
    #                                             TimedBarsDaily.py                                                    #
    ####################################################################################################################

    def OnTimedBarsResolved(self, cqgTimedBars, cqgError):
        """
        Ref: https://partners.cqg.com/sites/default/files/docs/CQGAPI_4.0R/webframe.html#CQG~CQGTimedBar_members.html
        """
        if (cqgError is not None):
            self.eventBarDone.set()
            return
        bars = win32com.client.Dispatch(cqgTimedBars)
        cqg_symbol = bars.Request.Symbol
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        # if we already had bar data list, we will update it in OnTimedBarsAdded method
        if self.subscribe_bars_data.get(security):
            self.eventBarDone.set()
            return
        self.subscribe_bars_data[security] = []
        for i in range(0, bars.Count):
            # self.dumpBar(win32com.client.Dispatch(bars.Item(i)), i)
            cqg_bar = bars.Item(i)
            bar_time = cqg_bar.Timestamp
            bar_time = datetime(
                bar_time.year,
                bar_time.month,
                bar_time.day,
                bar_time.hour,
                bar_time.minute,
                bar_time.second
            )
            bar = Bar(
                datetime=bar_time,
                security=security,
                open=cqg_bar.Open,
                high=cqg_bar.High,
                low=cqg_bar.Low,
                close=cqg_bar.Close,
                volume=cqg_bar.ActualVolume
            )
            # If the value is invalid, use previous bar data to replace it (forward fill)
            # This treatment will not be applied to the last bar, as it will be updated in OnTimedBarsUpdated
            # [Note] if the first bar (i=0) is INVALID_VALUE, then we could still end up with meaningless
            # bars in self.subscribe_bars_data[security]. We will use a backward fill to make the data
            # meaningful in method `get_subscribe_bars_data`
            if len(self.subscribe_bars_data[security]) > 0 and i < bars.Count-1:
                prev_bar = self.subscribe_bars_data[security][-1]
                bar.open = prev_bar.open if bar.open == INVALID_VALUE else bar.open
                bar.high = prev_bar.high if bar.high == INVALID_VALUE else bar.high
                bar.low = prev_bar.low if bar.low == INVALID_VALUE else bar.low
                bar.close = prev_bar.close if bar.close == INVALID_VALUE else bar.close
                bar.volume = prev_bar.volume if bar.volume == INVALID_VALUE else bar.volume
            self.subscribe_bars_data[security].append(bar)

        # The last bar must be meaningful, otherwise it should be updated in OnTimedBarsUpdated
        if (bar.open != INVALID_VALUE and
            bar.high != INVALID_VALUE and
            bar.low != INVALID_VALUE and
            bar.close != INVALID_VALUE and
            bar.volume != INVALID_VALUE
        ):
            self.eventBarDone.set()

    def OnTimedBarsAdded(self, cqgTimedBars):
        bars = win32com.client.Dispatch(cqgTimedBars)
        cqg_symbol = bars.Request.Symbol
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        assert self.subscribe_bars_data[security] is not None, "We should have initiated data in OnTimedBarsResolved!"
        cqg_bar = bars.Item(bars.Count - 1)
        bar_time = cqg_bar.Timestamp
        bar_time = datetime(
            bar_time.year,
            bar_time.month,
            bar_time.day,
            bar_time.hour,
            bar_time.minute,
            bar_time.second
        )
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=cqg_bar.Open,
            high=cqg_bar.High,
            low=cqg_bar.Low,
            close=cqg_bar.Close,
            volume=cqg_bar.ActualVolume
        )
        # Note: sometimes we receive invalid data (INVALID_VALUE = -2147483648), and we will need to update it
        # subsequently in OnTimedBarsUpdated. We will need to check (i.e., blocking wait for) it when we call
        # get_recent_bar in cqg_gateway.
        self.eventBarDone.clear()
        self.subscribe_bars_data[security].pop(0)
        self.subscribe_bars_data[security].append(bar)
        if (bar.open != INVALID_VALUE and
            bar.high != INVALID_VALUE and
            bar.low != INVALID_VALUE and
            bar.close != INVALID_VALUE and
            bar.volume != INVALID_VALUE
        ):
            self.eventBarDone.set()


    def OnTimedBarsUpdated(self, cqgTimedBars, index):
        bars = win32com.client.Dispatch(cqgTimedBars)
        cqg_symbol = bars.Request.Symbol
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        assert self.subscribe_bars_data[security] is not None, "We should have initiated data in OnTimedBarsResolved!"
        cqg_bar = win32com.client.Dispatch(bars.Item(index))
        bar_time = cqg_bar.Timestamp
        bar_time = datetime(
            bar_time.year,
            bar_time.month,
            bar_time.day,
            bar_time.hour,
            bar_time.minute,
            bar_time.second
        )
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=cqg_bar.Open,
            high=cqg_bar.High,
            low=cqg_bar.Low,
            close=cqg_bar.Close,
            volume=cqg_bar.ActualVolume
        )
        # update the last bar to avoid invalid data (INVALID_VALUE = -2147483648). Only after the latest bar is updated
        # to meaningful value, we can start to use it in get_recent_bar in cqg_gateway.
        if (bar.open != INVALID_VALUE and
            bar.high != INVALID_VALUE and
            bar.low != INVALID_VALUE and
            bar.close != INVALID_VALUE and
            bar.volume != INVALID_VALUE
        ):
            self.subscribe_bars_data[security][index] = bar
            self.eventBarDone.set()


    def OnTimedBarsRemoved(self, cqgTimedBars, index):
        pass

    def dumpBar(self, bar, index):
        Trace("   Bar index: {} Timestamp {} Open {} High {} Low {} Close {} "
              "ActualVolumeFractional {} CommodityVolumeFractional {} ContractVolumeFractional {} TickVolume {}".format(
                  index, bar.Timestamp, bar.Open, bar.High, bar.Low, bar.Close,
                  bar.ActualVolumeFractional, bar.CommodityVolumeFractional, bar.ContractVolumeFractional, bar.TickVolume))

    ####################################################################################################################
    #                                                                                                                  #
    #                                             OrdersPlacing.py                                                     #
    ####################################################################################################################

    def place_order(self, order:Order) -> str:
        cqg_symbol = self.gateway.get_cqg_symbol_from_qt_security(order.security)
        instrument = self.subscribe_quotes_map[cqg_symbol]
        # dispatchedInstrument = win32com.client.Dispatch(instrument)
        if order.order_type==OrderType.LIMIT:
            order_type = win32com.client.constants.otLimit
        elif order.order_type==OrderType.MARKET:
            order_type = win32com.client.constants.otMarket
        elif order.order_type==OrderType.STOP:
            order_type = win32com.client.constants.otStop
        else:
            raise ValueError(f"Unsupported order type {order.order_type}")

        if order.direction==Direction.LONG:
            order_side = win32com.client.constants.osdBuy
        elif order.direction==Direction.SHORT:
            order_side = win32com.client.constants.osdSell
        else:
            raise ValueError(f"Unsupported order side {order.direction}")

        if order.order_type==OrderType.MARKET:
            cqgOrder = win32com.client.Dispatch(self.celEnvironment.cqgCEL.CreateOrder(
                order_type,
                instrument,
                self.cqg_account,
                order.quantity,
                order_side)
            )
        elif order.order_type==OrderType.LIMIT:
            cqgOrder = win32com.client.Dispatch(self.celEnvironment.cqgCEL.CreateOrder(
                order_type,
                instrument,
                self.cqg_account,
                order.quantity,
                order_side,
                order.price)
            )
        elif order.order_type==OrderType.STOP:
            cqgOrder = win32com.client.Dispatch(self.celEnvironment.cqgCEL.CreateOrder(
                order_type,
                instrument,
                self.cqg_account,
                order.quantity,
                order_side,
                order.price,
                order.stop_price)
            )

        # cqgOrder.QuantityFractional = 1
        cqgOrder.Place()
        AssertMessage(self.eventOrderPlaced.wait(TIMEOUT), f"Order {order} placing timeout!")
        # Blocking here, wait for update in OnOrderChanged method
        orderid = self.order_id.get()
        return orderid

    def cancel_order(self, orderid:str):
        cqgOrder = self.celEnvironment.cqgCEL.Orders.ItemByGuid(orderid)
        cqgOrder.Cancel()

    def OnGWConnectionStatusChanged(self, connectionStatus):
        if connectionStatus == win32com.client.constants.csConnectionUp:
            Trace("GW connection is UP!")
            self.eventGatewayIsUp.set()
        if connectionStatus == win32com.client.constants.csConnectionDown:
            Trace("GW connection is DOWN!")
            self.eventGatewayIsDown.set()

    def OnAccountChanged(self, change, cqgAccount, cqgPosition):
        if change == win32com.client.constants.actPositionAdded:
            account = win32com.client.Dispatch(cqgAccount)
            position = win32com.client.Dispatch(cqgPosition)
            Trace("OnAccountChanged - open position is added for {} account - "
                  "instrument: {} average price: {} quantity: {} OTE: {} PL: {}".format(account.GWAccountName,
                                                                                        position.InstrumentName,
                                                                                        position.AveragePrice,
                                                                                        position.Quantity,
                                                                                        position.OTE,
                                                                                        position.ProfitLoss))

        if change == win32com.client.constants.actPositionChanged:
            account = win32com.client.Dispatch(cqgAccount)
            position = win32com.client.Dispatch(cqgPosition)
            Trace("OnAccountChanged - open position is changed for {} account - "
                  "instrument: {} average price: {} quantity: {} OTE: {} PL: {}".format(account.GWAccountName,
                                                                                        position.InstrumentName,
                                                                                        position.AveragePrice,
                                                                                        position.Quantity,
                                                                                        position.OTE,
                                                                                        position.ProfitLoss))

        if change == win32com.client.constants.actAccountsReloaded:
            Trace("OnAccountChanged - Accounts are ready!")
            self.eventAccountIsReady.set()

    def OnOrderChanged(self, changeType, cqgOrder,
                       oldProperties, cqgFill, cqgError):
        if cqgError is not None:
            return

        # Handle order
        dispatchedOrder = win32com.client.Dispatch(cqgOrder)
        properties = win32com.client.Dispatch(dispatchedOrder.Properties)
        gwStatus = properties(win32com.client.constants.opGWStatus)
        guid = properties(win32com.client.constants.opGUID)
        server_time = properties(win32com.client.constants.opEventServerTimestamp).Value

        updated_time = datetime(
            year=server_time.year,
            month=server_time.month,
            day=server_time.day,
            hour=server_time.hour,
            minute=server_time.minute,
            second=server_time.second
        )
        filled_avg_price = properties(win32com.client.constants.opAverageFillPrice).Value
        filled_quantity = properties(win32com.client.constants.opFilledQuantity).Value
        status = convert_orderstatus_cqg2qt(gwStatus.Value)
        order_id = guid.Value

        if changeType == win32com.client.constants.ctAdded:
            self.order_id.put(order_id)
            self.eventOrderPlaced.set()
        elif changeType in (win32com.client.constants.ctChanged, win32com.client.constants.ctRemoved):
            self.gateway.process_order(dict(
                order_id=order_id,
                updated_time=updated_time,
                filled_avg_price=filled_avg_price,
                filled_quantity=filled_quantity,
                status=status)
            )

        # Handle fill
        if cqgFill:
            dispatchedFill = win32com.client.Dispatch(cqgFill)
            if dispatchedFill.Status == win32com.client.constants.fsNormal:
                server_time = dispatchedFill.Timestamp
                updated_time = datetime(
                    year=server_time.year,
                    month=server_time.month,
                    day=server_time.day,
                    hour=server_time.hour,
                    minute=server_time.minute,
                    second=server_time.second
                )
                filled_avg_price = dispatchedFill.Price
                filled_quantity = dispatchedFill.Quantity
                order_id = dispatchedFill.Order.GUID
                deal_id = dispatchedFill.Id
                self.gateway.process_deal(dict(
                    deal_id=deal_id,
                    order_id=order_id,
                    updated_time=updated_time,
                    filled_avg_price=filled_avg_price,
                    filled_quantity=filled_quantity)
                )

def QuoteType2String(quoteType):
    return {
        win32com.client.constants.qtAsk: "Best ask",
        win32com.client.constants.qtBid: "Best bid",
        win32com.client.constants.qtCohUndAsk: "Coherent underlying price for the best ask",
        win32com.client.constants.qtCohUndBid: "Coherent underlying price for the best bid",
        win32com.client.constants.qtDayHigh: "Current day high price",
        win32com.client.constants.qtDayLow: "Current day low price",
        win32com.client.constants.qtDayOpen: "Day open price",
        win32com.client.constants.qtImpliedAsk: "Implied best ask",
        win32com.client.constants.qtImpliedBid: "Implied best bid",
        win32com.client.constants.qtIndicativeOpen: "Indicative open",
        win32com.client.constants.qtMarker: "Marker price",
        win32com.client.constants.qtOutrightAsk: "Outright best ask",
        win32com.client.constants.qtOutrightBid: "Outright best bid",
        win32com.client.constants.qtSettlement: "Settlement price",
        win32com.client.constants.qtTodayMarker: "Marker price",
        win32com.client.constants.qtTrade: "Last trade price",
        win32com.client.constants.qtYesterdaySettlement: "Yesterday's settlement price"
    }[quoteType]


def TickType2String(tickType):
    return {
        win32com.client.constants.tptAsk: "Ask price",
        win32com.client.constants.tptBid: "Bid price",
        win32com.client.constants.tptSettlement: "Settlement price",
        win32com.client.constants.tptTrade: "Trade price",
    }[tickType]

def convert_orderstatus_cqg2qt(status: win32com.client.constants) -> OrderStatus:
    """https://partners.cqg.com/sites/default/files/docs/CQGAPI_4.0R/CQG~eOrderStatus.html
    """
    if status in (win32com.client.constants.osActiveAt,
        win32com.client.constants.osContingent,
        win32com.client.constants.osInCancel,
        win32com.client.constants.osInClient,
        win32com.client.constants.osInModify,
        win32com.client.constants.osInTransit,
        win32com.client.constants.osNotSent,
        win32com.client.constants.osParked,
    ):
        return OrderStatus.UNKNOWN
    elif status in (win32com.client.constants.osInOrderBook,):
        return OrderStatus.SUBMITTED
    elif status in (win32com.client.constants.osFilled, win32com.client.constants.osBusted):
        return OrderStatus.FILLED
    elif status in (win32com.client.constants.osCanceled,):
        return OrderStatus.CANCELLED
    elif status in (win32com.client.constants.osRejectFCM,
        win32com.client.constants.osRejectGW,
        win32com.client.constants.osDisconnected,
        win32com.client.constants.osExpired,
        win32com.client.constants.osInTransitTimeout
    ):
        return OrderStatus.FAILED
    else:
        raise ValueError(f"Order status {status} is not valid to be handled by qtrader.")
