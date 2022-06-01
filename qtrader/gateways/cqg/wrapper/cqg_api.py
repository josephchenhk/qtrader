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
import time
from datetime import datetime
from dataclasses import replace
from typing import Dict
import pytz

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

# Events waiting time
TIMEOUT = 10
# Default timezone we are working on
TIMEZONE = pytz.timezone("Asia/Hong_Kong")
# CQG default invalid values to be ignored
INVALID_VALUE = -2147483648
# Lock to update account_balance and positions safely
lock = threading.Lock()


class CQGAPI(CELSinkBase):
    """CQG Integrated Client API"""

    def __init__(self):
        self.eventGatewayIsUp = threading.Event()
        self.eventGatewayIsDown = threading.Event()
        self.eventAccountIsReady = threading.Event()
        self.eventOrderPlaced = threading.Event()
        self.eventQuoteDone = threading.Event()
        self.eventOrderbookDone = threading.Event()
        self.eventBarDone = threading.Event()

        # historical bar data
        self.subscribe_bars_data = {}
        # Balance will be updated with thread lock
        self.account_balance = AccountBalance()
        # Positions (list of position data) will be updated with thread lock
        self.positions = []
        # If order is submitted by algo, orderid will be casted to this
        # variable
        self.order_id = None

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
        cqg_account = self.get_account_from_gwaccountid(
            GWAccountID=self.GWAccountID)
        self.eventGatewayIsDown.clear()
        self.celEnvironment.cqgCEL.GWLogoff()
        AssertMessage(
            self.eventGatewayIsDown.wait(TIMEOUT),
            "GW disconnection timeout!")
        Trace(f"Successfully logoff account {cqg_account.GWAccountName}")

    def get_instrument_from_cqg_symbol(self, cqg_symbol: str):
        instrument_ = None
        for instrument in self.celEnvironment.cqgCEL.Instruments:
            if cqg_symbol in instrument.FullName:
                instrument_ = instrument
                break
        return instrument_

    def get_timedbars_from_cqg_symbol(self, cqg_symbol: str):
        timedbars_ = None
        for timedbars in self.celEnvironment.cqgCEL.AllTimedBars:
            if cqg_symbol in timedbars.Request.Symbol:
                timedbars_ = timedbars
                break
        return timedbars_

    def get_ticks_from_cqg_symbol(self, cqg_symbol: str):
        ticks_ = None
        for ticks in self.celEnvironment.cqgCEL.AllTicks:
            if cqg_symbol in ticks.Request.Symbol:
                ticks_ = ticks
                break
        return ticks_

    def get_account_from_gwaccountid(self, GWAccountID: int):
        accounts = win32com.client.Dispatch(
            self.celEnvironment.cqgCEL.Accounts)
        account_ = None
        for account in accounts:
            Trace(
                "GW Account id: {} name: {}".format(
                    account.GWAccountID,
                    account.GWAccountName))
            if account.GWAccountID == GWAccountID:
                Trace(
                    "[Found] GW Account id: {} name: {}".format(
                        account.GWAccountID,
                        account.GWAccountName))
                account_ = account
                break
        return account_

    def subscribe_quote(self, cqg_symbol: str):
        self.eventQuoteDone.clear()
        self.celEnvironment.cqgCEL.NewInstrument(cqg_symbol)
        Trace(
            f"Waiting for eventQuoteDone in subscribe_quote({cqg_symbol})...")
        self.eventQuoteDone.wait(TIMEOUT)
        Trace("eventQuoteDone is done.")

    def unsubscribe_quote(self, cqg_symbol: str):
        cqgQuote = self.get_instrument_from_cqg_symbol(cqg_symbol)
        if cqgQuote:
            self.celEnvironment.cqgCEL.RemoveInstrument(cqgQuote)
            self.eventQuoteDone.clear()
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

        self.eventOrderbookDone.clear()
        self.celEnvironment.cqgCEL.RequestTicks(request)
        Trace(
            "Waiting for eventOrderbookDone in subscribe_orderbook("
            f"{cqg_symbol})...")
        self.eventOrderbookDone.wait(TIMEOUT)
        Trace("eventOrderbookDone is done.")

    def unsubscribe_orderbook(self, cqg_symbol: str):
        cqgOrderbooks = self.get_ticks_from_cqg_symbol(cqg_symbol)
        if cqgOrderbooks:
            self.celEnvironment.cqgCEL.RemoveTicks(cqgOrderbooks)
            self.eventOrderbookDone.clear()
            Trace(f"Successfully unsubscribe orderbook for {cqg_symbol}")

    def subscribe_bar(
            self,
            cqg_symbol: str,
            lookback_period: int,
            freq: str = "1Min"
    ):
        FREQ_ALLOWED = ("1Day", "1Min")
        if freq not in FREQ_ALLOWED:
            raise ValueError(
                f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} "
                f"are allowed.")
        Trace(f"Create {freq} timed bars request for {cqg_symbol}")
        request = win32com.client.Dispatch(
            self.celEnvironment.cqgCEL.CreateTimedBarsRequest())
        request.Symbol = cqg_symbol
        request.RangeStart = 0
        # add one more bar, since the last one coule be in the progress of
        # updating
        request.RangeEnd = -lookback_period - 1
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

        self.eventBarDone.clear()
        self.celEnvironment.cqgCEL.RequestTimedBars(request)
        Trace(f"Waiting for eventBarDone in subscribe_bar({cqg_symbol})...")
        self.eventBarDone.wait(TIMEOUT)
        Trace("eventBarDone is done.")

    @safe_call
    def unsubscribe_bar(self, cqg_symbol: str):
        cqgTimedBars = self.get_timedbars_from_cqg_symbol(cqg_symbol)
        if cqgTimedBars:
            self.celEnvironment.cqgCEL.RemoveTimedBars(cqgTimedBars)
            security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
            self.eventBarDone.clear()
            self.subscribe_bars_data.pop(security, None)
            Trace(f"Successfully unsubscribe timedbar for {cqg_symbol}")

    def subscribe_account_and_positions(self, GWAccountID: int):
        self.celEnvironment.cqgCEL.AccountSubscriptionLevel = win32com.client.constants.aslNone
        time.sleep(2)
        self.celEnvironment.cqgCEL.AccountSubscriptionLevel = win32com.client.constants.aslAccountUpdatesAndOrders
        Trace("Waiting for accounts coming...")
        AssertMessage(
            self.eventAccountIsReady.wait(TIMEOUT),
            "Accounts coming timeout!")
        Trace(f"Select account {GWAccountID}, subscribe to open positions.")

        # save GWAccountID to the gateway here, we still need to use it to find
        # out the corresponding account.
        self.GWAccountID = GWAccountID
        account = self.get_account_from_gwaccountid(GWAccountID)
        if account is None:
            raise ValueError(f"GWAccountID {GWAccountID} can not be found!")
        account.AutoSubscribeInstruments = True
        account.PositionSubcriptionLevel = win32com.client.constants.pslSnapshotAndUpdates

        self.update_account_balance()
        self.update_positions()

    def unsubscribe_account_and_positions(self, GWAccountID: int):
        self.celEnvironment.cqgCEL.AccountSubscriptionLevel = win32com.client.constants.aslNone
        account = self.get_account_from_gwaccountid(GWAccountID)
        if account is None:
            raise ValueError(f"GWAccountID {GWAccountID} can not be found!")
        account.AutoSubscribeInstruments = False
        account.PositionSubcriptionLevel = win32com.client.constants.pslNoPositions
        Trace(f"Unsubscribe account {GWAccountID}.")

    def update_account_balance(self):
        with lock:
            # update account balance
            try:
                cqg_account = self.get_account_from_gwaccountid(
                    self.GWAccountID)
                self.account_balance = AccountBalance()

                cash = cqg_account.Summary.Balance(0)
                collaterals = cqg_account.Summary.Collaterals(1)
                avail_cash = cash - collaterals
                net_cash = cqg_account.Summary.NLV(0)
                maintenance_margin = cqg_account.Summary.MaintenanceMargin(1)
                realized_pnl = cqg_account.Summary.ProfitLoss(0)
                unrealized_pnl = cqg_account.Summary.UPL(0)

                self.account_balance.cash = cash
                self.account_balance.cash_by_currency = None
                self.account_balance.available_cash = avail_cash
                self.account_balance.max_power_short = None
                self.account_balance.net_cash_power = net_cash
                self.account_balance.maintenance_margin = maintenance_margin
                self.account_balance.realized_pnl = realized_pnl
                self.account_balance.unrealized_pnl = unrealized_pnl
            except Exception as e:
                # TODO: CQG doesn't come up with a solution yet
                print(f"{datetime.now()}: {e}")

    def update_positions(self):
        with lock:
            # update positions
            self.positions = []
            cqg_account = self.get_account_from_gwaccountid(
                GWAccountID=self.GWAccountID)
            num_pos = cqg_account.Positions.Count
            for n in range(num_pos):
                cqg_pos = cqg_account.Positions.ItemByIndex(n)
                if cqg_pos.Quantity:
                    cqg_symbol = cqg_pos.InstrumentName.split(".")[-1]
                    security = self.gateway.get_qt_security_from_cqg_symbol(
                        cqg_symbol)
                    if security is None:
                        continue
                    if cqg_pos.Side == win32com.client.constants.osdBuy:
                        direction = Direction.LONG
                    elif cqg_pos.Side == win32com.client.constants.osdSell:
                        direction = Direction.SHORT
                    else:
                        raise ValueError(
                            f"Return CQG Position Side {cqg_pos.Side} is "
                            "invalid!")
                    position_data = PositionData(
                        security=security,
                        direction=direction,
                        holding_price=cqg_pos.AveragePrice,
                        quantity=abs(cqg_pos.Quantity),
                        update_time=datetime.now(),
                    )
                    self.positions.append(position_data)

    def get_subscribe_bars_data(self, security):
        bars = self.subscribe_bars_data.get(security)

        if bars is None or len(bars) == 0:
            return

        # forward fill the bar if necessary
        for i in list(range(1, len(bars), 1)):
            bar = bars[i]
            prev_bar = bars[i - 1]
            bar.open = (
                prev_bar.open
                if (bar.open == INVALID_VALUE and prev_bar.open != INVALID_VALUE)
                else bar.open
            )
            bar.high = (
                prev_bar.high
                if (bar.high == INVALID_VALUE and prev_bar.high != INVALID_VALUE)
                else bar.high
            )
            bar.low = (
                prev_bar.low
                if (bar.low == INVALID_VALUE and prev_bar.low != INVALID_VALUE)
                else bar.low
            )
            bar.close = (
                prev_bar.close
                if (bar.close == INVALID_VALUE and prev_bar.close != INVALID_VALUE)
                else bar.close
            )
            bar.volume = (
                prev_bar.volume
                if (bar.volume == INVALID_VALUE and prev_bar.volume != INVALID_VALUE)
                else bar.volume
            )
            bars[i] = bar

        # backfill the bar if necessary
        for i in list(range(len(bars) - 2, -1, -1)):
            bar = bars[i]
            next_bar = bars[i + 1]
            bar.open = (
                next_bar.open
                if (bar.open == INVALID_VALUE and next_bar.open != INVALID_VALUE)
                else bar.open
            )
            bar.high = (
                next_bar.high
                if (bar.high == INVALID_VALUE and next_bar.high != INVALID_VALUE)
                else bar.high
            )
            bar.low = (
                next_bar.low
                if (bar.low == INVALID_VALUE and next_bar.low != INVALID_VALUE)
                else bar.low
            )
            bar.close = (
                next_bar.close
                if (bar.close == INVALID_VALUE and next_bar.close != INVALID_VALUE)
                else bar.close
            )
            bar.volume = (
                next_bar.volume
                if (bar.volume == INVALID_VALUE and next_bar.volume != INVALID_VALUE)
                else bar.volume
            )
            bars[i] = bar

        # The last bar must be valid
        last_bar = bars[-1]
        if (
            last_bar.open == INVALID_VALUE
            or last_bar.high == INVALID_VALUE
            or last_bar.low == INVALID_VALUE
            or last_bar.close == INVALID_VALUE
            or last_bar.volume == INVALID_VALUE
        ):
            raise ValueError(f"The latest bar is INVALID: {last_bar}.")
        return bars

    ##########################################################################
    #                                                                        #
    #                RealTimeMarketDataReceiving.py                          #
    ##########################################################################

    def OnDataError(self, cqgError, errorDescription):
        # TODO: Handle different errors separately
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace(
                "OnDataError: Code: {} Description: {}".format(
                    dispatchedCQGError.Code,
                    dispatchedCQGError.Description))

    def OnInstrumentResolved(self, symbol, cqgInstrument, cqgError):
        if cqgError is not None:
            if not self.eventQuoteDone.is_set():
                self.eventQuoteDone.set()
            return
        instrument = win32com.client.Dispatch(cqgInstrument)
        instrument.DataSubscriptionLevel = win32com.client.constants.dsQuotesAndBBA
        if not self.eventQuoteDone.is_set():
            self.eventQuoteDone.set()

    def OnInstrumentChanged(
            self,
            cqgInstrument,
            cqgQuotes,
            cqgInstrumentProperties
    ):
        instrument = win32com.client.Dispatch(cqgInstrument)
        quotes = win32com.client.Dispatch(cqgQuotes)
        for quote in quotes:
            if (quote.IsValid):
                cqg_symbol = instrument.FullName.split(".")[-1]
                security = self.gateway.get_qt_security_from_cqg_symbol(
                    cqg_symbol)
                if security is None:
                    continue
                if self.gateway.cqg_quotes[security] is None:
                    qt_quote = Quote(
                        security=security,
                        exchange=security.exchange,
                        datetime=datetime.now()
                    )
                else:
                    qt_quote = self.gateway.cqg_quotes[security]

                if QuoteType2String(quote.Type) == "Last trade price":
                    qt_quote = replace(
                        qt_quote,
                        last_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Best bid":
                    qt_quote = replace(
                        qt_quote,
                        bid_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Best ask":
                    qt_quote = replace(
                        qt_quote,
                        ask_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Day open price":
                    qt_quote = replace(
                        qt_quote,
                        open_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Current day high price":
                    qt_quote = replace(
                        qt_quote,
                        high_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Current day low price":
                    qt_quote = replace(
                        qt_quote,
                        low_price=quote.price,
                        datetime=datetime.now())
                elif QuoteType2String(quote.Type) == "Yesterday's settlement price":
                    qt_quote = replace(
                        qt_quote,
                        prev_close_price=quote.price,
                        datetime=datetime.now())
                # else:
                #     print("QuoteType:", QuoteType2String(quote.Type))
                self.gateway.cqg_quotes[security] = qt_quote
                if (
                    qt_quote.last_price != 0
                    or (qt_quote.bid_price != 0 and qt_quote.ask_price != 0)
                ):
                    self.gateway.process_quote(qt_quote)

    ##########################################################################
    #                                                                        #
    #                    TicksRequestRealtime.py                             #
    ##########################################################################

    def OnTicksResolved(self, cqgTicks, cqgError):
        if cqgError is not None:
            if not self.eventOrderbookDone.is_set():
                self.eventOrderbookDone.set()
            return
        if not self.eventOrderbookDone.is_set():
            self.eventOrderbookDone.set()

    def OnTicksAdded(self, cqgTicks, addedTicksCount):
        dispatchedCQGTicks = win32com.client.Dispatch(cqgTicks)
        ticksCount = range(
            dispatchedCQGTicks.Count - addedTicksCount,
            dispatchedCQGTicks.Count)
        for i in ticksCount:
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
            if (
                orderbook.bid_price_1 != 0
                and orderbook.ask_price_1 != 0
                and orderbook.bid_volume_1 != 0
                and orderbook.ask_volume_1 != 0
            ):
                self.gateway.process_orderbook(orderbook)

    def OnTicksRemoved(self, cqgTicks, removedTickIndex):
        pass

    ##########################################################################
    #                                                                        #
    #                         TimedBarsDaily.py                              #
    ##########################################################################

    def OnTimedBarsResolved(self, cqgTimedBars, cqgError):
        """
        Ref: https://partners.cqg.com/sites/default/files/docs/CQGAPI_4.0R/webframe.html#CQG~CQGTimedBar_members.html
        """
        # print(f"[1] OnTimedBarsResolved: {datetime.now()}")
        if (cqgError is not None):
            if not self.eventBarDone.is_set():
                self.eventBarDone.set()
            return
        bars = win32com.client.Dispatch(cqgTimedBars)
        if bars.Count == 0:
            if not self.eventBarDone.is_set():
                self.eventBarDone.set()
            return
        cqg_symbol = bars.Request.Symbol
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        # if we already had bar data list, we will update it in
        # OnTimedBarsAdded method
        if self.subscribe_bars_data.get(security):
            if not self.eventBarDone.is_set():
                self.eventBarDone.set()
            return
        self.subscribe_bars_data[security] = []
        # Do not use the last bar as it might be in the progress of updating
        for i in range(0, bars.Count - 1):
            cqg_bar = bars.Item(i)
            bar_time = cqg_bar.Timestamp
            bar_time = datetime(
                bar_time.year,
                bar_time.month,
                bar_time.day,
                bar_time.hour,
                bar_time.minute,
                bar_time.second)
            bar = Bar(
                datetime=bar_time,
                security=security,
                open=cqg_bar.Open,
                high=cqg_bar.High,
                low=cqg_bar.Low,
                close=cqg_bar.Close,
                volume=cqg_bar.ActualVolume)
            # If the value is INVALID_VALUE (usually means there is no done
            # deal during this period), we will use a forward fill to make
            # the data meaningful in method `get_subscribe_bars_data`
            self.subscribe_bars_data[security].append(bar)
        if not self.eventBarDone.is_set():
            self.eventBarDone.set()

    def OnTimedBarsAdded(self, cqgTimedBars):
        # print(f"[1] OnTimedBarsAdded: {datetime.now()}")
        bars = win32com.client.Dispatch(cqgTimedBars)
        # print(f"Number of bars: {bars.Count}")

        if bars.Count < 2:
            return

        cqg_symbol = bars.Request.Symbol
        security = self.gateway.get_qt_security_from_cqg_symbol(cqg_symbol)
        assert self.subscribe_bars_data[security] is not None, (
            "We should have initiated data in OnTimedBarsResolved first!"
        )
        # the last bar is added, so the 2nd last has been completed
        cqg_bar = bars.Item(bars.Count - 2)
        bar_time = cqg_bar.Timestamp
        bar_time = datetime(
            bar_time.year,
            bar_time.month,
            bar_time.day,
            bar_time.hour,
            bar_time.minute,
            bar_time.second)
        bar = Bar(
            datetime=bar_time,
            security=security,
            open=cqg_bar.Open,
            high=cqg_bar.High,
            low=cqg_bar.Low,
            close=cqg_bar.Close,
            volume=cqg_bar.ActualVolume)
        last_bar = self.subscribe_bars_data[security][-1]
        if bar.datetime > last_bar.datetime:
            print(f"[2] OnTimedBarsAdded: {bar}")
            self.subscribe_bars_data[security].pop(0)
            self.subscribe_bars_data[security].append(bar)

    def OnTimedBarsUpdated(self, cqgTimedBars, index):
        # print(f"OnTimedBarsUpdated: {datetime.now()}, {index}")
        pass

    def OnTimedBarsRemoved(self, cqgTimedBars, index):
        # print(f"OnTimedBarsRemoved: {datetime.now()}")
        pass

    def dumpBar(self, bar, index):
        Trace(
            "Bar index: {} Timestamp {} Open {} High {} Low {} Close {} "
            "ActualVolumeFractional {} CommodityVolumeFractional {} "
            "ContractVolumeFractional {} TickVolume {}".format(
                index,
                bar.Timestamp,
                bar.Open,
                bar.High,
                bar.Low,
                bar.Close,
                bar.ActualVolumeFractional,
                bar.CommodityVolumeFractional,
                bar.ContractVolumeFractional,
                bar.TickVolume))

    ##########################################################################
    #                                                                        #
    #                        OrdersPlacing.py                                #
    ##########################################################################

    def place_order(self, order: Order) -> str:
        cqg_symbol = self.gateway.get_cqg_symbol_from_qt_security(
            order.security)
        instrument = self.get_instrument_from_cqg_symbol(cqg_symbol)
        if instrument is None:
            raise ValueError(
                f"cqg_symbol {cqg_symbol} has not been subscribed.")
        # dispatchedInstrument = win32com.client.Dispatch(instrument)
        if order.order_type == OrderType.LIMIT:
            order_type = win32com.client.constants.otLimit
        elif order.order_type == OrderType.MARKET:
            order_type = win32com.client.constants.otMarket
        elif order.order_type == OrderType.STOP:
            order_type = win32com.client.constants.otStop
        else:
            raise ValueError(f"Unsupported order type {order.order_type}")

        if order.direction == Direction.LONG:
            order_side = win32com.client.constants.osdBuy
        elif order.direction == Direction.SHORT:
            order_side = win32com.client.constants.osdSell
        else:
            raise ValueError(f"Unsupported order side {order.direction}")

        cqg_account = self.get_account_from_gwaccountid(
            GWAccountID=self.GWAccountID)
        if order.order_type == OrderType.MARKET:
            cqgOrder = win32com.client.Dispatch(
                self.celEnvironment.cqgCEL.CreateOrder(
                    order_type,
                    instrument,
                    cqg_account,
                    order.quantity,
                    order_side)
            )
        elif order.order_type == OrderType.LIMIT:
            cqgOrder = win32com.client.Dispatch(
                self.celEnvironment.cqgCEL.CreateOrder(
                    order_type,
                    instrument,
                    cqg_account,
                    order.quantity,
                    order_side,
                    order.price)
            )
        elif order.order_type == OrderType.STOP:
            cqgOrder = win32com.client.Dispatch(
                self.celEnvironment.cqgCEL.CreateOrder(
                    order_type,
                    instrument,
                    cqg_account,
                    order.quantity,
                    order_side,
                    order.price,
                    order.stop_price)
            )

        # Update 2022-03-10: set Manual flag to false as we are
        # doing automatic trading
        cqgOrder.Manual = 0

        # cqgOrder.QuantityFractional = 1

        # Blocking here, wait for orderid received in OnOrderChanged
        self.eventOrderPlaced.clear()
        cqgOrder.Place()
        AssertMessage(
            self.eventOrderPlaced.wait(TIMEOUT),
            f"Order placing timeout!\n{order} ")
        orderid = self.order_id
        self.order_id = None
        if orderid is None:
            raise ValueError(f"Order id can not be obtained!\n{order}")
        return orderid

    def cancel_order(self, orderid: str):
        cqgOrder = self.celEnvironment.cqgCEL.Orders.ItemByGuid(orderid)
        cqgOrder.Cancel()

    def OnAccountChanged(self, change, cqgAccount, cqgPosition):
        if change == win32com.client.constants.actPositionAdded:
            account = win32com.client.Dispatch(cqgAccount)
            position = win32com.client.Dispatch(cqgPosition)
            Trace(
                "OnAccountChanged - open position is added for {} account - "
                "instrument: {} average price: {} quantity: {} OTE: {} PL: {}"
                "".format(
                    account.GWAccountName,
                    position.InstrumentName,
                    position.AveragePrice,
                    position.Quantity,
                    position.OTE,
                    position.ProfitLoss))

        if change == win32com.client.constants.actPositionChanged:
            account = win32com.client.Dispatch(cqgAccount)
            position = win32com.client.Dispatch(cqgPosition)
            Trace(
                "OnAccountChanged - open position is changed for {} account - "
                "instrument: {} average price: {} quantity: {} OTE: {} PL: {}"
                "".format(
                    account.GWAccountName,
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
        server_time = properties(
            win32com.client.constants.opEventServerTimestamp).Value

        updated_time = datetime(
            year=server_time.year,
            month=server_time.month,
            day=server_time.day,
            hour=server_time.hour,
            minute=server_time.minute,
            second=server_time.second)
        filled_avg_price = properties(
            win32com.client.constants.opAverageFillPrice).Value
        filled_quantity = properties(
            win32com.client.constants.opFilledQuantity).Value
        status = convert_orderstatus_cqg2qt(gwStatus.Value)
        order_id = guid.Value

        if changeType == win32com.client.constants.ctAdded:
            if self.eventOrderPlaced.is_set():
                print(f"orderid {order_id} is not placed by algo")
                return
            self.order_id = order_id
            self.eventOrderPlaced.set()
        elif changeType in (
                win32com.client.constants.ctChanged,
                win32com.client.constants.ctRemoved
        ):
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

    def OnGWConnectionStatusChanged(self, connectionStatus):
        """
        Value	                | number  |  Description
        ------------------------|---------|--------------------------------------
        csConnectionDelayed	        1	     Connection is delayed.
        csConnectionDown	        2	     Connection is down.
        csConnectionNotLoggedOn  	4	     Connected but not logged on Gateway.
        csConnectionTrouble	        3	     Trouble with Gateway connection.
        csConnectionUp	            0	     Connection is up.

        :param newStatus:
        :return:
        """
        if connectionStatus == win32com.client.constants.csConnectionUp:
            Trace("GW connection is UP!")
            self.eventGatewayIsUp.set()
        if connectionStatus == win32com.client.constants.csConnectionDown:
            Trace("GW connection is DOWN!")
            self.eventGatewayIsDown.set()

    def OnDataConnectionStatusChanged(self, newStatus):
        """
        Value	                | number  |  Description
        ------------------------|---------|--------------------------------------
        csConnectionDelayed	        1	     Connection is delayed.
        csConnectionDown	        2	     Connection is down.
        csConnectionNotLoggedOn  	4	     Connected but not logged on Gateway.
        csConnectionTrouble	        3	     Trouble with Gateway connection.
        csConnectionUp	            0	     Connection is up.

        :param newStatus:
        :return:
        """
        print(f"DataConnectionStatus={newStatus}")


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


def convert_orderstatus_cqg2qt(
        status: win32com.client.constants) -> OrderStatus:
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
        raise ValueError(
            f"Order status {status} is not valid to be handled by qtrader.")
