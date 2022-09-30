# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:58 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: backtest_gateway.py
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
from datetime import datetime
from datetime import timedelta
from datetime import time as Time
from typing import List, Dict, Union
from dateutil.relativedelta import relativedelta

import pandas as pd

from qtrader_config import DATA_PATH, DATA_MODEL, TIME_STEP
from qtrader.core.balance import AccountBalance
from qtrader.core.constants import TradeMode, OrderStatus, Direction, OrderType
from qtrader.core.data import Quote
from qtrader.core.data import OrderBook
from qtrader.core.data import Bar
from qtrader.core.data import CapitalDistribution
from qtrader.core.data import get_trading_day
from qtrader.core.data import _load_historical_bars_in_reverse
from qtrader.core.data import _get_data
from qtrader.core.data import _get_data_path
from qtrader.core.data import _get_data_iterator
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.position import PositionData
from qtrader.core.security import Stock, Security
from qtrader.core.utility import is_trading_time
from qtrader.gateways import BaseGateway
from qtrader.gateways.base_gateway import BaseFees


assert set(DATA_PATH.keys()) == set(DATA_MODEL.keys()), (
    "`DATA_PATH` and `DATA_MODEL` keys are not aligned! Please check "
    "qtrader_config.py"
)


class BacktestFees(BaseFees):
    """
    Backtest fee model
    """

    def __init__(self, *deals: Deal):
        # Platform fees (to the platform)
        commissions = 0
        platform_fees = 0
        # Agency fees (to other parties such as exchange, tax authorities)
        system_fees = 0
        settlement_fees = 0
        stamp_fees = 0
        trade_fees = 0
        transaction_fees = 0

        for deal in deals:
            price = deal.filled_avg_price
            quantity = deal.filled_quantity
            commissions += price * quantity * 0.0005  # assume 5%% cost in general

        # Total fees
        total_fees = (
            commissions
            + platform_fees
            + system_fees
            + settlement_fees
            + stamp_fees
            + trade_fees
            + transaction_fees
        )

        self.commissions = commissions
        self.platform_fees = platform_fees
        self.system_fees = system_fees
        self.settlement_fees = settlement_fees
        self.stamp_fees = stamp_fees
        self.trade_fees = trade_fees
        self.transaction_fees = transaction_fees
        self.total_fees = total_fees


class BacktestGateway(BaseGateway):
    """Backtest gateway"""

    # Minimal time step, which was read from config
    TIME_STEP = TIME_STEP

    # Short interest rate, e.g., 0.0098 for HK stock
    SHORT_INTEREST_RATE = 0.0

    # Name of the gateway
    NAME = "BACKTEST"

    # Specified data types
    DTYPES = None

    def __init__(
            self,
            securities: List[Stock],
            gateway_name: str,
            start: datetime,
            end: datetime,
            dtypes: Dict[str, List[str]] = dict(
                kline=["time_key",
                       "open",
                       "high",
                       "low",
                       "close",
                       "volume"]),
            fees: BaseFees = BacktestFees,
            **kwargs
    ):
        self.set_trade_mode(TradeMode.BACKTEST)
        self.DTYPES = dtypes
        assert (set(dtypes.keys()) == set(DATA_PATH.keys())), (
            f"In __init__ of {self.__class__.__name__}, "
            "the input param `dtypes` must be consistent with "
            f"config in DATA_PATH({DATA_PATH}).\nAccording to "
            f"DATA_PATH, `dtypes` should include: {','.join(DATA_PATH.keys())},"
            f"however, only the followings were passed in: "
            f"{','.join(dtypes.keys())}.")
        if "trading_sessions" in kwargs:
            super().__init__(
                securities=securities,
                gateway_name=gateway_name,
                trading_sessions=kwargs.get("trading_sessions")
            )
        else:
            super().__init__(
                securities=securities,
                gateway_name=gateway_name
            )
        self.fees = fees

        # if DATA_FFILL:
        #     first_time_key = None
        #     time_keys = set()
        #     for security in securities:
        #         for dfield in DATA_PATH.keys():  # kline | capdist
        #             # Read data and get its time_keys
        #             data = _get_data(
        #                 security=security,
        #                 start=start,
        #                 end=end,
        #                 dfield=dfield,
        #                 dtype=dtypes[dfield])
        #             if first_time_key is None:
        #                 first_time_key = data.iloc[0]["time_key"]
        #             else:
        #                 first_time_key = max(
        #                     first_time_key,
        #                     data.iloc[0]["time_key"]
        #                 )
        #             time_keys.update(data.time_key.to_list())
        #     time_keys = sorted(time_keys)
        #     time_keys = [t for t in time_keys if t >= first_time_key]

        data_iterators = dict()
        prev_cache = dict()
        next_cache = dict()
        trading_days = dict()
        for security in securities:
            data_iterators[security] = dict()
            prev_cache[security] = dict()
            next_cache[security] = dict()
            for dfield in DATA_PATH.keys():  # kline | capdist
                # data_iterators is a dictionary that stores data iterators
                data = _get_data(
                    security=security,
                    start=start,
                    end=end,
                    dfield=dfield,
                    dtype=dtypes[dfield])
                # if DATA_FFILL:
                #     data = data[data.time_key >= first_time_key]
                #     ffill_data = pd.DataFrame(index=time_keys)
                #     ffill_data = ffill_data.join(data.set_index('time_key')).sort_index()
                #     ffill_data = ffill_data.reset_index().rename(columns={"index": "time_key"})
                #     ffill_data['open'] = ffill_data['open'].ffill()
                #     ffill_data['high'] = ffill_data['high'].ffill()
                #     ffill_data['low'] = ffill_data['low'].ffill()
                #     ffill_data['close'] = ffill_data['close'].ffill()
                #     ffill_data['volume'] = ffill_data['volume'].fillna(0)
                #     data = ffill_data
                data_it = _get_data_iterator(
                    security=security,
                    full_data=data,
                    class_name=DATA_MODEL[dfield])
                data_iterators[security][dfield] = data_it
                # initialize cache data
                prev_cache[security][dfield] = None
                next_cache[security][dfield] = None
                # Sort the available dates in history
                if dfield == "kline":
                    trading_days[security] = sorted(
                        set(pd.to_datetime(t).strftime("%Y-%m-%d")
                            for t in data["time_key"].values))
        self.data_iterators = data_iterators
        self.prev_cache = prev_cache
        self.next_cache = next_cache
        self.trading_days = trading_days
        trading_days_list = set()
        for k, v in self.trading_days.items():
            trading_days_list.update(v)
        self.trading_days_list = [datetime.strptime(
            d, "%Y-%m-%d").date() for d in sorted(trading_days_list)]

        self.start = start
        self.end = end
        self.market_datetime = start

    def close(self):
        """In backtest, no need to do anything"""
        pass

    def set_trade_mode(self, trade_mode: TradeMode):
        """Set trade mode (only BACKTEST is allowed here as it is the backtest
        gateway)"""
        if trade_mode != TradeMode.BACKTEST:
            raise ValueError(
                f"BacktestGateway only supports `BACKTEST` mode, {trade_mode} "
                "is invalid.")
        self.trade_mode = trade_mode

    def get_next_session_datetime(
            self,
            security: Security,
            cur_datetime: datetime) -> int:
        """return start datetime of next session
        """
        trading_sessions = self.trading_sessions[security.code]

        trading_day = cur_datetime.date()
        trading_day_sessions = []
        # normal trading day session within a calendar day
        if trading_sessions[-1][1] > trading_sessions[0][0]:
            for start, end in trading_sessions:
                session_start = datetime.combine(trading_day, start.time())
                session_end = datetime.combine(trading_day, end.time())
                trading_day_sessions.append([session_start, session_end])
        # trading day session crosses two calendar days
        else:
            next_trading_day = trading_day + timedelta(days=1)
            is_next_day = False
            for idx, (start, end) in enumerate(trading_sessions):
                if is_next_day:
                    session_start = datetime.combine(
                        next_trading_day, start.time())
                    session_end = datetime.combine(
                        next_trading_day, end.time())
                    trading_day_sessions.append([session_start, session_end])
                elif end.time() < start.time():
                    session_start = datetime.combine(trading_day, start.time())
                    session_end = datetime.combine(
                        next_trading_day, end.time())
                    trading_day_sessions.append([session_start, session_end])
                    is_next_day = True
                elif end.time() >= start.time():
                    session_start = datetime.combine(trading_day, start.time())
                    session_end = datetime.combine(trading_day, end.time())
                    trading_day_sessions.append([session_start, session_end])
                    if idx < len(trading_sessions) - 1:
                        next_start, next_end = trading_sessions[idx + 1]
                        if next_start.time() < end.time():
                            is_next_day = True

        for session in trading_day_sessions:
            if cur_datetime < session[0]:
                return session[0]
        return trading_day_sessions[0][0] + timedelta(days=1)

    def is_trading_time(self, cur_datetime: datetime) -> bool:
        """For given datetime, check whether it is in trading hours"""
        is_trading_day = cur_datetime.date() in self.trading_days_list
        if not is_trading_day:
            return False
        # If any security is found in trading session, we return True
        _is_trading_time = False
        for security in self.securities:
            _is_trading_time = self.is_security_trading_time(
                security, cur_datetime.time())
            if _is_trading_time:
                break
        return _is_trading_time

    def next_trading_datetime(
            self,
            cur_datetime: datetime,
            security: Security
    ) -> datetime:
        """Find next trading datetime; return None if not found"""
        # check whether cur_datetime is within the trading session
        _cur_is_trading_time = self.is_security_trading_time(
            security, cur_datetime.time())
        if _cur_is_trading_time:
            # Move one time step
            next_datetime = (
                cur_datetime
                + relativedelta(seconds=self.TIME_STEP / 1000.0)
            )
        else:
            # Move to openning time of next trading session
            next_datetime = self.get_next_session_datetime(
                security=security,
                cur_datetime=cur_datetime)
        return next_datetime

    def get_recent_data(
            self,
            security: Stock,
            cur_datetime: datetime,
            **kwargs
    ) -> Dict or Bar or CapitalDistribution:
        """Get recent data"""
        assert cur_datetime >= self.market_datetime, (
            f"Current datetime {cur_datetime} is earlier than "
            f"market datetime {self.market_datetime}."
        )
        if kwargs:
            assert "dfield" in kwargs, (
                f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
            )
            dfields = [kwargs["dfield"]]
        else:
            dfields = DATA_PATH
        data_it = dict()
        data_prev = dict()
        data_next = dict()
        for dfield in dfields:
            data_it[dfield] = self.data_iterators[security][dfield]
            data_prev[dfield] = self.prev_cache[security][dfield]
            data_next[dfield] = self.next_cache[security][dfield]

            if cur_datetime > self.end:
                pass

            elif (data_prev[dfield] is None) and (data_next[dfield] is None):
                data = next(data_it[dfield])
                if data.datetime > cur_datetime:
                    self.next_cache[security][dfield] = data
                else:
                    while data.datetime <= cur_datetime:
                        self.prev_cache[security][dfield] = data
                        data = next(data_it[dfield])
                    self.next_cache[security][dfield] = data

            else:
                if self.next_cache[security][dfield].datetime <= cur_datetime:
                    self.prev_cache[security][dfield] = self.next_cache[security][dfield]
                    try:
                        data = next(data_it[dfield])
                        while data.datetime <= cur_datetime:
                            self.prev_cache[security][dfield] = data
                            data = next(data_it[dfield])
                        self.next_cache[security][dfield] = data
                    except StopIteration:
                        pass

        self.market_datetime = cur_datetime
        if len(dfields) == 1:
            return self.prev_cache[security][dfield]
        return self.prev_cache[security]

    def place_order(self, order: Order) -> str:
        """In backtest, simply assume all orders are completely filled."""
        order.filled_time = self.market_datetime
        order.filled_quantity = order.quantity
        if order.order_type == OrderType.LIMIT:
            order.filled_avg_price = order.price
        elif order.order_type == OrderType.MARKET:
            bar = self.get_recent_data(order.security, order.create_time)
            if bar is not None:
                order.filled_avg_price = bar.close
            if order.filled_avg_price is None:
                raise ValueError("filled_avg_price is NOT available!")
        order.status = OrderStatus.FILLED
        orderid = "bt-order-" + str(uuid.uuid4())
        dealid = "bt-deal-" + str(uuid.uuid4())
        self.orders.put(orderid, order)

        deal = Deal(
            security=order.security,
            direction=order.direction,
            offset=order.offset,
            order_type=order.order_type,
            updated_time=self.market_datetime,
            filled_avg_price=order.filled_avg_price,
            filled_quantity=order.filled_quantity,
            dealid=dealid,
            orderid=orderid
        )
        self.deals.put(dealid, deal)
        return orderid

    def cancel_order(self, orderid):
        """Cancel order"""
        order = self.orders.get(orderid)
        if (
            order.status in (
                OrderStatus.FILLED,
                OrderStatus.CANCELLED,
                OrderStatus.FAILED)
        ):
            print(
                f"Fail to cancel order {orderid}, "
                f"its status is: {order.status}."
            )
            return
        order.status = OrderStatus.CANCELLED
        self.orders.put(orderid, order)

    def get_broker_balance(self) -> AccountBalance:
        """Not available for Backtest"""
        return None

    def get_broker_position(
            self,
            security: Stock,
            direction: Direction) -> PositionData:
        """Not available for Backtest"""
        return None

    def get_all_broker_positions(self) -> List[PositionData]:
        """Not available for Backtest"""
        return None

    def get_quote(self, security: Stock) -> Quote:
        """Not available for Backtest"""
        return None

    def get_orderbook(self, security: Stock) -> OrderBook:
        """Not available for Backtest"""
        return None

    def req_historical_bars(
            self,
            security: Security,
            periods: int,
            freq: str,
            cur_datetime: datetime,
            trading_sessions: List[datetime] = None,
            mode: str = "direct"
    ) -> List[Bar]:
        """request historical bar data.

        mode:
            'aggregate': use 1 min bar data to aggregate the different
                         granularity of bars. If in this mode, 'daily_open_time'
                         and 'daily_close_time' must also be provided.
            'direct': (default) directly load the corresponding granularity of
                      bars from CSV.
        """
        # Check params
        if mode not in ("aggregate", "direct"):
            raise ValueError(
                f"mode {mode} is invalid; only 'aggregate' or 'direct' are "
                "allowed.")
        if (
            freq == "1Day"
            and mode == "aggregate"
            and (trading_sessions is None or len(trading_sessions) == 0)
        ):
            raise ValueError(
                f"Parameters trading_sessions is mandatory if freq={freq} and "
                f"mode={mode}.")

        # return historical bar data
        if "Min" in freq:
            return _req_historical_min_bars(
                security=security,
                periods=periods,
                cur_datetime=cur_datetime,
                trading_sessions=trading_sessions,
                mode=mode,
                interval=freq.lower()
            )
        elif freq == "1Day":
            return _req_historical_day_bars(
                security=security,
                periods=periods,
                cur_datetime=cur_datetime,
                trading_sessions=trading_sessions,
                mode=mode,
                interval=freq.lower()
            )

        # freq is not valid
        FREQ_ALLOWED = ("1Day", "1Min", "10Min")
        raise ValueError(
            f"Parameter freq={freq} is Not supported. Only {FREQ_ALLOWED} are "
            "allowed.")


def _req_historical_min_bars(
        security: Security,
        periods: int,
        cur_datetime: datetime = None,
        trading_sessions: List[datetime] = None,
        mode: str = "direct",
        interval: str = "1min"
) -> List[Bar]:
    """Request historical 1min/10min bars."""
    # TODO: aggregate mode has not been finished.
    if mode not in ("direct", ):
        raise ValueError(f"mode {mode} is invalid; only 'direct' is allowed.")
    if mode == "aggregate":
        data_path = _get_data_path(security, "kline", interval="1min")
    elif mode == "direct":
        data_path = _get_data_path(security, "kline", interval=interval)

    # Use trading sessions to determine daily open & close time
    daily_open_time = None
    daily_close_time = None
    if (
        (trading_sessions is not None)
        and (len(trading_sessions) > 0)
    ):
        daily_open_time = trading_sessions[0][0].time()
        daily_close_time = trading_sessions[-1][1].time()

    interval_value = int(interval.replace("min", ""))
    hist_csv_files = _load_historical_bars_in_reverse(security, cur_datetime)
    bars = []
    for n, hist_csv_file in enumerate(hist_csv_files):
        df = pd.read_csv(f"{data_path}/{hist_csv_file}")
        df["time_key"] = pd.to_datetime(df["time_key"])
        df = df[df.time_key <= cur_datetime] # if n == 0 else df
        for _, row in df.iloc[::-1].iterrows():
            bar_datetime = row.time_key.to_pydatetime()
            if (
                (daily_open_time is not None)
                and (daily_close_time is not None)
                and (daily_open_time < daily_close_time)
                and (bar_datetime.time() < daily_open_time
                     or bar_datetime.time() > daily_close_time)
            ):
                continue
            elif (
                (daily_open_time is not None)
                and (daily_close_time is not None)
                and (daily_open_time > daily_close_time)
                and (daily_close_time < bar_datetime.time() < daily_open_time)
            ):
                continue

            bar = Bar(
                security=security,
                datetime=bar_datetime,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume
            )

            # fill with previous bar if current bar is not available
            if len(bars) == 0:
                cur_dt = datetime(
                    year=cur_datetime.year,
                    month=cur_datetime.month,
                    day=cur_datetime.day,
                    hour=cur_datetime.hour,
                    minute=cur_datetime.minute,
                    second=cur_datetime.second
                )
                while cur_dt > bar_datetime:
                    ffill_bar_datetime = cur_dt
                    _is_trading_time = is_trading_time(
                        cur_time=ffill_bar_datetime.time(),
                        trading_sessions=trading_sessions
                    )
                    if _is_trading_time:
                        ffill_bar = Bar(
                            security=security,
                            datetime=ffill_bar_datetime,
                            open=row.open,
                            high=row.high,
                            low=row.low,
                            close=row.close,
                            volume=row.volume
                        )
                        bars.append(ffill_bar)
                    cur_dt -= timedelta(minutes=interval_value)
            else:
                time_delta = int(
                    (bars[-1].datetime - bar_datetime).total_seconds() / 60)
                while time_delta > interval_value:
                    ffill_bar_datetime = cur_dt
                    ffill_bar_datetime = (
                        bars[-1].datetime
                        - timedelta(minutes=interval_value)
                    )
                    _is_trading_time = is_trading_time(
                        cur_time=ffill_bar_datetime.time(),
                        trading_sessions=trading_sessions
                    )
                    if _is_trading_time:
                        ffill_bar = Bar(
                            security=security,
                            datetime=ffill_bar_datetime,
                            open=row.open,
                            high=row.high,
                            low=row.low,
                            close=row.close,
                            volume=row.volume
                        )
                        bars.append(ffill_bar)
                    time_delta -= interval_value

            bars.append(bar)
            if len(bars) >= periods:
                return bars[:periods][::-1]
    raise ValueError(
        f"There is not sufficient historical 1min data for {security.code}. "
        f"We want {periods} data points, but only got {len(bars)}.")


def _req_historical_day_bars(
        security: Security,
        periods: int,
        cur_datetime: datetime = None,
        trading_sessions: List[datetime] = None,
        mode: str = "direct",
        interval: str = "1day"
) -> List[Bar]:
    """Request historical daily bars."""
    if mode not in ("aggregate", "direct"):
        raise ValueError(
            f"mode {mode} is invalid; only 'aggregate' or 'direct' are allowed.")
    if mode == "aggregate":
        data_path = _get_data_path(security, "kline", interval="1min")
    elif mode == "direct":
        data_path = _get_data_path(security, "kline", interval=interval)

    # Use trading sessions to determine daily open & close time
    daily_open_time = None
    daily_close_time = None
    if (
        (trading_sessions is not None)
        and (len(trading_sessions) > 0)
    ):
        daily_open_time = trading_sessions[0][0].time()
        daily_close_time = trading_sessions[-1][1].time()

    if mode == "direct":
        hist_csv_files = _load_historical_bars_in_reverse(
            security,
            cur_datetime,
            interval=interval)
        hist_csv_files = sorted(hist_csv_files, reverse=False)
        hist_data = pd.DataFrame()
        for n, hist_csv_file in enumerate(hist_csv_files):
            df = pd.read_csv(f"{data_path}/{hist_csv_file}")
            df["time_key"] = pd.to_datetime(df["time_key"])
            hist_data = pd.concat([hist_data, df])
        hist_data = hist_data[hist_data.time_key < datetime.combine(
            cur_datetime.date(), Time(0, 0, 0))]
        assert hist_data.shape[0] >= periods, (
            f"There is not sufficient historical 1day data for {security.code}."
            f" We want {periods} data points, but only got "
            f"{hist_data.shape[0]}.")
        bars = []
        for _, row in hist_data.iloc[-periods:].iterrows():
            bar = Bar(
                security=security,
                datetime=row.time_key.to_pydatetime(),
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume
            )
            bars.append(bar)
        return bars

    elif mode == "aggregate":
        assert daily_open_time != daily_close_time, (
            "open and close time could not be the same!")
        hist_csv_files = _load_historical_bars_in_reverse(
            security, cur_datetime)
        bars = []
        bar_in_progress = False
        trading_day = None
        count = 0
        for n, hist_csv_file in enumerate(hist_csv_files):
            df = pd.read_csv(
                f"{data_path}/{hist_csv_file}")
            df["time_key"] = pd.to_datetime(df["time_key"])
            df = df[df.time_key < cur_datetime] if n == 0 else df
            for _, row in df.iloc[::-1].iterrows():
                bar_datetime = row.time_key.to_pydatetime()
                count += 1
                if count > 60 * 24 * 3:
                    raise TimeoutError(
                        "It takes too long to retrieve data, "
                        f"please check daily_open_time: {daily_open_time}, "
                        f"and daily_close_time: {daily_close_time}.\n"
                        "The data fed in should cross these two timestamps.")
                if not bar_in_progress:
                    # trading sessions is within the same calendar day
                    is_bar_end_1 = (
                        daily_open_time < bar_datetime.time() <= daily_close_time)
                    # trading sessions cross two calendar days
                    is_bar_end_2 = (
                        bar_datetime.time() <= daily_close_time < daily_open_time)
                    if is_bar_end_1 or is_bar_end_2:
                        daily_bar_datetime = bar_datetime
                        daily_close = row.close
                        daily_high = -float('inf')
                        daily_low = float('inf')
                        daily_volume = 0
                        bar_in_progress = True
                        trading_day = get_trading_day(
                            bar_datetime, daily_open_time, daily_close_time)
                        count = 0
                elif bar_in_progress:
                    # trading sessions is within the same calendar day
                    is_bar_begin_1 = (
                        bar_datetime.time() <= daily_open_time < daily_close_time
                        and bar_datetime.date() == trading_day
                    )
                    # trading sessions cross two calendar days
                    is_bar_begin_2 = (daily_close_time < bar_datetime.time(
                    ) <= daily_open_time and bar_datetime.date() < trading_day)
                    if is_bar_begin_1 or is_bar_begin_2:
                        daily_open = row.open
                        daily_high = max(daily_high, row.high)
                        daily_low = min(daily_low, row.low)
                        daily_volume += row.volume
                        bar_in_progress = False
                        bar = Bar(
                            security=security,
                            datetime=daily_bar_datetime,
                            open=daily_open,
                            high=daily_high,
                            low=daily_low,
                            close=daily_close,
                            volume=daily_volume
                        )
                        bars.append(bar)
                        if len(bars) == periods:
                            return bars[::-1]
                    else:
                        daily_high = max(daily_high, row.high)
                        daily_low = min(daily_low, row.low)
                        daily_volume += row.volume
        raise ValueError(
            f"There is not sufficient historical 1day data for {security.code}."
            f" We want {periods} data points, but only got {len(bars)}.")
