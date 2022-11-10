# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:00 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: constants.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from enum import Enum


class TradeMode(Enum):
    """Trading mode"""
    BACKTEST = "BACKTEST"
    LIVETRADE = "LIVETRADE"
    SIMULATE = "SIMULATE"


class Direction(Enum):
    """Direction of order/trade/position."""
    LONG = "LONG"
    SHORT = "SHORT"
    NET = "NET"


class Offset(Enum):
    """Offset of order/trade."""
    NONE = ""
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSETODAY = "CLOSETODAY"
    CLOSEYESTERDAY = "CLOSEYESTERDAY"


class OrderType(Enum):
    """Order type."""
    LIMIT = "LMT"
    MARKET = "MKT"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"


class OrderStatus(Enum):
    """Order status"""
    UNKNOWN = "UNKNOWN"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PART_FILLED = "PART_FILLED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class Exchange(Enum):
    """Exchanges"""
    SEHK = "SEHK"           # Stock Exchange of Hong Kong
    HKFE = "HKFE"           # Hong Kong Futures Exchange
    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange
    CME = "CME"             # S&P Index, AUDUSD, etc
    COMEX = "COMEX"         # Gold, silver, copper, etc
    NYMEX = "NYMEX"         # Brent Oil, etc
    ECBOT = "ECBOT"         # Bonds, soybean, rice, etc
    SGE = "SGE"             # Shanghai Gold Exchange
    IDEALPRO = "IDEALPRO"   # currency
    GLOBEX = "GLOBEX"       # futures
    SMART = "SMART"         # SMART in IB
    SGX = "SGX"             # Singapore Exchange (https://www.sgx.com/)
    ICE = "ICE"             # Products: QO (Brent Oil)


class Cash(Enum):
    """Currency"""
    NONE = "UNKNOWN"
    HKD = "HKD"
    USD = "USD"
    CNH = "CNH"
