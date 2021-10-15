# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:00 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: constants.py
# @Software: PyCharm
from enum import Enum

class TradeMode(Enum):
    """
    Trading mode
    """
    BACKTEST = "BACKTEST"
    LIVETRADE = "LIVETRADE"
    SIMULATE = "SIMULATE"

class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "LONG"
    SHORT = "SHORT"
    NET = "NET"

class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    CLOSETODAY = "CLOSETODAY"
    CLOSEYESTERDAY = "CLOSEYESTERDAY"

class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "LMT"
    MARKET = "MKT"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"

class OrderStatus(Enum):
    """
    Order status
    """
    UNKNOWN = "UNKNOWN"
    SUBMITTING = "SUBMITTING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PART_FILLED = "PART_FILLED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"

class Exchange(Enum):
    """
    Exchanges
    """
    SEHK = "SEHK"           # Stock Exchange of Hong Kong
    HKFE = "HKFE"           # Hong Kong Futures Exchange
    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange
    COMEX = "COMEX"         # New York Mercantile Exchange
    SGE = "SGE"             # Shanghai Gold Exchange
    IDEALPRO = "IDEALPRO"   # currency
    GLOBEX = "GLOBEX"       # futures
    NYMEX = "NYMEX"         # COMEX (gold) futures
    SMART = "SMART"


class Cash(Enum):
    """
    Currency
    """
    NONE = "UNKNOWN"
    HKD = "HKD"
    USD = "USD"
    CNH = "CNH"



