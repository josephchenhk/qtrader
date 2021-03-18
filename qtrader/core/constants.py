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
    BACKTEST = "回测"
    LIVETRADE = "实盘"
    SIMULATE = "仿真"


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "多"
    SHORT = "空"
    NET = "净"

class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "开"
    CLOSE = "平"
    CLOSETODAY = "平今"
    CLOSEYESTERDAY = "平昨"

class OrderType(Enum):
    """
    Order type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "STOP"
    FAK = "FAK"
    FOK = "FOK"

class OrderStatus(Enum):
    """
    Order status
    """
    UNKNOWN = "未知"
    SUBMITTED = "已提交"
    FILLED = "全部成交"
    PART_FILLED = "部分成交"
    CANCELLED = "已取消"
    FAILED = "提交失败"

class Exchange(Enum):
    SEHK = "SEHK"           # Stock Exchange of Hong Kong
    HKFE = "HKFE"           # Hong Kong Futures Exchange

    SSE = "SSE"             # Shanghai Stock Exchange
    SZSE = "SZSE"           # Shenzhen Stock Exchange

