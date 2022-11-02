# -*- coding: utf-8 -*-
# @Time    : 5/3/2021 12:05 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: security.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from dataclasses import dataclass

from qtrader.core.constants import Exchange


@dataclass(frozen=True)
class Security:
    """Base class for different asset types"""
    code: str
    security_name: str
    lot_size: int = None
    exchange: Exchange = None
    expiry_date: str = None

    def __eq__(self, other):
        return (self.code == other.code) and (
            self.security_name == other.security_name)

    def __hash__(self):
        return hash(f"{self.security_name}|{self.code}|{self.exchange.value}")


@dataclass(frozen=True)
class Stock(Security):
    """Cash equity"""
    code: str
    security_name: str
    lot_size: int = 1                   # default to 1 lot
    exchange: Exchange = Exchange.SEHK  # default to HK market
    expiry_date = None

    def __post_init__(self):
        pass


@dataclass(frozen=True)
class Currency(Security):
    """Foreign exchange"""
    code: str
    security_name: str
    lot_size: int = 1000                    # default to 1000
    exchange: Exchange = Exchange.IDEALPRO  # default to IDEALPRO
    expiry_date = None

    def __post_init__(self):
        pass


@dataclass(frozen=True)
class Commodity(Security):
    """Commodity"""
    code: str
    security_name: str
    lot_size: int = 1000                    # default to 1000
    exchange: Exchange = Exchange.IDEALPRO  # default to IDEALPRO
    expiry_date = None

    def __post_init__(self):
        pass


@dataclass(frozen=True)
class Futures(Security):
    """Futures"""
    code: str
    security_name: str
    lot_size: int = 1000                 # default to 1000
    exchange: Exchange = Exchange.SMART  # default to SMART
    expiry_date = ""

    def __post_init__(self):
        pass
