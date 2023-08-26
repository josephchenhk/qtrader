# -*- coding: utf-8 -*-
# @Time    : 6/9/2021 3:53 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: __init__.py.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

try:
    from .ib_gateway import IbGateway
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")

try:
    from .ib_fees import (
        IbHKEquityFees, IbSHSZHKConnectEquityFees, IbUSFuturesFees)
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")
