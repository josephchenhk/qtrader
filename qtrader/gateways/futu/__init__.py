# -*- coding: utf-8 -*-
# @Time    : 15/3/2021 4:55 PM
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
    from .futu_gateway import FutuGateway, FutuFuturesGateway
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")

try:
    from .futu_fees import FutuFeesSEHK, FutuFeesHKFE
except ImportError as e:
    print(f"{e.__class__}: {e.msg}")