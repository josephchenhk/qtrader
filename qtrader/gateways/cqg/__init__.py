# -*- coding: utf-8 -*-
# @Time    : 11/22/2021 11:32 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: __init__.py.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""
from pywintypes import com_error

try:
    from .cqg_gateway import CqgGateway
except (ImportError, com_error) as e:
    print(f"{e.__class__}: {e}")

try:
    from .cqg_fees import CQGFees
except (ImportError, com_error) as e:
    print(f"{e.__class__}: {e}")