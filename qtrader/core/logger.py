# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:08 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: logger.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

import os
from datetime import datetime
import logging

# (1) create a logger and set its logging level
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# (2) create a file handler and set its logging level
if "log" not in os.listdir():
    os.mkdir(os.path.join(os.getcwd(), "log"))
logfile = f'./log/{datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")}.txt'
fh = logging.FileHandler(logfile, mode='a', encoding="utf-8")
fh.setLevel(logging.ERROR)

# (3) create a stream handler(output to console) and set its logging level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# (4) define the output format of the two handlers above
formatter = logging.Formatter(
    "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
)
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# (5) add the two handlers to logger
logger.addHandler(fh)
logger.addHandler(ch)
