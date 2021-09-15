# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 10:08 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: logger.py
# @Software: PyCharm

import os
from datetime import datetime
import logging

# 第一步，创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Log等级总开关

# 第二步，创建一个handler，用于写入日志文件
if "log" not in os.listdir():
    os.mkdir(os.path.join(os.getcwd(),"log"))
logfile = f'./log/{datetime.now().strftime("%Y-%m-%d %H-%M-%S.%f")}.txt'
fh = logging.FileHandler(logfile, mode='a', encoding="utf-8")
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关

# 第三步，再创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # 输出到console的log等级的开关

# 第四步，定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# 第五步，将logger添加到handler里面
logger.addHandler(fh)
logger.addHandler(ch)