# -*- coding: future_fstrings -*-
import os
import time
import pytz
import pandas as pd
from datetime import datetime
from datetime import timedelta
from quantkits.logger import logger

from data_adapters.feather_adapter import FeatherDataHandler
from curves_builder import price_to_yield
from curves_builder.Nelson_Siegel_Svensson import build_curves
from config import FEATHER_DATA_PATH
from signal_trigger.simple import trading_signals

from trader.constant import OrderType, Status
from trader.script_engine import ScriptEngine


def run(engine: ScriptEngine):
    """
    脚本策略的主函数说明：
    1. 唯一入参是脚本引擎ScriptEngine对象，通用它来完成查询和请求操作
    2. 该函数会通过一个独立的线程来启动运行，区别于其他策略模块的事件驱动
    3. while循环的维护，请通过engine.strategy_active状态来判断，实现可控退出

    脚本策略的应用举例：
    1. 自定义篮子委托执行执行算法
    2. 股指期货和一篮子股票之间的对冲策略
    3. 国内外商品、数字货币跨交易所的套利
    4. 自定义组合指数行情监控以及消息通知
    5. 股票市场扫描选股类交易策略（龙一、龙二）
    6. 等等~~~
    """
    issuer = "Greenland Global Investment Ltd"
    # isins = get_isins(issuer)

    eff_isins = ["XS1662749743",
        "XS1081321595",
        "XS1840467762",
        "XS1960762554",
        "XS2055399054",
        "XS2076775233",
        "XS1760376878",
        "XS2055403930",
        "XS1937203740",
        "XS1760383577",
        "XS2016768439",
        "XS2108075784"
    ]

    symbols = ["{}.BONDSIM".format(isn) for isn in eff_isins]

    start = datetime(2020, 6, 19, 8, 0, 0)
    end = datetime(2020, 6, 23, 7, 59 ,59)
    tz = pytz.timezone("Asia/Hong_Kong")
    start = tz.localize(start)
    end = tz.localize(end)

    # 加载BONDSIM gateway
    gateway = engine.main_engine.gateways["BONDSIM"]

    # 订阅行情
    # engine.subscribe(eff_isins)

    tic = start
    while engine.strategy_active:
        engine.write_log(f"当前时间 {tic}")
        gateway.quote_ctx.send_timer_to_server(tic)
        # time.sleep(1)
        ticks = engine.get_ticks(vt_symbols=symbols)
        print("收到订阅行情：", ticks)
        tic += timedelta(seconds=1)


