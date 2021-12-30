# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:59 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: main_demo.py

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

########################################################################################################################
#                                                                                                                      #
#                                                 Demo strategy                                                        #
########################################################################################################################

from datetime import datetime
from datetime import time as Time

from qtrader.core.constants import TradeMode, Exchange
from qtrader.core.event_engine import BarEventEngineRecorder, BarEventEngine
from qtrader.core.security import Stock
from qtrader.core.engine import Engine
from qtrader.gateways import BacktestGateway

from examples.demo_strategy import DemoStrategy

if __name__=="__main__":
    # Security
    stock_list = [
        Stock(code="HK.01157", lot_size=100, security_name="中联重科", exchange=Exchange.SEHK),
    ]

    # Gateway
    gateway_name = "Backtest"
    gateway = BacktestGateway(
        securities=stock_list,
        start=datetime(2021, 3, 15, 9, 30, 0, 0),
        end=datetime(2021, 3, 17, 16, 0, 0, 0),
        gateway_name=gateway_name,
    )
    gateway.SHORT_INTEREST_RATE = 0.0
    gateway.set_trade_mode(TradeMode.BACKTEST)
    gateway.TRADING_HOURS_AM = [Time(9, 30, 0), Time(12, 0, 0)]
    gateway.TRADING_HOURS_PM = [Time(13, 0, 0), Time(16, 0, 0)]

    # Core engine
    engine = Engine(gateways={gateway_name: gateway})

    # Strategy initialization
    init_capital = 100000
    strategy_account = "DemoStrategy"
    strategy_version = "1.0"
    strategy = DemoStrategy(
        securities={gateway_name: stock_list},
        strategy_account=strategy_account,
        strategy_version=strategy_version,
        init_strategy_cash={gateway_name: init_capital},
        engine=engine)
    strategy.init_strategy()

    # Recorder
    recorder = BarEventEngineRecorder()

    # Event engine
    event_engine = BarEventEngine(strategy, recorder)

    # Start event engine
    event_engine.run()

    # Save results and shutdown program
    result_path = recorder.save_csv()
    plugins = engine.get_plugins()
    if "analysis" in plugins:
        plot_pnl = plugins["analysis"].plot_pnl
        plot_pnl(result_path=result_path)
    engine.log.info("Program shutdown normally.")