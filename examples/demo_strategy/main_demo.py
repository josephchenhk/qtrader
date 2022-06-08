# -*- coding: utf-8 -*-
# @Time    : 9/15/2021 4:45 PM
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

##########################################################################
#
#                          Demo strategy
##########################################################################
from datetime import datetime

from qtrader.core.constants import TradeMode, Exchange
from qtrader.core.event_engine import BarEventEngineRecorder, BarEventEngine
from qtrader.core.security import Futures
from qtrader.core.engine import Engine
from qtrader.gateways.cqg import CQGFees

from demo_strategy import DemoStrategy


if __name__ == "__main__":

    trade_mode = TradeMode.BACKTEST
    fees = CQGFees
    if trade_mode == TradeMode.BACKTEST:
        from qtrader.gateways import BacktestGateway
        gateway_name = "Backtest"  # "Futufutures", "Backtest", "Cqg", "Ib"
        UseGateway = BacktestGateway
        start = datetime(2021, 3, 15, 15, 0, 0)
        end = datetime(2021, 3, 17, 23, 0, 0)
    elif trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
        from qtrader.gateways import CqgGateway
        gateway_name = "Cqg"
        UseGateway = CqgGateway
        start = None
        today = datetime.today()
        end = datetime(today.year, today.month, today.day, 23, 0, 0)

    stock_list = [
        Futures(code="FUT.GC", lot_size=100, security_name="GCQ2",
                exchange=Exchange.NYMEX, expiry_date="20220828"),
        Futures(code="FUT.SI", lot_size=5000, security_name="SIN2",
                exchange=Exchange.NYMEX, expiry_date="20220727"),
    ]

    init_capital = 100000
    gateway = UseGateway(
        securities=stock_list,
        start=start,
        end=end,
        gateway_name=gateway_name,
        fees=fees,
        num_of_1min_bar=180
    )

    gateway.SHORT_INTEREST_RATE = 0.0
    gateway.trade_mode = trade_mode
    if gateway.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE):
        assert datetime.now() < gateway.end, (
            "Gateway end time must be later than current datetime!")

    # Engine
    engine = Engine(gateways={gateway_name: gateway})

    # get activated plugins
    plugins = engine.get_plugins()

    # Initialize strategy
    strategy_account = "CTAStrategy"
    strategy_version = "1.0"
    strategy = DemoStrategy(
        securities={gateway_name: stock_list},
        strategy_account=strategy_account,
        strategy_version=strategy_version,
        init_strategy_cash={gateway_name: init_capital},
        engine=engine,
        strategy_trading_sessions={
            "FUT.GC": [[datetime(1970, 1, 1, 15, 0, 0),
                        datetime(1970, 1, 1, 23, 0, 0)]],
            "FUT.SI": [[datetime(1970, 1, 1, 15, 0, 0),
                        datetime(1970, 1, 1, 23, 0, 0)]]
        }
    )
    strategy.init_strategy()

    # Event recorder
    recorder = BarEventEngineRecorder(datetime=[],
                                      bar_datetime=[],
                                      open=[],
                                      high=[],
                                      low=[],
                                      close=[],
                                      volume=[],
                                      trend=[],
                                      trend_up=[],
                                      trend_down=[],
                                      signal=[])
    event_engine = BarEventEngine(
        {"cta": strategy},
        {"cta": recorder},
        engine
    )

    if "telegram" in plugins:
        telegram_bot = plugins["telegram"].bot
        telegram_bot.send_message(f"{datetime.now()} {telegram_bot.__doc__}")
    event_engine.run()

    result_path = recorder.save_csv()

    if "analysis" in plugins:
        plot_pnl = plugins["analysis"].plot_pnl
        plot_pnl(result_path=result_path, freq="daily")
    engine.log.info("Program shutdown normally.")
