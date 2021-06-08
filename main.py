# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:59 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: main_pairtrading.py
# @Software: PyCharm

from datetime import datetime

from qtrader.core.constants import TradeMode
from qtrader.core.event_engine import BarEventEngineRecorder, BarEventEngine
from qtrader.core.security import Stock
from qtrader.core.engine import Engine
from qtrader.gateways import BacktestGateway, FutuGateway
from examples.demo_strategy import DemoStrategy


if __name__=="__main__":

    stock_list = [
        Stock(code="HK.01157", lot_size=200, stock_name="中联重科"),
    ]

    market = BacktestGateway(
        securities=stock_list,
        start=datetime(2021, 3, 15, 9, 30, 0, 0),
        end=datetime(2021, 3, 17, 16, 0, 0, 0),
    )
    market.set_trade_mode(TradeMode.BACKTEST)

    # market = FutuGateway(
    #     securities=stock_list,
    #     end=datetime(2021, 4, 21, 16, 0, 0, 0),
    # )
    # market.set_trade_mode(TradeMode.SIMULATE)


    # 执行引擎
    engine = Engine(market)

    # 初始化策略
    strategy_account = "DemoStrategy"
    strategy_version = "1.0"
    strategy = DemoStrategy(
        securities=stock_list,
        strategy_account=strategy_account,
        strategy_version=strategy_version,
        init_strategy_cash=10000,
        engine=engine)
    strategy.init_strategy()

    # 事件引擎启动
    recorder = BarEventEngineRecorder()
    event_engine = BarEventEngine(strategy, recorder)
    event_engine.run()

    recorder.save_csv()
    plugins = engine.get_plugins()
    if "analysis" in plugins:
        plot_pnl = plugins["analysis"].plot_pnl
        plot_pnl(recorder.datetime, recorder.portfolio_value)
    engine.log.info("程序正常退出")