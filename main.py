# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:59 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: main-tmp.py
# @Software: PyCharm

from datetime import datetime

from qtrader.core.constants import TradeMode
from qtrader.core.event_engine import BarEventEngineRecorder, BarEventEngine
from qtrader.core.security import Stock
from qtrader.core.position import Position
from qtrader.core.balance import AccountBalance
from qtrader.core.portfolio import Portfolio
from qtrader.core.engine import Engine
from qtrader.analysis.pnl import plot_pnl
from qtrader.gateways import BacktestGateway
from qtrader.gateways import FutuGateway
from examples.demo_strategy import DemoStrategy


if __name__=="__main__":

    stock = Stock(code="HK.01157", lot_size=200, stock_name="中联重科")
    stock_list = [stock]

    market = BacktestGateway(
        securities=stock_list,
        start=datetime(2021, 3, 15, 9, 30, 0, 0),
        end=datetime(2021, 3, 17, 16, 0, 0, 0),
    )

    # market = FutuGateway(
    #     securities=stock_list,
    #     end=datetime(2021, 3, 18, 16, 0, 0, 0),
    # )

    # 头寸管理
    position = Position()
    # 账户余额
    account_balance = AccountBalance()
    # 投资组合管理
    portfolio = Portfolio(account_balance=account_balance,
                          position=position,
                          market=market)
    # 执行引擎
    engine = Engine(portfolio)

    # 初始化策略
    strategy = DemoStrategy(securities=stock_list, engine=engine)
    strategy.init_strategy()

    # 事件引擎启动
    recorder = BarEventEngineRecorder()
    event_engine = BarEventEngine(strategy, recorder, trade_mode=TradeMode.BACKTEST)
    event_engine.run()

    # plot_pnl(event_engine.recorder.datetime, event_engine.recorder.portfolio_value)
    print("Done.")