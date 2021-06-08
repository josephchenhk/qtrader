# -*- coding: utf-8 -*-
# @Time    : 17/3/2021 3:59 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: main_pairtrading.py
# @Software: PyCharm

import pandas as pd
from datetime import datetime
from itertools import combinations

from qtrader.config import DATA_PATH
from qtrader.core.constants import TradeMode
from qtrader.core.security import Stock
from qtrader.core.position import Position
from qtrader.core.balance import AccountBalance
from qtrader.core.portfolio import Portfolio
from qtrader.core.engine import Engine
from qtrader.core.event_engine import BarEventEngine, BarEventEngineRecorder
from qtrader.plugins.analysis import plot_pnl
from qtrader.gateways import BacktestGateway
from examples.pairtrade_strategy import PairTradeStrategy


# DATA_PATH = "/Users/joseph/Dropbox/code/stat-arb/data"

def get_stock_full_list(data_name:str="HK.BK1050-2021-02-25"):
    """
    获取全部关注的股票

    :param data_name:
    :return:
    """
    stock_list_full_df = pd.read_csv(f"{DATA_PATH}/stock_list/{data_name}.csv")
    stock_list_full = []
    for _, row in stock_list_full_df.iterrows():
        code = row["code"]
        lot_size = row["lot_size"]
        stock_name = row["stock_name"]
        stock = Stock(code=code, lot_size=lot_size, stock_name=stock_name)
        stock_list_full.append(stock)
    return stock_list_full

if __name__=="__main__":

    stocks = [
        'HK.00928',
        'HK.01477',
        'HK.01521',
        'HK.09969',
        'HK.01873',
        'HK.02359',
        'HK.01349',
        'HK.03347',
        'HK.01877',
        'HK.06855',
        'HK.06826',
        # 'HK.02170'
    ]

    stock_full_list = get_stock_full_list(data_name="HK.BK1050-2021-02-25")
    stock_list = [s for s in stock_full_list if s.code in stocks]

    comb = combinations(stock_list, r=2)
    pairs = []
    for c in comb:
        pairs.append(c)

    # 市场仿真器
    market = BacktestGateway(
        securities=stock_list,
        start=datetime(2021, 1, 25, 9, 30, 0, 0),
        end=datetime(2021, 2, 1, 12, 0, 0, 0),
    )

    # market = FutuGateway(
    #     securities=stock_list,
    #     # start=datetime(2021, 1, 25, 9, 30, 0, 0),
    #     end=datetime(2021, 3, 17, 23, 30, 0, 0),
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
    strategy = PairTradeStrategy(pairs, engine=engine)
    strategy.init_strategy()

    # 开始跑实盘
    recorder = BarEventEngineRecorder()
    event_engine = BarEventEngine(strategy, recorder, trade_mode=TradeMode.BACKTEST)
    event_engine.run()

    plot_pnl(recorder.datetime, recorder.portfolio_value)

