# -*- coding: utf-8 -*-
# @Time    : 6/2/2021 9:37 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: pair_trade_strategy.py
# @Software: PyCharm
import time
from datetime import datetime, timedelta

import math
import pandas as pd
import numpy as np
import scipy.stats as st
from functools import reduce
from typing import Dict, List

from futu import *
from qtrader.core.constant import Direction, Offset, OrderType
from qtrader.core.script_engine import ScriptEngine
from qtrader.gateway import BaseGateway
from qtrader.strategy.fit_distributions import best_fit_distribution
from qtrader.gateway.futu.futu_gateway import convert_symbol_vt2futu
from qtrader.core.constant import Exchange
from qtrader.core.object import OrderData

def line_total_least_squares(x, y):
    n = len(x)

    x_m = np.sum(x) / n
    y_m = np.sum(y) / n

    # Calculate the x~ and y~
    x1 = x - x_m
    y1 = y - y_m

    # Create the matrix array
    X = np.vstack((x1, y1))
    X_t = np.transpose(X)

    # Finding A_T_A and it's Find smallest eigenvalue::
    prd = np.dot(X, X_t)
    W, V = np.linalg.eig(prd)
    small_eig_index = W.argmin()
    a, b = V[:, small_eig_index]

    # Compute C:
    c = (-1 * a * x_m) + (-1 * b * y_m)

    return a, b, c

def fees(*trades:Dict)->float:
    """
    港股融资融券（8332）套餐一（适合一般交易者）
    融资利率: 年利率6.8%

    佣金: 0.03%， 最低3港元
    平台使用费: 15港元/笔

    交易系统使用费（香港交易所）: 每笔成交0.50港元
    交收费（香港结算所）: 0.002%， 最低2港元，最高100港元
    印花税（香港政府）: 0.1%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取
    交易费（香港交易所）: 0.005%*成交金额，最低0.01港元
    交易征费（香港证监会）: 0.0027*成交金额，最低0.01港元
    -----------------------
    港股融资融券（8332）套餐二（适合高频交易者）
    融资利率: 年利率6.8%

    佣金: 0.03%， 最低3港元
    平台使用费: 阶梯式收费（以自然月计算）
              每月累计订单           费用（港币/每笔订单）
              ---------            ----------------
              1-5                  30
              6-20                 15
              21-50                10
              51-100               9
              101-500              8
              501-1000             7
              1001-2000            6
              2001-3000            5
              3001-4000            4
              4001-5000            3
              5001-6000            2
              6001及以上            1

    交易系统使用费（香港交易所）: 每笔成交0.50港元
    交收费（香港结算所）: 0.002%， 最低2港元，最高100港元
    印花税（香港政府）: 0.1%*成交金额，不足1港元作1港元计，窝轮、牛熊证此费用不收取
    交易费（香港交易所）: 0.005%*成交金额，最低0.01港元
    交易征费（香港证监会）: 0.0027*成交金额，最低0.01港元
    """
    # 富途收费
    commissions = 0       # 佣金
    platform_fees = 0     # 平台使用费
    # 富途代收费
    system_fees = 0       # 交易系统使用费
    settlement_fees = 0   # 交收费
    stamp_fees = 0        # 印花税
    trade_fees = 0        # 交易费
    transaction_fees = 0  # 交易征费

    total_trade_amount = 0
    total_number_of_trades = 0
    for trade in trades:
        price = trade.get("price")
        size = trade.get("size")
        trade_amount = price * size
        total_number_of_trades += 1
        total_trade_amount += trade_amount

        # 交易系统使用费
        system_fee = round(0.50, 2)
        system_fees += system_fee

        # 交收费
        settlement_fee = 0.00002*trade_amount
        if settlement_fee<2.0:
            settlement_fee = 2.0
        elif settlement_fee>100.0:
            settlement_fee = 100.0
        settlement_fee = round(settlement_fee, 2)
        settlement_fees += settlement_fee

        # 印花税
        stamp_fee = math.ceil(0.001*trade_amount)
        stamp_fees += stamp_fee

        # 交易费
        trade_fee = max(0.00005*trade_amount, 0.01)
        trade_fee = round(trade_fee, 2)
        trade_fees += trade_fee

        # 交易征费
        transaction_fee = max(0.000027*trade_amount, 0.01)
        transaction_fee = round(transaction_fee, 2)
        transaction_fees += transaction_fee

    # 佣金
    commissions = max(0.0003*total_trade_amount, 3)
    commissions = round(commissions, 2)

    # 平台使用费
    platform_fees = 15

    # 总费用
    total_fees = commissions + platform_fees + system_fees + settlement_fees + stamp_fees + trade_fees + transaction_fees
    return commissions, platform_fees, system_fees, settlement_fees, stamp_fees, trade_fees, transaction_fees, total_fees

def compute_amounts(beta):
    """
    1个单位A，对应beta个单位B
    :param beta:
    :return: A手数，B手数
    """
    if 0<beta<1:
        return round(1/beta), 1
    elif beta>1:
        return 1, round(beta)


def get_cur_kline(gateway:BaseGateway, code_list:List[str], subtype_list:List[SubType], num:int=1000):
    """
    获取当前时间的 k 线

    :param gateway:
    :param code_list:
    :param subtype_list:
    :param num: k线根数，不能超过1000
    :return:
    """
    ret_sub, err_message = gateway.quote_ctx.subscribe(
        code_list=code_list,
        subtype_list=subtype_list,
        subscribe_push=False
    )

    if ret_sub == RET_OK:  # 订阅成功
        ret_data = {}
        for code, ktype in zip(code_list, subtype_list):
            ret, data = gateway.quote_ctx.get_cur_kline(code, num, ktype, autype=AuType.QFQ)  # 获取港股00700最近2个K线数据
            if ret == RET_OK:
                ret_data[code] = data
            else:
                print('error:', data)
                ret_data[code] = None
        return ret_data
    else:
        print('subscription failed', err_message)

def get_close_data(cur_klines: Dict[str, pd.DataFrame], num:int=1):
    """

    :param cur_klines:
    :param num: kline的根数，不可以超过1000
    :return:
    """
    # 提取收盘价
    sec_data = []
    for code, df in cur_klines.items():
        df = df[["time_key", "close"]]
        df.columns = ["time", f"close_{code}"]
        sec_data.append(df)
    # 组合不同instrument的数据
    close_result = reduce(lambda left,right: pd.merge(left,right,on='time'), sec_data)
    close_result = close_result.set_index("time")
    return close_result.tail(num)

def send_aggressive_limit_order(
        engine:ScriptEngine,
        vt_symbol:str,
        price:float,
        volume:float,
        direction:Direction,
        offset:Offset,
        order_type:OrderType
    ):
    """"""

    order_id = engine.send_order(
        vt_symbol=vt_symbol,
        price=price,
        volume=volume,
        direction=direction,
        offset=offset,
        order_type=order_type
    )

    order_info = f"""OrderRequest[
            vt_symbol={vt_symbol},
            price={price},
            volume={volume},
            direction={direction},
            offset={offset},
            order_type={order_type}]
            """

    if order_id=="":
        engine.write_log(f"提交订单失败: {order_info}")
        return

    t = 0
    order = engine.get_order(order_id)
    while order.status!=OrderStatus.FILLED_ALL:

        if t>15:
            remaining_volume = volume - order.traded
            engine.cancel_order(order_id)
            time.sleep(0.1)
            tick = engine.get_tick(vt_symbol=vt_symbol)
            price = tick.bid_price_1 if direction == Direction.SHORT else tick.ask_price_1
            order_id = engine.send_order(
                vt_symbol=vt_symbol,
                price=price,
                volume=remaining_volume,
                direction=direction,
                offset=offset,
                order_type=order_type
            )
            t = 0
            engine.write_log(f"Update order price to {price}")

        time.sleep(1)
        t += 1
        order = engine.get_order(order_id)

        engine.write_log(f"成功执行订单: {order_info}")


def is_trading_hours(now:datetime)->bool:
    """
    判断当前时间是否交易时间

    :param now:
    :return:
    """
    from datetime import time
    am_openTime = time(hour=9, minute=30, second=0)
    am_closeTime = time(hour=12, minute=0, second=0)

    pm_openTime = time(hour=13, minute=0, second=0)
    pm_closeTime = time(hour=16, minute=0, second=0)

    return (am_openTime<=now.time()<=am_closeTime) or (pm_openTime<=now.time()<=pm_closeTime)


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

    hold = False     # 初始假设无持仓
    hold_price_A = 0 # 初始假设A持仓成本为零
    hold_price_B = 0 # 初始假设B持仓成本为零
    hold_state = 0   # 0 (no holdings); 1 (A:long B:short); -1 (A:short B:long)
    profit_sum = 0   # 当前累计盈利
    time_list = []   # 记录历史时间戳
    profit_list = [] # 记录历史盈利曲线
    prices_A = []    # 记录股票A历史价格
    prices_B = []    # 记录股票B历史价格
    short_interest_rate = 0.0098 # 融券利息

    open_time = []        # 开仓的时间点
    take_profit_time = [] # 止盈的时间点
    stop_loss_time = []   # 止损的时间点

    window = 600          # 观测移动窗口长度
    price_A_window = []   # A证券在观测窗口的历史价格
    price_B_window = []   # B证券在观测窗口的历史价格

    calibration_window = timedelta(days=1)
    calibration_time = None


    # 2359药明康德；2269药明生物
    exchange = Exchange.SEHK
    code_A = "02359"
    code_B = "02269"
    symbols = [f"{code_A}.{exchange.value}", f"{code_B}.{exchange.value}"]
    vt_symbol_A, vt_symbol_B = symbols
    lotA = 100
    lotB = 500

    # 订阅行情
    engine.subscribe(symbols)
    time.sleep(10)

    # 加载FUTU gateway
    gateway = engine.main_engine.gateways["FUTU"]
    futu_code_A = convert_symbol_vt2futu(code_A, exchange)
    futu_code_B = convert_symbol_vt2futu(code_B, exchange)
    cur_klines = get_cur_kline(gateway, [futu_code_A, futu_code_B], [SubType.K_1M, SubType.K_1M], 1000)
    klines = get_close_data(cur_klines, window) # 获取window长度的历史分钟k线
    price_A_window = klines[f"close_{futu_code_A}"].values.tolist()
    price_B_window = klines[f"close_{futu_code_B}"].values.tolist()

    while engine.strategy_active:
        cur_time = datetime.now()

        # 非交易时间，等待1分钟后再判断
        if not is_trading_hours(cur_time):
            engine.write_log(f"当前时间{cur_time}非交易时间")
            time.sleep(60)
            continue

        # ticks = engine.get_ticks(vt_symbols=symbols)
        tickA = engine.get_tick(vt_symbol=vt_symbol_A)
        tickB = engine.get_tick(vt_symbol=vt_symbol_B)

        # 收集窗口数据
        pA = tickA.last_price
        pB = tickB.last_price
        price_A_window.append(pA)
        price_B_window.append(pB)
        engine.write_log(f"当前时间 {cur_time}, 添加{vt_symbol_A}={pA}, {vt_symbol_B}={pB}")

        # 未收集足够数据，不进行操作
        if len(price_A_window) <= window or len(price_B_window) <= window:
            time.sleep(60)
            continue

        # 移动窗口（永远保持窗口长度不变）
        price_A_window.pop(0)
        price_B_window.pop(0)
        # 保证窗口长度符合预期
        assert len(price_A_window) == window, f"Length of A window is not equal to {window}"
        assert len(price_B_window) == window, f"Length of B window is not equal to {window}"

        # 空仓时才设定进出场信号，持仓时不能修改信号，否则策略无意义
        if ((calibration_time is None) or (cur_time >= calibration_time)) and (hold == False):
            # 设定下一次recalibrate的时间
            calibration_time = cur_time + calibration_window
            engine.write_log(f"当前时间: {cur_time}，下一次calibrate时间: {calibration_time}")
            # 取时间窗口的价格做回归，取价差做目标值
            SA = pd.Series(price_A_window, name="priceA") * lotA
            SB = pd.Series(price_B_window, name="priceB") * lotB

            # # 普通OLS方法，回归系数非对称，不是一个好方法
            # results = sm.OLS(SA, sm.add_constant(SB)).fit()
            # b = results.params['priceB']

            # 采用TLS方法，回归系数对称
            a1, b1, c1 = line_total_least_squares(SB, SA)
            SA_tls = -1 * (a1 / b1) * SB + (-1 * (c1 / b1))
            beta = -1 * (a1 / b1)
            mu = -1 * (c1 / b1)
            engine.write_log(f"beta={beta}, mu={mu}")
            # 需要保证beta系数为正数，否则spread的意义不明朗
            if beta < 0.3 or beta > 3:
                engine.write_log(f"beta={beta}<0, calibration time重置")
                price_A_window = price_A_window[100:]
                price_B_window = price_B_window[100:]
                calibration_time = None
                continue
            spread = SA - SA_tls
            spread_mean = spread.mean()

            # 预期赚的钱：
            best_fit_name, best_fit_params, sse_scores, fit_params = best_fit_distribution(
                spread,
                distributions=[st.powerlognorm]
            )
            best_dist = getattr(st, best_fit_name)
            arg = best_fit_params[:-2]
            loc = best_fit_params[-2]
            scale = best_fit_params[-1]

            # 预期费用
            NA, NB = compute_amounts(beta)
            feeA = fees({"price": pA, "size": lotA * NA})
            feeB = fees({"price": pB, "size": lotB * NB})
            shortA_interest = pA * lotA * NA * 7 / 365 * short_interest_rate  # 预计平均持仓7天
            shortB_interest = pB * lotB * NB * 7 / 365 * short_interest_rate  # 预计平均持仓7天

            """
            预期收益
            (1). long A，short B (mspread < entry) 计算收益
             entry: pA0*lotA - beta*pB0*lotB - mu = entry
              exit: pA1*lotA - beta*pB1*lotB - mu = spread_mean
            profit: NA*(pA1-pA0)*lotA - NB*(pB1-pB0)*lotB
                    ~ NA*[(pA1-pA0)*lotA - beta*(pB1-pB0)*lotB]
                    = NA*(spread_mean - entry)
            预期收益扣除预估费用后，需要为正数
            NA*(spread_mean - entry) - feeA[-1] - feeB[-1] - shortB_interest > delta
            => entry = spread_mean - (feeA[-1] + feeB[-1] + shortB_interest + delta)/NA

            (2). long B，short A (mspread > entry) 计算收益
             entry: pA0*lotA - beta*pB0*lotB - mu = entry
              exit: pA1*lotA - beta*pB1*lotB - mu = spread_mean
            profit: NB*(pB1-pB0)*lotB - NA*(pA1-pA0)*lotA
                    ~ NA*[beta*(pB1-pB0)*lotB - (pA1-pA0)*lotA]
                    = NA*(entry - spread_mean)
            预期收益扣除预估费用后，需要为正数
            NA*(entry - spread_mean) - feeA[-1] - feeB[-1] - shortA_interest > delta
            => entry = spread_mean + (feeA[-1] + feeB[-1] + shortA_interest + delta)/NA
            """
            delta = 1000  # 预期每笔交易赚取的钱
            threshold1 = (feeA[-1] + feeB[-1] + shortB_interest + delta) / NA
            threshold2 = (feeA[-1] + feeB[-1] + shortA_interest + delta) / NA
            # 设置开仓和止损阈值
            exit_ = spread_mean
            entry = spread_mean - threshold1, spread_mean + threshold2
            # 检查开仓的百分位
            entry_prob = [best_dist.cdf(e, *arg, loc=loc, scale=scale) for e in entry]
            stop_prob = [0.0001, 0.9999]

            msg = f"entry_prob[0]-stop_prob[0]={entry_prob[0]}-{stop_prob[0]}={entry_prob[0]-stop_prob[0]}<0.05? " + \
                  f"stop_prob[1]-entry_prob[1]={stop_prob[1]}-{entry_prob[1]}={stop_prob[1]-entry_prob[1]}<0.05?"
            engine.write_log(msg)

            if entry_prob[0]-stop_prob[0]<0.05 or stop_prob[1]-entry_prob[1]<0.05:
                engine.write_log(f"{entry_prob[0]-stop_prob[0]<0.05}, {stop_prob[1]-entry_prob[1]<0.05}")
                price_A_window = price_A_window[5:]
                price_B_window = price_B_window[5:]
                calibration_time = None
                continue

            stop = [best_dist.ppf(s, *arg, loc=loc, scale=scale) for s in stop_prob]
            if stop[1]==np.inf:
                engine.write_log(f"stop={stop}, change stop[1] to 2*entry[1]=2*{entry[1]}={2*entry[1]}")
                stop[1] = 2*entry[1]
            engine.write_log(f"开仓条件: [{entry}], 止损条件: [{stop}], 止盈条件: {exit_}")

        # 计算当前监测spread的值
        mspread = (pA * lotA - beta * pB * lotB - mu)

        # 显示当前持仓状态
        pos_long_A = engine.get_position(vt_positionid=f"{vt_symbol_A}.多")
        pos_long_B = engine.get_position(vt_positionid=f"{vt_symbol_B}.多")
        pos_short_A = engine.get_position(vt_positionid=f"{vt_symbol_A}.空")
        pos_short_B = engine.get_position(vt_positionid=f"{vt_symbol_B}.空")
        pos_info = f"""
        \t{vt_symbol_A}.多: {pos_long_A}
        \t{vt_symbol_B}.多: {pos_long_B}
        \t{vt_symbol_A}.空: {pos_short_A}
        \t{vt_symbol_B}.空: {pos_short_B}
        """
        engine.write_log(f"当前spread={mspread}{pos_info}")

        # 进行触发信号判断
        if hold == False:  # 没有持仓
            if mspread >= entry[1] and mspread < stop[1]:  # A-B大于开仓阈值，同时小于止损阈值，A股价相对过高

                engine.write_log(f"触发信号开仓, short {vt_symbol_A}, long {vt_symbol_B}")

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine = engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.bid_price_1,
                    volume=NA * lotA,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.ask_price_1,
                    volume=NB * lotB,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = -1
                hold = True
                open_time.append(cur_time)
            elif mspread <= entry[0] and mspread > stop[0]:  # B-A大于开仓阈值，同时小于止损阈值，B股价相对过高

                engine.write_log(f"触发信号开仓, short {vt_symbol_B}, long {vt_symbol_A}")

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.bid_price_1,
                    volume=NB * lotB,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.ask_price_1,
                    volume=NA * lotA,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = 1
                hold = True
                open_time.append(cur_time)
        else:  # 已有持仓
            if mspread >= stop[1] and hold_state == -1:  # 止损：A-B大于止损阈值，且(A:short B:long)，平仓离场

                engine.write_log(f"触发信号止损, short {vt_symbol_B}, long {vt_symbol_A}")

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.bid_price_1,
                    volume=NB * lotB,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.ask_price_1,
                    volume=NA * lotA,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = 0
                hold = False
                # 止损后设置冷静期（清空监测窗口数据）
                price_A_window = []
                price_B_window = []
                stop_loss_time.append(cur_time)
            if mspread <= stop[0] and hold_state == 1:  # 止损：B-A大于止损阈值，且(A:long B:short)，平仓离场

                engine.write_log(f"触发信号止损, short {vt_symbol_A}, long {vt_symbol_B}")

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.bid_price_1,
                    volume=NA * lotA,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.ask_price_1,
                    volume=NB * lotB,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = 0
                hold = False
                # 止损后设置冷静期（清空监测窗口数据）
                price_A_window = []
                price_B_window = []
                stop_loss_time.append(cur_time)
            if mspread <= exit_ and hold_state == -1:  # 止盈：A<=B，且(A:short B:long)，平仓离场

                engine.write_log(f"触发信号止盈, short {vt_symbol_B}, long {vt_symbol_A}")

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.bid_price_1,
                    volume=NB * lotB,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.ask_price_1,
                    volume=NA * lotA,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = 0
                hold = False
                take_profit_time.append(cur_time)
            if mspread >= exit_ and hold_state == 1:  # 止盈：A>=B，且(A:long B:short)，平仓离场

                engine.write_log(f"触发信号止盈, short {vt_symbol_A}, long {vt_symbol_B}")

                tickA = engine.get_tick(vt_symbol=vt_symbol_A)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_A,
                    price=tickA.bid_price_1,
                    volume=NA * lotA,
                    direction=Direction.SHORT,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                tickB = engine.get_tick(vt_symbol=vt_symbol_B)
                send_aggressive_limit_order(
                    engine=engine,
                    vt_symbol=vt_symbol_B,
                    price=tickB.ask_price_1,
                    volume=NB * lotB,
                    direction=Direction.LONG,
                    offset=Offset.OPEN,
                    order_type=OrderType.LIMIT
                )

                hold_state = 0
                hold = False
                take_profit_time.append(cur_time)

        time.sleep(60)