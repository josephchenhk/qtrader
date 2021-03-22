# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:20 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: balance.py
# @Software: PyCharm
from dataclasses import dataclass

from qtrader.core.constants import Currency

@dataclass
class AccountBalance:

    """账户资金信息"""
    total = 0.0                           # 初始现金
    power: float = None                   # 最大购买力
    max_power_short: float = None         # 卖空购买力
    net_cash_power: float = None          # 现金购买力
    total_assets: float = None            # 资产净值
    cash: float = None                    # 现金
    market_val: float = None              # 证券市值，仅证券账户适用
    long_mv: float = None                 # 多头市值
    short_mv: float = None                # 空头市值
    pending_asset: float = None           # 在途资产
    interest_charged_amount: float = None # 计息金额
    frozen_cash: float = None             # 冻结资金
    avl_withdrawal_cash: float = None     # 现金可提（仅证券账户适用）
    max_withdrawal: float = None          # 最大可提，仅富途证券的证券账户适用（最低
    currency: Currency = None             # 本次查询所用币种（仅期货账户适用）
    available_funds: float = None         # 可用资金（仅期货账户适用）
    unrealized_pl: float = None           # 未实现盈亏（仅期货账户适用）
    realized_pl: float = None             # 已实现盈亏（仅期货账户适用）
    # risk_level: CltRiskLevel = None       # 风控状态（仅期货账户适用）
    # risk_status: CltRiskStatus = None     # 风险状态（仅证券账户适用），共分9个等级， LEVEL1是最安全，LEVEL9是最危险
    initial_margin: float = None          # 初始保证金（最低
    margin_call_margin: float = None      # Margin Call 保证金
    maintenance_margin: float = None      # 维持保证金
    hk_cash: float = None                 # 港元现金（仅期货账户适用）
    hk_avl_withdrawal_cash: float = None  # 港元可提（仅期货账户适用）
    us_cash: float = None                 # 美元现金（仅期货账户适用）
    us_avl_withdrawal_cash: float = None  # 美元可提（仅期货账户适用）