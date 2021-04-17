# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: engine.py
# @Software: PyCharm
import importlib
from datetime import datetime
from typing import List, Union, Any, Dict

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, Offset, OrderType, TradeMode
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.portfolio import Portfolio
from qtrader.core.position import PositionData
from qtrader.core.security import Stock
from qtrader.core.data import Bar, CapitalDistribution, OrderBook, Quote
from qtrader.core.data import _get_data
from qtrader.core.logger import logger
from qtrader.config import DATA_MODEL, DATA_PATH
from qtrader.config import ACTIVATED_PLUGINS
from qtrader.config import GATEWAY

class Engine:

    """Execution engine"""

    def __init__(self, portfolio:Portfolio, plugins:Dict[str, Any]=dict()):
        self.portfolio = portfolio
        self.market = portfolio.market
        self.log = logger
        for plugin in ACTIVATED_PLUGINS:
            plugins[plugin] = importlib.import_module(f"qtrader.plugins.{plugin}")
        self.plugins = plugins
        if "sqlite3" in self.plugins:
            DB = getattr(self.plugins["sqlite3"], "DB")
            self.db = DB()

    def get_plugins(self)->Dict[str, Any]:
        """engine启动的插件"""
        return self.plugins

    def has_db(self):
        """判断是否启动db"""
        return hasattr(self, "db")

    def sync_broker_balance(self):
        """同步券商资金"""
        broker_balance = self.get_broker_balance()
        if broker_balance is None:
            return
        if not self.has_db():
            self.portfolio.account_balance = broker_balance
            return
        # 对db数据进行处理
        db_balance = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
        )
        if db_balance.empty:
            account_ids = self.db.select_records(table_name="balance", columns=["broker_account_id", "strategy_account_id"])
            if account_ids.empty:
                broker_account_id = 1
                strategy_account_id = 1
            else:
                broker_account_id = max(account_ids["broker_account_id"]) + 1
                strategy_account_id = max(account_ids["strategy_account_id"]) + 1
            # 先创建一条default记录
            self.db.insert_balance_table(
                broker_name=GATEWAY["broker_name"],
                broker_environment=self.market.trade_mode.name,
                broker_account_id=broker_account_id,
                broker_account=GATEWAY["broker_account"],
                strategy_account_id=strategy_account_id,
                strategy_account="default",
                strategy_version=self.strategy_version,
                strategy_version_desc="manual trading",
                strategy_status="active",
                cash=broker_balance.cash - self.portfolio.account_balance.cash,
                power=broker_balance.power - self.portfolio.account_balance.power,
                max_power_short=-1,
                net_cash_power=-1,
                update_time=datetime.now(),
                remark="N/A"
            )
            # 再创建策略balance记录
            self.db.insert_balance_table(
                broker_name = GATEWAY["broker_name"],
                broker_environment = self.market.trade_mode.name,
                broker_account_id = broker_account_id,
                broker_account = GATEWAY["broker_account"],
                strategy_account_id = strategy_account_id + 1,
                strategy_account = self.strategy_account,
                strategy_version = self.strategy_version,
                strategy_version_desc = "",
                strategy_status="active",
                cash = self.portfolio.account_balance.cash,
                power = self.portfolio.account_balance.power,
                max_power_short = -1,
                net_cash_power = -1,
                update_time = datetime.now(),
                remark = "N/A"
            )
        else:
            # update default strategy
            id_ = db_balance[db_balance["strategy_account"]=="default"]["id"].values[0]
            fields = ("cash", "power")
            for field in fields:
                delta_val = getattr(broker_balance, field) - sum(db_balance[field])
                current_val = db_balance[db_balance["strategy_account"]=="default"][field].values[0]
                if delta_val!=0:
                    sql = (
                        "UPDATE balance " +
                        f"SET {field} = {current_val+delta_val} "
                        f"WHERE id={id_}"
                    )
                    self.db.execute(sql)

        db_balance = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
            strategy_account=self.strategy_account,
            strategy_version=self.strategy_version,
        )

        assert db_balance.shape[0]==1, f"There are more than one rows in db: {self.strategy_account} {self.strategy_version}"
        self.portfolio.account_balance.cash = db_balance["cash"].values[0]
        self.portfolio.account_balance.power = db_balance["power"].values[0]
        self.portfolio.account_balance.max_power_short = db_balance["max_power_short"].values[0]
        self.portfolio.account_balance.net_cash_power = db_balance["net_cash_power"].values[0]


    def sync_broker_position(self):
        """同步券商持仓"""
        all_broker_positions = self.get_all_broker_positions()
        if all_broker_positions is None:
            return
        for broker_position in all_broker_positions:
            self.portfolio.position.update(position_data=broker_position, offset=Offset.OPEN)

    def send_order(self,
        security:Stock,
        price:float,
        quantity:float,
        direction:Direction,
        offset:Offset,
        order_type:OrderType
        )->str:
        """发出订单"""
        create_time = self.market.market_datetime
        order = Order(
            security = security,
            price = price,
            quantity = quantity,
            direction = direction,
            offset = offset,
            order_type = order_type,
            create_time = create_time
        )
        orderid = self.market.place_order(order)
        return orderid

    def cancel_order(self, orderid):
        """取消订单"""
        self.market.cancel_order(orderid)

    def get_order(self, orderid)->Order:
        """获取订单的状态"""
        return self.market.get_order(orderid)

    def get_recent_data(self,
                       security:Stock,
                       cur_datetime:datetime=datetime.now(),
                       **kwargs,
        )->Union[Bar, List[Bar]]:
        """
        获取最接近当前时间的数据点 (或者最近一段bar数据)
        如果传入kwargs，则传入 dfield="k1m" 或其他指定数据，需要预先在DATA_PATH进行指定
        """
        return self.market.get_recent_data(security, cur_datetime, **kwargs)

    def get_history_data(self,
                        security: Stock,
                        start: datetime,
                        end: datetime,
                        **kwargs,
        )->Dict[str, List[Any]]:
        """
        获取历史时间段的数据
        """
        if kwargs:
            assert "dfield" in kwargs, f"`dfield` should be passed in as kwargs, but kwargs={kwargs}"
            dfields = [kwargs["dfield"]]
        else:
            dfields = DATA_PATH
        data = dict()
        for dfield in dfields:
            if "dtype" in kwargs:
                data[dfield] = self.get_history_data_by_dfield(
                    security=security,
                    start=start,
                    end=end,
                    dfield=dfield,
                    dtype=kwargs["dtype"]
                )
            else:
                data[dfield] = self.get_history_data_by_dfield(
                    security=security,
                    start=start,
                    end=end,
                    dfield=dfield
                )
        if len(dfields)==1:
            return data[dfield]
        return data

    def get_history_data_by_dfield(self,
                        security: Stock,
                        start: datetime,
                        end: datetime,
                        dfield: str,
                        dtype: List[str] = None,
        )->List[Any]:
        """
        获取历史时间段的数据 (传入指定dfield)
        """
        df = _get_data(security=security, start=start, end=end, dfield=dfield, dtype=dtype)
        if dtype is None:
            time_cols = [c for c in df.columns if "time" in c or "Time" in c]
            assert len(time_cols)==1, f"There should be one column related to `*time*`, but we got {df.columns}"
            time_col = time_cols[0]
        else:
            assert "time" in dtype[0] or "Time" in dtype[0], f"The first column in dtype should be related to `*time*`, but we got {dtype[0]}"
            time_col = dtype[0]
        data_cols = [col for col in df.columns if col!=time_col] # 除了time_col之外的所有其他数据
        data_cls = getattr(importlib.import_module("qtrader.core.data"), DATA_MODEL[dfield])
        datas = []
        for _, row in df.iterrows():
            cur_time = datetime.strptime(row[time_col], "%Y-%m-%d %H:%M:%S")
            kwargs = {"datetime": cur_time, "security": security}
            for col in data_cols:
                kwargs[col] = row[col]
            data = data_cls(**kwargs)
            datas.append(data)
        return datas

    def find_deals_with_orderid(self, orderid:str)->List[Deal]:
        """根据orderid找出成交的deal"""
        return self.market.find_deals_with_orderid(orderid)

    def get_balance(self)->AccountBalance:
        """balance"""
        return self.portfolio.account_balance

    def get_broker_balance(self)->AccountBalance:
        """broker balance"""
        return self.market.get_broker_balance()

    def get_position(self, security:Stock, direction:Direction)->PositionData:
        """position"""
        return self.portfolio.position.get_position(security, direction)

    def get_broker_position(self, security:Stock, direction:Direction)->PositionData:
        """broker position"""
        return self.market.get_broker_position(security, direction)

    def get_all_positions(self)->List[PositionData]:
        """all positions"""
        return self.portfolio.position.get_all_positions()

    def get_all_broker_positions(self)->List[PositionData]:
        """all broker positions"""
        return self.market.get_all_broker_positions()

    def get_quote(self, security:Stock)->Quote:
        """获取最新quote (回测模式下暂不支持）"""
        return self.market.get_quote(security)

    def get_orderbook(self, security:Stock)->OrderBook:
        """获取最新orderbook （回测模式下暂不支持）"""
        return self.market.get_orderbook(security)

    def get_capital_distribution(self, security:Stock)->CapitalDistribution:
        """获取资金分布"""
        return self.market.get_capital_distribution(security)
