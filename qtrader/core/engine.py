# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 9:50 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: engine.py
# @Software: PyCharm
import importlib
from datetime import datetime
from time import sleep
from typing import List, Union, Any, Dict
from threading import Thread

from qtrader.core.balance import AccountBalance
from qtrader.core.constants import Direction, Offset, OrderType, TradeMode, OrderStatus
from qtrader.core.deal import Deal
from qtrader.core.order import Order
from qtrader.core.portfolio import Portfolio
from qtrader.core.position import PositionData, Position
from qtrader.core.security import Stock
from qtrader.core.data import Bar, CapitalDistribution, OrderBook, Quote
from qtrader.core.data import _get_data
from qtrader.core.logger import logger
from qtrader.config import DATA_MODEL, DATA_PATH
from qtrader.config import ACTIVATED_PLUGINS
from qtrader.config import GATEWAY
from qtrader.gateways import BaseGateway

PERSIST_TIME_INTERVAL = 5

class Engine:

    """Execution engine"""

    def __init__(self, market:BaseGateway):
        self.market = market
        self.log = logger
        self.plugins = dict()
        for plugin in ACTIVATED_PLUGINS:
            self.plugins[plugin] = importlib.import_module(f"qtrader.plugins.{plugin}")
        # 只有仿真和实盘环境才会进行数据持久化，回测环境下即使在config里指定启用db，也会被忽略
        if (market.trade_mode in (TradeMode.SIMULATE, TradeMode.LIVETRADE)) and ("sqlite3" in self.plugins):
            DB = getattr(self.plugins["sqlite3"], "DB")
            self.db = DB()
            # 新建线程，定期将数据持久化到数据库
            self.persist_active: bool = False
            self._persist_t: Thread = Thread(target=persist_data, args=(self,), name="persist_thread")

    def start(self):
        """启动engine，在事件循环开始之前启动"""
        if self.has_db():
            self.persist_active = True
            self._persist_t.start()
        self.log.info("Engine starts")

    def stop(self):
        """停止engine，在事件循环结束之后/或者手动停止循环之后"""
        if self.has_db():
            self.persist_active = False
            sleep(PERSIST_TIME_INTERVAL+2) # wait for the persist thread stop
            self.db.close()
        self.log.info("Engine stops")

    def init_portfolio(self, strategy_account:str, strategy_version:str, init_strategy_cash:float):
        """初始化投资组合相关信息"""
        self.strategy_account = strategy_account
        self.strategy_version = strategy_version
        # 先初始化投资组合管理，account_balance和position会在后面进行同步sync
        self.portfolio = Portfolio(
            account_balance=AccountBalance(cash=init_strategy_cash), # 账户余额
            position=Position(),                                     # 头寸管理
            market=self.market                                       # 交易通道
        )

    def get_plugins(self)->Dict[str, Any]:
        """engine启动的插件"""
        return self.plugins

    def has_db(self):
        """判断是否启动db"""
        return hasattr(self, "db")

    def get_balance_id(self):
        """获取数据库里面的balance id"""
        balance_df = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
            strategy_account=self.strategy_account,
            strategy_version=self.strategy_version,
        )
        if balance_df.empty:
            self.log.info(
                "[get_balance_id] Balance id is not available in the DB yet, need to sync balance first."
            )
            return
        assert balance_df.shape[0] == 1, f"There are more than 1 records found in Balance. Check\n{balance_df}"
        balance_id = balance_df["id"].values[0]
        return balance_id

    def get_db_order(self, balance_id:int, broker_order_id:str)->Order:
        """获取数据库的order记录（如果数据库无记录，返回None）"""
        order_df = self.db.select_records(
            table_name="trading_order",
            balance_id=balance_id,
            broker_order_id=broker_order_id,
        )
        if order_df.empty:
            return None
        assert order_df.shape[0]==1, f"There are more than one rows in order: balance_id={balance_id} broker_order_id={broker_order_id}"
        order = Order(
            security = Stock(stock_name=order_df["security_name"].values[0], code=order_df["security_code"].values[0]),
            price = order_df["price"].values[0],
            quantity = order_df["quantity"].values[0],
            direction = convert_direction_db2qt(order_df["direction"].values[0]),
            offset = convert_offset_db2qt(order_df["offset"].values[0]),
            order_type = convert_order_type_db2qt(order_df["order_type"].values[0]),
            create_time = datetime.strptime(order_df["create_time"].values[0], "%Y-%m-%d %H:%M:%S"),
            updated_time = datetime.strptime(order_df["update_time"].values[0], "%Y-%m-%d %H:%M:%S"),
            filled_avg_price = order_df["filled_avg_price"].values[0],
            filled_quantity = order_df["filled_quantity"].values[0],
            status = convert_order_status_db2qt(order_df["status"].values[0]),
            orderid = order_df["broker_order_id"].values[0],
        )
        return order

    def get_db_deal(self, balance_id:int, broker_deal_id:str):
        """获取数据库的deal记录（如果数据库无记录，返回None）"""
        deal_df = self.db.select_records(
            table_name="trading_deal",
            balance_id=balance_id,
            broker_deal_id=broker_deal_id,
        )
        if deal_df.empty:
            return None
        assert deal_df.shape[0]==1, f"There are more than one rows in order: balance_id={balance_id} broker_order_id={broker_order_id}"
        deal = Deal(
            security = Stock(stock_name=deal_df["security_name"].values[0], code=deal_df["security_code"].values[0]),
            direction = convert_direction_db2qt(deal_df["direction"].values[0]),
            offset = convert_offset_db2qt(deal_df["offset"].values[0]),
            order_type = convert_order_type_db2qt(deal_df["order_type"].values[0]),
            updated_time = datetime.strptime(deal_df["update_time"].values[0], "%Y-%m-%d %H:%M:%S"),
            filled_avg_price = deal_df["filled_avg_price"].values[0],
            filled_quantity = deal_df["filled_quantity"].values[0],
            dealid = deal_df["broker_deal_id"].values[0],
            orderid = deal_df["broker_order_id"].values[0],
        )
        return deal

    def get_db_balance(self, strategy_account:str, strategy_version:str)->AccountBalance:
        """从数据库加载balance（如果数据库无记录，返回None）"""
        balance_df = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
            strategy_account=strategy_account,
            strategy_version=strategy_version,
        )
        if balance_df.empty:
            return None
        assert balance_df.shape[0]==1, f"There are more than one rows in balance: {self.strategy_account} {self.strategy_version}"
        account_balance = AccountBalance(
            cash=balance_df["cash"].values[0],
            power=balance_df["power"].values[0],
            max_power_short=balance_df["max_power_short"].values[0],
            net_cash_power=balance_df["net_cash_power"].values[0]
        )
        return account_balance

    def get_db_position(self, balance_id:int)->Position:
        """从数据库加载position（如果数据库无记录，返回None）"""
        position_df = self.db.select_records(
            table_name="position",
            balance_id=balance_id,
        )
        if position_df.empty:
            return None
        position = Position()
        for _, row in position_df.iterrows():
            security = Stock(code=row["security_code"], stock_name=row["security_name"])
            direction = convert_direction_db2qt(row["direction"])
            position_data = PositionData(
                security=security,
                direction=direction,
                holding_price=row["holding_price"],
                quantity=row["quantity"],
                update_time=datetime.strptime(row["update_time"], "%Y-%m-%d %H:%M:%S")
            )
            offset = Offset.OPEN
            position.update(position_data=position_data, offset=offset)
        return position

    def sync_broker_balance(self):
        """同步券商资金"""
        broker_balance = self.get_broker_balance()
        if broker_balance is None: return
        if not self.has_db():
            self.portfolio.account_balance = broker_balance
            return
        # 对db数据进行处理
        balance_df = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
        )
        if balance_df.empty:
            account_ids = self.db.select_records(table_name="balance", columns=["broker_account_id", "strategy_account_id"])
            if account_ids.empty:
                broker_account_id = 1
                strategy_account_id = 1
            else:
                broker_account_id = max(account_ids["broker_account_id"]) + 1
                strategy_account_id = max(account_ids["strategy_account_id"]) + 1
            # 先创建一条default记录
            cash = broker_balance.cash - self.portfolio.account_balance.cash
            power = broker_balance.power - self.portfolio.account_balance.power
            assert cash >= 0, f"{self.strategy_account}({self.strategy_version}) 现金占用的额度不能超过broker总可用现金"
            assert power >= 0, f"{self.strategy_account}({self.strategy_version}) 购买力占用的额度不能超过broker总购买力"
            self.db.insert_records(
                table_name="balance",
                broker_name=GATEWAY["broker_name"],
                broker_environment=self.market.trade_mode.name,
                broker_account_id=broker_account_id,
                broker_account=GATEWAY["broker_account"],
                strategy_account_id=strategy_account_id,
                strategy_account="default",
                strategy_version=self.strategy_version,
                strategy_version_desc="manual trading",
                strategy_status="active",
                cash=cash,
                power=power,
                max_power_short=-1,
                net_cash_power=-1,
                update_time=datetime.now(),
                remark=""
            )
            # 再创建策略balance记录
            self.db.insert_records(
                table_name="balance",
                broker_name = GATEWAY["broker_name"],
                broker_environment = self.market.trade_mode.name,
                broker_account_id = broker_account_id,         # 与default同一个broker account id
                broker_account = GATEWAY["broker_account"],
                strategy_account_id = strategy_account_id + 1, # 与default不同strategy account id
                strategy_account = self.strategy_account,
                strategy_version = self.strategy_version,
                strategy_version_desc = "",
                strategy_status="active",
                cash = self.portfolio.account_balance.cash,
                power = self.portfolio.account_balance.power,
                max_power_short = -1,
                net_cash_power = -1,
                update_time = datetime.now(),
                remark = ""
            )
        else:
            # update default strategy
            id_ = balance_df[balance_df["strategy_account"]=="default"]["id"].values[0]
            fields = ("cash", "power")
            for field in fields:
                delta_val = getattr(broker_balance, field) - sum(balance_df[field])
                current_val = balance_df[balance_df["strategy_account"]=="default"][field].values[0]
                assert current_val+delta_val>=0, (
                    f"Check balance (id={id_}:\n"
                    f"Current {field}: {current_val}, but we want to modify to a negative number:"
                    f"new {field}: {current_val+delta_val} ({current_val} + {delta_val})"
                )
                if delta_val!=0:
                    self.db.update_records(
                        table_name="balance",
                        columns={field: current_val+delta_val},
                        id=id_
                    )
        # 因为前面已经插入了数据，所以这里一定找得到
        self.portfolio.account_balance = self.get_db_balance(
            strategy_account=self.strategy_account,
            strategy_version=self.strategy_version
        )

    def sync_broker_position(self):
        """同步券商持仓"""
        all_broker_positions = self.get_all_broker_positions()
        if all_broker_positions is None: return
        if not self.has_db():
            for broker_position in all_broker_positions:
                self.portfolio.position.update(position_data=broker_position, offset=Offset.OPEN)
            return

        # 对db数据进行处理
        balance_df = self.db.select_records(
            table_name="balance",
            broker_name=GATEWAY["broker_name"],
            broker_environment=self.market.trade_mode.name,
            broker_account=GATEWAY["broker_account"],
        )

        assert not balance_df.empty, ("balance should not be empty, as it should have already been "
                                      f"inserted in sync_broker_balance, please check "
                                      f"broker_name={GATEWAY['broker_name']}, "
                                      f"broker_environment={self.market.trade_mode.name}, "
                                      f"broker_account={GATEWAY['broker_account']} ")
        strat_balance_id = balance_df[(balance_df["strategy_account"]==self.strategy_account)
                                      & (balance_df["strategy_version"]==self.strategy_version) ]["id"].values[0]
        balance_ids = balance_df["id"].values.tolist()
        position_df = self.db.select_records(
            table_name="position",
            condition_str=f"balance_id in ({','.join(str(id) for id in balance_ids)})"
        )

        # 先找到default账户(假定default账户永远只有1.0版本)
        default_balance_df = balance_df[
            (balance_df["strategy_account"] == "default") &
            (balance_df["strategy_version"] == "1.0")]
        assert default_balance_df.shape[0] == 1, (
            "There should be an unique record in `balance` table, "
            f"but {default_balance_df.shape[0]} records are found. Check "
            "strategy_account=default, strategy_version=1.0"
        )
        default_balance_id = default_balance_df["id"].values[0]

        # 如果该broker账户在数据库里无记录：
        if position_df.empty:
            # 将所有broker position 写入数据库 default账户
            for broker_position in all_broker_positions:
                self.db.insert_records(
                    table_name="position",
                    balance_id=default_balance_id,
                    security_name=broker_position.security.stock_name,
                    security_code=broker_position.security.code,
                    direction=broker_position.direction.name,
                    holding_price=broker_position.holding_price,
                    quantity=broker_position.quantity,
                    update_time=broker_position.update_time
                )
            # 当前策略的position则为空仓
            self.portfolio.position = Position()

        # 如果该broker账户在数据库里有记录：
        else:
            #
            default_position_df = position_df[position_df["balance_id"]==default_balance_id]
            nondefault_position_df = position_df[position_df["balance_id"]!=default_balance_id]

            strat_positions = []
            for _, row in nondefault_position_df.iterrows():
                security = Stock(code=row["security_code"], stock_name=row["security_name"])
                direction = convert_direction_db2qt(row["direction"])
                db_pos = PositionData(
                    security=security, # TODO: lot_size is not available
                    direction=direction,
                    holding_price=row["holding_price"],
                    quantity=row["quantity"],
                    update_time=datetime.strptime(row["update_time"], "%Y-%m-%d %H:%M:%S")
                )
                # 当前策略的底仓
                if row["balance_id"]==strat_balance_id:
                    strat_positions.append(db_pos)
                # 将nondefault_position从all_broker_positions里面扣除
                for idx, brk_pos in enumerate(all_broker_positions):
                    if brk_pos.security!=db_pos.security or brk_pos.direction!=db_pos.direction:
                        continue
                    quantity = brk_pos.quantity - db_pos.quantity
                    if quantity>0:
                        holding_price = (brk_pos.holding_price*brk_pos.quantity - db_pos.holding_price*db_pos.quantity)/quantity
                    elif quantity==0:
                        holding_price = -1 #  该记录将不会进行入库
                    else:
                        raise ValueError("Position data in database is larger than the number in broker server. "
                                         f"Check broker_position={brk_pos} "
                                         f"and db_positin={db_pos}")
                    all_broker_positions[idx] = PositionData(
                        security=security,
                        direction=direction,
                        holding_price=holding_price,
                        quantity=quantity,
                        update_time=datetime.strptime(row["update_time"], "%Y-%m-%d %H:%M:%S")
                    )

            # 更新数据库：剩下的broker positions全部进入default账户
            self.db.delete_records(
                table_name="position",
                balance_id=default_balance_id,
            )
            for broker_position in all_broker_positions:
                if broker_position.quantity>0:
                    self.db.insert_records(
                        table_name="position",
                        balance_id=default_balance_id,
                        security_name=broker_position.security.stock_name,
                        security_code=broker_position.security.code,
                        direction=broker_position.direction.name,
                        holding_price=broker_position.holding_price,
                        quantity=broker_position.quantity,
                        update_time=broker_position.update_time
                    )
            # 更新当前账户的持仓
            position = self.get_db_position(balance_id=strat_balance_id)
            self.portfolio.position = Position() if position is None else position

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

    def get_all_orders(self)->List[Order]:
        """all orders"""
        return self.market.get_all_orders()

    def get_all_deals(self)->List[Deal]:
        """all deals"""
        return self.market.get_all_deals()

    def get_quote(self, security:Stock)->Quote:
        """获取最新quote (回测模式下暂不支持）"""
        return self.market.get_quote(security)

    def get_orderbook(self, security:Stock)->OrderBook:
        """获取最新orderbook （回测模式下暂不支持）"""
        return self.market.get_orderbook(security)

    def get_capital_distribution(self, security:Stock)->CapitalDistribution:
        """获取资金分布"""
        return self.market.get_capital_distribution(security)


def convert_direction_db2qt(direction:str) -> Direction:
    """将交易方向由string转换成qtrader的Direction类"""
    if direction=="LONG":
        return Direction.LONG
    elif direction=="SHORT":
        return Direction.SHORT
    elif direction=="NET":
        return Direction.NET
    else:
        raise ValueError(f"Direction {direction} in database is invalid!")

def convert_offset_db2qt(offset:str)->Offset:
    """将交易offset由string转换成qtrader的Offset类"""
    if offset=="NONE":
        return Offset.NONE
    elif offset=="OPEN":
        return Offset.OPEN
    elif offset=="CLOSE":
        return Offset.CLOSE
    elif offset=="CLOSETODAY":
        return Offset.CLOSETODAY
    elif offset=="CLOSEYESTERDAY":
        return Offset.CLOSEYESTERDAY
    else:
        raise ValueError(f"Offset {offset} in database is invalid!")

def convert_order_type_db2qt(order_type:str)->OrderType:
    """将订单类型由string转换成qtrader的OrderType类"""
    if order_type=="LIMIT":
        return OrderType.LIMIT
    elif order_type=="MARKET":
        return OrderType.MARKET
    elif order_type=="STOP":
        return OrderType.STOP
    elif order_type=="FAK":
        return OrderType.FAK
    elif order_type=="FOK":
        return OrderType.FOK
    else:
        raise ValueError(f"Order Type {order_type} in database is invalid!")

def convert_order_status_db2qt(order_status:str)->OrderStatus:
    """将订单状态由string转换成qtrader的OrderStatus类"""
    if order_status=="UNKNOWN":
        return OrderStatus.UNKNOWN
    elif order_status=="SUBMITTED":
        return OrderStatus.SUBMITTED
    elif order_status=="FILLED":
        return OrderStatus.FILLED
    elif order_status=="PART_FILLED":
        return OrderStatus.PART_FILLED
    elif order_status=="CANCELLED":
        return OrderStatus.CANCELLED
    elif order_status=="FAILED":
        return OrderStatus.FAILED
    else:
        raise ValueError(f"Order Status {order_status} in database is invalid!")

def persist_data(engine, time_interval=PERSIST_TIME_INTERVAL):
    """定期更新数据库里的数据"""
    # 将主线程的数据库连接暂时保存起来
    db_main = engine.db
    # 在新线程里重新建立数据库连接
    DB = getattr(engine.plugins["sqlite3"], "DB")
    setattr(engine, "db", DB())
    # 定期进行数据入库
    while engine.persist_active:
        persist_account_balance(engine)
        engine.log.info("[Account balance] is persisted")
        persist_position(engine)
        engine.log.info("[Position] is persisted")
        persist_order(engine)
        engine.log.info("[Order] is persisted")
        persist_deal(engine)
        engine.log.info("[Deal] is persisted")
        sleep(time_interval)
    engine.db.close()
    # 还原主线程的数据库连接
    setattr(engine, "db", db_main)
    engine.log.info("Gracefully stop persisting data.")

def persist_account_balance(engine):
    """账户有任何更新，就写入数据库"""
    db_balance = engine.get_db_balance(strategy_account=engine.strategy_account,
                                     strategy_version=engine.strategy_version)
    if db_balance is None:
        engine.log.info(
            "[persist_account_balance] Account Balance is not available in the DB yet, need to sync balance first."
        )
        return
    updates = dict()
    for field in ("cash", "power", "max_power_short", "net_cash_power"):
        if getattr(engine.portfolio.account_balance, field) != getattr(db_balance, field):
            updates[field] = getattr(engine.portfolio.account_balance, field)
    if updates:
        engine.db.update_records(
            table_name="balance",
            columns=updates,
            strategy_account=engine.strategy_account,
            strategy_version=engine.strategy_version,
        )

def persist_position(engine):
    """头寸有任何更新，就写入数据库"""
    engine_position = engine.portfolio.position
    engine_positions = engine_position.get_all_positions()
    balance_id = engine.get_balance_id()
    db_position = engine.get_db_position(balance_id=balance_id)
    # if db_position:
    #     engine.db.delete_records(
    #         table_name="position",
    #         balance_id=balance_id,
    #     )
    # for position_data in engine.portfolio.position.get_all_positions():
    #     engine.db.insert_records(
    #         table_name="position",
    #         balance_id=balance_id,
    #         security_name=position_data.security.stock_name,
    #         security_code=position_data.security.code,
    #         direction=position_data.direction.name,
    #         holding_price=position_data.holding_price,
    #         quantity=position_data.quantity,
    #         update_time=position_data.update_time
    #     )

    if db_position is None:
        for position_data in engine_positions:
            engine.db.insert_records(
                table_name="position",
                balance_id=balance_id,
                security_name=position_data.security.stock_name,
                security_code=position_data.security.code,
                direction=position_data.direction.name,
                holding_price=position_data.holding_price,
                quantity=position_data.quantity,
                update_time=position_data.update_time
            )
    else:
        # 将存在与db，但在内存记录里找不到的position删除，因为该头寸已经平掉
        for db_position_data in db_position.get_all_positions():
            engine_position_data = engine.portfolio.position.get_position(
                security=db_position_data.security,
                direction=db_position_data.direction,
            )
            if engine_position_data is None:
                engine.db.delete_records(
                    table_name="position",
                    balance_id=balance_id,
                    security_name=db_position_data.security.stock_name,
                    security_code=db_position_data.security.code,
                    direction=db_position_data.direction.name,
                )
        # 现在数据库的记录是内存记录的子集了。根据内存记录里的position数据，更新数据库的记录
        for position_data in engine_positions:
            db_position_data = db_position.get_position(
                security=position_data.security,
                direction=position_data.direction
            )
            if db_position_data is None:
                engine.db.insert_records(
                    table_name="position",
                    balance_id=balance_id,
                    security_name=position_data.security.stock_name,
                    security_code=position_data.security.code,
                    direction=position_data.direction.name,
                    holding_price=position_data.holding_price,
                    quantity=position_data.quantity,
                    update_time=position_data.update_time
                )
            elif position_data.quantity!=db_position_data.quantity:
                engine.db.update_records(
                    table_name="position",
                    columns=dict(
                        holding_price=position_data.holding_price,
                        quantity=position_data.quantity,
                        update_time=position_data.update_time
                    ),
                    balance_id=balance_id,
                    security_name=position_data.security.stock_name,
                    security_code=position_data.security.code,
                    direction=position_data.direction.name,
                )

def persist_order(engine):
    """订单有任何更新，就写入数据库"""
    balance_id = engine.get_balance_id()
    orders = engine.get_all_orders()
    engine.log.info(orders)
    for order in orders:
        db_order = engine.get_db_order(balance_id=balance_id, broker_order_id=order.orderid)
        if db_order is None:
            engine.db.insert_records(
                table_name="trading_order",
                broker_order_id=order.orderid,
                balance_id=balance_id,
                security_name=order.security.stock_name,
                security_code=order.security.code,
                price=order.price,
                quantity=order.quantity,
                direction=order.direction.name,
                offset=order.offset.name,
                order_type=order.order_type.name,
                create_time=order.create_time,
                update_time=order.updated_time,
                filled_avg_price=order.filled_avg_price,
                filled_quantity=order.filled_quantity,
                status=order.status.name,
                remark="",
            )
        elif order.status!=db_order.status:
            engine.db.update_records(
                table_name="trading_order",
                columns=dict(
                    update_time=order.updated_time,
                    filled_avg_price=order.filled_avg_price,
                    filled_quantity=order.filled_quantity,
                    status=order.status.name,
                ),
                balance_id=balance_id,
                broker_order_id=order.orderid
            )

def persist_deal(engine):
    """成交有任何更新，就写入数据库"""
    balance_id = engine.get_balance_id()
    deals = engine.get_all_deals()
    engine.log.info(deals)
    for deal in deals:
        db_deal = engine.get_db_deal(balance_id=balance_id, broker_deal_id=deal.dealid)
        if db_deal is None:
            # 同步order在同步deal之前，所以这里应该是一定找得到
            order_df = engine.db.select_records(
                table_name="trading_order",
                balance_id=balance_id,
                broker_order_id=deal.orderid,
            )
            assert not order_df.empty, (
                f"Records not found in order table. Check balance_id={balance_id}, broker_order_id={deal.orderid}"
            )
            assert order_df.shape[0]==1, (
                f"More than 1 records were found in order table. Check balance_id={balance_id}, broker_order_id={deal.orderid}"
            )
            order_id = order_df["id"].values[0]
            engine.db.insert_records(
                table_name="trading_deal",
                broker_deal_id=deal.dealid,
                broker_order_id=deal.orderid,
                order_id=order_id,
                balance_id=balance_id,
                security_name=deal.security.stock_name,
                security_code=deal.security.code,
                direction=deal.direction.name,
                offset=deal.offset.name,
                order_type=deal.order_type.name,
                update_time=deal.updated_time,
                filled_avg_price=deal.filled_avg_price,
                filled_quantity=deal.filled_quantity,
                remark="",
            )