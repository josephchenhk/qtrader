# -*- coding: utf-8 -*-
# @Time    : 13/9/2020 12:33 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: bondsim_server.py
# @Software: PyCharm
import os
import socket
import pytz
import pandas as pd
from datetime import datetime
import json
import time
import threading
import ast
from dateutil.parser import parse

from qtrader.setting import FEATHER_DATA_PATH
from qtrader.data_adaptors.feather_adapter import FeatherDataHandler
from qtrader.core.utility import main_path

os.chdir(main_path)

with open(f'setting/connect_bondsim.json') as json_file:
    setting = json.load(json_file)
    HOST = setting["地址"]
    PORT = setting["端口"]

def get_isins(issuer:str)->list:
    """Get vanilla bonds from the same issuer."""
    # TODO(joseph): Treasury products need to be considered seperately.
    data_path = "{}/real_estate_bonds_check.xlsx".format(FEATHER_DATA_PATH)
    df = pd.read_excel(data_path)
    df = df[(df["Issuer Name"] == issuer)
            & (df["Maturity Type"] == "AT MATURITY")
            & (df["Data Available"] == "yes")]
    isins = df["ISIN"].to_list()
    return isins

def get_bond_factsheet(isin:str)->dict:
    """Get bond information such as maturity date, issue date, and coupon."""
    # TODO(joseph): Treasury products need to be considered seperately.
    df = pd.read_excel("{}/real_estate_bonds_check.xlsx".format(FEATHER_DATA_PATH))
    df = df[df["ISIN"]==isin]
    if df.empty:
        return None
    df = df[["Issue Date", "Maturity", "Cpn Freq Des", "Cpn"]]
    res = dict(zip(df.columns, df.iloc[0].values))
    res["Maturity"] = datetime.strptime(res["Maturity"], "%m/%d/%Y")
    res["Issue Date"] = datetime.strptime(res["Issue Date"], "%m/%d/%Y")
    tz = pytz.timezone("Asia/Hong_Kong")
    res["Maturity"] = tz.localize(res["Maturity"])
    res["Issue Date"] = tz.localize(res["Issue Date"])
    return res

def prepare_data():
    issuer = "Greenland Global Investment Ltd"
    isins = get_isins(issuer)

    start = datetime(2020, 6, 19, 8, 0, 0)
    end = datetime(2020, 6, 23, 7, 59, 59)
    tz = pytz.timezone("Asia/Hong_Kong")
    start = tz.localize(start)
    end = tz.localize(end)

    source = "{}/{}".format(FEATHER_DATA_PATH, issuer)
    handlers = {}
    nodata_isins = []
    print("开始准备回测数据 ... ")
    for isin in isins:
        ticker = "{}@BGN Corp".format(isin)
        # TODO(joseph): each feather file might contain timestamps in two days, therefore should consider end+1
        handler = FeatherDataHandler(source, ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        tick = handler.read_tick()
        if tick is None:
            nodata_isins.append(isin)
        handler = FeatherDataHandler(source, ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        handlers["{}_handler".format(isin)] = handler

    # Remove tickers without data
    eff_isins = list(set(isins) - set(nodata_isins))

    # Get factsheets
    bond_factsheets = {}
    for isin in eff_isins:
        bond_factsheets[isin] = get_bond_factsheet(isin)
    print("数据准备完成")
    return start, eff_isins, handlers, bond_factsheets


class SimulatorServer:

    HOST = HOST        # Standard loopback interface address (localhost)
    PORT = PORT        # Port to listen on (non-privileged ports are > 1023)

    def __init__(self, cur_time:datetime, symbols:list, handlers:dict, factsheets:dict):
        self.cur_time = cur_time
        self.symbols = symbols
        self.handlers = handlers
        self.factsheets = factsheets
        self.subscribed_symbols = []
        self.last_snapshot_cache = {}
        self.snapshot_cache = {}
        print("开始校准历史数据时间戳 ...")
        self.generate_snapshot() # 先将时间校准至当前时间cur_time
        print("历史数据校准完毕")

    def subscribe(self, symbols:list):
        """ 订阅市场数据 """
        # TODO(joseph): give explicit hint on which symbol is not available
        assert set(symbols).issubset(set(self.symbols)), "Data not available!"
        self.subscribed_symbols = symbols

    def update_timer(self, time: datetime):
        """ 更新时钟，据此分发市场快照数据 """
        self.cur_time = time

    def generate_snapshot(self):
        """ 负责分发市场数据 """
        snapshot = {}
        for symbol in self.symbols:
            print(symbol)
            handler = self.handlers.get("{}_handler".format(symbol))
            # 如果有缓存，则看缓存市场数据的时间戳是否在当前时间之后
            ss_cache = self.snapshot_cache.get(symbol, None)
            if ss_cache is None:
                # 前面数据已经清洗过了，这里默认必须有数据
                ss_cache = handler.read_tick()
                assert ss_cache is not None, "Symbol {} does not have data!"
                self.last_snapshot_cache[symbol] = ss_cache
                self.snapshot_cache[symbol] = ss_cache            
            if ss_cache["datetime"] > self.cur_time:
                snapshot[symbol] = self.last_snapshot_cache.get(symbol, None)
                continue
            else:
                while ss_cache["datetime"] <= self.cur_time:
                    lss_cache = ss_cache
                    ss_cache = handler.read_tick()
                self.last_snapshot_cache[symbol] = lss_cache
                self.snapshot_cache[symbol] = ss_cache
                snapshot[symbol] = lss_cache

        self.snapshot = snapshot
        return snapshot

    def start_server(self):
        """ 另起独立线程处理连接"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, self.PORT))
            s.listen()
            print("等待client接入 ...")
            conn, addr = s.accept()   # accept是block函数，直到client运行connect以后才会运行
            with conn:                # conn是新建立的socket连接，该连接表示与客户端相连的连接
                print('Connected by', addr)
                while True:
                    data = conn.recv(1024)     # recv是block函数，直到接收到数据以后才会运行
                    print(data)
                    if not data:
                        # break
                        continue
                    data_dict = ast.literal_eval(data.decode("utf-8"))
                    if data_dict["type"] == "TIMER":  # 每次收到更新timer信号，推送市场快照
                        timer = parse(data_dict["data"])
                        self.update_timer(timer)
                        snapshot = self.generate_snapshot()
                        snapshot_data = {}
                        for symbol, tick in snapshot.items():
                            tick_data = {k:v for k,v in tick.items() if k!="datetime"}
                            tick_data.update(dict(datetime=tick["datetime"].isoformat()))
                            snapshot_data[symbol] = tick_data
                        ret_snapshot = {"type":"SNAPSHOT", "data":snapshot_data}
                        ret_snapshot = json.dumps(ret_snapshot) + "\n"  # 以换行符作结束信号
                        conn.sendall(bytes(ret_snapshot, 'utf-8'))


    def start(self):
        """"""
        t = threading.Thread(target=self.start_server)
        t.setDaemon(True)
        t.setName("SimServerThread")
        t.start()
        while True:
            time.sleep(1)


if __name__=="__main__":
    cur_time, eff_isins, handlers, bond_factsheets = prepare_data()
    s = SimulatorServer(cur_time=cur_time, symbols=eff_isins, handlers=handlers, factsheets=bond_factsheets)
    s.start()
