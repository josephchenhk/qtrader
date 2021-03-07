# -*- coding: utf-8 -*-
# @Time    : 6/2/2021 9:34 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: test_pair_trade_strategy.py
# @Software: PyCharm

import json
import sys

from qtrader.core.constant import Exchange, Product
from qtrader.core.event import EventEngine
from qtrader.core.engine import MainEngine
from qtrader.core.object import ContractData
from qtrader.core.script_engine import ScriptEngine
from qtrader.core.utility import TEMP_DIR
from qtrader.core.utility import PATH
from qtrader.gateway.futusim import FutusimGateway
from qtrader.database.data_recorder import RecorderEngine

strategy_path = PATH["strategy_path"]
sys.path.append(strategy_path)

def run():
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(FutusimGateway)

    with open(f'{TEMP_DIR}/connect_futu.json') as json_file:
        setting = json.load(json_file)
        script_engine = ScriptEngine(main_engine, event_engine)
        script_engine.connect_gateway(setting=setting, gateway_name="FUTU")
        script_engine.write_log("已连接FUTU接口")
        script_engine.start_strategy(script_path="pair_trade_strategy.py")

if __name__=="__main__":
    run()