# -*- coding: utf-8 -*-
# @Time    : 8/9/2020 8:20 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: test_bond_strategy.py
# @Software: PyCharm

import json
import sys

from qtrader.core.event import EventEngine
from qtrader.core.engine import MainEngine
from qtrader.core.script_engine import ScriptEngine
from qtrader.core.utility import TEMP_DIR
from qtrader.core.utility import PATH
from qtrader.gateway.bondsim import BondsimGateway

strategy_path = PATH["strategy_path"]
sys.path.append(strategy_path)

def run():
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(BondsimGateway)

    with open(f'{TEMP_DIR}/connect_bondsim.json') as json_file:
        setting = json.load(json_file)
        script_engine = ScriptEngine(main_engine, event_engine)
        script_engine.connect_gateway(setting=setting, gateway_name="BONDSIM")
        script_engine.write_log("连接BONDSIM接口")
        script_engine.start_strategy(script_path="bond_strategy.py")

if __name__=="__main__":
    run()