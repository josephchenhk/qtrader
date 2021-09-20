# -*- coding: utf-8 -*-
# @Time    : 9/20/2021 9:59 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: process_data.py
# @Software: PyCharm

"""
This script is used to process 1 min bar data to other granularity.
"""

import os
from typing import List
from datetime import datetime
from pathlib import Path

import pandas as pd
from qtrader.config import DATA_PATH


def get_time_key(bars:List[pd.Series]):
    return datetime.strptime(bars[-1]["time_key"], "%Y-%m-%d %H:%M:%S")

def get_open(bars:List[pd.Series]):
    return bars[0]["open"]

def get_high(bars:List[pd.Series]):
    high = -float("Inf")
    for bar in bars:
        if bar["high"]>high:
            high = bar["high"]
    return high

def get_low(bars:List[pd.Series]):
    low = float("Inf")
    for bar in bars:
        if bar["low"]<low:
            low = bar["low"]
    return low

def get_close(bars:List[pd.Series]):
    return bars[-1]["close"]

def get_volume(bars:List[pd.Series]):
    volume = 0
    for bar in bars:
        volume += bar["volume"]
    return volume

def validate_bar_interval(bars:List[pd.Series], bar_interval:int):
    time_key1 = datetime.strptime(bars[0]["time_key"], "%Y-%m-%d %H:%M:%S")
    time_key2 = datetime.strptime(bars[-1]["time_key"], "%Y-%m-%d %H:%M:%S")
    time_key_diff = (time_key2 - time_key1).total_seconds() / 60.0
    return time_key_diff==bar_interval-1

def create_folder_if_not_exists(path:Path):
    path.mkdir(parents=True, exist_ok=True)

path = Path(DATA_PATH.get("k1m"))

ticker = "GC"
bar_interval = 3

data_files = os.listdir(path.joinpath(ticker))
data_files = sorted(data_files)
bar_folder_name = f"K_{int(bar_interval)}M"
save_path = path.parent.joinpath(bar_folder_name).joinpath(ticker)
create_folder_if_not_exists(save_path)

bars = []
new_bars = []
for data_file in data_files:
    df = pd.read_csv(f"{DATA_PATH.get('k1m')}/{ticker}/{data_file}")

    for idx, row in df.iterrows():
        if len(bars)>=bar_interval:
            bars.pop(0)
        bars.append(row)
        if len(bars)<bar_interval:
            continue
        assert len(bars)==bar_interval, f"Length of bars {len(bars)} does not match {bar_interval}!"

        # If the time difference is not as expected, we drop the oldest data
        if not validate_bar_interval(bars, bar_interval):
            bars.pop(0)
            continue

        # Now we reconstruct bar data from 1 min bar
        time_key = get_time_key(bars)
        open_ = get_open(bars)
        high = get_high(bars)
        low = get_low(bars)
        close_ = get_close(bars)
        volume = get_volume(bars)

        # clear bars vector and be ready to move to next bar period
        bars = []

        new_bar = {"time_key": time_key, "open": open_, "high": high, "low": low, "close": close_, "volume": volume}
        if len(new_bars)==0:
            new_bars.append(new_bar)
        elif new_bar["time_key"].date() == new_bars[-1]["time_key"].date():
            new_bars.append(new_bar)
        else:
            # save new bar data based on dates
            date = new_bars[-1]["time_key"].strftime("%Y-%m-%d")
            pd.DataFrame(new_bars).to_csv(save_path.joinpath(f"{date}.csv"), index=False)
            print(f"Save {bar_interval}M bar data on {date}.")
            # move to next bar period
            new_bars = []
            new_bars.append(new_bar)

