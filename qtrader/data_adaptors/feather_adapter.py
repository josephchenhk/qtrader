# -*- coding: utf-8 -*-
# @Time    : 2/9/2020 9:21 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: feather_adapter.py
# @Software: PyCharm


import os
from pyarrow.feather import read_feather
import pandas as pd
from pathlib import Path
from datetime import datetime
from quantkits.time.conversion import from_timestamp_to_datetime
from quantkits.logger import logger

from qtrader.data_adaptors.base_adapter import BaseDataHandler

class FeatherDataHandler(BaseDataHandler):
    """A reader that handles data in Apache feather format
    """

    def __init__(self, source:str, ticker:str, start:datetime, end:datetime):
        """Allow only dates within given range"""
        self.start = start
        self.end = end
        # start = int(start.replace("-", ""))
        # end = int(end.replace("-", ""))
        start = int(start.strftime("%Y%m%d"))
        end = int(end.strftime("%Y%m%d"))
        assert end>start, "End date {} must be larger than start date {}".format(end, start)
        #print(source, ticker)
        data_path = os.path.join(source, ticker)
        # set an indicator to show whether we have data or not
        self.has_data = True
        if not Path(data_path).exists():
            self.has_data = False
        else:
            data_files = [f for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f))]
            data_files = sorted(data_files)
            data_dates = []
            # Note: each feather file includes two trading days' data: for example, 20200619.feather file contains data
            # in 2020-06-18 and 2020-06-19. Therefore we need to include the feather file in next trading day after `end`
            # Theoretically we don't need to include feather file in the previous trading day before `start`, but in order
            # to avoid data-not-available errors when we assign a particular beginning timestamp such that there is no
            # snapshot data in that day, we also include the previous trading day data here.
            start_idx = None
            end_idx = None
            for idx, data_file in enumerate(data_files):
                data_date = int(data_file.replace(".feather", ""))
                if data_date<start:
                    start_idx = idx
                    continue
                elif data_date>=start and data_date<=end:
                    data_dates.append(data_date)
                elif data_date>end:
                    end_idx = idx
                    break
            if start_idx is not None:
                start_idx_date = int(data_files[start_idx].replace(".feather", ""))
                if start_idx_date not in data_dates:
                    data_dates.insert(0, start_idx_date)
            else:
                logger.warn("start_idx is not available. You might want to check data availability of {} before {}".format(ticker, start))
            if end_idx is not None:
                end_idx_date = int(data_files[end_idx].replace(".feather", ""))
                if end_idx_date not in data_dates:
                    data_dates.append(end_idx_date)
            else:
                logger.warn("end_idx is not available. You might want to check data availability of {} after {}".format(ticker, end))

            self.data_dates = data_dates
            self.data_path = data_path
            self.data = self._read_row()
            self._tick = None
            self._snapshot = None

    def read_file(self, source:str)->pd.DataFrame:
        """Read feather data to dataframe (using pyarrow)"""
        return read_feather(source)

    def pdread_file(self, source:str)->pd.DataFrame:
        """Read feather data to dataframe (using Pandas)"""
        return pd.read_feather(source)

    def _read_row(self)->pd.Series:
        """Read a row of data each time"""
        for date in self.data_dates:
            source = os.path.join(self.data_path, "{}.feather".format(date))
            df = self.read_file(source)
            for _, row in df.iterrows():
                yield row

    def _reverse_read_row(self)->pd.Series:
        """Read a row of data from the end to the beginning"""
        for date in reversed(self.data_dates):
            source = os.path.join(self.data_path, "{}.feather".format(date))
            df = self.read_file(source)
            for _, row in df.iloc[::-1].iterrows():
                yield row


    def read_tick(self)->dict:
        """Read 3 rows of data to form tick data (including Bid, Ask, and Trade)"""
        try:
            bid_info = next(self.data)
            ask_info = next(self.data)
            trd_info = next(self.data)
        except StopIteration:
            return None

        # extract data
        attrs = ["bid", "ask", "trd"]
        tick_data = {}
        for attr in attrs:
            tick_data["{}_datetime".format(attr)] = from_timestamp_to_datetime(locals()["{}_info".format(attr)]['index'])
            tick_data["{}_type".format(attr)] = locals()["{}_info".format(attr)]['typ']
            tick_data["{}_value".format(attr)] = locals()["{}_info".format(attr)]['value']
            tick_data["{}_size".format(attr)] = locals()["{}_info".format(attr)]['volume']
            # tick_data["{}_exch".format(attr)] = locals()["{}_info".format(attr)]['exch']

        # validate data integrity
        assert tick_data["bid_datetime"]==tick_data["ask_datetime"] and tick_data["bid_datetime"]==tick_data["trd_datetime"], \
            "Time stamps do not align!"
        # assert tick_data["bid_exch"]==tick_data["ask_exch"] and tick_data["bid_exch"]==tick_data["trd_exch"], \
        #     "Exchanges do not align!"

        tick = {
            "datetime": tick_data["bid_datetime"],
            "bid": tick_data["bid_value"],
            "bid_size": tick_data["bid_size"],
            "ask": tick_data["ask_value"],
            "ask_size": tick_data["ask_size"],
            "trd": tick_data["trd_value"],
            "trd_size": tick_data["trd_size"],
            # "exch": tick_data["bid_exch"],
        }

        self._tick = tick
        return tick

    @property
    def tick(self):
        """"""
        return self._tick

    @property
    def snapshot(self):
        """"""
        return self._snapshot

    @snapshot.setter
    def snapshot(self, snapshot:dict):
        """"""
        self._snapshot = snapshot

    def read_snapshot(self, time:datetime)->dict:
        """Return snapshot of market data most close to the given time"""
        if self.tick is None:
            self.read_tick()
        n = 0 # count update times
        while self.tick["datetime"]<time:
            self.snapshot = self.tick # use setter to update snapshot
            self.read_tick()
            n += 1
        return self.snapshot

    def check_data_availability(self, start:datetime=None, end:datetime=None)->bool:
        """Check if data is available within the given range"""
        # If no data at all, obviously there won't be any data within the date range
        if not self.has_data:
            return False
        start = self.start if start is None else start
        end = self.end if end is None else end

        data = self._read_row()
        reversed_data = self._reverse_read_row()
        first_data = None
        last_data = None
        try:
            first_data = next(data)
        except StopIteration:
            pass
        try:
            last_data = next(reversed_data)
        except StopIteration:
            pass

        if (first_data is None) or (last_data is None):
            return False

        first_data_dt = from_timestamp_to_datetime(first_data['index'])
        last_data_dt = from_timestamp_to_datetime(last_data['index'])

        # check if data available at the period
        if first_data_dt>start:
            logger.error(
                "Data is only available after {}, but we want to have it starting from {}".format(first_data_dt, start)
            )
            return False
        if last_data_dt<end:
            logger.error(
                "Data is not available after {}, but we want to have it till {}".format(last_data_dt, end)
            )
            return False
        return True







if __name__=="__main__":

    from datetime import timedelta
    import pytz
    source = "../../data/Greenland Global Investment Ltd"
    ticker = "XS1662749743@BGN Corp"
    start = "2020-06-18"
    end = "2020-06-26"
    handler = FeatherDataHandler(source, ticker, start, end)

    start = datetime(2020, 6, 18, 8, 50, 0)
    end = datetime(2020, 6, 24, 0, 0, 10)
    tz = pytz.timezone("Asia/Hong_Kong")
    start = tz.localize(start)
    end = tz.localize(end)

    tic = start
    while tic<end:
        snapshot = handler.read_snapshot(time=tic)
        print(tic, snapshot)
        # tick = handler.read_tick()

        tic += timedelta(seconds=20)






