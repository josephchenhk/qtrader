# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:52 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: utility.py
# @Software: PyCharm

"""
Copyright (C) 2020 Joseph Chen - All Rights Reserved
You may use, distribute and modify this code under the
terms of the JXW license, which unfortunately won't be
written for another century.

You should have received a copy of the JXW license with
this file. If not, please write to: josephchenhk@gmail.com
"""

from functools import wraps
from timeit import default_timer as timer
from datetime import datetime
from datetime import time as Time
import threading
import queue
from typing import Any, List

import func_timeout


class BlockingDict(object):
    """Blocking dict can be used to store orders and deals in the gateway

    Ref: https://stackoverflow.com/questions/26586328/blocking-dict-in-python
    """

    def __init__(self):
        self.queue = {}
        self.cv = threading.Condition()
        self.count = 0

    def put(self, key, value):
        with self.cv:
            self.queue[key] = value
            self.cv.notify_all()

    def pop(self) -> Any:
        with self.cv:
            while not self.queue:
                self.cv.wait()
            return self.queue.popitem()

    def get(self, key, timeout: float = None, default_item: Any = None) -> Any:
        with self.cv:
            while key not in self.queue:
                if not self.cv.wait(timeout):
                    return default_item
            return self.queue.get(key)

    def __iter__(self):
        return self

    def __next__(self):
        with self.cv:
            if self.count == len(self.queue):
                self.count = 0
                raise StopIteration
            self.count += 1
            return list(self.queue.keys())[self.count - 1]


class DefaultQueue:
    """Default Queue returns a default value if not available"""

    def __init__(self, *args, **kwargs):
        self._queue = queue.Queue(*args, **kwargs)

    def qsize(self):
        return self._queue.qsize()

    def put(self, item: Any, block: bool = True, timeout: float = None):
        self._queue.put(item, block, timeout)

    def get(
            self,
            block: bool = True,
            timeout: float = None,
            default_item: Any = None
    ) -> Any:
        try:
            item = self._queue.get(block, timeout)
        except queue.Empty:
            item = default_item
        return item


def timeit(func):
    """Measure execution time of a function"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = timer()
        res = func(*args, **kwargs)
        toc = timer()
        print(
            "{0} Elapsed time: {1:.3f} seconds".format(
                func.__name__,
                toc - tic))
        return res
    return wrapper


def safe_call(func):
    """Safe call

    Try to call a function. If encounter error, just give warning and skip,
    will not interrupt the program.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
            return res
        except Exception as e:
            return e
    return wrapper


def try_parsing_datetime(
        text: str,
        default: datetime = None
) -> datetime:
    """Parsing different datetime format string, if can not be parsed, return
    the default datetime(default is set to now).
    """
    dt_formats = (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d  %H:%M:%S"
    )
    for fmt in dt_formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    print(f"{text} is not a valid date format! Return default datetime instead.")
    if default is None:
        return datetime.now()
    elif isinstance(default, datetime):
        return default
    raise ValueError(
        f"default = {default} is of type {type(default)}, only datetime is "
        "valid.")


def cast_value(
        value: Any,
        if_: Any = None,
        then: Any = None
) -> Any:
    """Cast a value to `then` if it is equal to `if_`, else return the value
    itself."""
    if value == if_:
        return then
    else:
        return value


def get_kline_dfield_from_seconds(time_step: int) -> str:
    """Get kline dfield (names) from given time steps (note: maximum interval
    is day) """

    if time_step < 60:
        return f"K_{time_step}S"
    elif time_step < 3600:
        assert time_step % 60 == 0, (
            "Given timestep should be multiple of 60 seconds, but "
            f"{time_step}*1,000 was given."
        )
        time_step_in_mins = int(time_step / 60)
        return f"K_{time_step_in_mins}M"
    elif time_step < 3600 * 24:
        assert time_step % 3600 == 0, (
            "Given timestep should be multiple of 3600 seconds, but "
            f"{time_step}*1,000 was given."
        )
        time_step_in_hours = int(time_step / 3600)
        return f"K_{time_step_in_hours}H"
    else:
        assert time_step == 3600 * 24, (
            "Given timestep can not exceed 3600*24 seconds, but "
            f"{time_step}*1,000 was given."
        )
        return "K_1D"


def run_function(function, args, kwargs, max_wait, default_value):
    """Run a function with limited execution time

    Ref: How to Limit the Execution Time of a Function Call?
    https://blog.finxter.com/how-to-limit-the-execution-time-of-a-function-call/
    """

    try:
        return func_timeout.func_timeout(max_wait, function, args, kwargs)
    except func_timeout.FunctionTimedOut:
        pass
    return default_value


def is_trading_time(
        cur_time: Time,
        trading_sessions: List[datetime]
) -> bool:
    """Check whether the security is whitin trading time"""

    _is_trading_time = False
    for session in trading_sessions:
        if session[0].time() <= session[1].time():
            if session[0].time() <= cur_time <= session[1].time():
                _is_trading_time = True
                break
        elif session[0].time() > session[1].time():
            if session[0].time() <= cur_time <= Time(23, 59, 59, 999999):
                _is_trading_time = True
                break
            elif Time(0, 0, 0) <= cur_time <= session[1].time():
                _is_trading_time = True
                break
    return _is_trading_time


if __name__ == "__main__":
    blockdict = BlockingDict()
    blockdict.put(1, "a")
    blockdict.put(2, "b")
    for bd in blockdict:
        print(bd, blockdict.get(bd))
    for bd in blockdict:
        print(bd, blockdict.get(bd))
    print(3, blockdict.get(3, timeout=1, default_item="cc"))
