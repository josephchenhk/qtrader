# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:52 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: utility.py
# @Software: PyCharm

from functools import total_ordering, wraps
from timeit import default_timer as timer
from datetime import datetime
import threading

@total_ordering
class Time:
    """
    Customized timer
    """

    def __init__(self, hour:int, minute:int, second:int):
        self.hour = hour
        self.minute = minute
        self.second = second

    def __eq__(self, other):
        return (self.hour==other.hour) and (self.minute==other.minute) and (self.second==other.second)

    def __gt__(self, other):
        if self.hour>other.hour:
            return True
        elif self.hour<other.hour:
            return False

        if self.minute>other.minute:
            return True
        elif self.minute<other.minute:
            return False

        if self.second>other.second:
            return True
        else:
            return False

    def __str__(self):
        return f"Time[{self.hour}:{self.minute}:{self.second}]"
    __repr__=__str__


class BlockingDict(object):
    """
    Blocking dict can be used to store orders and deals in the gateway
    Ref: https://stackoverflow.com/questions/26586328/blocking-dict-in-python
    """
    def __init__(self):
        self.queue = {}
        self.cv = threading.Condition()
        self.count = 0

    def put(self, key, value):
        with self.cv:
            self.queue[key] = value
            # self.cv.notify()
            self.cv.notify_all()

    def pop(self):
        with self.cv:
            while not self.queue:
                self.cv.wait()
            return self.queue.popitem()

    def get(self, key):
        with self.cv:
            while key not in self.queue:
                self.cv.wait()
            return self.queue.get(key)

    def __iter__(self):
        return self

    def __next__(self):
        with self.cv:
            if self.count == len(self.queue):
                self.count = 0
                raise StopIteration
            self.count += 1
            return list(self.queue.keys())[self.count-1]


def timeit(func):
    """
    Measure execution time of a function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = timer()
        res = func(*args, **kwargs)
        toc = timer()
        print("{} Elapsed time: {} seconds".format(func.__name__, toc - tic))
        return res

    return wrapper


def try_parsing_datetime(text:str):
    """
    Parsing different datetime format strings
    """
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    # raise ValueError(f"{text} is not a valid date format!")
    # 如果不能解析时间文本（有可能为空），则返回当前本机系统时间
    return datetime.now()


def get_kline_dfield_from_seconds(time_step:int)->str:
    """
    Get kline dfield (names) from given time steps (note: maximum interval is day)
    :param time_step:
    :return:
    """
    if time_step<60:
        return f"k{time_step}s"
    elif time_step<3600:
        assert time_step % 60==0, f"Given timestep should be multiple of 60 seconds, but {time_step} was given."
        time_step_in_mins = int(time_step / 60)
        return f"k{time_step_in_mins}m"
    elif time_step<3600*24:
        assert time_step % 3600==0, f"Given timestep should be multiple of 3600 seconds, but {time_step} was given."
        time_step_in_hours = int(time_step / 3600)
        return f"k{time_step_in_hours}h"
    else:
        assert time_step == 3600*24, f"Given timestep can not exceed 3600*24 seconds, but {time_step} was given."
        return f"k1d"

if __name__=="__main__":
    blockdict = BlockingDict()
    blockdict.put(1, "a")
    blockdict.put(2, "b")
    for bd in blockdict:
        print(bd, blockdict.get(bd))
    for bd in blockdict:
        print(bd, blockdict.get(bd))
