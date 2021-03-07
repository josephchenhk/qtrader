# -*- coding: utf-8 -*-
# @Time    : 7/3/2021 8:52 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: utility.py
# @Software: PyCharm

from functools import total_ordering, wraps
from timeit import default_timer as timer


@total_ordering
class Time:
    """ Customized timer"""

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


def timeit(func):
    """Measure execution time of a function"""

    @wraps(func)
    def wrapper(*args, **kwargs):
        tic = timer()
        res = func(*args, **kwargs)
        toc = timer()
        print("{} Elapsed time: {} seconds".format(func.__name__, toc - tic))
        return res

    return wrapper