# -*- coding: utf-8 -*-
# @Time    : 2/9/2020 9:49 AM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: base_adapter.py
# @Software: PyCharm


from abc import ABC
from typing import Any

class BaseDataHandler(ABC):
    """A Base class that should be implemented.
    """

    def read(self, source:str)->Any:
        return





