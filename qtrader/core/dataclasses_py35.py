# -*- coding: utf-8 -*-
# @Time    : 10/9/2020 7:02 PM
# @Author  : Joseph Chen
# @Email   : josephchenhk@gmail.com
# @FileName: dataclasses_py35.py
# @Software: PyCharm

def dataclass(cls=None, *, init=True):
    """自定义dataclass装饰器，仅支持@dataclass调用，不支持带参数调用"""
    def wrap(cls):
        return _process_class(cls, init)

    # We're called as @dataclass without parens.
    return wrap(cls)

def _process_class(cls, init):
    """"""
    # 获取类定义变量
    vars = {}
    callable_vars = {}
    for idx, _cls in enumerate(cls.__mro__):
        var_lst = []
        callable_var_lst = []
        for k in _cls.__dict__.keys():
            if k.startswith("__"):
                continue
            tmp = _cls.__dict__[k]
            if callable(tmp):
                callable_var_lst.append(k)
            else:
                var_lst.append(k)
        vars[idx] = var_lst
        callable_vars[idx] = callable_var_lst

    # 为所有变量创建init方法
    super_params = ""
    for idx in vars.keys():
        for p in vars[idx]:
            v = cls.__mro__[idx].__dict__[p]
            v = None if v=="" else v
            super_params += ", {}={}".format(p, v) # parent parameters are passed as kwargs
    init_txt = "def __init__(self{}):".format(super_params)

    for idx in vars.keys():
        for var in vars[idx]:
            init_txt += "\n   self.{} = {}".format(var, var)
    if "__post_init__" in cls.__dict__.keys():
        post_init_func = cls.__dict__["__post_init__"]
        init_txt += "\n   self.__post_init__()"
    ns = {}
    exec(init_txt, globals(), ns)
    init_func = ns["__init__"]

    # 类方法需要作为attrs传递
    callable_attrs = {}
    for idx in callable_vars.keys():
        callable_vars_lst = callable_vars[idx]
        for callable_name in callable_vars_lst:
            callable_attrs[callable_name] = cls.__mro__[idx].__dict__[callable_name]

    # 返回经过改造后的类
    class_name = cls.__mro__[0].__name__
    parent_classes = cls.__mro__[1:]
    if "__post_init__" in cls.__dict__.keys():
        attrs = dict(__init__=init_func, __post_init__=post_init_func)
    else:
        attrs = dict(__init__=init_func)
    for idx in vars:
        for var in vars[idx]:
            attrs[var] = cls.__mro__[idx].__dict__[var]
    attrs.update(callable_attrs)
    new_cls = type(class_name, parent_classes, attrs)
    return new_cls

@dataclass
class BaseData:
    """
    Any data object needs a gateway_name as source
    and should inherit base data.
    """

    gateway_name = None # : str


@dataclass
class TickData(BaseData):
    """
    Tick data contains information about:
        * last trade in market
        * orderbook snapshot
        * intraday market statistics.
    """

    symbol = None   # : str
    exchange = None # : Exchange
    datetime = None # : datetime

    name = ""       # : str
    volume = 0        # : float
    open_interest = 0 # : float
    last_price = 0    # : float
    last_volume = 0   # : float
    limit_up = 0      # : float
    limit_down = 0    # : float

    open_price = 0    # : float
    high_price = 0    # : float
    low_price = 0     # : float
    pre_close = 0     # : float

    bid_price_1 = 0   # : float
    bid_price_2 = 0   # : float
    bid_price_3 = 0   # : float
    bid_price_4 = 0   # : float
    bid_price_5 = 0   # : float

    ask_price_1 = 0   # : float
    ask_price_2 = 0   # : float
    ask_price_3 = 0   # : float
    ask_price_4 = 0   # : float
    ask_price_5 = 0   # : float

    bid_volume_1 = 0  # : float
    bid_volume_2 = 0  # : float
    bid_volume_3 = 0  # : float
    bid_volume_4 = 0  # : float
    bid_volume_5 = 0  # : float

    ask_volume_1 = 0  # : float
    ask_volume_2 = 0  # : float
    ask_volume_3 = 0  # : float
    ask_volume_4 = 0  # : float
    ask_volume_5 = 0  # : float

    def __post_init__(self):
        """"""
        self.vt_symbol = "{}.{}".format(self.symbol, self.exchange.value)

@dataclass
class OrderData(BaseData):
    """
    Order data contains information for tracking lastest status
    of a specific order.
    """

    symbol = None   # : str
    exchange = type('Exchange', (object,), dict(value="BONDSIM")) # : Exchange
    orderid = None  # : str

    type = None     # : OrderType
    direction = ""             # : Direction
    offset = None       # : Offset
    price = 0                  # : float
    volume = 0                 # : float
    traded = 0                 # : float
    status = None # : Status
    time = ""                  # : str

    def __post_init__(self):
        """"""
        self.vt_symbol = "{}.{}".format(self.symbol, self.exchange.value)
        self.vt_orderid = "{}.{}".format(self.gateway_name, self.orderid)

    def is_active(self) -> bool:
        """
        Check if the order is active.
        """
        if self.status is None:
            return True
        else:
            return False

    def create_cancel_request(self) -> "CancelRequest":
        """
        Create cancel request object from order.
        """
        req = "CancelRequest"
        return req

if __name__=="__main__":
    from datetime import datetime
    # tick = TickData(
    #     symbol="xx",
    #     exchange=type('Exchange', (object,), dict(value="BONDSIM")),
    #     datetime=datetime.now(),
    #     gateway_name="GATEWAY",
    # )
    order = OrderData()
    print()



