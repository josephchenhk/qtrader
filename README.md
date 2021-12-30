# QTrader: A Light Event-Driven Algorithmic Trading Engine

**Latest update on 12/30/2021**

QTrader is a light and flexible event-driven algorithmic trading engine that can be used to backtest strategies, 
and seamlessly switch to live trading without any pain.

## Key features

* Completely **same code** for backtesting / simulation / live trading 

* Support trading of various assets: equity, futures

## Quick install

You may run the folllowing command to install QTrader immediately:

```python
# python 3.8 or above is supported
>> conda create -n qtrader python=3.8
>> conda activate qtrader
>> pip install git+https://github.com/josephchenhk/qtrader@master
```

## Get the data ready

QTrader supports bar data at the moment. What you need to do is creating a folder with the name of the security you
are interested in. Let's say you want to backtest or trade HK equity **"HK.01157"** in frequency of **1 minute**, your 
data folder should be like this (where "K_1M" stands for 1 minute; you can also find a sample from 
qtrader/examples/data):

![alt text](./contents/data_folder.png "data folder")

And you can prepare OHLCV data in CSV format, with dates as their file names, e.g., **"yyyy-mm-dd.csv"**: 

![alt text](./contents/data_folder_details.png "data folder details")

Inside each csv file, the data columns should look like this:

![alt text](./contents/bar_data_sample.png "bar data sample")

Now you can specify the path of data folder in `qtrader/config/config.py`. For example, set

```python
DATA_PATH = {
    "kline": "path_to_your_qtrader_folder/examples/data/k_line/K_1M",
}
```

## How to implement a strategy

To implement a strategy is simple in QTrader. A strategy needs to implement `init_strategy` and `on_bar` methods in 
`BaseStrategy`. Here is a quick sample:

```python
from qtrader.core.strategy import BaseStrategy

class MyStrategy(BaseStrategy):

    def init_strategy(self):
        pass
        
    def on_bar(self, cur_data:Dict[str, Dict[Security, Bar]]):
        print(cur_data)
```

        
## How to record anything I want

QTrader provides a module named `BarEventEngineRecorder` to record variables during backtesting and/or trading. By 
default, it saves `datetime`, `portfolio_value` and `action` at every time step. 

If you want to record additional variables (let's say it is called `var`), you need to write a method called `get_var` 
in your strategy:

```python
from qtrader.core.strategy import BaseStrategy

class MyStrategy(BaseStrategy):

    def get_var(self):
        return var
```

And initialize your `BarEventEngineRecorder` with the same vairable `var=[]`(if you want to record every timestep) or 
`var=None`(if you want to record only the last updated value):

```python
recorder = BarEventEngineRecorder(var=[])
```
    

## Run a backtest

Now we are ready to run a backtest. Here is a sample of running a backtest in QTrader:

```python
# Security 
stock_list = [
    Stock(code="HK.01157", lot_size=100, security_name="中联重科", exchange=Exchange.SEHK),
]

# Gateway
gateway_name = "Backtest"
gateway = BacktestGateway(
    securities=stock_list,
    start=datetime(2021, 3, 15, 9, 30, 0, 0),
    end=datetime(2021, 3, 17, 16, 0, 0, 0),
    gateway_name=gateway_name,
)
gateway.SHORT_INTEREST_RATE = 0.0
gateway.set_trade_mode(TradeMode.BACKTEST)
gateway.TRADING_HOURS_AM = [Time(9, 30, 0), Time(12, 0, 0)]
gateway.TRADING_HOURS_PM = [Time(13, 0, 0), Time(16, 0, 0)]

# Core engine
engine = Engine(gateways={gateway_name: gateway})

# Strategy initialization
init_capital = 100000
strategy_account = "DemoStrategy"
strategy_version = "1.0"
strategy = DemoStrategy(
    securities={gateway_name: stock_list},
    strategy_account=strategy_account,
    strategy_version=strategy_version,
    init_strategy_cash={gateway_name: init_capital},
    engine=engine)
strategy.init_strategy()

# Recorder
recorder = BarEventEngineRecorder()

# Event engine
event_engine = BarEventEngine(strategy, recorder)

# Start event engine
event_engine.run()

# Save results and shutdown program
result_path = recorder.save_csv()
plugins = engine.get_plugins()
if "analysis" in plugins:
    plot_pnl = plugins["analysis"].plot_pnl
    plot_pnl(result_path=result_path)
engine.log.info("Program shutdown normally.")
```

After shutdown, you will be able to find the results in qtrader/results, with the folder name of latest time stamp:

![alt text](./contents/results.png "results")

The result.csv file saves everything you want to record in `BarEventEngineRecorder`; while pnl.html is an interactive 
plot of the equity curve of your running strategy:

![alt text](./contents/pnl.png "pnl")

## Simulation / Live trading

Ok, your strategy looks good now. How can you put it to paper trading and/or live trading? In QTrader it is
extremely easy to switch from backtest mode to simulation or live trading mode. What you need to modify is just 
**two** lines:

```python
# Currently you can use "Futu", "Ib", and "Cqg" 
gateway_name = "Futu"  

# Use FutuGateway, IbGateway, or CqgGateway accordingly
# End time should be set to a future time stamp when you expect the program terminates
gateway = FutuGateway(
    securities=stock_list,
    end=datetime(2022, 12, 31, 16, 0, 0, 0),  
    gateway_name=gateway_name,
)

# Choose either TradeMode.SIMULATE or TradeMode.LIVETRADE
gateway.set_trade_mode(TradeMode.SIMULATE)
```

That's it! You switch from backtest to simulation / live trading mode now. 

**Important Notice**: In the demo sample, the live trading mode will keep sending orders, please be aware 
of the risk when running it.

