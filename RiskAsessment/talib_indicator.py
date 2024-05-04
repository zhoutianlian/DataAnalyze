import tushare as ts
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import talib as ta

# df8135e1162c13b7126dafe5f2691536bf5e46ba5b32b5cfb368238f
# pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspider

pro = ts.pro_api('df8135e1162c13b7126dafe5f2691536bf5e46ba5b32b5cfb368238f')
df = pro.index_daily(ts_code='399300.SZ',start_date='20200101', end_date='20210110')
close = [float(x) for x in df['close']]
df['MA10'] = ta.MA(np.array(close), timeperiod=10)
df['MA100'] = ta.MA(np.array(close), timeperiod=100)
df['U50'] = ta.MAX(np.array(close), timeperiod=50)
df['D50'] = ta.MIN(np.array(close), timeperiod=50)
df['D25'] = ta.MIN(np.array(close), timeperiod=25)
MA10 = df['MA10'].iloc[-1]
MA100 = df['MA100'].iloc[-1]
U50 = df['U50'].iloc[-1]
D25 = df['D50'].iloc[-1]
D50 = df['D25'].iloc[-1]
print(MA10)
C = close[-1]
def indicate_trend():
    if C >U50 & MA10>MA100:
        trend = 'rise'
    elif C<D25:
        trend = 'draw back'
    elif C<D50:
        trend = 'decline'
    elif C >U50 & MA10<MA100:
        trend = 'rebound'
    return trend
def indicate_status():
    if C >U50 & MA10>MA100:
        trend = 'big bull'
    elif C<D25:
        trend = 'draw back'
    elif C<D50:
        trend = 'decline'
    elif C >U50 & MA10<MA100:
        trend = 'rebound'
    return trend


# MA(10)
# MA(100)
# HHV(50)
# LLV(50)
# LLV(25)
