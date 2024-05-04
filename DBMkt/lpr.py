"""
银行贷款利率每日变化
"""
import requests
import pandas as pd
import numpy as np
import akshare as ak
import time
from time import sleep
import tushare as ts
import datetime
from threading import Thread

# macroLeverage
from database_mkt.utils import to_sql_replace

def update_lpr(n):
    lpr = ak.macro_china_lpr()
    lpr = lpr.reset_index()
    lpr = lpr[['TRADE_DATE','LPR1Y','LPR5Y','RATE_1']]
    lpr.columns = ['trade_date','lpr_1y','lpr_5y','lpr_6m']
    lpr['trade_date'] = lpr['trade_date'].map(lambda x:x.year*10000+x.month*100+x.day)
    to_sql_replace(table='macro_trade_balance', data=lpr[:n])