"""
指数走势每日更新
"""
import requests
import pandas as pd
import numpy as np
import akshare as ak
import time
import tushare as ts
import datetime

from database_mkt.utils import to_sql_replace


def get_shsz_index():
    index_dict = {
        'csi800' :'sh000906',
        'csi300' :'sz399300',
        'csi500' :'sz399905',
        'gem' :'sz399006',
        #         'otc':'899307',
        'star50' :'sh000688'
    }

    index_hist_total = pd.DataFrame(columns = ['index_code' ,'trade_date' ,'close' ,'high' ,'low' ,'volume' ,'amount'])


    for index_code in index_dict.values():
        stock_zh_index_daily = ak.stock_zh_index_daily_em(symbol=index_code)
        stock_zh_index_daily = stock_zh_index_daily[['date' ,'close' ,'high' ,'low' ,'volume' ,'amount']]
        stock_zh_index_daily = stock_zh_index_daily.rename(columns = {'date' :'trade_date'})
        stock_zh_index_daily['trade_date'] = stock_zh_index_daily['trade_date'].apply(pd.Timestamp)

        stock_zh_index_daily['index_code'] = index_code
        index_hist_total = index_hist_total.append(stock_zh_index_daily)

    return index_hist_total

def update_index():
    df = get_shsz_index()
    to_sql_replace(table = 'index_quote', data = df)

if __name__ == "__main__":
    update_index()