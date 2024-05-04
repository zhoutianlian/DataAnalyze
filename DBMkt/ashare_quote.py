"""
A 股交易数据更新
每天定时执行
"""
import requests
import tempfile
import xlwings as xw
import numpy as np
import pandas as pd
import os
import re
from glob import glob
from datetime import datetime
import time

from database_fs.GetStockList import get_stock_list
from database_mkt.utils import to_sql_replace


def get_quote(code):

    code_1 = '0' +code if code[0] == '6' else '1'+code
    url = f'http://quotes.money.163.com/service/chddata.html?code={code_1}'
    sess = requests.session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    response = sess.get(url, headers=headers)
    with open('timed_data.csv', 'wb') as file:
        file.write(response.content)
    data = pd.read_csv('timed_data.csv',encoding = 'gb2312')
    data = data.dropna(how='all', axis=1)
    dict_rename = {
            '日期' :'trade_date',
            '股票代码' :'stock_code',
            '名称' :'stock_name',
            '收盘价' :'close',
            '最高价' :'high',
            '最低价' :'low',
            '开盘价' :'open',
            '前收盘' :'pre',
            '换手率' :'turnover',
            '成交量' :'volume',
            '成交金额' :'amount',
            '总市值' :'mkt_value',
        }
    data = data.rename(
        columns = dict_rename
    )

    data = data[[i for i in dict_rename.values()]]
    def turn_num(x):
            try:
                x = float(x)
            except:
                pass
            return x
    data = data.applymap(turn_num)
    data = data.replace(0,np.nan)
    data = data.fillna(method = 'bfill')
    print(data)
    data['outstanding_share'] = data['mkt_value'] / data['close']
    if code[0] == '6':
        code = 'sh ' +code
    else:
        code = 'sz' + code

    data['trade_date'] = pd.to_datetime(data['trade_date'])
    data['stock_code'] = code
    print(data)
    return data
# 每日获取所有上市公司所有历史
# 线则更新前n天的数据，默认一天
# 可以指定某几家公司更新
def update_ashare(n=1,stocks = ['000001']):
    if not stocks:
        stock_list = get_stock_list()
        stocks = [i[2:] for i in stock_list['stock_code'] if i[:2] in ['sz', 'sh']]
    for c in stocks:
        print(c)
        try:
            df_i = get_quote(code=c)
            df_i = df_i.sort_values(by = 'trade_date',ascending = False)
            to_sql_replace(table='a_share_trade_data', data=df_i[:n])
            print('ok')
        except Exception as e:
            print(e)



if __name__ =='__main__':
    update_ashare(n=1, stocks=[])


