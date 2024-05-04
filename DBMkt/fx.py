"""
汇率数据每日定时更新
"""
import requests
import pandas as pd
import numpy as np
import akshare as ak

import tushare as ts
import datetime
# 获取当天最新汇率报价
# 历史数据接口目前不可用
from sqlalchemy import create_engine

from database_mkt.CONFIG.Config import config
from database_mkt.utils import to_sql_replace


def update_fx():
    today = datetime.datetime.now()
    # today = int(str(today.year) + "%02d" % (today.month) + "%02d" % (today.day))
    fx_spot = ak.fx_spot_quote()

    def func(x):
        x['midprice'] = (float(x['bidPrc']) + float(x['askPrc'])) / 2
        if x['ccyPair'] == '100JPY/CNY':
            x['midprice'] = x['midprice'] / 100
            x['ccyPair'] = 'JPY/CNY'
        return x

    fx_spot = fx_spot.apply(func, axis=1)
    fx_spot['trade_date'] = today
    fx_spot = fx_spot[['midprice', 'trade_date', 'ccyPair']]
    fx_spot = fx_spot.rename(columns={'midprice': 'fx_rate', 'ccyPair': 'currency_pair'})
    to_sql_replace(table='fx_trade_data', data=fx_spot)


if __name__ =='__main__':
    update_fx()

    # setting = dict(host=config['DEFAULT']['sql_host'] + ':%s' % (config['DEFAULT']['sql_port']),
    #                user=config['DEFAULT']['sql_user'],
    #                password=config['DEFAULT']['sql_password'],
    #                database='modgo_quant')
    #
    # engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
    #                        encoding='utf-8')
    # df = pd.read_sql('fx_trade_data',engine)
    # print(df)

