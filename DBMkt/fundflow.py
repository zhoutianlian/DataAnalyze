"""
个股资金流向数据更新
各种基金每日持股变动
每天定时执行

"""
import requests
import pandas as pd
import numpy as np
import akshare as ak
import time
from time import sleep
import tushare as ts
import datetime

from database_mkt.utils import to_sql_replace
from threading import Thread


def get_stock_fund_flow():
    codes = ak.stock_info_a_code_name()['code']
    df_total = pd.DataFrame(columns = ['update_date','stock_code','large_deal_net_value','super_large_deal_net_value','large_deal_ratio','super_large_deal_ratio'])
    for c in codes:
        if c[0] == '6':
            m = 'sh'
        else:
            m = 'sz'
        s = m+c
        df = ak.stock_individual_fund_flow(stock=c, market=m)
        df = df[['日期','大单净流入-净额','超大单净流入-净额','大单净流入-净占比','超大单净流入-净占比']]
        df.columns = ['update_date','large_deal_net_value','super_large_deal_net_value','large_deal_ratio','super_large_deal_ratio']
        df['update_date'] = df['update_date'].apply(lambda x: int(x[:4])*10000+ int(x[5:7])*100 +int(x[8:10]))
        df_total['stock_code'] = s
        df_total = df_total.append(df)
    to_sql_replace(table='stock_fund_flow', data=df_total)

def get_market_fund_flow():
    codes = ak.stock_info_a_code_name()['code']

    df = ak.stock_market_fund_flow()
    df = df[['日期','大单净流入-净额','超大单净流入-净额','大单净流入-净占比','超大单净流入-净占比']]
    df.columns = ['update_date','large_deal_net_value','super_large_deal_net_value','large_deal_ratio','super_large_deal_ratio']
    df['update_date'] = df['update_date'].apply(lambda x: int(x[:4])*10000+ int(x[5:7])*100 +int(x[8:10]))
    df['symnol'] = 'market'
    to_sql_replace(table='market_fund_flow', data=df)


def get_style_fund_flow():
    today = datetime.datetime.now()
    t = today.year * 10000 + today.month * 100 + today.day
    dict_style = {"行业资金流": 'industry',
                  "概念资金流": 'theme',
                  "地域资金流": 'location'}
    codes = ak.stock_info_a_code_name()['code']
    df_total = pd.DataFrame(
        columns=['update_date', 'sector', 'large_deal_net_value', 'super_large_deal_net_value', 'large_deal_ratio',
                 'super_large_deal_ratio'])
    for s in dict_style.keys():
        df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type=s)
        df = df[['今日大单净流入-净额', '今日超大单净流入-净额', '今日大单净流入-净占比', '今日超大单净流入-净占比']]
        df.columns = ['large_deal_net_value', 'super_large_deal_net_value', 'large_deal_ratio',
                      'super_large_deal_ratio']

        df['update_date'] = t
        df_total['sector'] = dict_style[s]
        df_total = df_total.append(df)
    to_sql_replace(table='style_fund_flow', data=df_total)



def update_fundflow():
    threads = list()
    threads.append(Thread(target=get_stock_fund_flow,args = ()))
    threads.append(Thread(target=get_market_fund_flow, args=()))
    threads.append(Thread(target=get_style_fund_flow, args=()))
    for thread in threads:
        thread.start()
if __name__ == "__main__":
    update_fundflow()