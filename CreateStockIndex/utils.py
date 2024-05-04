import pymongo

import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import time
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os


# add pre close, adjust back adjustment factor
from pymongo import MongoClient


def trade_data_make_up(stock_code, start, end):
    quote_data = ak.stock_zh_a_daily(symbol=stock_code, start_date=start, end_date=end)
    back_adjust_factor = ak.stock_zh_a_daily(symbol=stock_code, start_date=start, end_date=end, adjust="hfq-factor")

    quote_data = pd.concat([quote_data, back_adjust_factor], axis=1)
    full_quote = quote_data.sort_index().ffill().dropna()
    pre_orig = full_quote['close'].rolling(2).apply(lambda x: x[0])
    pre_adj = full_quote['hfq_factor'].rolling(2).apply(lambda x: x[0] / x[1])
    full_quote['pre'] = pre_orig * pre_adj
    full_quote['hfq_factor'] = full_quote['hfq_factor'].astype(float)
    db_data = full_quote.reset_index()[
        ['date', 'pre', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share', 'hfq_factor']]
    db_data['date'] = db_data['date'].apply(lambda x: x.year * 10000 + x.month * 100 + x.day)
    gather_data = db_data[['date', 'pre', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share', 'hfq_factor']]
    gather_data['stock_code'] = stock_code
    gather_data = gather_data.rename(columns={'date': 'trade_date', 'hfq_factor': 'back_adjustment_factor'})

    return gather_data


# transfer daily quote to weekly quote
def gather_week_quote_index(data):
    gdata = data.loc[:, :]

    def get_index(x):
        return pd.Series(datetime.datetime(x // 10000, x // 100 % 100, x % 100).isocalendar())

    def gather(df):
        idf = df.sort_values(by='weekday').reset_index(drop=True)

        wDate = idf.iloc[-1]['trade_date']
        wOpen = idf.iloc[0]['open']
        wHigh = idf['high'].max()
        wLow = idf['low'].min()
        wClose = idf.iloc[-1]['close']
        wVolume = idf.volume.sum()

        return pd.Series(
            {'trade_date': wDate, 'open': wOpen, 'high': wHigh, 'low': wLow, 'close': wClose, 'volume': wVolume})

    gdata[['year', 'week', 'weekday']] = gdata['trade_date'].apply(get_index)
    result = gdata.groupby(['year', 'week']).apply(gather)
    result = result.reset_index(drop=True)
    result['trade_date'] = result['trade_date'].astype(int)
    return result




# transfer daily quote to monthly quote
def gather_month_quote(data):
    gdata = data.loc[:, :]

    def get_index(x):
        return pd.Series([x // 10000, x // 100 % 100, x % 100])

    def gather(df):
        idf = df.sort_values(by='day').reset_index(drop=True)
        p = idf[['pre', 'open', 'high', 'low', 'close']].mul(idf['hfq_factor'], axis=0)
        v = idf.eval('volume / hfq_factor')

        wDate = idf.iloc[-1]['date']
        wPre = p.iloc[0]['pre']
        wOpen = p.iloc[0]['open']
        wHigh = p['high'].max()
        wLow = p['low'].min()
        wClose = p.iloc[-1]['close']
        wVolume = v.sum()

        return pd.Series(
            {'date': wDate, 'pre': wPre, 'open': wOpen, 'high': wHigh, 'low': wLow, 'close': wClose, 'volume': wVolume})

    gdata[['year', 'month', 'day']] = gdata['date'].apply(get_index)
    result = gdata.groupby(['year', 'month']).apply(gather)
    result = result.reset_index(drop=True)
    result['date'] = result['date'].astype(int)
    return result



# transfer daily quote to quartly quote
def gather_season_quote(data):
    gdata = data.loc[:, :]

    def get_index(x):
        return pd.Series([x // 10000, (x // 100 % 100 - 1) // 3 + 1, x % 10000])

    def gather(df):
        idf = df.sort_values(by='day').reset_index(drop=True)
        p = idf[['pre', 'open', 'high', 'low', 'close']].mul(idf['hfq_factor'], axis=0)
        v = idf.eval('volume / hfq_factor')

        wDate = idf.iloc[-1]['date']
        wPre = p.iloc[0]['pre']
        wOpen = p.iloc[0]['open']
        wHigh = p['high'].max()
        wLow = p['low'].min()
        wClose = p.iloc[-1]['close']
        wVolume = v.sum()

        return pd.Series(
            {'date': wDate, 'pre': wPre, 'open': wOpen, 'high': wHigh, 'low': wLow, 'close': wClose, 'volume': wVolume})

    gdata[['year', 'season', 'day']] = gdata['date'].apply(get_index)
    result = gdata.groupby(['year', 'season']).apply(gather)
    result = result.reset_index(drop=True)
    result['date'] = result['date'].astype(int)
    return result




# transfer daily quote to half yearly quote
def gather_halfyear_quote(data):
    gdata = data.loc[:, :]

    def get_index(x):
        return pd.Series([x // 10000, (x // 100 % 100 - 1) // 6 + 1, x % 10000])

    def gather(df):
        idf = df.sort_values(by='day').reset_index(drop=True)
        p = idf[['pre', 'open', 'high', 'low', 'close']].mul(idf['hfq_factor'], axis=0)
        v = idf.eval('volume / hfq_factor')

        wDate = idf.iloc[-1]['date']
        wPre = p.iloc[0]['pre']
        wOpen = p.iloc[0]['open']
        wHigh = p['high'].max()
        wLow = p['low'].min()
        wClose = p.iloc[-1]['close']
        wVolume = v.sum()

        return pd.Series(
            {'date': wDate, 'pre': wPre, 'open': wOpen, 'high': wHigh, 'low': wLow, 'close': wClose, 'volume': wVolume})

    gdata[['year', 'halfyear', 'day']] = gdata['date'].apply(get_index)
    result = gdata.groupby(['year', 'halfyear']).apply(gather)
    result = result.reset_index(drop=True)
    result['date'] = result['date'].astype(int)
    return result



# transfer daily quote to yearly
def gather_year_quote(data):
    gdata = data.loc[:, :]

    def get_index(x):
        return pd.Series([x // 10000, x % 10000])

    def gather(df):
        idf = df.sort_values(by='day').reset_index(drop=True)
        p = idf[['pre', 'open', 'high', 'low', 'close']].mul(idf['hfq_factor'], axis=0)
        v = idf.eval('volume / hfq_factor')

        wDate = idf.iloc[-1]['date']
        wPre = p.iloc[0]['pre']
        wOpen = p.iloc[0]['open']
        wHigh = p['high'].max()
        wLow = p['low'].min()
        wClose = p.iloc[-1]['close']
        wVolume = v.sum()

        return pd.Series(
            {'date': wDate, 'pre': wPre, 'open': wOpen, 'high': wHigh, 'low': wLow, 'close': wClose, 'volume': wVolume})

    gdata[['year', 'day']] = gdata['date'].apply(get_index)
    result = gdata.groupby('year').apply(gather)
    result = result.reset_index(drop=True)
    result['date'] = result['date'].astype(int)
    return result





# transfer back to fore adjustment price
def turn_hfq_to_qfq(hfq_quote, orig_quote):
    new_close = orig_quote.sort_values(by='date').iloc[-1]['close']  # 获得最新价
    hfq_close = hfq_quote.iloc[-1]['close']  # 获得后复权最新价
    adj = new_close / hfq_close

    qfq_quote = hfq_quote['date']
    qfq_quote = pd.concat([qfq_quote, hfq_quote[['pre', 'open', 'high', 'low', 'close']] * adj], axis=1)
    return qfq_quote

def get_today():
    now_time = datetime.datetime.now()
    str = datetime.datetime.strftime(now_time, '%Y-%m-%d %H:%M:%S')
    return str[:10].replace('-', '')

def get_n_month_before(n=0):

    now_time = datetime.datetime.now()
    y = now_time.year
    m = now_time.month
    d = now_time.day
    if m+n <0:
        y = y-1
        m = 12-(-m-n)
    else:
        m= m+n
    arr = (y, m, d)
    return "".join("%s" %i for i in arr)

mongo_uri='mongodb://192.168.2.114:27017/'

def connect_mongo(mongo_uri = 'mongodb://192.168.2.114:27017/'):
    conn = MongoClient(mongo_uri)
    return conn


def read_mongo(db_name, collection_name,conditions = {}, query = [],sort_item = '',n = 0):

    conn = connect_mongo()
    db = conn[db_name]
    collection = db[collection_name]
    if query:
        cursor = collection.find(conditions,query)
    else:
        cursor = collection.find(conditions)
    if sort_item:
        cursor = cursor.sort(sort_item, pymongo.DESCENDING)
    if n:
        cursor = cursor[:n]
    temp_list = []
    for single_item in cursor:
        temp_list.append(single_item)

    df = pd.DataFrame(temp_list)
    conn.close()
    return df

def save_mongo(df_data,db_name,tb_name,if_del = 0,mongo_uri = 'mongodb://192.168.2.114:27017/'):
    conn = MongoClient(mongo_uri)
    db = conn[db_name]
    collection = db[tb_name]
    if if_del:
        collection.delete_many({})
    list_data = df_data.to_dict('record')
    collection.insert_many(list_data)


if __name__ == '__main__':

    gather_month_quote(quote)
    gather_season_quote(quote)
    gather_halfyear_quote(quote)

    gather_year_quote(quote)

    turn_hfq_to_qfq(gather_month_quote(db_data), db_data)

    index_data_w = gather_week_quote_index(index_data)
