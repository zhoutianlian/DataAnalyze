import datetime

from utils import read_mongo
import pandas as pd
import numpy as np

def cal_inter_tb(tb_numerator, tb_denominator, numerator, denominator, ratio, data_type, delta, retrospect=0):
    if retrospect:
        today = datetime.datetime.now()
        t = (today.year - retrospect) * 10000 + 101
        conditions = {'report_date': {'$gt': t}}
    else:
        conditions = {}

    dict_tb = {
        'is': {'cum': 'incomeStatement',
               'q': 'incomeStatement_q',
               'ttm': 'incomeStatement_ttm'},
        'cfs': {'cum': 'cashflowStatement',
                'q': 'cashflowStatement_q',
                'ttm': 'cashflowStatement_ttm'},
        'bs': {
            'cum': 'balanceSheet',
            'q': 'balanceSheet',
            'ttm': 'balanceSheet_ttm'
        }
    }

    df_numerator = read_mongo(db_name='modgo_quant', collection_name=dict_tb[tb_numerator][data_type],
                              conditions=conditions, query=[], sort_item='', n=0)

    df_numerator['report_date'] = df_numerator['report_date'].astype(float)
    df_denominator = read_mongo(db_name='modgo_quant', collection_name=dict_tb[tb_denominator][data_type],
                                conditions=conditions, query=[], sort_item='', n=0)
    df_denominator['report_date'] = df_denominator['report_date'].astype(float)

    df = pd.merge(df_numerator, df_denominator,
                  on=['stock_code', 'report_date'], how='inner')

    df[ratio] = np.where(df[denominator] > 0,
                         df[numerator] / df[denominator], df[numerator] / (df[denominator] + 2 * df[numerator]))
    df[ratio] = np.where((df[denominator] + 2 * df[denominator] < 0) & (df[denominator] < 0),
                         np.NaN, df[ratio])
    df = df.drop_duplicates(['stock_code', 'report_date'], keep='first')
    df = df[['stock_code', 'report_date', ratio]]
    df = df.dropna()

    def func(x):
        name = delta + '_' + ratio
        x = x.sort_values(by=['report_date'])
        if delta == 'qoq':
            x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
        elif delta == 'yoy':
            x[name] = (x[ratio] - x[ratio].shift(4)) / x[ratio].shift(4)
        else:
            if len(x) > 5:
                x[ratio] = (x[ratio] - x[ratio].shift(4)) / x[ratio].shift(4)
                x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
            else:
                x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
        x = x[[name, 'report_date', ]]

        x = x.dropna()
        if not x.empty:
            x = x[x['report_date'] == x['report_date'].max()]

        return x

    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')
    return df


# conditions = {'report_date':'$gt':20161231}
def cal_same_tb(tb, numerator, denominator, ratio, data_type, delta, retrospect=0):
    if retrospect:
        today = datetime.datetime.now()
        t = (today.year - retrospect) * 10000 + 101
        conditions = {'report_date': {'$gt': t}}
    else:
        conditions = {}

    dict_tb = {
        'is': {'cum': 'incomeStatement',
               'q': 'incomeStatement_q',
               'ttm': 'incomeStatement_ttm'},
        'cfs': {'cum': 'cashflowStatement',
                'q': 'cashflowStatement_q',
                'ttm': 'cashflowStatement_ttm'},
        'bs': {
            'cum': 'balanceSheet',
            'q': 'balanceSheet',
            'ttm': 'balanceSheet_ttm'
        }
    }

    df = read_mongo(db_name='modgo_quant', collection_name=dict_tb[tb][data_type], conditions=conditions, query=[],
                    sort_item='', n=0)

    df[ratio] = np.where(df[denominator] > 0,
                         df[numerator] / df[denominator], df[numerator] / (df[denominator] + 2 * df[numerator]))
    df[ratio] = np.where((df[denominator] + 2 * df[denominator] < 0) & (df[denominator] < 0),
                         np.NaN, df[ratio])

    df = df.drop_duplicates(['stock_code', 'report_date'], keep='first')
    df = df[['stock_code', 'report_date', ratio]]
    df = df.dropna()

    def func(x):
        name = delta + '_' + ratio
        x = x.sort_values(by=['report_date'])
        if delta == 'qoq':
            x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
        elif delta == 'yoy':
            x[name] = (x[ratio] - x[ratio].shift(4)) / x[ratio].shift(4)
        else:
            if len(x) > 5:
                x[ratio] = (x[ratio] - x[ratio].shift(4)) / x[ratio].shift(4)
                x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
            else:
                x[name] = (x[ratio] - x[ratio].shift(1)) / x[ratio].shift(1)
        x = x[[name, 'report_date', ]]

        x = x.dropna()
        if not x.empty:
            x = x[x['report_date'] == x['report_date'].max()]
        return x

    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')
    return df


# conditions = {'report_date':'$gt':20161231}
def cal_growth(target_tb, target, data_type, delta='', retrospect=0):
    if retrospect:
        today = datetime.datetime.now()
        t = (today.year - retrospect) * 10000 + 101
        conditions = {'report_date': {'$gt': t}}
    else:
        conditions = {}

    dict_tb = {
        'is': {'cum': 'incomeStatement',
               'q': 'incomeStatement_q',
               'ttm': 'incomeStatement_ttm'},
        'cfs': {'cum': 'cashflowStatement',
                'q': 'cashflowStatement_q',
                'ttm': 'cashflowStatement_ttm'},
        'bs': {
            'cum': 'balanceSheet',
            'q': 'balanceSheet',
            'ttm': 'balanceSheet_ttm'
        }
    }

    df = read_mongo(db_name='modgo_quant', collection_name=dict_tb[target_tb][data_type], conditions=conditions,
                    query=[], sort_item='', n=0)

    def func(x):
        name = delta + '_' + target
        x = x.sort_values(by=['report_date'])
        if delta == 'qoq':
            x[name] = (x[target] - x[target].shift(1)) / x[target].shift(1)
        elif delta == 'yoy':
            x[name] = (x[target] - x[target].shift(4)) / x[target].shift(4)
        else:
            if len(x) > 5:
                x[target] = (x[target] - x[target].shift(4)) / x[target].shift(4)
                x[name] = (x[target] - x[target].shift(1)) / x[target].shift(1)
            else:
                x[name] = (x[target] - x[target].shift(1)) / x[target].shift(1)
        x = x[[name, 'report_date']]
        x = x.dropna()
        if not x.empty:
            x = x[x['report_date'] == x['report_date'].max()]
        return x

    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')
    return df
