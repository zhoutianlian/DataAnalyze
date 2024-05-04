import pandas as pd
import numpy as np
from CONFIG import mongodb
from datetime import datetime
from time import time
from CONFIG.mongodb import *
import os


def get_lastq(today):
    today = datetime.strptime(today, '%Y%m%d')
    quarter = (today.month - 1) / 3 + 1
    if quarter == 1:
        return datetime(today.now().year - 1, 12, 31).strftime('%Y%m%d')
    elif quarter == 2:
        return datetime(today.year, 3, 31).strftime('%Y%m%d')
    elif quarter == 3:
        return datetime(today.year, 6, 30).strftime('%Y%m%d')
    else:
        return datetime(today.year, 9, 30).strftime('%Y%m%d')

def df2dict(df):
    today = str(datetime.now().year) + str(datetime.now().month) + str(datetime.now().day).zfill(2)
    last_q  = pd.to_datetime(get_lastq(today))
    df['update_date'] = df['update_date'].map(lambda x: 4 * (last_q.year - x.year) + int((last_q.month - x.month) / 3))
    df = df[df['update_date']>0]
    df = df.dropna()
    df = df.set_index('update_date', drop = True)
    dict_0 = {}

    min = np.min(df.index)
    for i,v in df.iterrows():
        dict_0[i] = v[0]
    for i in range(min):
        dict_0[i] = dict_0[min]
    return dict_0

def get_mondgo( database, collection, column, name, mongo_uri='mongodb://192.168.2.114:27017/'):
    today = str(datetime.now().year) + str(datetime.now().month) + str(datetime.now().day).zfill(2)
    df = read_mongo_columns(database, collection, column)
    df.drop(['_id'], axis=1, inplace=True)
    df['update_date'] = pd.to_datetime(df['update_date'])
    df.rename(columns={column[-1]: name}, inplace=True)
    df = df[df['update_date'] <= pd.to_datetime(get_lastq(today))]
    try:
        df = df.drop_duplicates(keep='first', subset=['stock_code','update_date'])
    except:
        df = df.drop_duplicates(keep='first', subset=[ 'update_date'])
    return df

def q2ttm_sum_macro(df,name):

    df = df.sort_values('update_date', ascending=True)
    df[name]= df[name].rolling(window = 4, min_periods = 1).sum().fillna(method = 'bfill')
    df = df[['update_date',name]]
    return df

def q2ttm_avg(df, name):
    def func(name):
        def f(df):
            df = df.sort_values('update_date', ascending=True)
            df[name] = df[name].rolling(window=4, min_periods=1).mean()
            return df

        return f

    df = df.groupby('stock_code')['update_date', name].apply(func(name))
    df = df.reset_index()
    df = df[['stock_code','update_date', name]]
    return df


def y2q_macro(df):
    df = df.set_index('update_date')
    df.drop_duplicates(keep='first', inplace=True)
    df = df.resample('Q').interpolate('linear').fillna(method='bfill')
    df = df.reset_index()
    return df


def cal_is_bs(df_ni, df_equity, numerator, denominator, ratio):

    df_roe = pd.merge(df_ni, df_equity,
                      on=['update_date', ], how='inner')
    df_roe[ratio] = np.where(df_roe[denominator] > 0,
                             df_roe[numerator] / df_roe[denominator],
                             df_roe[numerator] / (df_roe[denominator] + 2 * df_roe[numerator]))
    df_roe[ratio] = np.where((df_roe[denominator] + 2 * df_roe[denominator] < 0) & (df_roe[denominator] < 0),
                             np.NaN, df_roe[ratio])
    df_roe = df_roe.dropna()
    df_roe = df_roe.drop_duplicates(['update_date'], keep='first')
    df_roe = df_roe[['update_date', ratio]]
    return df_roe


def cal_bs_bs(df_ca, df_cl, numerator, denominator, ratio):

    df_cr = pd.merge(df_ca, df_cl,
                     on=['update_date'], how='inner')

    df_cr[ratio] = np.where((df_cr[denominator] > 0) & (df_cr[numerator] > 0),
                            df_cr[numerator] / df_cr[denominator],
                            0)

    df_cr[ratio] = np.where(df_cr[denominator] <= 0, np.NaN,
                            df_cr[ratio])
    df_cr = df_cr.dropna()
    df_cr.drop_duplicates(subset=[ 'update_date'], keep='first', inplace=True)
    df_cr = df_cr[['update_date', ratio]]
    return df_cr


# filter the stocks belong to certain gics4
def match_gics(df, gics4):
    df_gics4_stock = pd.read_csv(os.path.join(os.getcwd(),'static/gics4_stock.csv'), header=None)
    df_gics4_stock.columns = ['gics4', 'stock_code']
    df_gics4_stock = df_gics4_stock.drop_duplicates(subset=['stock_code'])
    df_gics4_stock = df_gics4_stock[df_gics4_stock['gics4'] == gics4]

    df_1 = pd.merge(df_gics4_stock, df, how='outer')
    df_1 = df_1.fillna(method='bfill')
    df_1.drop(columns=['gics4'], axis=1)
    return df_1


def cal_industry_avg(df, name):
    df = df.groupby(['update_date'])[name].sum()
    df = pd.DataFrame(df.values, index=df.index, columns=['{}_industry'.format(name)])
    df = df.sort_index()
    df = df.reset_index()
    return df


def cal_delta(df,name,n):
    df = df.sort_values('update_date')
    df[name] = np.log(df[name] / df[name].shift(n))
    df = df.dropna()
    df[name] = np.where((df[name] == np.inf) | (df[name] == -np.inf), 0, df[name])
    df[name] = df[name].fillna(method='bfill')
    df[name] = df[name].fillna(0)
    return df


def cal_outlier(data_series, scale):
    interval = scale * (data_series.quantile(0.75) - data_series.quantile(0.25))
    bound_low = data_series.quantile(0.25) - interval
    bound_up = data_series.quantile(0.75) + interval
    rule_low = (data_series < bound_low)
    rule_up = (data_series > bound_up)
    miu = np.mean(data_series)
    sigma = np.std(data_series)
    return (rule_low, rule_up), (bound_low, bound_up), (miu, sigma)


def filter_outlier(data, list_column, scale=3):
    data_1 = data.copy()
    for i in range(len(list_column)):

        data_series = data_1[list_column[i]]
        rule, bound, parameter = cal_outlier(data_series, scale=scale)

        index_outlier_low = np.arange(data_series.shape[0])[rule[0]]
        outliers = data_series.iloc[index_outlier_low]
        # print("detailed data below lower bound:")
        # print(pd.Series(outliers).describe())

        index_outlier_up = np.arange(data_series.shape[0])[rule[1]]
        outliers = data_series.iloc[index_outlier_up]
        # print("detailed data above upper bound:")
        # print(pd.Series(outliers).describe())

        index_low = np.arange(data_series.shape[0])[rule[0]]
        if 2 * (bound[1] - bound[0]) > parameter[1]:
            data[list_column[i]].iloc[index_low] = -np.abs(np.random.randn(1)[0]) * parameter[1] * scale + bound[0]
        else:
            data[list_column[i]].iloc[index_low] = -np.abs(np.random.randn(1)[0]) * (bound[1] - bound[0]) * scale + \
                                                   bound[0]

        index_up = np.arange(data_series.shape[0])[rule[1]]
        if 2 * (bound[1] - bound[0]) > parameter[1]:
            data[list_column[i]].iloc[index_up] = np.abs(np.random.randn(1)[0]) * parameter[1] * scale + bound[1]
        else:
            data[list_column[i]].iloc[index_up] = np.abs(np.random.randn(1)[0]) * (bound[1] - bound[0]) * scale + \
                                                  bound[1]


    return data