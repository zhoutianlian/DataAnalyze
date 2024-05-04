"""
指数走势记录
"""

import datetime
import pandas as pd
import numpy as np
import os
import akshare as ak


# make up the pre close price of stock quotes
# through close * back adjustment factor t-1 / back adjustment factor t
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

# download data of individual stocks from akshare
def get_trade_data(members):
    start = str(members['start_date'].min())
    end = str(members['end_date'].max())
    total_list = []
    for m in members['member_list']:
        member_list = m.split(',')
        total_list.extend(member_list)
    total_list = list(set(total_list))
    trade_data = pd.DataFrame(
        columns=['trade_date', 'pre', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share',
                 'back_adjustment_factor'])
    for i in total_list:
        x = trade_data_make_up(i, start, end)
        trade_data = trade_data.append(x)

    return trade_data

# make up the back adjustment factor by fillna
# initially, back adjustment factor = 1
def backfill(df):
    df['back_adjustment_factor'] = df['back_adjustment_factor'].bfill()
    df['back_adjustment_factor'] = df['back_adjustment_factor'].fillna(1)
    return df

# weighted by market value
def weight_index_return(df):
    c = (df.close * df.outstanding_share).sum() / df.pre_mktcap.sum()
    if c > 1.1:
        c = 1
    o = (df.open * df.outstanding_share).sum() / (df.close * df.outstanding_share).sum()
    h = (df.high * df.outstanding_share).sum() / (df.close * df.outstanding_share).sum()
    l = (df.low * df.outstanding_share).sum() / (df.close * df.outstanding_share).sum()
    v = df.volume.sum() * df.close.mean()
    res = pd.DataFrame({'close': [c], 'open': [o], 'high': [h], 'low': [l], 'volume': [v]})

    return res

# use the quote data of individual stocks from API akshare
# calculate the quotes and volume of index, high low close open
def calculate_index_quote_akshare(trade_data, members_history: pd.DataFrame):

    df_total = pd.DataFrame(columns=['trade_date', 'close', 'open', 'high', 'low', 'volume'])
    trade_data = trade_data.sort_values(by=['stock_code', 'trade_date']).reset_index(drop=True)

    for i, d in members_history.iterrows():
        start_date = d.start_date
        end_date = d.end_date

        start_year = start_date // 10000
        start_month = start_date % 10000 // 100
        start_day = start_date % 100

        end_year = end_date // 10000
        end_month = end_date % 10000 // 100
        end_day = end_date % 100

        length = (end_year - start_year) * 260 + (end_month - start_month) * 25 + (end_day - start_day)

        member_list = d.member_list.split(',')

        size = len(member_list) * length

        quote_data = trade_data.query('@start_date<=trade_date < @end_date')
        num_cols = ['pre', 'open', 'high', 'low', 'close', 'volume', 'outstanding_share', 'back_adjustment_factor']
        quote_data[num_cols] = quote_data[num_cols].astype(float)
        quote_data = quote_data.dropna()

        quote_data['pre_mktcap'] = quote_data.eval('pre * outstanding_share')
        df = quote_data.groupby('trade_date').apply(weight_index_return)
        df = df.reset_index()
        print(df)

        df_total = df_total.append(df)
    df_total = df_total[['trade_date', 'close', 'open', 'high', 'low', 'volume']]
    print(df_total.describe())

    df_total['close'] = df_total['close'].cumprod()

    for i in ['open', 'high', 'low']:
        df_total[i] = df_total[i] * df_total['close']
    df_total.to_csv(os.path.join(os.getcwd(),'static','index_quote.csv'))

    return df_total

# get start date, end date and corresponding constituents from the record of stock pool
# dataframe: start; end; string made of stock codes of the pool
def get_members():
    stock_pool = pd.read_csv(os.path.join(os.getcwd(),'static','stock_pool.csv'))
    max_end = stock_pool['end_date'].max()
    today = datetime.datetime.today()
    t = today.year * 10000 + today.month * 100 + today.day - 1
    stock_pool['end_date'] = np.where(stock_pool['end_date']==max_end,t, stock_pool['end_date'])
    list_start = stock_pool['start_date'].unique()
    list_start.sort()
    list_start = list(list_start)
    list_end = stock_pool['end_date'].unique()
    list_end.sort()
    list_end = list(list_end)
    dates = list(zip(list_start,list_end))
    members = pd.DataFrame(columns=['start_date', 'end_date', 'member_list'],)
    for d in dates:
        s = d[0]
        e = d[1]


        stocks = list(stock_pool.query('start_date == @s & end_date == @e')['stock_code'])
        m = pd.DataFrame({
            'start_date':[d[0]],
            'end_date':[d[1]],
            'member_list':[','.join(stocks)]
        })
        members = members.append(m)
    return members


def compare_index_benchmark():
    fy = pd.read_csv('D:/python_zhoutianlian/Fuyou50/static/index_quote.csv')
    fy = fy[['trade_date', 'close']]
    fy = fy.rename(columns={'close': 'fuyou'})

    def func(x):
        x = x.year * 10000 + x.month * 100 + x.day
        return x

    hs300 = ak.stock_zh_index_daily(symbol="sh000300")
    hs300 = hs300.reset_index()
    hs300['date'] = hs300['date'].apply(func)
    hs300 = hs300[['date', 'close']]
    hs300 = hs300.rename(columns={'close': 'hs300', 'date': 'trade_date'})
    df = pd.merge(fy, hs300, on='trade_date', how='left')
    adjust = df.iloc[0]['fuyou'] / df.iloc[0]['hs300']
    df['hs300'] = df['hs300'] * adjust
    print(df)
    list_total = list()
    list_total.append(list(zip(df['trade_date'], df['fuyou'])))
    list_total.append(list(zip(df['trade_date'], df['hs300'])))
    return list_total



if __name__ == '__main__':
    # members = pd.DataFrame(
    #     [[20190101, 20200201,
    #       'sz000338,sz000100,sh601865,sh600276,sz300760,sz002508,sh600660,sh600048,sz000661,sh603501,sh688012,sh603068,sh688008,sh600360,sh600667,sh600171,sh603005,sh600584,sh601899,sh603179,sz300427,sh600406,sh600399,sz300450,sz002460,sh600699,sz000550,sh603179,sh603186,sz002460,sh600699,sz300083,sz000550,sz300618,sz002709,sh688388,sz002030,sz300677,sh600216,sz002332,sh600436,sz300326,sh603939,sz000423,sz300601'],
    #      [20200101, 20210201,
    #       'sz000338,sz000100,sh601865,sh600276,sz300760,sz002508,sh600660,sh600048,sz000661,sh603501,sh688012,sh603068,sh688008,sh600360,sh600667,sh600171,sh603005,sh600584,sh601899,sh603179,sz300427,sh600406,sh600399,sz300450,sz002460,sh600699,sz000550,sh603179,sh603186,sz002460,sh600699,sz300083,sz000550,sz300618,sz002709,sh688388,sz002030,sz300677,sh600216,sz002332,sh600436,sz300326,sh603939,sz000423,sz300601']],
    #     columns=['start_date', 'end_date', 'member_list'], )
    members = get_members()
    print(members)
    trade_data = get_trade_data(members)
    index_data = calculate_index_quote_akshare(trade_data, members)
    print(index_data)
