"""
指数展示页面各项指标计算
"""
import time
import datetime
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import mplfinance as mpf
from cycler import cycler
import matplotlib as mpl
import akshare as ak

# update market capital, everyday
# calculate market cap
from config import dict_style
from utils import read_mongo


def update_mktcap(df_pool):
    stock_spot = ak.stock_zh_a_spot()
    df_spot = stock_spot.query('code in @list_stock')[['code','mktcap']]

    def func(x):
        s = x['stock_code']
        mkt = df_spot.query('code == @s')['mktcap']
        x['mktcap'] = mkt

        return x

    df_pool.apply(func,axis = 1)

    df_pool.to_csv(os.path.join(os.getcwd(),'static','stock_match_index.csv'))


# update benchmark code, every quarter
# cons without sz sh
# calculate distribution among industries
def update_sw_cons():

    sw_index_spot = ak.sw_index_spot()
    list_sw = sw_index_spot['指数代码'].tolist()

    df_stock_index = pd.DataFrame(columns=['stock_code', 'index_code'])
    for i in list_sw:
        df_i = pd.DataFrame(columns=['stock_code', 'index_code','index_name'])
        sw_index_cons = ak.sw_index_cons(index_code=i)
        df_i['stock_code'] = sw_index_cons['stock_code']
        df_i['index_code'] =i
        df_i['index_name'] = sw_index_spot.query('指数代码 == @i')['指数名称'].values[0]
        df_stock_index = df_stock_index.append(df_i)

    def func(x):
        if x['stock_code'][0] =='6':
            code = 'sh'+x['stock_code']
        else:
            code = 'sz'+x['stock_code']
        x['stock_code'] = code
        return x

    df_stock_index = df_stock_index.apply(func, axis = 1)
    df_stock_index.drop_duplicates(subset = 'stock_code', keep = 'first', inplace = True)
    df_stock_index.to_csv(os.path.join(os.getcwd(),'static','stock_match_industry.csv'))


def update_theme_index_cons():
    dict_theme = {
        '消费升级' : ['931480','930742','931007','930654','931005',
                  '930728','H30365','H30318','930717','930648',
                  '931494','399996','930711','931584',
                   'H11052','H30141','930721'],
        '创新医药' : ['930719','930720','931592','931011','931152',
                  '931440','931484','931639'],
        '新兴产业' : ['000964','H30368','930883','000891','930884'],
        '高端科技' : ['931441','931469','H30138','930713','930850',
                  '931071','931079','931491','931483','H30597',
                  '930733','931582','930722','930712','930725',
                  '930986']
    }
    df = pd.DataFrame(columns = ['stock_code','theme'])
    for k in dict_theme.keys():
        for i in dict_theme[k]:
            index_cons = ak.index_stock_cons(index=i)
            index_cons = pd.DataFrame({'stock_code':index_cons['品种代码'].tolist()})
        index_cons['theme'] = k
        df = df.append(index_cons,ignore_index=True)
        print(df)
    def func(x):
        if x['stock_code'][0] == '6':
            x['stock_code'] = 'sh'+x['stock_code']
        else:
            x['stock_code'] = 'sz'+x['stock_code']
            return x
    df = df.apply(func,axis =1)
    df.to_csv(os.path.join(os.getcwd(), 'static', 'stock_match_theme.csv'))




# get the latest constituents of styles index
def update_style_cons():

    df_stock_style = pd.DataFrame(columns=['stock_code', 'style'])
    for k in dict_style.keys():

        index_stock_cons_df = ak.index_stock_cons(index=dict_style[k])
        index_stock_cons_df['symbol'] = index_stock_cons_df['品种代码'].apply(ak.stock_a_code_to_symbol)
        df_i = pd.DataFrame(columns=['stock_code', 'style'])
        df_i['stock_code'] = index_stock_cons_df['symbol']
        df_i['style'] = k
        df_stock_style = df_stock_style.append(df_i)

    df_stock_style.drop_duplicates(subset='stock_code', keep='first', inplace=True)
    df_stock_style.to_csv(os.path.join(os.getcwd(), 'static', 'stock_match_style.csv'))


# update weight according to market capital or other rule, every quarter
def initialize_weight(base_date='20210104'):
    df_pool = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_pool.csv'), index_col=None)
    list_stock = df_pool['stock_code'].tolist()
    t = (datetime.datetime.now() - datetime.timedelta(days=90))
    d = t.year * 10000 + t.month * 100 + t.day
    df = read_mongo(db_name='modgo_quant', collection_name='aShareTradeData',
                    conditions={'trade_date': {'$gt': d}}, query=[], sort_item='', n=0)

    def func_mkt(x):
        if x['stock_code'].values[0] in list_stock:
            if base_date:
                x = x[x['trade_date'] == base_date]
            else:

                x = x[x['trade_date'] == x['trade_date'].max()]
            x['mkt_cap'] = x['outstanding_share'] * x['close']
            return pd.DataFrame(x['mkt_cap'])

    df = df.groupby('stock_code').apply(func_mkt)
    df = df.reset_index()
    del df['level_1']
    df['weight'] = df['mkt_cap'] / sum(df.mkt_cap)
    df.to_csv('stock_match_weight')



# match up stock with benchmark and theme every quarter
# cons with sz, sh
def match_up(update_industry = 0,update_style = 0,update_theme = 0,update_weight = 0):
    if update_industry:
        update_sw_cons()
    if update_style:
        update_style_cons()
    if update_theme:
        update_theme_index_cons()
    if update_weight:
        initialize_weight(base_date='20210104')
    df_pool = pd.read_csv(os.path.join(os.getcwd(),'static','stock_pool.csv'),index_col=None)
    df_industry = pd.read_csv(os.path.join(os.getcwd(),'static','stock_match_industry.csv'),index_col=0)
    df_theme = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_match_theme.csv'), index_col=0)
    df_style = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_match_style.csv'), index_col=None)
    df_weight = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_match_weight.csv'), index_col=None)

    def func(x):
        s = x['stock_code']
        try:
            industry = df_industry.query('stock_code == @s')['index_name'].values[0]
        except:
            industry = '制造业'
        try:
            theme = df_theme.query('stock_code == @s')['theme'].values[0]
        except:
            theme = '消费升级'
        try:
            style = df_style.query('stock_code == @s')['style'].values[0]
        except:
            style = 'largegrowth'
        x['industry'] = industry
        x['theme'] = theme
        x['style'] = style
        return x

    df_pool = df_pool.apply(func, axis = 1)
    df_pool = pd.merge(df_pool,df_weight,on = 'stock_code', how = 'left')
    df_pool.to_csv(os.path.join(os.getcwd(), 'static', 'stock_pool_full_1.csv'))






# update and calculate top 10 stock every day
# save as top10
def cal_top10(df_pool):

    df_pool.drop_duplicates(subset=['stock_code'],keep='first',inplace=True)
    df_pool = df_pool.sort_values(by = 'weight',ascending=False)
    df_pool = df_pool.iloc[:10]
    df_top = df_pool[['stock_code','weight']]
    df = ak.stock_zh_a_spot()
    df = df[['symbol','name']]
    df = df.rename(columns = {'symbol':'stock_code'})
    df_top = pd.merge(df_top,df, on  = 'stock_code', how = 'left')



    # df_top = pd.DataFrame(columns=['stock_code','stock_name','sw_name','trade_market','weight'])
    # df_sw = ak.sw_index_spot()
    # df_sw = df_sw[['指数代码','指数名称']]
    # df_sw.columns = ['index_code','index_name']
    # df_stock = ak.stock_zh_a_spot()
    # df_stock = df_stock[['symbol','name']]
    # for i, r in df_pool.iterrows():
    #     stock_code = r['stock_code']
    #     name = df_stock.query('symbol ==@stock_code')['name'].values[0]
    #     index_code = r['benchmark_code']
    #     market = stock_code[:2]
    #     df = pd.DataFrame({'stock_code':[stock_code],'stock_name':[name],
    #                       'sw_name':[index_code],
    #                         'trade_market':[market],'weight':[r['weight']]})
    #     df_top = df_top.append(df)
    #
    # df_top.to_csv(os.path.join(os.getcwd(), 'static', 'top10.csv'))
    return df_top


# sz or sh market
def cal_distribition_market(df_pool):

    def func(x):
        x['trade_market'] = x['stock_code'][:2]
        return x

    df_pool = df_pool.apply(func, axis =1)
    df_dist_market = df_pool.groupby('trade_market')['weight'].sum()
    df_dist_market = df_dist_market.reset_index()
    print(df_dist_market)
    return df_dist_market

# sw level 1 industry
def cal_distribution_industry(df_pool):

    df_dist_industry = df_pool.groupby('industry')['weight'].sum()
    df_dist_industry = df_dist_industry.reset_index()
    return df_dist_industry

# theme, consists of different industry
# def cal_distribution_theme(df_pool):
#
#     df_dist_theme = df_pool.groupby('theme')['weight'].sum()
#     df_dist_theme = df_dist_theme.reset_index()
#     return df_dist_theme

# style, large, small, value, growth
def cal_distribution_style(df_pool):

    df_dist_style = df_pool.groupby('style')['weight'].sum()
    df_dist_style = df_dist_style.reset_index()

    return df_dist_style

# calculate volatility in different period
def cal_volatility(df_pool):

    logreturns = np.diff(np.log(df_pool['close']))
    annualVolatility_1 = (np.std(logreturns) / np.mean(logreturns)) / np.sqrt(len(df_pool) / 252)
    annualVolatility_3 = (np.std(logreturns) / np.mean(logreturns)) / np.sqrt(len(df_pool) / (252*3))
    annualVolatility_5 = (np.std(logreturns) / np.mean(logreturns)) / np.sqrt(len(df_pool) / (252 * 5))
    dict_volatility = {
        'v_1':annualVolatility_1,
        'v_3': annualVolatility_3,
        'v_5': annualVolatility_5,
    }

    return dict_volatility


# calculate all the indicators necessary to the index introduction

def cal_indicator(df_pool):

    list_stock = df_pool['stock_code'].tolist()
    total_indicator = pd.DataFrame(columns= ['stock_code','NI','NI_ttm','BV','div','mktcap','mktcap_w'])

    for s in list_stock:

        code = s[2:]
        stock_indicator = ak.stock_a_lg_indicator(stock=code)
        stock_indicator = stock_indicator.reset_index()
        stock_indicator = stock_indicator.iloc[0][['pe','pe_ttm','pb','dv_ttm','total_mv']]
        stock_indicator['stock_code'] = s
        weight = df_pool.query('stock_code==@s')['weight'].values[0]
        stock_indicator['NI'] = weight * stock_indicator['total_mv']/stock_indicator['pe']
        stock_indicator['NI_ttm'] = weight * stock_indicator['total_mv'] / stock_indicator['pe_ttm']
        stock_indicator['BV'] = weight * stock_indicator['total_mv'] / stock_indicator['pb']
        stock_indicator['div'] = weight * stock_indicator['total_mv']*stock_indicator['dv_ttm']
        stock_indicator['mktcap_w'] = weight * stock_indicator['total_mv']
        stock_indicator['mktcap'] = stock_indicator['total_mv']
        total_indicator = total_indicator.append(stock_indicator)
    total_indicator = total_indicator.fillna(0)
    total_indicator.to_csv(os.path.join(os.getcwd(), 'static', 'indicator.csv'))

    pe = sum(total_indicator.mktcap_w)/sum(total_indicator.NI)
    pe_ttm = sum(total_indicator.mktcap_w) / sum(total_indicator.NI_ttm)
    pb = sum(total_indicator.mktcap_w) / sum(total_indicator.BV)
    div_ratio = sum(total_indicator['div']) / sum(total_indicator.mktcap_w)
    roe = sum(total_indicator['NI_ttm']) / sum(total_indicator.BV)
    max_cap = max(total_indicator.mktcap)
    min_cap = min(total_indicator.mktcap)
    avg_cap = np.average(total_indicator.mktcap)
    sum_cap = sum(total_indicator.mktcap)
    sum_cap_index = sum(total_indicator.mktcap_w)

    dict_indicator = {
        'pe':pe,
        'pe_ttm':pe_ttm,
        'pb':pb,
        'div_ratio':div_ratio,
        'max_cap':max_cap,
        'min_cap':min_cap,
        'avg_cap':avg_cap,
        'sum_cap':sum_cap,
        'sum_cap_index':sum_cap_index,
        'roe':roe
    }

    for k in dict_indicator.keys():
        print(k,dict_indicator[k])
    return dict_indicator

if __name__ == '__main__':


    df_pool = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_pool.csv'))
    match_up(update_industry = 0,update_style = 0,update_theme = 0,update_weight = 1)





