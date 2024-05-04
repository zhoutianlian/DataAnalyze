"""
计算各项因子打分
"""
import pandas as pd
import numpy as np
import os
import akshare as ak
import time
import datetime
import tushare as ts

from config import list_consumption, list_technology, list_medicine, list_emerging
from utils import read_mongo
from utils_construction import cal_growth, cal_inter_tb

pro = ts.pro_api('df8135e1162c13b7126dafe5f2691536bf5e46ba5b32b5cfb368238f')
from pymongo import MongoClient
import pymongo
from scipy import stats
from keras_bert import get_custom_objects
from keras_bert import load_trained_model_from_checkpoint, Tokenizer
import codecs
import os
import re
# from keras.models import load_model


# macroeconomic_policy_factor
# list 从config直接读取
def cal_index_score(list_index, name):

    df = pd.DataFrame(columns=['stock_code', 'index_score', 'index'])
    for i in list_index:
        index_cons = ak.index_stock_cons(index=i)
        index_cons = index_cons[['品种代码', '纳入日期']]
        index_cons.columns = ['stock_code', 'date_in']

        def func(x):
            today = datetime.datetime.today()
            x['date_in'] = pd.to_datetime(x['date_in'])

            x['delta_q'] = (today.year - x['date_in'].year) * 4 + (today.month - x['date_in'].month) / 3

            if x['stock_code'][0] == '6':
                x['stock_code'] = 'sh' + x['stock_code']
            else:
                x['stock_code'] = 'sz' + x['stock_code']

            return x

        index_cons = index_cons.apply(func, axis=1)
        index_cons['index_score'] = np.exp(-index_cons['delta_q'] / 30)
        index_cons['index'] = i
        index_cons = index_cons[['stock_code', 'index_score', 'index']]
        df = df.append(index_cons, ignore_index=True)
    df = pd.DataFrame(df.groupby('stock_code')['index_score'].sum())
    #     q =  df['index_score'].quantile(0.4)
    #     df = df[df['index_score']>q]
    df = df.sort_values(by=['index_score'], ascending=False)

    df = df.rename(columns={'index_score': name})

    return df


def get_macroeconomic_policy_factor():
    dict_w = {
        'consumption_score': 0.25,
        'technology_score': 0.25,
        'medicine_score': 0.25,
        'emerging_score': 0.25,
    }

    df_consumption = cal_index_score(list_index=list_consumption, name='consumption_score') * dict_w[
        'consumption_score']
    df_technology = cal_index_score(list_index=list_technology, name='technology_score') * dict_w['technology_score']
    df_medicine = cal_index_score(list_index=list_medicine, name='medicine_score') * dict_w['medicine_score']
    df_emerging = cal_index_score(list_index=list_emerging, name='emerging_score') * dict_w['emerging_score']

    df = pd.concat([df_consumption, df_technology, df_medicine, df_emerging], axis=1)
    df = df.fillna(0)

    df['macroeconomic_policy_factor'] = df.sum(axis=1)

    return df


# mutual_fund_factor
def cal_mkt_cap_score():
    t = (datetime.datetime.now() - datetime.timedelta(days=30))
    d = t.year * 10000 + t.month * 100 + t.day
    df = read_mongo(db_name='modgo_quant', collection_name='aShareTradeData', conditions={'trade_date': {'$gt': d}},
                    query=[], sort_item='', n=0)

    def func(x):
        x = x[x['trade_date'] == x['trade_date'].max()]
        x['mkt'] = x['outstanding_share'] * x['close']
        return pd.DataFrame(x['mkt'])

    df = df.groupby('stock_code').apply(func)
    df['mkt_cap_score'] = 1 - df['mkt'].rank(method="first") / (len(df) + 1)
    df = df.reset_index()
    df.drop(['level_1'], axis=1, inplace=True)
    df = df.set_index('stock_code')
    return df


def cal_mf_position_score():
    df_mkt = cal_mkt_cap_score()
    df_mfp = read_mongo(db_name='modgo_quant', collection_name='mutual_fund_position', conditions={}, query=[],
                        sort_item='', n=0)
    df_final = pd.DataFrame()
    for r, x in df_mfp.iterrows():
        df_total = pd.DataFrame(columns=['fund_code', 'stock_code', 'rank'])
        for i in range(len(x['stock_code'])):
            s = x['stock_code'][i]
            if s[0] == '6':
                s = 'sh' + s
            else:
                s = 'sz' + s
            df = pd.DataFrame({'fund_code': [x['fund_code']], 'stock_code': [s], 'rank': [i + 1]})
            df_total = df_total.append(df)
        df_total['position_score'] = df_total['rank'] / (len(df_total) + 1)
        df_total = df_total.set_index('stock_code')
        df_total = pd.merge(df_total, df_mkt, left_index=True, right_index=True, how='left')
        df_total = df_total[['position_score', 'mkt_cap_score']]
        df_final = df_final.append(df_total)
    return df_final[['position_score', 'mkt_cap_score']]

def get_mutual_fund_factor():
    df = cal_mf_position_score()
    def func(x):
        x['mutual_fund_factor'] = x['position_score'] * x['mkt_cap_score']
        x = pd.DataFrame({'mutual_fund_factor':[x['mutual_fund_factor'].sum()]})
        return x
    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df.drop(['level_1'], axis = 1,inplace = True)
    df.drop_duplicates(subset=['stock_code'],keep='first',inplace=True)
    df = df.set_index('stock_code')
#     df = pd.DataFrame(df['mutual_fund_factor'])
    return df

# financial_factor

def cal_gpm(tb, data_type, delta, ratio='gross_profit_margin', retrospect=0):
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

    df[ratio] = (df['total_revenue'] - df['operating_cost']) / df['total_revenue']

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
    df = df.set_index('stock_code')
    return df


def cal_profitability_score():
    dict_w = {
        'roa': 0.25,
        'roe': 0.25,
        'gpm': 0.25,
        'sfgr': 0.25,
    }

    df_roa = cal_inter_tb(tb_numerator='is', tb_denominator='bs', numerator='net_profit', denominator='total_asset',
                          ratio='roa', data_type='q', delta='yoy')
    df_roa['roa_score'] = df_roa['yoy_roa'].rank(method="first") / (1 + len(df_roa))
    df_roa = pd.DataFrame(df_roa['roa_score']) * dict_w['roa']

    df_roe = cal_inter_tb(tb_numerator='is', tb_denominator='bs', numerator='net_profit', denominator='total_equity',
                          ratio='roe', data_type='ttm', delta='qoq')
    df_roe['roe_score'] = df_roe['qoq_roe'].rank(method="first") / (1 + len(df_roe))
    df_roe = pd.DataFrame(df_roe['roe_score']) * dict_w['roe']

    df_gpm = df_gpm = cal_gpm(tb='is', data_type='ttm', delta='yoy')
    df_gpm['gpm_score'] = df_gpm['yoy_gross_profit_margin'].rank(method="first") / (1 + len(df_gpm))
    df_gpm = pd.DataFrame(df_gpm['gpm_score']) * dict_w['gpm']

    df_sfgr = cal_same_tb(tb='is', numerator='sales_fee', denominator='total_revenue',
                          ratio='sale_rev', data_type='ttm', delta='qoq')
    df_sfgr['sfgr_score'] = 1 - df_sfgr['qoq_sale_rev'].rank(method="first") / (1 + len(df_sfgr))
    df_sfgr = pd.DataFrame(df_sfgr['sfgr_score']) * dict_w['sfgr']

    df = pd.concat([df_roe, df_roa, df_gpm, df_sfgr], axis=1).fillna(0)

    return df


def cal_solvency_score():
    dict_w = {
        'd2a': 0.33,
        'cr': 0.33,
        'cfogr': 0.33,
    }

    df_d2a = cal_same_tb(tb='bs', numerator='total_liability', denominator='total_asset', ratio='debt_asset',
                         data_type='ttm', delta='yoy')
    df_d2a['d2a_score'] = df_d2a['yoy_debt_asset'].rank(method="first") / (1 + len(df_d2a))
    df_d2a = pd.DataFrame(df_d2a['d2a_score']) * dict_w['d2a']

    df_cr = cal_same_tb(tb='bs', numerator='total_current_asset', denominator='total_current_liability',
                        ratio='current_ratio', data_type='ttm', delta='yoy')
    df_cr['cr_score'] = df_cr['yoy_current_ratio'].rank(method="first") / (1 + len(df_cr))
    df_cr = pd.DataFrame(df_cr['cr_score']) * dict_w['cr']

    df_cfogr = cal_inter_tb(tb_numerator='cfs', tb_denominator='is', numerator='net_cashflow_operating_activity',
                            denominator='total_revenue',
                            ratio='net_operating_cashflow_to_gross_revenue', data_type='ttm', delta='qoq')
    df_cfogr['cfogr_score'] = df_cfogr['qoq_net_operating_cashflow_to_gross_revenue'].rank(method="first") / (
                1 + len(df_cfogr))
    df_cfogr = pd.DataFrame(df_cfogr['cfogr_score']) * dict_w['cfogr']

    df = pd.concat([df_d2a, df_cr, df_cfogr], axis=1).fillna(0)
    return df


def cal_growth_score():
    dict_w = {
        'oi': 0.2,
        'tp': 0.2,
        'np': 0.2,
        'npp': 0.2,
        'cfo': 0.2,
    }

    df_oi = cal_growth(target_tb='is', target='operating_income', data_type='ttm', delta='yoy_qoq')
    df_oi['oi_score'] = df_oi['yoy_qoq_operating_income'].rank(method="first") / (len(df_oi) + 1)
    df_oi = pd.DataFrame(df_oi['oi_score']) * dict_w['oi']

    df_tp = cal_growth(target_tb='is', target='total_profit', data_type='cum', delta='yoy_qoq')
    df_tp['tp_score'] = df_tp['yoy_qoq_total_profit'].rank(method="first") / (1 + len(df_tp))
    df_tp = pd.DataFrame(df_tp['tp_score']) * dict_w['tp']

    df_np = cal_growth(target_tb='is', target='net_profit', data_type='cum', delta='yoy_qoq')
    df_np['np_score'] = df_np['yoy_qoq_net_profit'].rank(method="first") / (1 + len(df_np))
    df_np = pd.DataFrame(df_np['np_score']) * dict_w['np']

    df_npp = cal_growth(target_tb='is', target='net_income_parent', data_type='q', delta='yoy_qoq')
    df_npp['npp_score'] = df_npp['yoy_qoq_net_income_parent'].rank(method="first") / (1 + len(df_npp))
    df_npp = pd.DataFrame(df_npp['npp_score']) * dict_w['npp']

    df_cfo = cal_growth(target_tb='cfs', target='net_cashflow_operating_activity', data_type='ttm', delta='yoy')
    df_cfo['cfo_score'] = df_cfo['yoy_net_cashflow_operating_activity'].rank(method="first") / (1 + len(df_cfo))
    df_cfo = pd.DataFrame(df_cfo['cfo_score']) * dict_w['cfo']

    df = pd.concat([df_oi, df_tp, df_np, df_npp, df_cfo], axis=1).fillna(0)
    return df


def cal_efficiency_score():
    dict_w = {
        'at': 0.33,
        'it': 0.33,
        'art': 0.33,
    }

    df_at = cal_inter_tb(tb_numerator='is', tb_denominator='bs', numerator='total_revenue', denominator='total_asset',
                         ratio='asset_turnover', data_type='ttm', delta='yoy')
    df_at['at_score'] = df_at['yoy_asset_turnover'].rank(method="first") / (1 + len(df_at))
    df_at = pd.DataFrame(df_at['at_score']) * dict_w['at']

    df_it = cal_inter_tb(tb_numerator='is', tb_denominator='bs', numerator='total_revenue', denominator='inventory',
                         ratio='inventory_turnover', data_type='ttm', delta='qoq')
    df_it['it_score'] = df_it['qoq_inventory_turnover'].rank(method="first") / (1 + len(df_it))
    df_it = pd.DataFrame(df_it['it_score']) * dict_w['it']

    df_art = cal_inter_tb(tb_numerator='is', tb_denominator='bs', numerator='total_revenue',
                          denominator='account_receivable',
                          ratio='account_receivable_turnover', data_type='ttm', delta='yoy')
    df_art['art_score'] = df_art['yoy_account_receivable_turnover'].rank(method="first") / (1 + len(df_art))
    df_art = pd.DataFrame(df_art['art_score']) * dict_w['art']

    df = pd.concat([df_at, df_it, df_art], axis=1).fillna(0)
    return df


def get_financial_factor():
    dict_w = {
        'growth': 0.5,
        'efficiency': 0.1,
        'profitability': 0.2,
        'solvency': 0.2
    }
    df_growth = cal_growth_score() * dict_w['growth']
    df_efficiency = cal_efficiency_score() * dict_w['efficiency']
    df_profitability = cal_profitability_score() * dict_w['profitability']
    df_solvency = cal_solvency_score() * dict_w['solvency']

    df = pd.concat([df_growth, df_efficiency, df_profitability, df_solvency], axis=1).fillna(0)

    df['financial_factor'] = df.sum(axis=1)
    #     df = pd.DataFrame(df['financial_factor'])
    return df



# price_volume_factor
def cal_risk_adjusted_return_momentun_score():
    dict_rar = {
        'sp': 0.5,
        'ir': 0.5,

    }
    dict_m = {
        'mr': 0.33,
        'id': 0.33,
        'rsi': 0.33,
    }

    t = (datetime.datetime.now() - datetime.timedelta(days=90))
    d = t.year * 10000 + t.month * 100 + t.day
    trade_data = read_mongo(db_name='modgo_quant', collection_name='aShareTradeData',
                            conditions={'trade_date': {'$gt': d}}, query=[], sort_item='', n=0)

    bench_mark = read_mongo(db_name='modgo_quant', collection_name='indexTradeData',
                            conditions={'index_code': 'sh000300'}, query=[], sort_item='', n=0)
    bench_mark['return_bm'] = np.log(bench_mark['close']) - np.log(bench_mark['close'].shift(1))
    bench_mark = bench_mark[['trade_date', 'return_bm']]
    rf = read_mongo(db_name='modgo_quant', collection_name='treasuryRate_10', conditions={}, query=[],
                    sort_item='trade_date', n=1)
    rf = rf['treasury_rate']

    def func(x):
        N = 22
        x = x.sort_values(by=['trade_date'])
        x['return'] = np.log(x['close']) - np.log(x['close'].shift(1))
        x['adj_return'] = x['return'] - rf
        x = pd.merge(x, bench_mark, on=['trade_date'])
        x['ex_return'] = x['return'] - x['return_bm']
        x['sign'] = x['return'].apply(lambda x: np.sign(x))
        N_id = 5
        x['information_dispersion'] = x['sign'] * x['sign'].rolling(N_id).sum() / N_id
        if len(x) >= N:
            x = x.iloc[-N:, :]
        x = x.dropna()
        information_ratio = np.sqrt(N) * x['ex_return'].mean() / x['ex_return'].std()
        sharp_ratio = np.sqrt(N) * x['adj_return'].mean() / x['adj_return'].std()
        max_return = np.max(x['return'])
        rsi = x[x['return'] > 0]['return'].mean() / x[x['return'] > 0]['return'].mean() - x[x['return'] < 0][
            'return'].mean()
        information_dispersion = x['information_dispersion'].mean()
        ret = pd.DataFrame({'sharp_ratio': [sharp_ratio], 'information_ratio': [information_ratio],
                            'max_return': [max_return], 'information_dispersion': [information_dispersion],
                            'rsi': [rsi]})

        return ret

    trade_data = trade_data.groupby('stock_code').apply(func)
    trade_data = trade_data.reset_index()
    trade_data.drop(['level_1'], axis=1, inplace=True)
    trade_data = trade_data.set_index('stock_code')

    trade_data = trade_data.fillna(0)
    trade_data['sp_score'] = 1 - trade_data['sharp_ratio'].rank(method="first") / len(trade_data)
    trade_data['ir_score'] = 1 - trade_data['information_ratio'].rank(method="first") / len(trade_data)
    trade_data['mr_score'] = 1 - trade_data['max_return'].rank(method="first") / len(trade_data)
    trade_data['id_score'] = 1 - trade_data['information_dispersion'].rank(method="first") / len(trade_data)
    trade_data['rsi_score'] = 1 - trade_data['rsi'].rank(method="first") / len(trade_data)

    trade_data['risk_adjusted_return_score'] = trade_data['sp_score'] * dict_rar['sp'] + trade_data['ir_score'] * \
                                               dict_rar['ir']
    trade_data['momentun_score'] = trade_data['mr_score'] * dict_m['mr'] + trade_data['id_score'] * dict_m['id'] + \
                                   trade_data['rsi_score'] * dict_m['rsi']

    return pd.DataFrame(trade_data['risk_adjusted_return_score']), pd.DataFrame(trade_data['momentun_score'])

def get_price_volume_factor():
    dict_w = {
        'risk_adjusted_return': 0.7,
        'momentum': 0.3,
    }

    list_stock = read_mongo(db_name='modgo_quant', collection_name='stockList', conditions={'delisting_date': np.NaN},
                            query=['stock_code'], sort_item='', n=0)
    # list_stock = list_stock['stock_code'].to_list()
    df_risk_adjusted_return, df_momentum = cal_risk_adjusted_return_momentun_score()
    df = pd.concat([df_risk_adjusted_return * dict_w['risk_adjusted_return'], df_momentum ** dict_w['momentum']],
                   axis=1).fillna(0)
    df['price_volume_factor'] = df.sum(axis=1)
    return pd.DataFrame(df['price_volume_factor'])

# institution_expectation_factor
def cal_expectaion_score(tb, target):
    dict_w = {
        'e': 0.7,
        'n': 0.3,
    }

    df = read_mongo(db_name='modgo_quant', collection_name=tb, conditions={}, query=[], sort_item='', n=0)
    now = datetime.datetime.now()
    # delta = datetime.timedelta(days=-90)
    # start = now + delta
    start = now.year * 10000 + 1 * 100 + 1
    df = df[df['announcement_data'] > start]
    name_0 = target + '_current'
    name_1 = target + '_next'
    name = target + '_growth'

    def func(x):
        x[name] = x[name_1] / x[name_0] - 1
        growth = x[name].mean()
        n = len(x)
        ret = pd.DataFrame({'growth': [growth], 'n_institution': [n]})
        return ret

    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')
    score_e = 'e_' + target + '_score'
    score_n = 'n_' + target + '_score'
    df[score_e] = df['growth'].rank(method="first") / (len(df) + 1)
    df[score_n] = df['n_institution'].rank(method="first") / (len(df) + 1)
    df = df[[score_e, score_n]]
    score = target + '_score'
    df[score] = df[score_e] * dict_w['e'] + df[score_n] * dict_w['n']

    return pd.DataFrame(df[score])


def get_institution_expectation_factor():
    dict_w = {
        'rev': 0.25,
        'eps': 0.25,
        'ni': 0.25,
        'roe': 0.25,
    }

    df_rev = cal_expectaion_score(tb='Revenue_expectation', target='rev') * dict_w['rev']
    df_eps = cal_expectaion_score(tb='EPS_expectation', target='EPS') * dict_w['eps']
    df_ni = cal_expectaion_score(tb='NI_expectation', target='NI') * dict_w['ni']
    df_roe = cal_expectaion_score(tb='ROE_expectation', target='ROE') * dict_w['roe']

    df = pd.concat([df_rev, df_eps, df_ni, df_roe], axis=1)
    df['institution_expectation_factor'] = df.sum(axis=1)

    #     df = pd.DataFrame(df['institution_expectation_factor'])
    return df


# foreign_holding_factor
def get_fs_data():
    fs = os.path.join(os.getcwd(),'static','foreign_shareholding.csv')
    calendar = pro.query('trade_cal', start_date='20210101', end_date='20210301')
    calendar = calendar[calendar['is_open'] == 1]['cal_date'].tolist()
    pd.DataFrame(columns=['trade_date', 'stock_code', 'volume', 'ratio', 'exchange']).to_csv(fs, index=False)

    with open(fs, 'w', encoding='utf-8') as f:

        for d in calendar:
            df = pro.hk_hold(trade_date=d, exchange='SH')
            df = df[['trade_date', 'ts_code', 'vol', 'ratio', 'exchange']]
            df_1 = pro.hk_hold(trade_date=d, exchange='SZ')
            df_1 = df_1[['trade_date', 'ts_code', 'vol', 'ratio', 'exchange']]

            df.to_csv(f, mode='a+', index=False, header=False)
            df_1.to_csv(f, mode='a+', index=False, header=False)

def cal_fs_score():

    get_fs_data()
    fs = os.path.join(os.getcwd(), 'static', 'foreign_shareholding.csv')
    df_fs = pd.read_csv(fs, index_col=None)


    def func(x):
        x = x.sort_values(by=['trade_date'])
        x['delta_v_pct'] = (x['volume'] - x['volume'].shift(1)) / x['volume'].shift(1)
        x['delta_ratio_pct'] = (x['ratio'] - x['ratio'].shift(1)) / x['ratio'].shift(1)
        x = x[['trade_date', 'delta_ratio_pct', 'delta_v_pct']]
        x = x.dropna()
        x = x.set_index('trade_date')
        x = x.sum()
        return x

    df_fs_buy = df_fs.groupby('stock_code').apply(func)
    df_fs_buy['delta_v_score'] = 1 - df_fs_buy["delta_v_pct"].rank(method="first") / len(df_fs_buy)
    df_fs_buy['delta_ratio_score'] = 1 - df_fs_buy["delta_ratio_pct"].rank(method="first") / len(df_fs_buy)

    def func_1(x):
        x = x.sort_values(by=['trade_date'])
        x = x.set_index('trade_date')
        x = x['ratio'].mean()
        return x

    df_fs_position = pd.DataFrame(df_fs.groupby('stock_code').apply(func_1), columns=['ratio_avg'])
    df_fs_position['position_score'] = 1 - df_fs_position["ratio_avg"].rank(method="first") / len(df_fs_position)
    df = pd.merge(df_fs_buy[['delta_v_score', 'delta_ratio_score']], df_fs_position['position_score'], left_index=True,
                  right_index=True, how='inner')

    return df


def get_foreign_shareholder_factor():
    dict_w = {
        'delta_v': 0.33,
        'delta_ratio': 0.33,
        'position': 0.33
    }
    df = cal_fs_score()

    df['foreign_shareholder_factor'] = df['delta_v_score'] * dict_w['delta_v'] + df['delta_ratio_score'] * dict_w[
        'delta_ratio'] + df['position_score'] * dict_w['position']
    #     q = df['fs_score'].quantile(0.92)
    #     df = df.sort_values(by = ['fs_score'],ascending=False)
    #     df = df[df['fs_score']>q]
    #     df = pd.DataFrame(df['fs_score'])
    return df


# public_opinion_factor

def cal_sentiment_score(text):

    model = load_model(os.path.join(os.getcwd(), 'sentiment_model.h5'), custom_objects=get_custom_objects())
    if len(text) ==1:
        x1, x2 = tokenizer.encode(first=text)
        X1 = seq_padding([x1])
        X2 = seq_padding([x2])
        predict_test = model.predict([X1, X2])[0][0]
    else:
        X1 = list()
        X2= list()
        for i in range(len(text)):
            d = text[i]
            text = d[:250]
            x1, x2 = tokenizer.encode(first=text)
            X1.append(x1)
            X2.append(x2)
        X1 = seq_padding(X1)
        X2 = seq_padding(X2)
        predict_test = model.predict([X1, X2])
        predict_test = predict_test.flatten()

    return predict_test


def cal_heat_score():
    dict_w = {
        'heat': 0.5,
        'growth': 0.5,
    }
    df = read_mongo(db_name='spider_origin', collection_name='vCB_AllNewsStock',
                    conditions={'news_time': {'$gt': 20210201}}, query=[], sort_item='', n=0)

    def func(x):
        x['n_news'] = len(x)
        return x

    df = df.groupby(['stock_code', 'news_time']).apply(func)
    df = df[['stock_code', 'n_news', 'news_time']]

    def func_1(x):
        x = x.sort_values(by=['news_time'], ascending=False)
        N = min(len(x), 7)
        heat_score = x['n_news'][:N].mean()
        heat_growth_score = x['n_news'].pct_change()[:N].mean()
        return pd.DataFrame({'heat_score': [heat_score], 'heat_growth_score': [heat_growth_score]})

    df = df.groupby(['stock_code']).apply(func_1)
    df['heat_score'] = df["heat_score"].rank(method="first") / (len(df) + 1)
    df['heat_growth_score'] = df["heat_growth_score"].rank(method="first") / (len(df) + 1)
    df = df.reset_index().fillna(0)
    df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')
    return df

cn_prefix = 'chinese_L-12_H-768_A-12'
config_path = os.path.join(os.getcwd(), cn_prefix, 'bert_config.json')
checkpoint_path = os.path.join(os.getcwd(), cn_prefix, 'bert_model.ckpt')
vocab_path = os.path.join(os.getcwd(), cn_prefix, 'vocab.txt')

token_dict = {}
with codecs.open(vocab_path, 'r', 'utf8') as reader:
    for line in reader:
        token = line.strip()
        token_dict[token] = len(token_dict)
tokenizer = Tokenizer(token_dict)


def seq_padding(X, padding=0):
    L = [len(x) for x in X]
    # max(L)
    ML = 512
    return np.array([
        np.concatenate([x, [padding] * (ML - len(x))]) if len(x) < ML else x for x in X
    ])


def get_sentiment_score(content):
    model = load_model(os.path.join(os.getcwd(), 'sentiment_model.h5'), custom_objects=get_custom_objects())
    if len(content) == 1:
        text = content[0]
        text = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", text)
        text = text[:300]
        x1, x2 = tokenizer.encode(first=text)
        X1 = seq_padding([x1])
        X2 = seq_padding([x2])
        predict_test = model.predict([X1, X2])[0]
    else:
        X1 = list()
        X2 = list()
        for i in range(len(content)):
            text = content[i]
            text = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", text)
            text = text[:300]
            x1, x2 = tokenizer.encode(first=text)
            X1.append(x1)
            X2.append(x2)
        X1 = seq_padding(X1)
        X2 = seq_padding(X2)
        predict_test = model.predict([X1, X2])
        predict_test = predict_test.flatten()

    return predict_test


def cal_sentiment_score():
    today = datetime.datetime.now()
    start = today + datetime.timedelta(days=-7)
    start = start.year * 10000 + start.month * 100 + start.day

    df = read_mongo(db_name='spider_origin', collection_name='vCB_AllNewsStock',
                    conditions={'news_time': {'$gt': start}}, query=[], sort_item='', n=0)
    df = df.sort_values(by='news_time')

    def func(x):
        today = datetime.datetime.now()
        x['sentiment_score'] = get_sentiment_score(content=[x['content']])
        news_time = pd.to_datetime(str(x['news_time']))
        delta_t = (today.year - news_time.year) * 365 + (today.month - news_time.month) * 30 + today.day - news_time.day
        x['sentiment_score'] = x['sentiment_score'] * np.exp(-delta_t)
        return x[['sentiment_score', 'stock_code']]

    df = df.apply(func, axis=1)

    df = df.groupby('stock_code')['sentiment_score'].sum()
    df = df.reset_index()
    # df.drop(['level_1'], axis=1, inplace=True)
    df.drop_duplicates(subset=['stock_code'], keep='first', inplace=True)
    df = df.set_index('stock_code')

    return df


def get_public_opinion_factor():
    dict_w = {
        'heat': 0.5,
        'sentiment': 0.5
    }
    df_heat_score = cal_heat_score() * dict_w['heat']
    df_sentiment = cal_sentiment_score() * dict_w['sentiment']
    df = pd.concat([df_heat_score, df_sentiment], axis=1)
    df['public_opinion_factor'] = df.sum(axis=1)

    return df

