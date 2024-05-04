"""
依据各项因子打分的结果
加权后筛选出标的证券构建指数

"""
import pandas as pd
import numpy as np
import os
import akshare as ak
import time
import datetime
import tushare as ts
from config import semiconductor_pool, new_material_pool, new_energy_pool, medicine_pool, electronics_pool
from factor_function import get_macroeconomic_policy_factor, get_public_opinion_factor, get_foreign_shareholder_factor, \
    get_institution_expectation_factor, get_price_volume_factor, get_financial_factor, get_mutual_fund_factor

pro = ts.pro_api('df8135e1162c13b7126dafe5f2691536bf5e46ba5b32b5cfb368238f')

# 获取7*因子，所有标的，所有score及factor（score分类加权）shujv
# 保存一份原始数据
# 返回按因子类别加权的total factor及所有factor数据
def collect_factor():
    list_total = list()

    try:
        df_macroeconomic_policy_factor = get_macroeconomic_policy_factor()
        print('ok macroeconomic_policy')
        list_total.append(df_macroeconomic_policy_factor)
    except Exception as e:
        print('error macroeconomic_policy')
        print(e)
    try:
        df_public_opinion_factor = get_public_opinion_factor()
        print('ok public_opinion')
        list_total.append(df_public_opinion_factor)
    except Exception as e:
        print('error public_opinion')
        print(e)

    try:
        df_foreign_shareholder_factor = get_foreign_shareholder_factor()
        list_total.append(df_foreign_shareholder_factor)
    except Exception as e:
        print('foreign_shareholder')
        print(e)
    try:
        df_institution_expectation_factor = get_institution_expectation_factor()
        list_total.append(df_institution_expectation_factor)
    except Exception as e:
        print('error institution_expectation')
        print(e)
    try:
        df_price_volume_factor = get_price_volume_factor()
        list_total.append(df_price_volume_factor)
    except Exception as e:
        print('error price_volume')
        print(e)
    try:
        df_financial_factor = get_financial_factor()
        list_total.append(df_financial_factor)
    except Exception as e:
        print('error financial')
        print(e)
    try:
        df_mutual_fund_factor = get_mutual_fund_factor()
        list_total.append(df_mutual_fund_factor)
    except Exception as e:
        print('error mutual_fund')
        print(e)

    df = pd.concat(list_total, axis=1)
    df = df.fillna(0)
    df.to_csv(os.path.join(os.getcwd(),'static','total_score_factor.csv'))

    list_result = []
    dict_w = {
        'macroeconomic_policy_factor':1/7,
    'public_opinion_factor':1/7,
    'foreign_shareholder_factor':1/7,
    'institution_expectation_factor':1/7,
    'price_volume_factor':1/7,
    'financial_factor':1/7,
    'mutual_fund_factor':1/7,
    }
    for i in dict_w.keys():
        j = dict_w[i] * (df[i] - df[i].min()) / (df[i].max() - df[i].min())
        list_result.append(j)
    df_result = pd.concat(list_result, axis=1)
    df_result['total_factor'] = df_result.sum(axis=1)
    return df_result

# 获取统跑50，更新股票池
# 插入新代码，输入最新时间
# 不插入数据更新代码
def update_pool(if_new = 0):

    df = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_pool.csv'),index_col=None)
    df = df[['stock_code','start_date','end_date']]
    t = datetime.datetime.now()
    today = t.year * 10000 + t.month * 100 + t.day
    if if_new:
        new_stock = pd.read_csv(os.path.join(os.getcwd(), 'static', 'stock_top_50.csv'),index_col=0)
        new_stock = list(new_stock.index)
        df_new = pd.DataFrame({'stock_code':new_stock})
        df_new['start_date'] = df['end_date'].max()
        df_new['end_date'] = today
        df = df.append(df_new)
    df['end_date'] = np.where(df['end_date'] == df['end_date'].max(),today,df['end_date'])
    df.to_csv(os.path.join(os.getcwd(), 'static', 'stock_pool.csv'))

# 获取所有factor数据
# 增加股票名字
# 返回所有数据及top 50 数据
def record_total_factor(df):

    df = df.sort_values(by='total_factor', ascending=False)
    df = df[['macroeconomic_policy_factor','public_opinion_factor','foreign_shareholder_factor',
              'institution_expectation_factor','price_volume_factor',
             'financial_factor','mutual_fund_factor','total_factor']]
    df_name = ak.stock_info_a_code_name()
    def func(x):
        if x[0] == '6':
            x = 'sh' + x
        else:
            x = 'sz' + x
        return x

    df_name['code'] = df_name['code'].apply(func)
    df_name = pd.DataFrame(df_name.set_index('code'))
    df = pd.merge(df,df_name,right_index=True, left_index=True, how = 'inner')
    t = datetime.datetime.now()
    df['update_date'] = t.year*10000 + t.month*100 + t.day

    df_top = df.iloc[:50, :]
    df_top = df_top[['total_factor','name']]
    df_top.to_csv(os.path.join(os.getcwd(), 'static', 'stock_top_50.csv'))
    df.to_csv(os.path.join(os.getcwd(), 'static', 'stock_factor.csv'))

# class ConstructIndex(object):

if __name__ =='__main__':
    # 模拟计算factor全过程
    df_final = pd.read_csv(os.path.join(os.getcwd(),'static','df_result.csv'),index_col = 0)
    df_mf = pd.read_csv(os.path.join(os.getcwd(),'static','df_mf.csv'),index_col=0)
    df_pv = pd.read_csv(os.path.join(os.getcwd(), 'static', 'df_pv.csv'), index_col=0)
    df = pd.concat([df_final, df_mf, df_pv], axis=1)
    df = df.fillna(0)

    list_result = []
    dict_w = {
        'macroeconomic_policy_factor': 1 / 7,
        'public_opinion_factor': 1 / 7,
        'foreign_shareholder_factor': 1 / 7,
        'institution_expectation_factor': 1 / 7,
        'price_volume_factor': 1 / 7,
        'financial_factor': 1 / 7,
        'mutual_fund_factor': 1 / 7,
    }
    for i in dict_w.keys():
        j = dict_w[i] * (df[i] - df[i].min()) / (df[i].max() - df[i].min())
        list_result.append(j)
    df_result = pd.concat(list_result, axis=1)
    df_result['total_factor'] = df_result.sum(axis=1)
    #####################################################
    record_total_factor(df =df_result)
    update_pool(if_new=0)






    # df_final = df_final.set_index('sto')

    # df_finanl = collect_factor()

# pip install --default-timeout=10000 -i https://pypi.tuna.tsinghua.edu.cn/simple tensorflow