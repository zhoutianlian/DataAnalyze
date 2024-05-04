# -*- coding: utf-8 -*-：
import pandas as pd
import numpy as np
import os
import time, datetime
import re

from CONFIG.mongodb import read_mongo_limit, read_mongo_columns
from sentiment.senti_analy import get_analysis


async def get_rz_qk(name, ex_usd):

    df_rz_qk = read_mongo_limit('tyc_data', 'Financing', {'enterpriseName': name},['enterpriseName', 'money', 'pubTime'])

    def func(x):
        if len(re.findall(r"\d+\.?\d*", x)) == 0:
            x = 5 * ('数' in x) + 100 * ('未' in x)
        else:
            x = float(re.findall(r"\d+\.?\d*", x)[0])
        return x

    if not df_rz_qk.empty:
        df_rz_qk['date'] = df_rz_qk['pubTime'].map(
            lambda x: pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(x / 1000)))).year)
        df_rz_qk['money_num'] = df_rz_qk['money'].apply(func)
        df_rz_qk['money_unit'] = df_rz_qk['money'].map(lambda x: (('美元' in x) * ex_usd + (not ('美元' in x)) * 1) * (
                    ('千' in x) * 10000000 + ('百' in x) * 1000000 + ('亿' in x) * 100000000 + 10000))
        df_rz_qk['money_1'] = df_rz_qk['money_num'] * df_rz_qk['money_unit']
        df_final = df_rz_qk.groupby('date').money_1.agg(['sum', 'count'])
        df_final.columns = ['m', 'n']
    else:
        df_final = pd.DataFrame()
    return df_final


async def get_hgjjzzl():

    time_now = datetime.datetime.now()
    time_0 = datetime.datetime.strptime(f'{time_now.year-3}-01-01', "%Y-%m-%d")

    df = read_mongo_columns('AM_origin', 'valuation_china_indicator_year', ['update_date', 'gdp'])
    if not df.empty:
        df['update_date'] = pd.to_datetime(df['update_date'])
        df = df.sort_values(by =['update_date'])
        df = df[df['update_date'] > time_0]
        df['date'] = df['update_date'].map(lambda x: x.year)
        df = df[['date', 'gdp']]
        df = df.drop_duplicates(keep='first').sort_values('date')
        df['delta_gdp'] = df['gdp'].pct_change()
        df = df.set_index(df['date'])
        df = df.dropna()
    else:
        df = pd.DataFrame()
    return df



def growth(df_0):

    def g(df):
        for i in df_0.region.values:
            if i in df['regLocation']:
                df['g'] = df_0[df_0['region'] == i]['growth_rate'].values[0]
                break
            else:
                df['g'] = df_0[df_0['region'] == 'avg']['growth_rate'].values[0]
        return df

    return g


async def get_dqjjzzl(name):

    df = read_mongo_limit('tyc_data', 'TycEnterpriseInfo',{'name':name},['name', 'regLocation', 'province'])
    if not df.empty:
        current_path = os.path.dirname(__file__)
        df_g = pd.read_csv(current_path + '/Data.csv')

        if len(df) > 0:
            df = df.drop_duplicates(keep='first')
            try:
                df['regLocation'] = np.where(df['regLocation'] == np.NaN, df['province'], df['regLocation'])
            except:
                df['regLocation'] = np.where(df['regLocation'] == np.NaN,'上海市工商行政管理局', df['regLocation'])
            df = df.apply(growth(df_g), axis=1)
            g = df.iloc[0]['g']
        else:
            g = df_g[df_g['region'] == 'avg']['growth_rate'].values[0]
    else:
        g = 0
    return g


async def get_industryFinance(name,ex_usd ):

    try:
        df_industry = read_mongo_limit('tyc_data', 'TycEnterpriseInfo', {'name':name},['name', 'industry'])
        df_industry = df_industry.drop_duplicates(keep='first')
        df_industry = df_industry.dropna()
        industry = df_industry['industry'].values[0]
        list_industry = df_industry[df_industry['industry'] == industry]
        df_finance = read_mongo_columns('tyc_data', 'Financing', ['enterpriseName', 'money', 'pubTime'])
        df_finance = df_finance[df_finance['enterpriseName'].isin(list_industry)]
        df_finance['date'] = df_finance['pubTime'].map(
            lambda x: pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(x / 1000)))).year)

        df_finance['money_num'] = df_finance['money'].map(
            lambda x: float(re.findall(r"\d+\.?\d*", x)[0]) + 5 * ('数' in x) + 100 * ('未' in x))
        df_finance['money_unit'] = df_finance['money'].map(lambda x: (('美元' in x) * ex_usd + (not ('美元' in x)) * 1) * (
                    ('千' in x) * 10000000 + ('百' in x) * 1000000 + ('亿' in x) * 100000000 + 10000))
        df_finance['money_1'] = df_finance['money_num'] * df_finance['money_unit']
        df_finance = df_finance.groupby('date').money_1.agg(['sum'])
        df_finance = df_finance.fillna(0)

    except:
        df_finance = pd.DataFrame()

    return df_finance


# myDF.groupby(myDF["车的ID"]).agg("count")
# df_rzqk = get_rzqk(name)
# df_rzqk = get_rzqk(name)
# df_devPotential = create_df_devPotential()

# def input_industryFinance(df_0, df_1):
#     year_now = datetime.datetime.now().year
#     if not df_0.empty:
#         try:
#             for i in df_0.index:
#                 df_1.loc[7, 'm{}'.format(year_now - i)] = df_0.loc[i]
#         except:
#             pass
#         finally:
#             df_1 = df_1.filnna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1


async def get_leagalInfo(name):

    time_now = datetime.datetime.now()
    time_0 = datetime.datetime.strptime(f'{time_now.year-3}-01-01', "%Y-%m-%d")

    df = read_mongo_limit('tyc_data', 'LegalAction',{'enterpriseName':name},
                            ['enterpriseName', 'judgetime', 'plaintiffs', 'defendants', 'casetype', 'title'])

    if not df.empty:
        df['judgetime'] = pd.to_datetime(df['judgetime'])
        #df = df[df['judgetime'] > time_0]
        def func(x):
            if x.year<2016:
                x = 2016
            elif x.year>2019:
                x = 2019
            else:
                x = x.year
                return x
        df['date'] = df['judgetime'].map(func)
        df_plaintiffs = df[['date', 'plaintiffs']]
        df_plaintiffs = df_plaintiffs.groupby('date').agg('count')
        df_defendants = df[['date', 'defendants']]
        df_defendants = df_defendants.groupby('date').agg('count')

        df_casetype = df[['date', 'casetype']]
        df_msss = df_casetype[df_casetype.casetype.str.contains('民事')]
        df_msss = df_msss.groupby('date').agg('count')
        df_msss.columns = ['msss']
        df_xsss = df_casetype[df_casetype.casetype.str.contains('刑事')]
        df_xsss = df_xsss.groupby('date').agg('count')
        df_xsss.columns = ['xsss']
        df_xzss = df_casetype[df_casetype.casetype.str.contains('行政')]
        df_xzss = df_xzss.groupby('date').agg('count')
        df_xzss.columns = ['xzss']
        df_detail = df[['date', 'title']]
        bool = df_detail.title.str.contains('纠纷')
        df_htjf = df_detail[bool]
        df_htjf = df_htjf.groupby('date').agg('count')
        df_htjf.columns = ['htjf']
        df_legalInfo = pd.concat([df_plaintiffs, df_defendants, df_msss, df_xsss, df_xzss, df_htjf], axis=1)
        df_legalInfo =  df_legalInfo.fillna(0)

    else:
        df_legalInfo = pd.DataFrame()

    return df_legalInfo


# def input_legalInfo(df_legalInfo, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_legalInfo.empty:
#         for i in df_legalInfo['plaintiffs'].index:
#             df_1.loc[0, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'plaintiffs']
#
#         for i in df_legalInfo['defendants'].index:
#             df_1.loc[1, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'defendants']
#
#         for i in df_legalInfo['msss'].index:
#             df_1.loc[6, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'msss']
#
#         for i in df_legalInfo['xsss'].index:
#             df_1.loc[7, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'xsss']
#
#         for i in df_legalInfo['xzss'].index:
#             df_1.loc[8, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'xzss']
#
#         for i in df_legalInfo['htjf'].index:
#             df_1.loc[9, 'n{}'.format(year_now - i)] = df_legalInfo.loc[i, 'htjf']
#         df_1 = df_1.fillna(0)
#
#     else:
#         df_1 = pd.DataFrame()
#
#     return df_1


async def get_tm(name):

    year_now = datetime.datetime.now().year
    year_0 = year_now - 4
    df = read_mongo_limit('tyc_data', 'TM',{'enterpriseName':name}, {'enterpriseName': 1, 'appDate': 1, 'regNo': 1, "_id": 0})
    if len(df) > 0:
        df.dropna(inplace=True)
        try:
            df['date'] = df['appDate'].map(
                lambda x: pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(int(x) / 1000)))).year)
        except:
            df['date'] = year_now-1
        finally:
            df['date'] = np.where(df['date'] > year_0, df['date'], year_0+1)
            df = df.groupby('date').regNo.agg('count')
    else:
        df = pd.DataFrame()

    return df

#
# def input_tm(df_tm, df_1):
#     start = time.time()
#
#     year_now = datetime.datetime.now().year
#     if not df_tm.empty:
#         try:
#             # for i, v in df_tm.items():
#             #     df_1.loc[0, 'n{}'.format(year_now - i)] = v
#             for i in df_tm.index:
#                 df_1.loc[0, 'n{}'.format(year_now - i)] = df_tm.loc[i]
#         except:
#             pass
#         finally:
#             df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     print('input tm:{}'.format(time.time()))
#     return df_1
#

async def get_patent(name):

    time_now = datetime.datetime.now()

    year_0 = datetime.datetime.now().year - 4
    df = read_mongo_limit('tyc_data', 'Patent', {'enterpriseName': name},
                          {'enterpriseName': 1, 'applicationTime': 1, 'patentName': 1, "_id": 0})
    if not df.empty:
        df.dropna(inplace=True)
        df['applicationTime'] = pd.to_datetime(df['applicationTime'])
        df['date'] = df['applicationTime'].map(lambda x: x.year)
        df['date'] = np.where(df['date'] > year_0, df['date'], year_0 + 1)
        df = df.groupby('date').patentName.agg('count')
    else:
        df = pd.DataFrame()
    return df
#
#
# def input_patent(df_patent, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_patent.empty:
#         for i in df_patent.index:
#             df_1.loc[1, 'n{}'.format(year_now - i)] = df_patent.loc[i]
#         df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1
#

async def get_software(name):

    year_now = datetime.datetime.now().year
    year_0 = year_now - 4
    df = read_mongo_limit('tyc_data', 'CR',{'enterpriseName':name}, {'enterpriseName': 1, 'regnum': 1, 'regtime': 1, "_id": 0})
    if not df.empty:
        df['regtime'].fillna(1568041600000, inplace=True)
        df['date'] = df['regtime'].map(
            lambda x: pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(int(x) / 1000)))).year)
        df['date'] = np.where(df['date'] > year_0, df['date'], year_0 + 1)
        df = df.groupby('date').regnum.agg('count')
    else:
        df = pd.DataFrame()
    return df

#
# def input_software(df_software, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_software.empty:
#         for i in df_software.index:
#             df_1.loc[2, 'n{}'.format(year_now - i)] = df_software.loc[i]
#         df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1
#

async def get_work(name):

    year_now = datetime.datetime.now().year
    year_0 = year_now - 4
    df = read_mongo_limit('tyc_data', 'CRW',{'enterpriseName':name}, {'enterpriseName': 1, 'regnum': 1, 'regtime': 1, "_id": 0})
    if not df.empty:
        df.dropna(inplace=True)

        def func(df):
            if '-' in df['regtime']:
                df['date'] = pd.to_datetime(df['regtime']).year
            else:
                df['date'] = pd.to_datetime(
                    time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(int(df['regtime']) / 1000)))).year
            return df

        df = df.apply(func, axis=1)
        if len(df) > 0:
            df['date'] = np.where(df['date'] > year_0, df['date'], year_0 + 1)
            df = df.groupby('date').regnum.agg('count')
        else:
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    return df

#
# def input_work(df_work, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_work.empty:
#
#         try:
#             for i in df_work.index:
#                 df_1.loc[3, 'n{}'.format(year_now - i)] = df_work.loc[i]
#         except:
#             pass
#         finally:
#             df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1
#

async def get_IPCase(name):

    time_now = datetime.datetime.now()
    time_0 = datetime.datetime.strptime(f'{time_now.year-3}-01-01', "%Y-%m-%d")
    df = read_mongo_limit('tyc_data', 'LegalAction',{'enterpriseName':name},
                            ['enterpriseName', 'judgetime', 'plaintiffs', 'defendants', 'title'])

    if not df.empty:
        df['judgetime'] = pd.to_datetime(df['judgetime'])
        #df = df[df['judgetime'] > time_0]
        def func(x):
            if x.year<2016:
                x = 2016
            elif x.year>2019:
                x = 2019
            else:
                x = x.year
                return x
        df['date'] = df['judgetime'].map(func)
        bool = df.title.str.contains('知识')
        df = df[bool]
        df = df.groupby('date').title.agg('count')
    else:
        df = pd.DataFrame()
    return df

#
# def input_IPCase(df_IPCase, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_IPCase.empty:
#         for i in df_IPCase.index:
#             df_1.loc[4, 'n{}'.format(year_now - i)] = df_IPCase.loc[i]
#         df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1

#
# def get_noIP(name):
#
#     time_now = datetime.datetime.now()
#     year_now = time_now.year
#
#     time_0 = datetime.datetime.strptime(f'{time_now.year-3}-01-01', "%Y-%m-%d")
#     df_patent = read_mongo_limit('tyc_data', 'Patent',{'enterpriseName':name},{'enterpriseName': 1, 'applicationTime': 1, 'patentName': 1, "_id": 0})
#     if not df_patent.empty:
#         df_patent['applicationTime'] = pd.to_datetime(df_patent['applicationTime'])
#         df_patent = df_patent[df_patent['applicationTime']>time_0]
#         df_patent['date'] = df_patent['applicationTime'].map(lambda x: x.year)
#         df_patent = df_patent.groupby('date').patentName.agg('count')
#
#         df_tm = read_mongo_columns('tyc_data', 'TM', ['enterpriseName', 'appDate', 'regNo'])
#         df_tm.dropna(inplace=True)
#         try:
#             df_tm['date'] = df_tm['appDate'].map(
#                 lambda x: pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(int(x) / 1000)))).year)
#         except:
#             df_tm['date'] = year_now-1
#         finally:
#             df_tm = df_tm.groupby('date').regNo.agg('count')
#
#         df_check = pd.concat([df_patent, df_tm], axis=1)
#         df_check['num'] = df_check['patentName'] + df_check['regNo']
#         df_check = df_check.groupby('date').num.agg('sum')
#         df_check = df_check.fillna(0)
#     else:
#         df_check = pd.DataFrame()
#     return df_check

#
# def input_noIP(df_noIP, df_1):
#
#     year_now = datetime.datetime.now().year
#     if not df_noIP.empty:
#         for i in range(3):
#             df_1.loc[5, 'n{}'.format(i + 1)] = 1
#
#         for i in df_noIP.index:
#             df_1.loc[5, 'n{}'.format(year_now - i)] = (df_noIP.loc[i] == 0) * 0 + (df_noIP.loc[i] != 0) * 1
#             df_1 = df_1.fillna(0)
#     else:
#         df_1 = pd.DataFrame()
#     return df_1
#

def get_time(data):
    if "time" in data:
        T = pd.to_datetime(data['time']).year
    elif "rtm" in data:
        T = pd.to_datetime(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(int(data['rtm'])/1000))).year
    else:
        T = datetime.datetime.now().year-3
    return T


def get_news(name):

    time_now = datetime.datetime.now()
    year_0 = time_now.year-4
    df = read_mongo_limit('tyc_data', 'New',{'enterpriseName':name}, {'_id':0,'_class':0})

    if not df.empty:
        df["date"] = df.apply(get_time, axis=1)
        df['date'] = np.where(df['date'] > year_0, df['date'], year_0 + 1)
        df['date'] = np.where(df['date'] < year_0 + 4, df['date'], year_0 + 3)
        s = get_analysis()

        def func_sentiment(s):
            def f(x):
                s.set_sentence(x)
                score = round(s.get_score(), 5)
                return score
            return f
        df['sentiment'] = df['title'].map(func_sentiment(s))
        df['sign'] = df['sentiment'].map(lambda x: np.sign(x))
        # random 0-10
        df['authority'] = df['sentiment'].map(lambda x: np.abs(8*np.random.randn(1)[0]))
        df['hot'] = df['sentiment'].map(lambda x: np.abs(8*np.random.randn(1)[0]))
        # random 0-1
        df['correlation'] = df['sentiment'].map(lambda x: np.random.randn(1)[0])
        df = df[['date', 'sign', 'authority', 'hot', 'correlation']]
        df = df.set_index('date')
        df_p = df.copy()
        df_p = df_p[df_p['sign'] > 0]
        df_n = df.copy()
        df_n = df_n[df_n['sign'] < 0]
        try:
            df_p = df_p.groupby('date').agg({'authority': np.mean, 'hot': np.mean, 'correlation': np.mean, 'sign': 'count'})
            df_p.rename(columns={'sign': 'n'}, inplace=True)
            df_n = df_n.groupby('date').agg({'authority': np.mean, 'hot': np.mean, 'correlation': np.mean, 'sign': 'count'})
            df_n.rename(columns={'sign': 'n'}, inplace=True)
            df_p = df_p.fillna(0)
            df_n = df_n.fillna(0)
        except:
            pass

    else:
        df_p = pd.DataFrame()
        df_n = pd.DataFrame()

    return df_p, df_n



if __name__ == '__main__':
    name = '江苏中信博新能源科技股份有限公司'
