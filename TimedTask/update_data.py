"""

调用tushare接口，获取最新上市公司列表，包含公司全称
调用akshare 获取公司基本信息
更新上市公司基本信息
每日更新，用于页面展示
"""
# todo 用爬虫获取公司全称数据
import requests
import numpy as np
import pandas as pd
from pymongo import MongoClient
from CONFIG.config import config
import time
import datetime
import pymongo
import tushare as ts
import akshare as ak
pro = ts.pro_api('df8135e1162c13b7126dafe5f2691536bf5e46ba5b32b5cfb368238f')

class Get_Info():

    def __init__(self):

        self.url = 'balance'
        self.out_host = config['DEFAULT']['out_mongo_host']
        self.out_username = config['DEFAULT']['out_mongo_user']
        self.out_password = config['DEFAULT']['out_mongo_password']
        self.out_port = int(config['DEFAULT']['out_mongo_port'])

        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = int(config['DEFAULT']['mongo_port'])

    def read_mongo(self, db_name, collection_name, conditions={}, query=[], sort_item='', n=0,
                    if_test=1):

        if if_test:
            mongo_uri_test = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                             (self.username, self.password, self.host, self.port, db_name)
            conn = MongoClient(mongo_uri_test)
        else:
            mongo_uri_local = 'mongodb://%s:%s@%s:%s/' % \
                             (self.out_username, self.out_password, self.out_host, self.out_port)
            conn = MongoClient(mongo_uri_local)
        db = conn[db_name]
        collection = db[collection_name]
        if query:
            cursor = collection.find(conditions, query)
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



    def get_fs(self, symbol, count, report_type='all', is_detail=True, summary=False):

        base_url = 'https://xueqiu.com'
        s = requests.session()
        HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                 '(KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
                   }
        s.get(url=base_url, headers=HEADERS)

        url = 'https://stock.xueqiu.com/v5/stock/finance/cn/%s.json?symbol=%s&type=%s&is_detail=%s&count=%d'
        response = s.get(url=url % ('balance', symbol, report_type, True, count), headers=HEADERS).text

        response = eval(response.replace('null', 'np.nan'))['data']
        data = pd.DataFrame(response['list'])


        data.report_date = data.report_date.apply(lambda x: datetime.datetime.fromtimestamp(x / 1000))

        def func(x):
            t = time.gmtime(x / 1000)
            x = str(t.tm_year) + '-' + '{:0>2d}'.format(t.tm_mon) + '-' + '{:0>2d}'.format(t.tm_mday)
            return x

        data.iloc[:, 3:] = data.iloc[:, 3:].applymap(lambda x: x[0])

        return data

    def get_shsz(self):

        df_list = pro.stock_basic(fields='ts_code,name,fullname,list_status')
        df_list.columns = ['stock_code', 'stock_name', 'enterprise_name', 'list_status']

        def func(x):
            prefix = x['stock_name'][0]
            if x['stock_code'][:3] == '688':
                x['listed_market'] = 3
            else:
                x['listed_market'] = 1
            if x['list_status'] == 'D' or x['list_status'] == 'P' or prefix in ['S', '*', 'N', 'C', 'U']:
                x['wel_delete'] = 1
            else:
                x['wel_delete'] = 0
            return x[['stock_name', 'stock_code', 'listed_market', 'wel_delete', 'enterprise_name']]

        df_list = df_list.apply(func, axis=1)

        df_spot = ak.stock_zh_a_spot()
        df_spot = df_spot[['code', 'mktcap', 'settlement']]
        df_spot.columns = ['stock_code', 'market_value', 'settlement']
        df_spot['market_value'] = df_spot['market_value'] * 10000

        def func_spot(x):
            x['total_number_of_shares'] = np.int(x['market_value'] / x['settlement'])
            if x['stock_code'][0] == '6':
                x['stock_code'] = x['stock_code'] + '.SH'
            else:
                x['stock_code'] = x['stock_code'] + '.SZ'
            return x[['stock_code', 'total_number_of_shares', 'market_value']]

        df_spot = df_spot.apply(func_spot, axis=1)

        df_ret = pd.merge(df_list, df_spot, on='stock_code', how='left')
        today = time.mktime(datetime.datetime.now().date().timetuple())
        today = datetime.datetime.fromtimestamp(today)
        df_ret['update_time'] = today
        df_ret = df_ret[['stock_code', 'total_number_of_shares', 'market_value',
                     'stock_name', 'listed_market', 'wel_delete',
                     'enterprise_name', 'update_time']]

        return df_ret

    def get_OTC(self):
        e_name = self.read_mongo(db_name='modgo_quant', collection_name='OTC_company_info', conditions={},
                            query=['company_name', 'stock_name'], sort_item='', n=0, if_test=0)
        e_name = e_name.dropna()
        e_name = e_name.drop_duplicates(keep='first')

        url = """http://69.push2.eastmoney.com/api/qt/clist/get?"""
        page = 1
        stocks_per_page = 50000

        parameters = """&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:81+s:!4&"""
        columns = [12, 2, 14, 20, 23, 38]
        col_names = ','.join(['f%d' % c for c in columns])

        table = requests.get(url + 'pn=%d&pz=%d' % (page, stocks_per_page) + parameters + 'fields=%s' % col_names).text
        table = table.replace('null', 'nan')
        table = eval(table)
        data = pd.DataFrame(table['data']['diff'])
        data.columns = ['close', 'stock_code', 'stock_name', 'market_value', 'pb', 'total_number_of_shares']
        data = data.applymap(lambda x: 0 if x == '-' else x)

        def func(x):
            sn = x['stock_name']
            x['stock_code'] = 'OC' + x['stock_code']
            x['wel_delete'] = 0
            if x['total_number_of_shares'] == 0:
                x['wel_delete'] = 1
            else:
                if x['close'] <= 0:
                    try:
                        fs = self.get_fs(symbol=x['stock_code'], count=1, report_type='all',
                                    is_detail=True, summary=False)[['total_holders_equity', 'total_assets', 'shares']]
                        if fs['total_holders_equity'].values[0] / x['total_number_of_shares'] <= 1:
                            x['market_value'] = fs['total_assets'].values[0]
                        else:
                            x['market_value'] = fs['total_holders_equity'].values[0]
                    except:
                        print('fs err')
                        x['market_value'] = 1.5 * x['total_number_of_shares']
                elif x['close'] < 1:
                    if (x['pb'] < 1) and (x['pb'] > 0):
                        x['market_value'] = x['total_number_of_shares'] * x['close'] / x['pb']
                    else:
                        try:
                            fs = self.get_fs(symbol=x['stock_code'], count=1, report_type='all',
                                        is_detail=True, summary=False)[
                                ['total_holders_equity', 'total_assets', 'shares']]
                            if fs['total_holders_equity'].values[0] / x['total_number_of_shares'] <= 1:
                                x['market_value'] = fs['total_assets'].values[0]
                            else:
                                x['market_value'] = fs['total_holders_equity'].values[0]
                        except:
                            print('fs err')
                            x['market_value'] = 1.5 * x['total_number_of_shares']

                else:
                    if (x['pb'] < 1) and (x['pb'] > 0):
                        x['market_value'] = x['total_number_of_shares'] * x['close'] / x['pb']

            try:
                x['enterprise_name'] = e_name.query('stock_name == @sn')['company_name'].values[0]
            except:
                x['enterprise_name'] = x['stock_name']
            del x['close']
            del x['pb']
            x['stock_code'] = x['stock_code'][2:] +'.'+x['stock_code'][:2]

            return x

        data = data.iloc[:100].apply(func, axis=1)
        today = time.mktime(datetime.datetime.now().date().timetuple())
        today = datetime.datetime.fromtimestamp(today)
        data['update_time'] = today
        data['listed_market'] = 2
        data = data[['stock_code', 'total_number_of_shares', 'market_value',
                            'stock_name', 'listed_market', 'wel_delete',
                 'enterprise_name', 'update_time']]

        return data
if __name__ == '__main__':

    gi = Get_Info()
    gi.get_shsz()
    # gi.get_OTC()
