""""
融资数据预处理
将创业邦爬取的数据清洗后存入financing_event
需要每天定时执行，在爬虫之后处理的第一步
"""

import re
import datetime
import pandas as pd
import numpy as np
from pymongo import MongoClient
import pymongo
from config.send_Dingding import send_to_dingding
from config.Config import config

class FinancePre():

    def __init__(self, if_del=0):
        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = config['DEFAULT']['mongo_port']

        self.if_del = if_del

    def read_mongo(self, db_name, collection_name, conditions={}, query=[], sort_item='', n=0):

        conn = MongoClient('mongodb://192.168.2.114:27017/')
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

    def save_mongo(self, df_data, db_name, tb_name, if_del=0):
        mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                         (self.username, self.password, self.host, self.port, db_name)

        conn = MongoClient(mongo_uri)
        db = conn[db_name]
        collection = db[tb_name]
        if if_del:
            collection.delete_many({})
        list_data = df_data.to_dict('record')
        collection.insert_many(list_data)

    def preprocess(self):

        t = datetime.datetime.now()
        today = t.year * 10000 + t.month * 100 + t.day
        df = self.read_mongo(db_name='spider_origin', collection_name='company_message_text',
                             conditions={}, query=[], sort_item='',
                             n=0)
        c = list(df.columns)
        c.remove('_id')
        df = df[c]

        def func_company(x):

            try:
                x['company_name'] = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "",
                                           x['company_name'])
                x['short_name'] = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "",
                                         x['short_name'])
                if len(x['company_name']) >= 2:
                    return x
                elif len(x['short_name']) >= 2:
                    x['company_name'] = x['short_name']
                    return x
                else:
                    pass
            except:
                pass

        df = df.apply(func_company, axis=1)
        df.dropna(subset=['company_name'], inplace=True)

        c_1 = c
        c_1.remove('company_name')
        c_1.append('last_financing_date')

        def func_lfd(x):
            x['len'] = x['data'].apply(lambda x: len(x))
            x = x[x['len'] == x['len'].max()]
            x = x.drop_duplicates(subset='company_name', keep='last')
            d = pd.to_datetime(x['data'].values[0][0]['publish_time'])
            x['last_financing_date'] = d.year * 10000 + d.month * 100 + d.day
            x = x[c_1]
            return x

        df = df.groupby(['company_name']).apply(func_lfd)
        df = df.reset_index()

        try:
            del df['level_1']
        except:
            pass

        def func_ind(x):
            list_ind = x['industry'].split(',')
            for ind in list_ind:
                if len(ind) < 2:
                    list_ind.remove(ind)
            list_ind = list_ind[:3]
            x['industry_2'] = ''
            x['industry_3'] = ''
            if len(list_ind) == 1:
                x['industry_1'] = list_ind[0]
            else:
                for i in range(len(list_ind)):
                    x['industry_%d' % (i + 1)] = list_ind[i]
            if not x['industry_2']:
                x['industry_2'] = x['industry_1']
            if not x['industry_3']:
                x['industry_3'] = x['industry_2']

            return x

        df = df.apply(func_ind, axis=1)

        df = df.applymap(lambda x: np.NaN if str(x) == '-' else x)

        def func_makeup(x):
            if type(x['establish_time']) == float:
                x['establish_time'] = x['data'][-1]['publish_time']
            regex = re.compile("[\u4e00-\u9fa5]+")
            cn = regex.findall(x['establish_time'])
            if cn:
                x['establish_time'] = x['data'][-1]['publish_time']
            if type(x['specific_business']) == float:
                x['specific_business'] = x['industry_3'] + '相关产业链'
            if type(x['tag']) == float:
                x['tag'] = x['industry_3']

            return x

        df = df.apply(func_makeup, axis=1)
        today = str(datetime.datetime.today())
        if not df.empty:
            result = '%s---- 融资快报每日更新结果：数据更新完成' % today
            send_to_dingding(result)
        else:
            result = '%s---- 融资快报更新结果：今日未有公司融资' % today
            send_to_dingding(result)

        self.save_mongo(df_data=df, db_name='spider_origin', tb_name='financing_event',
                        if_del=self.if_del)


if __name__ == '__main__':

    fp = FinancePre(if_del=1)
    fp.preprocess()
