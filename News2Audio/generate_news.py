"""
配合接口传入的参数
读取数据库已经生成的新闻
返回所需时间段的数据
"""

# import re
# import datetime
# import os
import pandas as pd
# import numpy as np
from pymongo import MongoClient
import pymongo
import time
from config.Config import config
# from aip import AipSpeech
# from ValuationRecord.SaveEnterprise import save as save_id

# from generate_news_origin import News
# from save_file import save
from log.log import logger

# import os
# APP_ID = '24041344'
# API_KEY = 'qPYe3aq3f3yfAE8h15YGU12h'
# SECRET_KEY = 'WanvzTjf4GVaG1mGsZWRhZHW1BdGl00r'
# client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)

class ReadNews():
    def __init__(self, update_date,page=1):
        self.username = config['DEFAULT']['mongo_user_master']
        self.password = config['DEFAULT']['mongo_password_master']
        self.host = config['DEFAULT']['mongo_host_master']
        self.port = config['DEFAULT']['mongo_port_master']
        self.update_date = update_date
        self.page = page


    def mongo_conn(self, db_name):
        # 测试
        self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                         (self.username, self.password, self.host, self.port, db_name)
        conn = MongoClient(self.mongo_uri)

        # 本地
        # self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=admin' % \
        #                  (self.username, self.password, self.host, self.port)
        # conn = MongoClient(self.mongo_uri)
        return conn


    def read_mongo(self,db_name, collection_name, conditions={}, query=[], sort_item='', n=0):
        conn = self.mongo_conn(db_name)
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

    def read(self):
        df = self.read_mongo(db_name = 'modgo_quant', collection_name = 'financing_news',
                        conditions={'update_date':self.update_date}, query=[], sort_item='', n=0)

        cases = []

        for i in range(len(df['cases'].values[0])):
            dict_1 = {}

            dict_1['content'] = df['cases'].values[0][i]
            highlight= df['highlight'].values[0]['cases'][i]
            title = df['title'].values[0][i]
            h = []

            for j in highlight:
                if ('轮' in j) or ('战略投资' in j) or 'IPO' in j:
                    pass
                else:
                    h.append(j)

            # if len(h) == 3:
            #     e_id = save_id(h[0])
            #     print(e_id)
            #     h.append(str(e_id))
            dict_1['highlight'] = h
            dict_1['title'] = title
            cases.append(dict_1)

        show_list = df['summary'].values[0].split('，')
        theme = show_list[4].split('：')[1]
        if len(theme) == 0:
            theme = '大消费，科技'
        dict_show = {
            'ammount': show_list[1] + show_list[2],
            'industry': show_list[3],
            'theme': theme
        }

        summary_highlight = []
        for i in df['highlight'].values[0]['summary'][0]:
            summary_highlight.append(i)
        # n = News()
        audio_url = ''
        if not df['download_url'].empty:
            audio_url = df['download_url'].values[0]
        # else:
        #     audio_url = n.get_audio(df['summary'].values[0], self.update_date)

        dict_ret = {
            'summary': {'content': df['summary'].values[0], 'highlight': summary_highlight},
            'cases': cases,
            'summary_show': dict_show,
            'audio_url': audio_url,

        }

        return dict_ret

    def hist(self):
        df = self.read_mongo(db_name='modgo_quant', collection_name='financing_news',
                             conditions={'update_date':{'$lt':self.update_date}}, query=[], sort_item='', n=0)
        dates = list(set(list(df['update_date'])))
        N = len(dates)
        dates.sort(reverse=True)

        if self.page > 1:
            dates = dates[(self.page - 1) * (10):(10) * self.page]
        else:
            dates = dates[:10]
        dict_ret = {
        'history':dates,
            'total_amount': N
    }
        return dict_ret

if __name__ == '__main__':
    RN = ReadNews(update_date=20210330,page = 1)
    ret = RN.read()

