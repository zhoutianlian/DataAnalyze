"""
依据每日更新的融资事件，更新t_financing_info中的融资信息
需要手动执行，没有接口
"""
# todo 改成自动脚本
import re
import datetime
import pandas as pd
import numpy as np
import pymysql
from pymongo import MongoClient
import pymongo
import time
from ValuationRecord.SaveEnterprise import save
from config.Config import config
import os
import sys
from config.send_Dingding import send_to_dingding
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

class FinancingSql():

    def __init__(self):
        self.dict_ret = self.search_sql()
        self.e_id_time = self.dict_ret['e_id_time']
        self.e_id_name = self.dict_ret['e_id_name']
        self.e_round = self.dict_ret['e_round']

        self.df_raw = self.get_financing_data()
        self.df_new = self.generate_sql_financing_record()

    def read_mongo(self, db_name, collection_name, conditions={}, query=[], sort_item='', n=0):

        mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                         (config['DEFAULT']['mongo_user'], config['DEFAULT']['mongo_password'],
                          config['DEFAULT']['mongo_host'], config['DEFAULT']['mongo_port'], db_name)
        conn = MongoClient(mongo_uri)
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

    def search_sql(self):
        db = pymysql.Connection(
            host=config['DEFAULT']['sql_host'],
            port=int(config['DEFAULT']['sql_port']),
            user=config['DEFAULT']['sql_user'],
            password=config['DEFAULT']['sql_password'],
            database='rdt_fintech'
        )

        cursor = db.cursor()

        sql_id_time = "select enterprise_id,m_time,c_time from t_financing_info"
        sql_id_name = "select id, enterprise_name from t_enterprise_info"

        sql_round = "select round_code, round_type from t_financing_round"
        cursor.execute(sql_id_time)
        e_id_time = pd.DataFrame(cursor.fetchall())
        e_id_time.columns = ['enterprise_id', 'm_time', 'c_time']
        cursor.execute(sql_id_name)
        e_id_name = pd.DataFrame(cursor.fetchall())
        e_id_name.columns = ['enterprise_id', 'enterprise_name']
        e_id_name = e_id_name.drop_duplicates()
        e_id_time = pd.merge(e_id_time, e_id_name, on='enterprise_id', how='left')
        cursor.execute(sql_round)
        e_round = pd.DataFrame(cursor.fetchall())
        e_round.columns = ['round_code', 'turn']

        def func(x):
            x = x.year * 10000 + x.month * 100 + x.day
            return x

        e_id_time['m_time'] = e_id_time['m_time'].apply(func)

        def func_1(x):
            x = x.sort_values(by='m_time')
            x = x.iloc[-1, :]
            x = x[['m_time', 'enterprise_id', 'c_time']]
            return x

        e_id_time = e_id_time.groupby('enterprise_name').apply(func_1)
        e_id_time = e_id_time.reset_index()

        dict_ret = {
            'e_id_name': e_id_name,
            'e_round': e_round,
            'e_id_time': e_id_time
        }
        return dict_ret

    def get_n_day_before(self, n=0):
        t = (datetime.datetime.now() - datetime.timedelta(days=n))
        start = t.year * 10000 + t.month * 100 + t.day
        return start

    def get_financing_data(self):

        df = self.read_mongo(db_name='spider_origin', collection_name='financing_event',
                        conditions={}, query=[], sort_item='', n=0)
        df = df[['last_financing_date', 'company_name', 'data']]

        def func_2(x):
            if_show = 0
            if (x['company_name'] in list(self.e_id_time['enterprise_name'])):
                if (self.e_id_time[self.e_id_time['enterprise_name'] == x['company_name']]['m_time'].values[0] < x[
                    'last_financing_date']):
                    if_show = 1
            else:
                if_show = 1
            x['if_show'] = if_show
            return x

        df = df.apply(func_2, axis=1)
        df = df[df['if_show'] == 1]

        return df

    def generate_sql_financing_record(self):

        df = self.df_raw
        ex_usd = self.read_mongo(db_name='modgo_quant', collection_name='fxTradeData',
                                 conditions={'$and': [{'trade_date': {'$gt': self.get_n_day_before(n=90)}},
                                                      {'currency_pair': 'USD/CNY'}]}, query=[], sort_item='',
                                 n=0)
        ex_usd = ex_usd['fx_rate'].mean()
        new_df = pd.DataFrame(columns=['enterprise_id','company_name', 'financing_date', 'financing_amount', 'turn', 'vc_name'])
        df = df.iloc[:1000]
        for n, x in df.iterrows():
            df_ret = pd.DataFrame(columns=['financing_date', 'financing_amount', 'turn', 'vc_name','financing_notes'])
            for i in range(len(x['data'])):
                data = x['data'][i]
                money = data['money']
                money_orig = data['money']
                date = data['publish_time']
                today = time.mktime(datetime.datetime.now().date().timetuple())
                today = datetime.datetime.fromtimestamp(today)

                if '至今' in date:
                    date = today
                else:
                    date = pd.to_datetime(date)
                #             date = date.year*10000 + date.month*100 + date.day
                amount = 0
                if len(re.findall(r"\d+\.?\d*", money)) == 0:
                    amount = 5 * ('数' in money)
                else:
                    amount = float(re.findall(r"\d+\.?\d*", money)[0])
                if amount:
                    unit = (('美元' in money) * ex_usd +
                            (not ('美元' in money)) * 1) * (
                                   ('千' in money) * 10000000 + ('百' in money) * 1000000 +
                                   ('亿' in money) * 100000000 + 10000)
                    money = amount * unit
                    df_i = pd.DataFrame({'financing_date': [date],
                                         'financing_amount': [money], 'turn': [data['turn']],
                                         'vc_name': [data['vc_name']],'financing_notes':[money_orig]})
                    df_ret = df_ret.append(df_i, ignore_index=True)
            if not df_ret.empty:
                df_ret = df_ret.sort_values(by='financing_date')
                df_ret['financing_amount'] = df_ret['financing_amount'].astype('float')
                df_ret = df_ret.drop_duplicates(subset='financing_date', keep='last')
                df_ret['company_name'] = x['company_name']

                new_df = new_df.append(df_ret, ignore_index=True)

        new_df = new_df[['company_name', 'financing_amount', 'financing_date',
                         'turn', 'vc_name','financing_notes']]
        new_df = pd.merge(new_df, self.e_round, on=['turn'], how='left')
        new_df['round_code'] = new_df['round_code'].fillna('0')
        list_round = list(self.e_round['round_code'])
        list_round.sort()

        def func(x):
            x['financing_date'] = x['financing_date'].bfill()
            x['financing_date'] = x['financing_date'].ffill()
            x = x.sort_values(by='financing_date')
            fd = x[x['round_code'] == x['round_code'].max()]['financing_date'].values[0]

            r = x['round_code'].max()

            def f(y):

                if (y['round_code'] == '0'):
                    print(y['financing_date'])
                    print(fd)
                    if (y['financing_date'] >= fd):
                        y['round_code'] = list_round[min(list_round.index(r) + 1, len(list_round) - 1)]
                    else:
                        y['round_code'] = list_round[max(list_round.index(r) - 1, 1)]
                return y

            x = x.apply(f, axis=1)

            try:
                eid = save(x['company_name'].values[0])
                if eid != 0:
                    x['enterprise_id'] = eid
                    x = x[['enterprise_id', 'financing_date', 'financing_amount', 'round_code', 'vc_name',
                           'financing_notes']]
                    return x
            except:
                pass


        new_df = new_df.groupby('company_name').apply(func)
        new_df = new_df.reset_index()
        new_df = new_df.dropna()
        new_df = new_df[['enterprise_id','company_name', 'financing_date', 'financing_amount', 'round_code', 'vc_name','financing_notes']]
        return new_df

    def insert_financing_record_sql(self, data, c_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")):

        db = pymysql.Connection(
            host=config['DEFAULT']['sql_host'],
            port=int(config['DEFAULT']['sql_port']),
            user=config['DEFAULT']['sql_user'],
            password=config['DEFAULT']['sql_password'],
            database='rdt_fintech'
        )

        cursor = db.cursor()

        sql_insert = """
        INSERT INTO t_financing_info (c_time,enterprise_id,financing_accomplish_amount,financing_accomplishdate,
            financing_announcementdate,financing_convertionprice,financing_investor, financing_issueprice,
            financing_notes,financing_parprice,financing_parrate,financing_plan_amount,
            financing_status,financing_type,m_time,whether_del,
            financing_purpose,financing_round,financing_share,financing_value) 
        VALUES (%s, %s, %s, %s, 
        %s, %s, %s, %s, 
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s)
        """
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        values = (c_time, str(data['enterprise_id']), str(data['financing_amount']), str(pd.to_datetime(data['financing_date'])),
                  str(pd.to_datetime(data['financing_date'])), '0', data['vc_name'], '0',
                  data['financing_notes'], '0', '0', '0',
                  '3', '11', now, '0',
                  '0', str(data['round_code']), '0', '0')
        print(values)
        try:
            if cursor.execute(sql_insert, values):
                print("successful")
                db.commit()
        except Exception as e:
            print(e)
            cursor = db.cursor()
            cursor.execute(sql_insert, values)
            print("Failed")
            db.rollback()

    def run(self):
        for i, r in self.df_new.iterrows():
            if (r['company_name'] in list(self.e_id_time['enterprise_name'])):
                c_time = self.e_id_time['c_time']
                if r['financing_date'] > self.e_id_time['m_time']:
                    self.insert_financing_record_sql(data=r, c_time=c_time)
            else:
                if r['enterprise_id']>0:
                    self.insert_financing_record_sql(data=r)

if __name__ == '__main__':
    f = FinancingSql()
    f.run()