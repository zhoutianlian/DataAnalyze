"""
向sql插入数据
"""
import datetime
import pickle

from ValuationRecord.SaveEnterprise import save
import pandas as pd

import pymysql
from CONFIG.config import config

from update_data import Get_Info
import csv
import redis
class SQL_Info():
    def __init__(self):
        self.out_host = config['DEFAULT']['out_mongo_host']
        self.out_username = config['DEFAULT']['out_mongo_user']
        self.out_password = config['DEFAULT']['out_mongo_password']
        self.out_port = int(config['DEFAULT']['out_mongo_port'])

        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = int(config['DEFAULT']['mongo_port'])
        self.path = './data.csv'


    def get_data(self):
        gi = Get_Info()
        now = datetime.datetime.now()
        now = now.year * 10000 + now.month * 100 + now.day

        redis_config = dict(host=config['DEFAULT']['redis_host'],
                            port=config['DEFAULT']['redis_port'],
                            db=config['DEFAULT']['db_fs'],
                            password=config['DEFAULT']['redis_password'])
        with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
            if con.exists('shsz'+str(now)):
                print('exist')
                shsz = con.get('shsz'+str(now))
                shsz = pickle.loads(shsz)
            else:
                shsz = gi.get_shsz()
                shsz = pickle.dumps(shsz)
                con.set('shsz'+str(now), shsz, ex=60*60*24)

            if con.exists('otc'+str(now)):
                print('exist')
                otc = con.get('otc'+str(now))
                otc = pickle.loads(otc)
            else:
                otc = gi.get_OTC()
                otc = pickle.dumps(otc)
                con.set('otc'+str(now), otc, ex=60 * 60 * 24)



        df = pd.concat([shsz, otc])
        gics = pd.read_excel('stock_gics.xlsx')

        df = pd.merge(df, gics[['stock_name', 'gics4']], on='stock_name', how='left')
        df['gics4'] = df['gics4'].fillna(0)
        df = df.rename(columns={'gics4': 'gics_code4'})
        def f(x):
            try:
                x = int(x)
            except:
                x = 0
            return x
        df['gics_code4']=df['gics_code4'].apply(f)

        def func(x):

            try:
                x = int(int(x) / 100)
            except:
                x = 0
            return x

        df['gics_code'] = df['gics_code4'].apply(func)
        print(df[['gics_code', 'gics_code4']])
        def func(x):

            with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config,decode_responses=True)) as con:
                if con.exists('eid' + x['enterprise_name']):
                    eid = con.get('eid' + x['enterprise_name'])
                else:
                    eid = save(name = x['enterprise_name'])
                    if eid is not None:
                        if eid>0:
                            con.set('eid' + x['enterprise_name'], eid, ex=60 * 60 * 24)
                    else:
                        eid = 0
            x['enterprise_id'] = int(eid)
            print(x['enterprise_name'],eid)
            return x

        df= df.apply(func,axis = 1)
        df = df[['stock_code', 'total_number_of_shares', 'market_value',
                            'stock_name', 'listed_market', 'wel_delete',
                 'enterprise_name', 'enterprise_id','update_time','gics_code','gics_code4']]

        return df

    def create_csv(self):
        try:
            pd.read_csv(self.path)
        except:
            with open(self.path, 'w') as f:
                csv_write = csv.writer(f)
                csv_head = ['stock_code', 'total_number_of_shares', 'market_value',
                            'stock_name', 'listed_market', 'wel_delete',
                 'enterprise_name', 'enterprise_id','update_time','gics_code','gics_code4']
                csv_write.writerow(csv_head)

    def write_csv(self):
        self.data = self.get_data()
        self.create_csv()
        with open(self.path, 'a+') as f:
            csv_write = csv.writer(f)
            for i,r in self.data.iterrows():
                csv_write.writerow(r)

    def update_data(self,):

        df = pd.read_csv(self.path,index_col=None,encoding='gbk')

        def func(x):
            x = x.sort_values(by = 'update_time')
            if 'stock_code' in list(x.columns):
                del x['stock_code']
            x=x.iloc[-1]
            return x
        df = df.groupby('stock_code').apply(func)
        df = df.reset_index()
        df = df[['stock_code', 'total_number_of_shares', 'market_value',
                 'stock_name', 'listed_market', 'wel_delete',
                 'enterprise_name', 'enterprise_id','update_time','gics_code','gics_code4']]

        # print(df.head(10))
        db = pymysql.Connection(
            host = config['DEFAULT']['mysql_host'],
            port = int(config['DEFAULT']['mysql_port']),
            user=config['DEFAULT']['mysql_user'],
            password=config['DEFAULT']['mysql_password'],
            database = 'rdt_fintech'
        )

        df = df.fillna(0)

        # replace
        cursor = db.cursor()
        try:
            keys = ','.join(df.columns)
        except:
            print(df.columns)
            print(df['stock_code'].unique())
            exit()
        values = df.to_records(index=False)
        values = [repr(row) for row in values]
        values = ','.join(values)
        sql = 'replace into %s (%s) values %s' % ('t_company_industry_code', keys, values)
        try:
            res = cursor.execute(sql)
            print('success')
            db.commit()
        except Exception as e:
            print(e)

        # update/insert
        #
        # for i,r in df.iterrows():
        #     cursor = db.cursor()
        #     sql_check = "SELECT 1 FROM t_company_listing_info WHERE stock_name = %s"
        #     if_exist = cursor.execute(sql_check,(r['stock_name']))
        #     if if_exist:
        #         sql = """UPDATE t_company_listing_info set market_value = %s,
        #               total_number_of_shares = %s,stock_name = %s ,update_time = %s WHERE stock_code = %s"""
        #         values = (r['market_value'],r['total_number_of_shares'],r['stock_name'],r['update_time'],r['stock_code'])
        #         print(r['stock_name'], 'update')
        #
        #     else:
        #         name = r['enterprise_name']
        #         e_id = save(name=name)
        #         data = r.to_dict()
        #         data['enterprise_id'] = e_id
        #         values=tuple(list(data.values()))
        #         sql="INSERT INTO t_company_listing_info (stock_code, total_number_of_shares, market_value, stock_name, listed_market, wel_delete, enterprise_name, update_time, enterprise_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        #         print(name,'new')
        #     try:
        #         if cursor.execute(sql,values):
        #             print("successful")
        #             db.commit()
        #     except Exception as e:
        #         print(e)
        #         cursor = db.cursor()
        #         cursor.execute(sql, values)
        #         print("Failed")
        #         db.rollback()


if __name__ == '__main__':


    si = SQL_Info()
    # si.write_csv()
    si.update_data()
