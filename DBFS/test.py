from  sqlalchemy import create_engine
import pandas as pd
from glob import glob
import re
from GetStockList import get_stock_list
from StretchThsFS import get_fs
import redis

def make_up():
    setting = dict(host='192.168.2.105',
                   port=3306,
                   user='rdt',
                   password='rdt',
                   database='modgo_quant',
                   charset='utf8mb4')

    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
                           encoding='utf-8')
    data = pd.read_sql_query('select stock_code from incomestatement_orig ', con=engine)
    print(len(data['stock_code'].unique()))
    redis_config = dict(host='localhost',
                        port=6379,
                        db='1',
                        password='zhoutianlian')

    stock_list = get_stock_list()
    codes = pd.read_sql_query('select stock_code from cashflowstatement_orig ', con=engine)
    codes = codes['stock_code'].unique()
    print(len(codes))
    for i, d in stock_list.iterrows():
        code = d['stock_code']
        if code not in codes:
            print(code)
            with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
                if not con.exists(code):
                    num = int(re.findall(r'.*?(\d{6})', code)[0])
                    print(num)
                    try:
                        cf = get_fs(num, kind='cash')
                        cf.to_csv('cash/%s.csv' % code)
                        print('success')
                        with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
                            con.set(code, '1', ex=60 * 24)
                    except:
                        pass
                else:
                    pass


if __name__ =='__main__':
    make_up()

