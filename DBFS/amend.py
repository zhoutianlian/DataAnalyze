
# from config import *
import pandas as pd
from database_fs.CONFIG.Config import config
from sqlalchemy import create_engine
from pymysql import connect
setting = dict(host=config['DEFAULT']['sql_host'] + ':%s' % (config['DEFAULT']['sql_port']),
                   user=config['DEFAULT']['sql_user'],
                   password=config['DEFAULT']['sql_password'],
                   database='modgo_quant')

    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
                           encoding='utf-8')
    #
    #
    # df = pd.read_sql(subset, engine)

    sql = 'select stock_code,report_date,int_cost,inc_cost from incomestatement_orig'
    df = pd.read_sql_query(sql, engine,)
    def func(x):
        new = 0

        try:
            if x['int_cost'] >0:
                new = x['int_cost']
            if x['in_cost'] >0:
                new = x['inc_cost']

        except:
            pass
        x['int_cost'] = new

        return x

    df = df.apply(func,axis = 1)
    del df['inc_cost']
    print(df.head(10))

    setting_sql = dict(host=config['DEFAULT']['sql_host'],
                   port=int(config['DEFAULT']['sql_port']),
                   user=config['DEFAULT']['sql_user'],
                   password=config['DEFAULT']['sql_password'],
                   database='modgo_quant',
                   charset='utf8mb4')

    with connect(**setting_sql) as con:
        cursor = con.cursor()
        try:
            keys = ','.join(df.columns)
        except:
            print(df.columns)
            exit()
        values = df.to_records(index=False)
        values = [repr(row) for row in values]
        values = ','.join(values)
        sql = 'replace into %s (%s) values %s' % ('incomestatement_orig', keys, values)
        try:
            res = cursor.execute(sql)
            print('success')
            con.commit()
        except Exception as e:
            print(e)