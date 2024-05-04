"""
报表转化为单季度

"""
import datetime

import pandas as pd
from glob import glob
import re
from sqlalchemy import create_engine
from CONFIG.Config import config
from to_mysql import add_new_fs
import redis
from threading import Thread

from database_fs.CONFIG.send_Dingding import send_to_dingding


def sub_fs(df):

    cols = [i for i in list(df.columns) if i not in ['category','stock_code','begn_cash_bal', 'end_cash_bal']]
    try:
        df[['begn_cash_bal', 'end_cash_bal']] = df[['begn_cash_bal', 'end_cash_bal']].bfill()
    except:
        pass
    df[cols] = df[cols].fillna(0)
    if len(df) > 1:
        df[cols] = df[cols].sub(df[cols].shift(1).fillna(0))
        month = df[['month']]

        df[cols] = df[cols] / month.values
    del df['month']

    return df

def sub_fs_1(df):

    cols = [i for i in list(df.columns) if i not in ['stock_code','cash_eqvlt_begn', 'cash_eqvlt_end','cash_bal_begn', 'cash_bal_end']]
    df[['cash_eqvlt_begn', 'cash_eqvlt_end','cash_bal_begn', 'cash_bal_end']]  =\
        df[['cash_eqvlt_begn', 'cash_eqvlt_end','cash_bal_begn', 'cash_bal_end']].bfill()
    if len(df) > 1:
        month = df[['month']]
        df[cols] = df[cols] / month.values
        df[cols] = df[cols].bfill()
    del df['month']
    return df

def get_q_ends(date):
    year, month = date.year, date.month
    if month in (1, 2, 3):
        return pd.Timestamp(year, 3, 31)
    elif month in (4, 5, 6):
        return pd.Timestamp(year, 6, 30)
    elif month in (7, 8, 9):
        return pd.Timestamp(year, 9, 30)
    elif month in (10, 11, 12):
        return pd.Timestamp(year, 12, 31)
    else:
        return None, None

def run(subset):
    setting = dict(host=config['DEFAULT']['sql_host']+':%s'%(config['DEFAULT']['sql_port']),
                   user=config['DEFAULT']['sql_user'],
                   password=config['DEFAULT']['sql_password'],
                   database='modgo_quant')

    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
                           encoding='utf-8')
    try:
        df = pd.read_sql(subset+'_orig', engine)
    except:
        df = pd.read_sql(subset, engine)
    df = df.reset_index()

    del df['id']
    del df['index']
    dict_type = {
        'cashflowstatement': 'cash',
        'incomestatement': 'ins',
        'income_component': 'insc'
    }
    redis_config = dict(host=config['DEFAULT']['redis_host'],
                        port=config['DEFAULT']['redis_port'],
                        db=config['DEFAULT']['db_q'],
                        password=config['DEFAULT']['redis_password'])
    print(df['stock_code'].unique())
    for code in df['stock_code'].unique():
        with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
            if not con.exists(code+subset):
                df_i = df.query('stock_code ==@code')
                if not df_i.empty:
                    try:
                        df_i = get_q_fs(df_i, report_type = dict_type[subset])
                        print(code,'success')
                    except:
                        print(code,'error')
                else:
                    print(code,'empty')

                if not df_i.empty:
                    try:
                        add_new_fs(subset+'_q', df_i)
                    except:
                        df_i.to_sql(name=subset+'_q', con=engine ,if_exists='append',index=False)
                        print('append')
                    print(code, 'save')
                    with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
                        con.set(code+subset, '1', ex=24*7*7)


def get_q_fs(fs, report_type):

    if report_type == 'debt':
        return fs
    if report_type == 'cash':
        cols_i = list(fs.loc[:,'net_prft':].columns)
        cols_d = [i for i in list(fs.columns) if i not in cols_i ]
        cols_i.extend(['c_time','report_date','stock_code'])
        fs_d = fs[cols_d]
        fs_i = fs[cols_i]
        direct = get_q_fs(fs_d, 'dir')
        indir = get_q_fs(fs_i, 'ind')
        return pd.merge(direct,indir,on = ['report_date','stock_code'])
    fs = fs.reset_index().sort_values(by=['stock_code','report_date', 'c_time'])
    fs = fs.drop_duplicates(subset=['stock_code','report_date'], keep='last')
    fs = fs.set_index('report_date')
    fs = fs.drop(columns=['c_time']).fillna(0)
    index = fs.index.to_series().apply(lambda x: pd.Timestamp(x))
    fs[['year', 'month']] = index.apply(lambda x: pd.Series([x.year, x.month]))
    fs = fs.sort_values(by=['year', 'month'])
    if report_type == 'ind':
        fs =fs.groupby(['stock_code','year'], as_index=False).apply(sub_fs_1)
    else:
        fs= fs.groupby(['stock_code','year'], as_index=False).apply(sub_fs)

    def func(fs):
        fs.index = fs.index.to_series().apply(pd.Timestamp)
        fs = fs.drop(columns=['year'])
        early = min(fs.index)
        early_first = pd.Timestamp(early.year, 1, 31)
        index = set(fs.index)
        index.add(early_first)
        late_last = get_q_ends(max(index))
        index.add(late_last)

        index = sorted(list(index))
        fs = fs.reindex(index=index).bfill().ffill()
        fs = fs.resample('M').bfill()
        fs = fs.resample('Q').sum()
        return fs
    fs = fs.groupby('stock_code').apply(func)

    fs = fs.reset_index()
    if 'index' in fs.columns:
        del fs['index']
    if 'c_time' in fs.columns:
        del fs['c_time']

    return fs



if __name__ == '__main__':
    # run('cashflowstatement')
    today = datetime.datetime.today()
    result = ''
    threads = list()
    for subset in ['incomestatement', 'cashflowstatement', 'income_component']:
        threads.append(Thread(target=run, args=(subset,)))
    for thread in threads:
        thread.start()
    result = '%s---- 上市公司财报单季度转换：成功' % (str(today))
    send_to_dingding(result)

