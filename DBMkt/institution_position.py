"""
机构持仓每日变化
"""

import calendar

import requests
import pandas as pd
import numpy as np
import akshare as ak
import time
from time import sleep
import tushare as ts
import datetime

from database_mkt.utils import to_sql_replace


def update_institution_position(if_new):
    today = datetime.datetime.now()
    target = []
    if if_new == 0:
        for n in range(2020, today.year + 1):
            for i in ['1', '2', '3', '4']:
                target.append(str(n) + i)
    else:

        q = int(today.month / 4) + 1
        target.append(str(today.year) + str(q))

    for q in target:

        df_institution = ak.stock_institute_hold(quarter=q)
        for c in list(df_institution['证券代码']):
            if c[0] == '6':
                code = 'sh' + c
            else:
                code = 'sz' + c
            try:
            df = ak.stock_institute_hold_detail(stock=c, quarter=q)
            if df is not None:
                df = df[['持股机构类型', '持股机构代码', '持股机构全称', '最新持股数', '最新持股比例', '最新占流通股比例']]
                df.columns = ['instituition_type', 'institution_code', 'institution_name',
                              'latest_position', 'latest_share_proportion',
                              'latest_circulation_stock_proportion']
            else:
                df = pd.DataFrame(
                    columns=['stock_code', 'update_date', 'instituition_type', 'institution_code', 'institution_name',
                             'latest_position', 'latest_share_proportion',
                             'latest_circulation_stock_proportion', 'n_institution', 'total_position',
                             'total_share_proportion'])

            df['stock_code'] = code
            y = int(q[:4])
            m = int(q[4:])
            firstDayWeekDay, monthRange = calendar.monthrange(y, m)
            t = '%d-%02d-%d' % (y, m, monthRange)
            t = pd.Timestamp(t)
            df['update_date'] = t
            df['n_institution'] = df_institution.query('证券代码 == @c')['机构数']
            df['total_position'] = df_institution.query('证券代码 == @c')['持股比例']
            df['total_share_proportion'] = df_institution.query('证券代码 == @c')['占流通股比例']
            to_sql_replace(table='institution_positio ', data=df)
            print(df)
            # df_total = df_total.append(df)



if __name__ == "__main__":
    update_institution_position(if_new = 0)
