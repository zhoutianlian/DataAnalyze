from pymysql import connect
import pandas as pd
from CONFIG.Config import config
import requests
import numpy as np
# from config import *
import pandas as pd
import time
import datetime
from datetime import timedelta
from database_fs.CONFIG.Config import config
from database_fs.to_mysql import add_new_fs
from sqlalchemy import create_engine
from pymysql import connect



setting = dict(host=config['DEFAULT']['sql_host'],
			   port=int(config['DEFAULT']['sql_port']),
                user=config['DEFAULT']['sql_user'],
                   password=config['DEFAULT']['sql_password'],
                   database='modgo_quant',
			   charset='utf8mb4')

def turn_sql_string(x):
	if isinstance(x, (float, int)):
		return format(x, '.2f')
	elif isinstance(x, str):
		return x
	else:
		return str(x)

def to_sql_replace(table: str, data: pd.DataFrame):
	data = data.fillna(0).applymap(turn_sql_string)
	for c in data.columns:
		if 'Unnamed' in c:
			del data[c]
	with connect(**setting) as con:
		cursor = con.cursor()
		try:
			keys = ','.join(data.columns)
		except:
			print(data.columns)
			print(data['stock_code'].unique())
			exit()
		values = data.to_records(index=False)
		values = [repr(row) for row in values]
		values = ','.join(values)
		sql = 'replace into %s (%s) values %s' % (table, keys, values)
		try:
			res = cursor.execute(sql)
			print('success')
			con.commit()
		except Exception as e:
			print(e)

		# print(res)


