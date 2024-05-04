"""
向sql插入数据

"""
from pymysql import connect
import pandas as pd
from CONFIG.Config import config

#显示所有列
pd.set_option('display.max_columns', None)
#显示所有行
pd.set_option('display.max_rows', None)

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

def add_new_fs(table: str, data: pd.DataFrame):
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


if __name__ == '__main__':
	from glob import glob
	import re

	all_fs = glob('struct/*.csv')
	for f in all_fs:
		code = re.findall(r'(sh|sz|oc)(\d{6})', f)[0]
		code = ''.join(code)
		print(code, end=':')
		try:
			df = pd.read_csv(f, index_col=0)
			df['stock_code'] = code
			df['report_date'] = df['report_date'].apply(pd.Timestamp)
			df = df[['stock_code', 'report_date', 'category', 'sales', 'cost', 'profit']]
			add_new_fs('income_component', df)
		except Exception as e:
			print('Error on %s: %s' % (code, e))

