import re
import pandas as pd
from CONFIG.Config import config

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


class GetFSData():

	def __init__(self):

		self.fs = self.get_data()

		self.list_em = ['stock_code', 'trade_date', 'report_date',
						'MVA', 'MVB', 'PE0A', 'PE0B', 'e_ni_parent', 'PE1A', 'PE1B', 'PCF0B', 'PEBT0B',
						'PB0A', 'PB0B', 'PTB0A', 'PTB0B', 'PFCFE0B', 'PS0B', 'PEG0A', 'PE0G0B', 'PE0G1B']
		self.list_im = ['stock_code', 'trade_date', 'report_date',
						'BVICA', 'BVICB', 'MVICA', 'MVICB', 'MVICS0A', 'MVICS0B', 'MVICS1A', 'MVICS1B',
						'MVICDE0A', 'MVICDE0B', 'MVICEBITDA0A', 'MVICEBITDA0B', 'MVICEBIT0A', 'MVICEBIT0B',
						'MVICFCFF0A', 'MVICBVIC0B', 'MVICFCFF0A']
		self.list_p = ['stock_code', 'report_date', 'ROE1', 'ROE2', 'ROIC1', 'ROIC2', ]

	def q2ttm(self, df):

		def func(x):
			x = x.sort_values('report_date', ascending=True)
			columns = x.columns.tolist()
			for i in ['_id', 'id', 'c_time', 'report_date', 'report_name', 'stock_code']:
				if i in columns:
					columns.remove(i)
			for c in columns:
				try:
					x[c] = x[c].rolling(window=4, min_periods=1).mean()
				except:
					print(c)
			if 'stock_code' in x.columns:
				del x['stock_code']
			return x

		df = df.groupby('stock_code').apply(func)
		df = df.reset_index()
		if 'level_1' in df.columns:
			del df['level_1']
		if '_id' in df.columns:
			del df['_id']
		df = df.fillna(0)
		return df

	def q2avg(self, df):

		def func(x):
			x = x.sort_values('report_date', ascending=True)
			columns = x.columns.tolist()
			for i in ['_id', 'id', 'c_time', 'report_date', 'report_name', 'stock_code']:
				if i in columns:
					columns.remove(i)
			for c in columns:
				try:
					x[c] = x[c].rolling(window=2, min_periods=1).mean()
				except:
					print(c)
			if 'stock_code' in x.columns:
				del x['stock_code']
			return x

		df = df.groupby('stock_code').apply(func)
		df = df.reset_index()
		if 'level_1' in df.columns:
			del df['level_1']
		if '_id' in df.columns:
			del df['_id']
		df = df.fillna(0)
		return df

	def get_data(self):
		def read_sql(subset):
			setting = dict(host=config['DEFAULT']['sql_host'] + ':%s' % (config['DEFAULT']['sql_port']),
						   user=config['DEFAULT']['sql_user'],
						   password=config['DEFAULT']['sql_password'],
						   database='modgo_quant')

			engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
								   encoding='utf-8')
			#
			#
			# df = pd.read_sql(subset, engine)

			sql = 'select * from %(subset)s' % {'subset': subset} + ' where stock_code in %(in)s'
			df = pd.read_sql_query(sql, engine,
								   params={'in': ['sh601633', 'sz000625', 'sz000002', 'sz000157', 'sz002258']})

			def func(x):
				try:
					x = float(x)
				except:
					pass
				return x

			df = df.applymap(func)
			return df

		self.ins_ttm = self.q2ttm(read_sql(subset='incomestatement_q'))
		self.ins_q = read_sql(subset='incomestatement_q')
		self.ins_avg = self.q2avg(read_sql(subset='incomestatement_q'))
		self.bs_ttm = self.q2ttm(read_sql(subset='balancesheet_orig'))
		self.bs_q = read_sql(subset='balancesheet_orig')
		self.cfs_ttm = self.q2ttm(read_sql(subset='cashflowstatement_q'))
		self.cfs_q = read_sql(subset='cashflowstatement_q')
		self.mkt = read_sql(subset='a_share_trade_data')
		self.e_rev = read_sql(subset='revenue_expectation')
		self.e_ni = read_sql(subset='ni_expectation')
		e = pd.merge(self.e_ni, self.e_rev, on=['stock_code', 'announcement_date'], how='outer')

		def func(x):
			s = x['stock_code'].values[0]
			d = x['announcement_date'].values[0]
			x['NI_next'] = np.where(x['NI_next'] > 0, x['NI_next'], x['NI_current'] * 1.05)
			x['rev_next'] = np.where(x['rev_next'] > 0, x['rev_next'], x['rev_current'] * 1.05)

			ret = pd.DataFrame({
				'e_ni': [x['NI_current'].mean()],
				'e_ni_1': [x['NI_next'].mean()],
				'e_rev': [x['rev_current'].mean()],
				'e_rev_1': [x['rev_next'].mean()]})
			try:
				ret['ni'] = self.ins_ttm.query('stock_code == @s & report_date == @d')['net_profit'].values[0]
				ret['rev'] = self.ins_ttm.query('stock_code == @s & report_date == @d')['operating_income'].values[0]
				ret['e_ni_growth'] = [(ret['e_ni'] / ret['ni'] - 1) + (ret['e_ni_1'] / ret['e_ni'] - 1)] / 2
				ret['e_rev_growth'] = [(ret['e_rev'] / ret['rev'] - 1) + (ret['e_rev_1'] / ret['e_rev'] - 1)] / 2
			except:
				ret['e_ni_growth'] = (ret['e_ni_1'] / ret['e_ni'] - 1)
				ret['e_rev_growth'] = (ret['e_rev_1'] / ret['e_rev'] - 1)

			return ret

		def func_date(x):
			m = x.month
			y = x.year
			if m > 10:
				d = y * 10000 + 630
			elif m > 7:
				d = y * 10000 + 930
			elif m > 4:
				d = y * 10000 + 331
			else:
				d = (y - 1) * 10000 + 1231
			return pd.to_datetime(str(d))

		e['announcement_date'] = e['announcement_date'].apply(func_date)
		e = e.groupby(['stock_code', 'announcement_date']).apply(func)

		e = e.reset_index()
		e.rename(columns={'announcement_date': 'report_date'}, inplace=True)
		for i in list(e.columns):
			if ('level' in i) or ('Unname' in i):
				del e[i]
		print(e.columns)
		self.e = e

		print(e.iloc[:50, :])
		fs = {
			'ins_ttm': self.ins_ttm,
			'ins_q': self.ins_q,
			'ins_avg': self.ins_avg,
			'bs_ttm': self.bs_ttm,
			'bs_q': self.bs_q,
			'cfs_ttm': self.cfs_ttm,
			'cfs_q': self.cfs_q,
			'mkt': self.mkt,
			'e': self.e,
		}
		return fs

	def merge(self, target, if_e, if_mkt):

		list_total = []
		df = pd.DataFrame()
		if target:
			for t in target:
				df = self.fs[t].set_index(['stock_code', 'report_date'])
				print(df)
				list_total.append(df)

			df = pd.concat(list_total)
			df = df.reset_index()

		if if_e:
			if not df.empty:
				df = pd.merge(df, self.fs['e'], on=['report_date', 'stock_code'])

		if if_mkt:
			if not df.empty:
				def func(x):
					x['report_date'] = x['report_date'].apply(lambda x: pd.to_datetime(x))
					x.drop_duplicates(subset=['report_date'], keep='first', inplace=True)
					x['trade_date'] = x['report_date']
					x = x.set_index('trade_date')
					x = x.resample('D').ffill()
					x = x.reset_index()
					if 'stock_code' in x.columns.tolist():
						del x['stock_code']
					return x

				df = df.groupby('stock_code').apply(func)
				df = df.reset_index()
				for i in list(df.columns):
					if ('level' in i) or ('Unname' in i):
						del df[i]
				df = pd.merge(df, self.mkt, on=['trade_date', 'stock_code'])
		df = df.fillna(0)


if __name__ == '__main__':
	print(uniform_name('投资、师傅的现金'))

