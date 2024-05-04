"""
在财报披露月份，批量更新四张报表

"""
import pandas as pd
from database_fs.GetStockList import get_stock_list
from database_fs.StretchThsFS import run as run_stretcher
from database_fs.utils import uniform_name
from configparser import ConfigParser
from glob import glob
import re
from database_fs.to_mysql import add_new_fs
from datetime import datetime
import os
from threading import Thread
import redis
from database_fs.GetQuarterlyFS import get_q_fs

from database_fs.CONFIG.send_Dingding import send_to_dingding


def sort_and_upload(report_type, config_dict, upload=False):
	all_fs = pd.DataFrame()
	log_path = 'duplicate_account/%s' % report_type
	table = {'debt': 'balancesheet',
	         'benefit': 'incomestatement',
	         'cash': 'cashflowstatement'}[report_type]

	for f in glob('%s/*.csv' % report_type):
		code = ''.join(re.findall(r'(sh|sz|oc)(\d{6})', f)[0])
		fs = pd.read_csv(f)
		width = fs.shape[1]
		orig_acc = fs.columns.to_series(index=range(width))
		uniform_fs = list()
		for c, ser in fs.iteritems():
			cn_name, scale = uniform_name(c)
			uniform_ser = ser.rename(cn_name)
			if scale is None:
				pass
			elif scale > 1:
				uniform_ser *= scale
			uniform_fs.append(uniform_ser)

		fs = pd.concat(uniform_fs, axis=1)
		cn_acc = fs.columns.to_series(index=range(width))
		fs.columns = fs.columns.map(config_dict)
		en_acc = fs.columns.to_series(index=range(width))
		# orig_acc = fs.columns.to_series(index=range(fs.shape[1]))
		# cn_acc = orig_acc.apply(lambda x: uniform_name(x)[0])
		# en_acc = cn_acc.map(config_dict)
		if len(en_acc) != len(set(en_acc)) or '' in en_acc:
			info = pd.concat([orig_acc, cn_acc, en_acc], axis=1)
			info.columns = ['orig', 'cn', 'en']
			info.to_excel('%s/%s.xlsx' % (log_path, code))
			print('%s find duplicated accounts! The info has been logged!' % code)
		else:
			fs.columns= en_acc
			fs['stock_code'] = code
			if upload:
				add_new_fs(table + '_orig', fs)
				# q_fs = get_q_fs(fs, report_type)
				# add_new_fs(table + '_q', fs)
				os.remove(f)
				print('%s %s file is removed!' % (code, report_type))
			else:
				all_fs = all_fs.append(fs, ignore_index=True)
			print('%s was added to %s!' % (code, table))

	if not upload:
		all_fs.iloc[:100].to_csv('%s.csv' % table, index=False)

def upload_struct():
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
			os.remove(f)
			print('%s %s file is removed!' % (code, 'struct'))
		except Exception as e:
			print('Error on %s: %s' % (code, e))

def run(limit=100):
	now = datetime.now()
	m = now.month
	if m in [2,3,4,7,8,10]:
		config = ConfigParser()
		config.read('account_map.ini', encoding='utf-8')
		with open('stretch_log.txt', 'w+') as f:
			f.write('Failed Task Log:\nCreate at %s\n' % datetime.now())

		stock_list = get_stock_list()
		while True:
			modify = run_stretcher(stock_list,limit = limit)
			print('='*20, modify)
			if modify:
				threads = list()

				for report_type in ['debt', 'benefit', 'cash']:
					threads.append(Thread(target=sort_and_upload, args=(report_type, dict(config.items(report_type)), True)))
				threads.append(Thread(target=upload_struct))

				for thread in threads:
					thread.start()
			else:
				break


	# sort_and_upload('cash', dict(config.items('cash')))



if __name__ == '__main__':

	today = datetime.datetime.today()
	result = ''
	run(limit=10)
	result = '%s---- 上市公司财报获取结果：成功' % (str(today))
	send_to_dingding(result)


