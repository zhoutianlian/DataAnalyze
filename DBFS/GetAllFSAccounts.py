"""
财报字段，中英文对照

"""
import pandas as pd
from glob import glob
from configparser import ConfigParser
from database_fs.utils import uniform_name

config = ConfigParser()
config.read('account_map.ini', encoding='utf-8')

for fs_type in ('debt', 'benefit', 'cash'):
	print(fs_type, 'treating...')
	acc_map = dict(config.items(fs_type))
	cn_name = set()
	total = glob('%s/*.csv' % fs_type)
	count = 0
	for fs in total:
		count += 1
		print('\r%s (%d)  ' % (fs, count), end='')
		cols = pd.read_csv(fs, nrows=0).columns
		cols = cols.to_series(index=range(len(cols)))
		cols = cols.apply(lambda x: uniform_name(x)[0])
		cn_name.update(cols.values)
	cn_name = {n for n in cn_name if len(n)}
	cn_name = pd.Series(sorted(list(cn_name)))
	en_name = cn_name.map(acc_map)
	name_dict = pd.concat([cn_name, en_name], axis=1)
	name_dict.columns = ['cn', 'en']
	name_dict.to_csv('%s_dict.csv' % fs_type, index=False, header=False)
	empty = name_dict.query('en == ""')
	if len(empty):
		print(empty)
	print()
