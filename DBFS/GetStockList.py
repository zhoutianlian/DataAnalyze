"""
通过东方财富接口，获取所有股票的列表（代码+简称）
"""

import pandas as pd
import requests

def to_stock_code(num):
    if num.startswith('6'):
        return 'sh' + num
    elif num.startswith(('8', '4')):
        return 'oc' + num
    elif num.startswith(('0', '3')):
        return 'sz' + num
    else:
        return num

def get_stock_list():
    url_update = 'http://31.push2.eastmoney.com/api/qt/clist/get?'
    page = 1
    stocks_per_page = 50000
    new = '&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:81+s:!4&'
    hsa = '&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23&'
    col_names = 'f2,f12,f14,f38,f39'

    stock_list = list()
    for param in [hsa, new]:
        table = requests.get(
            url_update + 'pn=%d&pz=%d' % (page, stocks_per_page) + param + 'fields=%s' % col_names).text
        table = eval(table)
        data = pd.DataFrame(table['data']['diff'])

        rename_data = data.rename(columns={'f2': 'new_price',
                                           'f12': 'stock_code',
                                           'f14': 'stock_name',
                                           'f38': 'total_share',
                                           'f39': 'circulated_share'})
        stock_list.append(rename_data)

    stock_list = pd.concat(stock_list, axis=0, ignore_index=True)
    stock_list['stock_code'] = stock_list['stock_code'].apply(to_stock_code)
    active_stock_list = stock_list.query('total_share != "-"')
    print('Find %d stocks, in which %d are active!' % (len(stock_list), len(active_stock_list)))
    return active_stock_list

"""
{
    'f1': 'f1',
    'f2': '收盘价',
    'f3': '涨跌幅',
    'f4': '涨跌',
    'f5': '交易量',
    'f6': '交易额',
    'f7': '振幅',
    'f8': '换手率',
    'f9': '市盈率',
    'f10': '量比',
    'f11': '5分钟涨跌幅',
    'f12': '股票代码',
    'f13': 'f13',
    'f14': '股票名称',
    'f15': '最高价',
    'f16': '最低价',
    'f17': '开盘价',
    'f18': '前收盘价',
    'f19': 'f19',
    'f20': '总市值',
    'f21': '流通市值',
    'f22': '涨速',
    'f23': '市净率',
    'f24': '60日涨跌幅',
    'f25': '年初至今涨跌幅',
    'f26': '上市日期',
    'f27': 'f27',
    'f28': 'f28',
    'f29': 'f29',
    'f30': 'f30',
    'f31': '买一',
    'f32': '卖一',
    'f33': '委比',
    'f34': '外盘',
    'f35': '内盘',
    'f36': 'f36',
    'f37': 'ROE',
    'f38': '总股本',
    'f39': '流通股本',
    'f40': '总收入',
    'f41': '总收入同比增长',
    'f42': '营业利润',
    'f43': '投资收益',
    'f44': '利润总额',
    'f45': '净利润',
    'f46': '净利润同比增长',
    'f47': '未分配利润',
    'f48': '每股未分配利润',
    'f49': '毛利率',
    'f50': '总资产',
    'f51': 'f51',
    'f52': 'f52',
    'f53': 'f53',
    'f54': 'f54',
    'f55': 'f55',
    'f56': 'f56',
    'f57': '负债率',
    'f58': '归属于母公司股东的权益合计',
    'f59': 'f59',
    'f60': 'f60',
    'f61': 'f61',
    'f62': '主力净流入',
    'f64': '超大单',
    'f66': '超大单净流入',
    'f67': '超大单净比',
    'f70': '大单',
    'f72': '大单净流入',
    'f76': '中单',
    'f78': '中单净流入',
    'f79': '大单净比',
    'f81': '中单净比',
    'f82': '小单',
    'f84': '小单净流入',
    'f87': '小单净比',
    'f100': '所属东财行业',
    'f102': '地域板块',
    'f103': '所属板块',
    'f112': 'EPS',
    'f164': '5日主力净流入',
    'f165': 'f165',
    'f166': '5日超大单',
    'f167': 'f167',
    'f168': '5日大单',
    'f169': 'f169',
    'f170': '5日中单',
    'f171': 'f171',
    'f172': '5日小单',
    'f174': '10日主力净流入',
    'f175': 'f175',
    'f176': '10日超大单',
    'f177': 'f177',
    'f178': '10日大单',
    'f179': 'f179',
    'f180': '10日中单',
    'f181': 'f181',
    'f182': '10日小单',
    'f184': '主力净比',
    'f265': '所属指数',
    'f267': '3日主力净流入',
    'f269': '3日超大单',
    'f270': 'f270',
    'f271': '3日大单',
    'f272': 'f272',
    'f273': '3日中单',
    'f274': 'f274',
    'f275': '3日小单',
}
"""