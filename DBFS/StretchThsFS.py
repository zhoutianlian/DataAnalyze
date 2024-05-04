"""
同花顺网站
获取利润表，资产负债表，现金流量表
"""
import requests
import tempfile
import xlwings as xw
import numpy as np
import pandas as pd
import os
import re
from glob import glob
from datetime import datetime
import time
import redis
import execjs
from CONFIG.Config import config

def deal_headers():
    v = get_cookie_V()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
               'cookie':'v=%s' % v,
               }
    return headers

def get_cookie_V():
    ti = str(('%.3f' % float(time.time())))
    with open('./aes.min.js', 'r') as f:
        jscontent = f.read()
    context = execjs.compile(jscontent)
    context.call('getParameter', ti)
    v = context.call("v")
    return v

def sleep():
    sec = np.random.rand()
    time.sleep(sec)

def get_fs(code_number, kind='debt'):
    """
    抓取同花顺财经接口财报
    :param code_number: 股票代码6位数字
    :param kind: debt-资产负债表 benefit-利润表 cash-现金流量表
    :return:
    """
    url = 'http://basic.10jqka.com.cn/api/stock/export.php?export=%s&type=report&code=%s' % (kind, code_number)
    # sess = requests.session()
    headers = deal_headers()
    response = requests.get(url, headers=headers)
    # print(response)
    if response.status_code == 200:
        pass
    else:
        print('cookie中的v参数失效')
        deal_headers()
        response = requests.get(url, headers=headers)

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, 'temp.xls')
        # print(path)
        with open(path, 'wb') as f:
            f.write(response.content)

        try:
            data = pd.read_excel(path)
            data = data.values

        # app = xw.App(visible=False, add_book=False)
        # app.display_alerts = False
        # wb = app.books.open(path)
        # sheet = wb.sheets[0]
        # data = sheet.used_range.value
        # D:\Python\lib\site - packages\xlrd\compdoc.py”
        except Exception as e:
            print(e)
        # finally:
        #     try:
        #         wb.close()
        #         app.quit()
        #     except:
        #             pass
    arr = np.array(data).T
    columns = arr[0, ...]
    values = arr[1:, ...]
    df = pd.DataFrame(values, columns=columns)
    df = df.set_index('科目\\时间')
    df = df.dropna(how='all', axis=1)
    df = df.applymap(turn_num)
    return df

def turn_num(x):
    if type(x) is str:
        find = re.findall(r'(-)?([0-9,.])+', x)
        if len(find):
            return float(''.join(find[0]))
        else:
            return 0
    else:
        return x

def to_number(x):
    if type(x) is str:
        find = re.findall(r'(-)?([0-9,.]+)(%)?', x)
        if len(find):
            v = find[0][0] + find[0][1].replace(',', '')
            v = float(v)
            if find[0][2] == '%':
                v *= 0.01
            return v
        else:
            return 0
    return x

def get_income_structure(code):
    """
    读取收入和利润构成表
    :param code:
    :return:
    """
    url = 'http://quotes.money.163.com/service/gszl_%06d.html' % code
    sess = requests.session()
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                             '(KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    response = sess.get(url, headers=headers)

    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, 'temp.xls')
        with open(path, 'wb') as f:
            f.write(response.content)

        data = pd.read_excel(path)
        data_rows = data.values
            # app = xw.App(visible=False, add_book=False)
            # app.display_alerts = False
            # wb = app.books.open(path)
            # sheet = wb.sheets[0]
            # data_rows = sheet.used_range.value
        # except Exception as e:
        #     print(e)
        # finally:
        #     wb.close()
        #     app.quit()

    year_list = list()
    year_list.append(list())
    for r in data_rows:
        if r is not None:
            year_list[-1].append(r.split(','))
        else:
            year_list.append(list())

    adj_list = list()
    for y in year_list:
        y = np.array(y)
        index = y[2:, 0]
        report_date = y[0, 4]
        columns = y[1, 1:]
        values = y[2:, 1:]
        df = pd.DataFrame(values, index=index, columns=columns)
        df = df.applymap(to_number)

        cols = list()
        for c, d in df.iteritems():
            if c.endswith('(万元)'):
                d = d.rename(c.replace('(万元)', ''))
                cols.append(d * 1e4)
            else:
                cols.append(d)
        adj = pd.concat(cols, axis=1)
        adj = adj.rename(columns={'收入': 'sales', '成本': 'cost', '利润': 'profit', '毛利率': 'gpm', '利润占比': 'profit_pct'})
        adj['report_date'] = pd.Timestamp(str(report_date))
        adj.index = adj.index.rename('category')
        adj = adj.reset_index()
        adj_list.append(adj)
    adj_all = pd.concat(adj_list, axis=0, ignore_index=True)
    return adj_all

def run(stock_list,limit):
    modify = False
    count = 0
    redis_config = dict(host=config['DEFAULT']['redis_host'],
                        port=config['DEFAULT']['redis_port'],
                        db=config['DEFAULT']['db_fs'],
                        password=config['DEFAULT']['redis_password'])

    for i, d in stock_list.iterrows():
        code = d['stock_code']
        num = int(re.findall(r'.*?(\d{6})', code)[0])

        print('Row %05d    Time: %s    Stock: %s' % (i, datetime.now().strftime('%H:%M:%S'), code), end='    ')

        task_ignore = [0, 0, 0, 0]
        with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
            if con.exists(code):
                task_ignore = eval(con.get(code).decode())
        any_modify = False
        if task_ignore[0] == 0:
            try:
                bs = get_fs(num, 'debt')
                bs.to_csv('debt/%s.csv' % code)
                task_ignore[0] = 1
                modify = True
                any_modify = True
                print('BS Success', end='    ')
            except Exception as e:
                print('BS Failed ', end='    ')
                with open('stretch_log.txt', 'a') as f:
                    f.write('bs %s: %s\n' % (code, e))
            finally:
                sleep()
        else:
            print('BS Ignored', end='    ')

        if task_ignore[1] == 0:
            try:
                pl = get_fs(num, 'benefit')
                pl.to_csv('benefit/%s.csv' % code)
                task_ignore[1] = 1
                modify = True
                any_modify = True
                print('PL Success', end='    ')
            except Exception as e:
                print('PL Failed ', end='    ')
                with open('stretch_log.txt', 'a') as f:
                    f.write('pl %s: %s\n' % (code, e))
            finally:
                sleep()
        else:
            print('PL Ignored', end='    ')

        if task_ignore[2] == 0:
            try:
                cf = get_fs(num, kind='cash')
                cf.to_csv('cash/%s.csv' % code)
                task_ignore[2] = 1
                modify = True
                any_modify = True
                print('CF Success', end='    ')
            except Exception as e:
                print('CF Failed ', end='    ')
                with open('stretch_log.txt', 'a') as f:
                    f.write('cf %s: %s\n' % (code, e))
            finally:
                sleep()
        else:
            print('CF Ignored', end='    ')

        if task_ignore[3] == 0:
            try:
                st = get_income_structure(num)
                st.to_csv('struct/%s.csv' % code)
                task_ignore[3] = 1
                modify = True
                any_modify = True
                print('ST Success', end='    ')
            except Exception as e:
                print('ST Failed ', end='    ')
                with open('stretch_log.txt', 'a') as f:
                    f.write('st %s: %s\n' % (code, e))
            finally:
                sleep()
        else:
            print('ST Ignored', end='    ')

        if any_modify:
            with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
                   # 保存一周，一周内不会对ignore的表再抓取
            count += 1

        print()
        if count >= limit:
            break
    return modify

if __name__ == '__main__':
    # 抓取所有股票列表
    # StockList = pd.read_csv('stock_list.csv')
    #
    # # 抓取报表
    # Modify = True
    # while Modify:
    #     Modify = run(StockList)
    get_fs('000001', kind='cash')
