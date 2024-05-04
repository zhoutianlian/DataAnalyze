"""
新闻生成及历史数据批量生成
"""
import re
import datetime
import pandas as pd
import numpy as np
import time
import random
from ValuationRecord.SaveEnterprise import save as save_id
from config.Config import config
import os
import sys
from utils import read_mongo, save_mongo

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
from aip import AipSpeech
APP_ID = '24041344'
API_KEY = 'qPYe3aq3f3yfAE8h15YGU12h'
SECRET_KEY = 'WanvzTjf4GVaG1mGsZWRhZHW1BdGl00r'
client = AipSpeech(APP_ID, API_KEY, SECRET_KEY)
from config.globalENV import global_var
from log.log import logger
from News2Audio import save


class News():

    def __init__(self, today=datetime.datetime.now()):

        self.today = today

        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = config['DEFAULT']['mongo_port']

        self.username_master = config['DEFAULT']['mongo_user_master']
        self.password_master = config['DEFAULT']['mongo_password_master']
        self.host_master = config['DEFAULT']['mongo_host_master']
        self.port_master = config['DEFAULT']['mongo_port_master']

        self.mongo_config_test = (self.username, self.password, self.host, self.port)
        self.mongo_config_master = (self.username_master, self.password_master,
                                  self.host_master, self.port_master)

        self.df = self.clean_event()
        self.df_latest_case = self.get_latest()



    def get_n_day_before(self, n=0):
        t = (self.today - datetime.timedelta(days=n))
        start = t.year * 10000 + t.month * 100 + t.day
        return start

    def get_unit(self,x):
        n = np.log10(x)
        if n < 6:
            unit = '万元'
            x = round(x / 10000, 2)
        elif n < 7:
            unit = '百万元'
            x = round(x / 1000000, 2)
        elif n < 8:
            unit = '千万元'
            x = round(x / 10000000, 2)
        else:
            unit = '亿元'
            x = round(x / 100000000, 2)
        return x, unit

    def get_latest(self):
        w = self.today.strftime("%w")
        if w == '1':
            self.last_day = self.get_n_day_before(n=3)
        else:
            self.last_day = self.get_n_day_before(n=1)
        df_latest_case = self.df[(self.df['last_financing_date'] >= self.last_day)]
        return df_latest_case

    def clean_event(self):
        df = read_mongo( mongo_config=self.mongo_config_test,db_name='spider_origin', collection_name='financing_event',
                             conditions={'last_financing_date': {'$gte': self.get_n_day_before(n=self.today.day),
                                                                 '$lt': self.get_n_day_before(n=0)}},
                             query=[], sort_item='', n=0)

        new_df = pd.DataFrame(
            columns=['company_name', 'short_name', 'financing_date', 'financing_amount', 'turn', 'vc_name',
                     'industry', 'tag', 'establish_time', 'specific_business', 'last_financing_date'])
        ex_usd = read_mongo( mongo_config=self.mongo_config_test,db_name='modgo_quant', collection_name='fxTradeData',
                                 conditions={'$and': [{'trade_date': {'$gt': self.get_n_day_before(n=90)}},
                                                      {'currency_pair': 'USD/CNY'}]}, query=[], sort_item='',
                                 n=0)
        ex_usd = ex_usd['fx_rate'].mean()
        for n, x in df.iterrows():
            df_ret = pd.DataFrame(columns=['financing_date', 'financing_amount', 'turn', 'vc_name'])
            for i in range(len(x['data'])):
                data = x['data'][i]
                money = data['money']
                date = data['publish_time']
                today = time.mktime(datetime.datetime.now().date().timetuple())
                today = datetime.datetime.fromtimestamp(today)
                if '至今' in date:
                    date = today
                else:
                    date = pd.to_datetime(date)
                amount = 0
                if len(re.findall(r"\d+\.?\d*", money)) == 0:
                    amount = 3 * ('数' in money) + 1 * ('级' in money)
                else:
                    amount = float(re.findall(r"\d+\.?\d*", money)[0])
                unit = (('美元' in money) * ex_usd +
                        (not ('美元' in money)) * 1) * (
                               ('千' in money) * 10000000 + ('百' in money) * 1000000 +
                               ('亿' in money) * 100000000 + 10000)
                money = amount * unit
                df_i = pd.DataFrame({'financing_date': [date],
                                     'financing_amount': [money], 'turn': [data['turn']], 'vc_name': [data['vc_name']]})

                df_ret = df_ret.append(df_i, ignore_index=True)
            if not df_ret.empty:
                df_ret = df_ret.sort_values(by='financing_date')
                df_ret['financing_amount'] = df_ret['financing_amount'].astype('float')
                df_ret = df_ret.drop_duplicates(subset='financing_date', keep='last')
                df_ret['financing_date'] = pd.to_datetime(df_ret['financing_date'])
                df_ret['company_name'] = x['company_name']
                df_ret['establish_time'] = x['establish_time']
                df_ret['tag'] = x['tag']
                df_ret['specific_business'] = x['specific_business']
                df_ret['industry'] = x['industry_3']
                df_ret['last_financing_date'] = x['last_financing_date']
                df_ret['short_name'] = x['short_name']
                new_df = new_df.append(df_ret, ignore_index=True)

        return new_df

    def get_text_summary(self):
        list_summary = []
        summary_highlight = []

        text_today = str(self.today.year) + '年' + str(self.today.month) + '月' + str(self.today.day) + '日'
        if not self.df_latest_case.empty:
            n = len(self.df_latest_case['company_name'].unique())
            df_latest_financing = self.df_latest_case[
                self.df_latest_case['financing_date'] >= pd.to_datetime(str(self.last_day))]
            total_amount = sum(df_latest_financing['financing_amount'])

            ta,ta_unit = self.get_unit(total_amount)

            industry = list(df_latest_financing['industry'])
            industry = ','.join(industry[:min(3, len(industry))])
            non_disclose = list(
                df_latest_financing[df_latest_financing['financing_amount'] == 0]['company_name'].unique())
            non_disclose = ','.join(non_disclose)


            text = "%s，最近交易日全国范围内共发生%d起融资事件，已披露总融资金额为人民币%.2f%s，行业分布于%s等" % (
                text_today, n, ta,ta_unit, industry)

            list_summary.append(text)
            if non_disclose:
                text_1 = "其中%s未披露融资金额" % (non_disclose)
                list_summary.append(text_1)
            summary_highlight.extend(['%d起'%(n), '%.2f'%(ta)+ta_unit])
        else:
            text = "%s，最近交易日全国范围内未发生融资事件" % (text_today)
            list_summary.append(text)

        df_acc = self.df[self.df['financing_date'] >= pd.to_datetime(str(self.get_n_day_before(n=self.today.day)))]
        total_amount_acc = sum(df_acc['financing_amount'])
        taa, taa_unit = self.get_unit(total_amount_acc)

        tag = list(df_acc['tag'])
        tag_1 = []
        for i in tag:
            i = i.split(',')
            tag_1.extend(i)

        tags = set(tag_1)
        df_tag = pd.DataFrame(columns=['tag', 'n'])
        for t in tags:
            if not t == '':
                df_tag = df_tag.append({'tag': t, 'n': tag_1.count(t)}, ignore_index=True)
        df_tag = df_tag.sort_values(by='n', ascending=False)
        l_tag = df_tag.iloc[:min(3, int(0.5 * len(df_tag)))]['tag'].to_list()
        str_tag = ','.join(l_tag)

        text_2 = "%d月累计融资人民币%.2f%s，热点板块关注：%s" % (self.today.month, taa,taa_unit, str_tag)
        list_summary.append(text_2)
        summary = ','.join(list_summary)
        summary_highlight.extend(['%.2f'%(taa)+taa_unit])

        summary_highlight = [str(x) for x in summary_highlight]

        #         summary_highlight = ';'.join(summary_highlight)

        return summary, summary_highlight

    def get_text_case(self):
        text_case = []
        case_highlight = []
        title_case = []

        for name in self.df_latest_case.company_name.unique():
            print(name)
            e_id= save_id(name)
            print(e_id)


            df_i = self.df_latest_case.query('company_name == @name')
            df_i = df_i[df_i['financing_date'] == df_i['financing_date'].max()]
            turn = df_i['turn'].values[0]
            vc = df_i['vc_name'].values[0]
            l_vc = vc.split(',')
            l_vc = l_vc[:min(len(l_vc), 3)]
            vc = ','.join(l_vc)
            money = df_i['financing_amount'].values[0]
            m,m_unit = self.get_unit(money)
            if money>0:
                fd = df_i.iloc[0]['financing_date']
                fd = str(fd.year) + '年' + str(fd.month) + '月' + str(fd.day) + '日'
                acc_amount = sum(self.df_latest_case.query('company_name == @name')['financing_amount'])
                aa,aa_unit = self.get_unit(acc_amount)
                et = pd.to_datetime(df_i['establish_time'].values[0])
                et = str(et.year) + '年' + str(et.month) + '月' + str(et.day) + '日'
                sb = df_i['specific_business'].values[0]
                sn = df_i['short_name'].values[0]
                industry = df_i['industry'].values[0]
                tag = df_i['tag'].values[0]
                dict_mkt = {
                    'v': ['引发', '获得', '收到'],
                    'mkt': ['全市场', '%s行业' % industry, '业内', '资本市场'],
                    'n': ['密切关注', '聚焦', '热点关注']
                }

                mkt = dict_mkt['v'][random.randint(0, 2)] + dict_mkt['mkt'][random.randint(0, 3)] + dict_mkt['n'][
                    random.randint(0, 2)]
                text = """
                %s,%s获得全市场市场人民币%.2f%s%s融资，%s，渠道资金来源于%s等机构，目前累积融资%.2f%s。%s成立于%s，定位%s行业，主营业务为%s，涉及%s等板块概念。
                """ % (fd, name, m,m_unit, turn, mkt,
                       vc, aa,aa_unit,
                       sn, et, industry, sb, tag)
                text_case.append(text)

                title = """
                %s获得人民币%.2f%s%s融资
                """%(sn,m,m_unit,turn)
                title_case.append(title)

                case_highlight.append([str(x) for x in [name, '%.2f'%(m)+m_unit,'%.2f'%(aa)+aa_unit,e_id]])
        return text_case, case_highlight,title_case


    def get_audio(self,text,update_date):
        result = client.synthesis(text, 'zh', 1, {
            'vol': 8,
            'per': 3
        })
        # 识别正确返回语音二进制 错误则返回dict 参照下面错误码
        audio_url = ''
        if not isinstance(result, dict):
            audio_path = global_var.path +'/audio/'
            if not os.path.exists(audio_path):
                os.makedirs(audio_path)
            audio_name = 'audio_%d.mp3'%(update_date)
            logger(str(audio_path))
            # audio_url = os.path.join(os.path.abspath(os.path.join(os.getcwd(), ".")),'audio/'
            #                        'audio_%d.mp3'%(self.update_date))
            with open(audio_path + audio_name, 'wb') as f:
                f.write(result)
            audio_url = save.get_file_id(audio_path, 'audio_%d.mp3' % (update_date))
            logger(str(audio_url))
            print(audio_url)
        return audio_url

    def generate_news(self):

        summary, summary_highlight = self.get_text_summary()
        cases, case_highlight,title = self.get_text_case()

        summary_highlight = [str(x) for x in summary_highlight]
        #         case_highlight = [str(x) for x in case_highlight]

        ud = self.today.year * 10000 + self.today.month * 100 + self.today.day
        audio_url =self.get_audio(text = summary, update_date = ud)
        ret_df = pd.DataFrame({'update_date': [ud], 'summary': [summary], 'cases': [cases],'title':[title],
                               'highlight': [{'summary': [summary_highlight], 'cases': case_highlight}],
                               'download_url':[audio_url]})

        save_mongo(mongo_config=self.mongo_config_master,df_data=ret_df, db_name='modgo_quant', tb_name='financing_news',
                        if_del=0, del_condition={'update_date': ud},keys = ['update_date'])
    def clean_fn(self):
        df = read_mongo(mongo_config=self.mongo_config_master,db_name='modgo_quant', collection_name='financing_news',
                        conditions={}, query=[], sort_item='', n=0)
        df = df.drop_duplicates(subset='update_date')
        save_mongo(mongo_config=self.mongo_config_master,df_data=df, db_name='modgo_quant', tb_name='financing_news',
                        if_del=1, del_condition={},keys = ['update_date'])
"""
读取融资事件，生成所有的新闻历史数据
无需输入参数
当generate_news 代码发生改变时，可以批量替换原有的新闻历史记录
"""

class SaveNews():

    def __init__(self):

        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = config['DEFAULT']['mongo_port']


        self.username_master = config['DEFAULT']['mongo_user_master']
        self.password_master = config['DEFAULT']['mongo_password_master']
        self.host_master = config['DEFAULT']['mongo_host_master']
        self.port_master = config['DEFAULT']['mongo_port_master']

        self.mongo_config_test = (self.username, self.password, self.host, self.port)
        self.mongo_config_master = (self.username_master, self.password_master,
                                  self.host_master, self.port_master)




    # def check_mongo(self, db_name, tb_name,):
    #     mongo_uri_test = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
    #                      (self.username, self.password, self.host, self.port, db_name)
    #     conn = MongoClient(mongo_uri_test)
    #
    #
    #     db = conn[db_name]
    #     collection = db[tb_name]
    #     cursor = collection.find()
    #     result = [doc for doc in cursor]
    #     if result:
    #         collection.delete_many({})
    #     else:
    #         pass

    def save(self,n):
        # 输入需要向前追溯存储的日期天数如果数据库存在，即获取并存入
        dates = read_mongo(mongo_config=self.mongo_config_test,db_name='spider_origin', collection_name='financing_event',
                                conditions={}, query=['last_financing_date'], sort_item='', n=0)
        dates = list(dates['last_financing_date'].unique())
        dates.sort(reverse=True)
        # self.check_mongo(db_name='modgo_quant', tb_name='financing_news')
        for t in dates[:n]:
            y = int(t / 10000)
            m = int((t - y * 10000) / 100)
            d = (t - y * 10000 - m * 100)
            N = News(today=datetime.datetime(y, m, d))
            N.generate_news()

if __name__ == '__main__':
    """
    批量存储历史数据，或修改新闻格式，修改代码后使用save
    获取每日最新数据，调用gebnerate news无需参数
    如特定调用某天新闻，或者历史遗漏某天新闻需要补充，将年月日作为参数输入
    """
    # today = datetime.datetime.today()
    # result = ''
    # SN = SaveNews()
    # SN.save()
    # N = News(today=datetime.datetime(2020, 1, 1))
    N = News()
    N.generate_news()
    # result = '%s---- 新闻生成结果：成功' % (str(today))
    # send_to_dingding(result)
    # try:
    #     N = News()
    #     N.generate_news()
    #     result = '%s---- 新闻生成结果：成功' % (str(today))
    # except Exception as e:
    #     result = '%s---- 结果：新闻生成失败，失败原因：%s' % (str(today), str(e))
    # finally:
    #     send_to_dingding(result)
    # # N.clean_fn()
