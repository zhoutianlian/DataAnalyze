"""
动态估值视频生成
数据预处理
视频初步生产
各项元素添加
视频完成
每一步会将视频存于不同的文件夹
最终结果去video_music查看
"""
# IPython.display.HTML(bcr_html)
import pickle
from functools import reduce
import numpy as np
import pandas as pd
import bar_chart_race as bcr
import redis
from IPython.display import HTML
import os
# 如果出现SSL错误,则全局取消证书验证
import ssl

from config.Config import config
from utils import read_mongo

ssl._create_default_https_context = ssl._create_unverified_context
import matplotlib as mpl
mpl.rcParams['font.sans-serif'] = ['SimHei']
# mpl.rcParams['font.sans-serif'] = ['KaiTi', 'SimHei', 'FangSong']
mpl.rcParams['font.size'] = 12
mpl.rcParams['axes.unicode_minus'] = False
from moviepy.editor import VideoFileClip
from pymongo import MongoClient
import time
import pathlib
from moviepy.editor import VideoFileClip, TextClip,ImageClip
from moviepy.editor import vfx
from moviepy.editor import CompositeVideoClip, concatenate_videoclips
from moviepy.video.tools.drawing import circle
from moviepy.video.tools.credits import credits1
from moviepy.video.VideoClip import TextClip
from PIL import Image, ImageDraw, ImageFont

import os
from moviepy.editor import *
import time
import datetime
import re

import cv2


class MakeVedio():

    def __init__(self, tag, size, vedio_name, file_name, target_company, music):
        """

        :param tag: 创业邦自定义的tag
        :param size: large medium small 选一个
        :param vedio_name: 同tag
        :param file_name: 英文名命名文件
        :param target_company: 可以为空，或者以列表输入
        :param music: bgm文件中的歌曲名字
        """
        # self.username = 'hub'
        # self.password = 'hubhub'
        # self.host = '192.168.2.105'
        # self.port = '27017'

        self.username = config['DEFAULT']['mongo_user']
        self.password = config['DEFAULT']['mongo_password']
        self.host = config['DEFAULT']['mongo_host']
        self.port = config['DEFAULT']['mongo_port']
        self.mongo_config_test = (self.username, self.password, self.host, self.port)

        self.tag = tag
        self.file_name = file_name
        self.vedio_name = vedio_name
        self.target_company = target_company
        self.abs_path_0 = 'D:/python_zhoutianlian/financing_data_process/static/'
        self.abs_path_1 = 'D:/project_zhoutianlian/financing_data_process/video_origin/'
        self.abs_path_2 = 'D:/project_zhoutianlian/financing_data_process/video_logo/'
        self.abs_path_3 = 'D:/project_zhoutianlian/financing_data_process/video_end/'
        self.abs_path_4 = 'D:/project_zhoutianlian/financing_data_process/video_cover/'
        self.abs_path_5 = 'D:/project_zhoutianlian/financing_data_process/video_music/'
        self.data = self.get_data()
        self.size = size
        self.dict_scale = {
            'large': '龙头',
            'median': '中市值',
            'small': '小市值'
        }
        self.dict_vedio_path = {
            'origin': self.abs_path_1 + self.file_name + '-' + self.size + '.mp4',
            'logo': self.abs_path_2 + self.file_name + '-' + self.size + '.mp4',
            'end': self.abs_path_3 + self.file_name + '-' + self.size + '.mp4',
            'cover': self.abs_path_4 + self.file_name + '-' + self.size + '.mp4',
            'music': self.abs_path_5 + self.file_name + '-' + self.size + '.mp4',
        }
        #         self.img_url ="D:/project_zhoutianlian/financing_data_process/demo.jpg"
        self.img_url = ''
        self.img_text = ''
        self.FONT_URL = 'C:/Users/Faye/AppData/Local/Microsoft/Windows/Fonts/CRSYT_GB2312.ttf'
        self.music_dir = r"D:\project_zhoutianlian\financing_data_process\bgm"

    #         self.FONT_URL = 'C:/Windows/Fonts/crsyt.ttf'



    def clean_data(self):

        if self.target_company:

            df = read_mongo(mongo_config=self.mongo_config_test,db_name='spider_origin', collection_name='financing_event',
                                 conditions={'$or':
                                     [
                                         {"company_name": "图麟信息科技上海有限公司"},
                                         {"company_name": "上海思亮信息技术股份有限公司"},
                                         {"company_name": "上海氪信信息技术有限公司"},
                                         {"company_name": "上海扩博智能技术有限公司"},
                                         {"company_name": "上海衡道医学病理诊断中心有限公司"},
                                         {"company_name": "上海烨睿信息科技有限公司"},
                                         {"company_name": "上海卓繁信息技术股份有限公司"},
                                         {"company_name": "斑马网络技术有限公司"},
                                         {"company_name": "上海极链网络科技有限公司"},
                                     ]
                                 },
                                 query=[], sort_item='', n=0)
        else:
            df = self.read_mongo(mongo_config=self.mongo_config_test,db_name='spider_origin', collection_name='financing_event',

                                 conditions={'last_financing_date': {'$gt': self.get_n_day_before(n=365)}},
                                 query=[], sort_item='', n=0)

            def func(x):
                l = x['tag'].split(',')
                if self.tag in l:
                    x['if_tag'] = 1
                return x

            df = df.apply(func, axis=1)
            df = df.query('if_tag == 1')
            del df['if_tag']

        # df = df.iloc[:10]
        ex_usd = self.read_mongo(mongo_config=self.mongo_config_test,db_name='modgo_quant', collection_name='fxTradeData',
                                 conditions={'$and': [{'trade_date': {'$gt': self.get_n_day_before(n=90)}},
                                                      {'currency_pair': 'USD/CNY'}]}, query=[], sort_item='',
                                 n=0)
        ex_usd = ex_usd['fx_rate'].mean()
        new_df = pd.DataFrame(columns=['company_name', 'financing_date', 'acc_financing'])
        for n, x in df.iterrows():
            df_ret = pd.DataFrame(columns=['financing_date', 'financing_amount'])
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
                #             date = date.year*10000 + date.month*100 + date.day
                amount = 0
                if len(re.findall(r"\d+\.?\d*", money)) == 0:
                    amount = 5 * ('数' in money)
                else:
                    amount = float(re.findall(r"\d+\.?\d*", money)[0])
                if amount:
                    unit = (('美元' in money) * ex_usd +
                            (not ('美元' in money)) * 1) * (
                                   ('千' in money) * 10000000 + ('百' in money) * 1000000 +
                                   ('亿' in money) * 100000000 + 10000)
                    money = amount * unit
                    df_i = pd.DataFrame({'financing_date': [date], 'financing_amount': [money]})
                    df_ret = df_ret.append(df_i, ignore_index=True)
            if not df_ret.empty:
                df_ret = df_ret.sort_values(by='financing_date')
                df_ret['acc_financing'] = df_ret['financing_amount'].cumsum()
                df_ret['acc_financing'] = df_ret['acc_financing'].astype('float')
                df_ret = df_ret.drop_duplicates(subset='financing_date', keep='last')
                df_ret['company_name'] = x['company_name']
                new_df = new_df.append(df_ret, ignore_index=True)

        new_df = new_df[['company_name', 'acc_financing', 'financing_date']]
        print(new_df)
        return new_df

    def get_today(self):
        today = time.mktime(datetime.datetime.now().date().timetuple())
        today = datetime.datetime.fromtimestamp(today)
        return today

    def get_n_day_before(self, n=0):
        t = (datetime.datetime.now() - datetime.timedelta(days=n))
        start = t.year * 10000 + t.month * 100 + t.day
        return start

    def transfer_data(self, tag='文化娱乐', file_name='entertainment'):
        today = self.get_today()
        rf = np.log(1 + 0.03)
        df = self.clean_data()

        total_list = []
        total_company = list(df['company_name'].unique())
        for name in total_company:
            df_i = df.query('company_name == @name')
            df_i = df_i[['financing_date', 'acc_financing']]
            delta_t = (today - df_i['financing_date'].max()).days
            amount = df_i['acc_financing'].max() * np.exp(rf * delta_t / 360)
            df_i = df_i.append(pd.DataFrame({'financing_date': [today], 'acc_financing': [amount]}))
            df_i = df_i.drop_duplicates(subset='financing_date', keep='first')
            df_i = df_i.set_index('financing_date')
            df_i = df_i['acc_financing']
            df_i = df_i.resample(rule='D').interpolate()
            df_i = df_i.reset_index()
            df_i = df_i.rename(columns={'acc_financing': name})
            df_i = df_i.set_index('financing_date')
            print(df_i)
            if df_i[name].sum() > 0:
                total_list.append(df_i)
        total_df = pd.concat(total_list, axis=1)
        # total_df.to_csv(os.path.join(self.abs_path_0, 'vedio_material_%s.csv' % (file_name)))
        return total_df

    # 从redis读取数据，如不存在，转换数据格式，存储数据
    # 以tag命名
    def get_data(self):
        redis_config = dict(host=config['DEFAULT']['redis_host'],
                            port=config['DEFAULT']['redis_port'],
                            db=config['DEFAULT']['redis_db'],
                            password=config['DEFAULT']['redis_password'])
        with redis.StrictRedis(connection_pool=redis.ConnectionPool(**redis_config)) as con:
            if con.exists(self.file_name):
                # df = pd.read_csv(os.path.join(self.abs_path_0, 'vedio_material_%s.csv' % (self.file_name)), index_col=0)
                df = con.get(self.file_name)
                df = pickle.loads(df)
            else:
                df = self.transfer_data(tag=self.tag, file_name=self.file_name)
                df_1 = pickle.dumps(df)
                con.set(self.file_name, df_1, ex=60*60*24*30)

        return df

    def data2bar_chart(self):

        self.data.index = pd.to_datetime(self.data.index)
        self.data = self.data.fillna(0)

        if len(list(self.data.columns)) > 50:
            q_1 = np.percentile(self.data.max().values, 30)
            q_2 = np.percentile(self.data.max().values, 70)
            q_3 = np.percentile(self.data.max().values, 100)
            dict_size = {
                'large': [q_2, q_3],
                'median': [q_1, q_2],
                'small': [10 ** 4, q_1]
            }
        elif len(list(self.data.columns)) > 30:
            q_1 = np.percentile(self.data.max().values, 25)
            q_2 = np.percentile(self.data.max().values, 50)
            q_3 = np.percentile(self.data.max().values, 75)
            q_4 = np.percentile(self.data.max().values, 100)
            dict_size = {
                'large': [q_2, q_4],
                'median': [q_1, q_3],
                'small': [10 ** 4, q_2]
            }
        else:
            q_1 = np.percentile(self.data.max().values, 10)
            q_2 = np.percentile(self.data.max().values, 20)
            q_3 = np.percentile(self.data.max().values, 80)
            q_4 = np.percentile(self.data.max().values, 90)
            q_5 = np.percentile(self.data.max().values, 100)
            dict_size = {
                'large': [q_2, q_5],
                'median': [q_1, q_4],
                'small': [10 ** 4, q_3]
            }

        data_i = self.data.copy()
        if self.target_company == 0:
            list_size = []

            def func_size(x):
                if (max(x) > dict_size[self.size][0]) and (max(x) < dict_size[self.size][1]):
                    list_size.append(x.name)

            data_i.apply(func_size)
            list_size = list_size[:min(len(list_size), 15)]
            data_i = data_i[list_size]

        def func_show(x):
            list1 = list(x.values)
            N = list1.count(0)
            #             res = reduce(lambda x, y: x * y, list1)
            #             x['if_show'] = np.sign(res)
            x['if_show'] = (list1.count(0) < len(list1) * 0.15) * 1
            return x

        data_i = data_i.apply(func_show, axis=1)
        data_i = data_i[data_i['if_show'] > 0]

        data_i['sum'] = data_i.sum(axis=1)
        data_i['growth'] = data_i['sum'].pct_change()

        data_i_1 = data_i.iloc[:int(0.95 * len(data_i)), :][
            data_i['growth'] > np.percentile(data_i['growth'].dropna().values, 5)]
        data_i_2 = data_i.iloc[int(0.95 * len(data_i)):, :]

        #         if len(data_i_1)>1500:
        #             data_i_1 = data_i_1[data_i_1['growth']>np.percentile(data_i_1['growth'].dropna().values,10)]
        #         elif len(data_i_1)>900:
        #             data_i_1  = data_i_1[data_i_1['growth']>np.percentile(data_i_1['growth'].dropna().values,5)]
        #         else:
        #             pass
        data_i = pd.concat([data_i_1, data_i_2])
        data_i.drop(['if_show', 'growth', 'sum'], axis=1, inplace=True)
        data_i = data_i.multiply(1 / 1000000)

        for i in range(50):
            data_i = data_i.append(data_i.iloc[-1])
        print(data_i)

        bcr.bar_chart_race(
            df=data_i,
            filename=self.dict_vedio_path['origin'],
            orientation='h',
            sort='desc',
            n_bars=None,
            fixed_order=False,
            fixed_max=False,
            steps_per_period=30,
            period_length=int(18000 / len(data_i)),
            interpolate_period=True,
            label_bars=True,
            bar_size=.90,
            period_label={'x': .98, 'y': .3, 'ha': 'right', 'va': 'center', 'size': 36, 'c': 'grey'},
            period_fmt='%Y-%m-%d',
            #         period_fmt='%B %d, %Y',
            period_summary_func=lambda v, r: {'x': .98, 'y': .22,
                                              's': f'融资总额(百万): {v.sum():,.2f}',
                                              'ha': 'right', 'size': 28, 'c': 'grey'},
            #         def func(values, ranks):
            #             total = values.sum()
            #             s = f'Worldwide deaths: {total}'
            #             return {'x': .85, 'y': .2, 's': s, 'ha': 'right', 'size': 11}
            perpendicular_bar_func=False,
            #         def func(values, ranks):
            #             return values.quantile(.75)
            #         figsize=(9, 16),
            figsize=(15.625 / 2, 33.833 / 2),
            cmap='video_colors',
            title='%s业估值动态(百万)' % (self.vedio_name),
            title_size=32,
            bar_label_size=20,
            tick_label_size=20,
            #         shared_fontdict= {
            #             'family' : 'SimHei',
            #             'weight' : 'bold',
            #             'color' : 'rebeccapurple'
            #         },
            scale='linear',
            fig=None,
            writer=None,
            dpi=250,
            bar_kwargs={
                'alpha': .4,
                'ec': 'white',
                'lw': 1.5
            },
            filter_column_colors=True
        )

    def add_logo(self):
        logo_path = os.path.join(self.abs_path_0, "logo.png")
        #         video_path =self.abs_path_1+self.file_name+'-'+self.size+'.mp4'
        #         video_path_1 =self.abs_path_1+self.file_name+'-'+self.size+'_logo'+'.mp4'

        video = VideoFileClip(self.dict_vedio_path['origin']).subclip()
        # 图片logo
        logo = (ImageClip(logo_path).set_duration(video.duration)  # 水印持续时间
                .resize(height=600)  # 水印的高度，会等比缩放
                .margin(right=0, bottom=0, opacity=1)  # 水印边距和透明度
                .set_pos(("right", "bottom")))  # 水印的位置

        final = CompositeVideoClip([video, logo])
        # mp4文件默认用libx264编码， 比特率单位bps
        final.write_videofile(self.dict_vedio_path['logo'], codec='mpeg4', fps=25, bitrate="10000000")

    def add_ending(self):
        ending_path = os.path.join(self.abs_path_0, 'ending.mp4')
        # 拼接，'compose'表示不管各种视频大小，以最大为基础
        v = VideoFileClip(self.dict_vedio_path['logo'])
        v_end = VideoFileClip(ending_path).resize(v.size)
        final_clip = concatenate_videoclips([v, v_end], method="compose", transition=None, bg_color=None, ismask=False,
                                            padding=0)
        final_clip.write_videofile(self.dict_vedio_path['end'], audio_codec='aac', fps=25, codec='mpeg4',
                                   bitrate="10000000", remove_temp=False)

    def add_text(self, img_url='', text='服务业估值动态', left=70, top=140, textColor=(0, 0, 0), textSize=50):

        img = cv2.imread(img_url)
        if (isinstance(img, numpy.ndarray)):  # 判断是否OpenCV图片类型
            img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        # 创建一个可以在给定图像上绘图的对象
        draw = ImageDraw.Draw(img)
        # 字体的格式
        fontStyle = ImageFont.truetype(
            self.FONT_URL, textSize, encoding="utf-8")
        # 绘制文本
        draw.text((left, top), text, textColor, font=fontStyle)
        img = cv2.cvtColor(numpy.asarray(img), cv2.COLOR_RGB2BGR)
        cv2.imwrite(img_url, img)

    def add_cover(self, if_text=1):

        video = VideoFileClip(self.dict_vedio_path['end'])
        if if_text:

            txt_clip = (TextClip(txt=self.vedio_name + "业" + "\n" + "估值动态",
                                 fontsize=(video.size[0] * 0.7) / (len(self.vedio_name) + 1), color='steelblue',
                                 font=self.FONT_URL)
                        .set_position('center')  # 随着时间移动
                        .set_duration(0.3))  # 水印持续时间
            result = CompositeVideoClip([video, txt_clip])
        else:
            if self.img_text:
                add_text(img_url=self.img_url, text=self.img_text, left=70, top=140, textColor=(0, 0, 0), textSize=50)

            img = (ImageClip(img_url)
                   .set_duration(1)  # 时长
                   .resize(width=video.size[0], height=video.size[1])  # 水印高度，等比缩放
                   .margin(left=10, top=10, opacity=0.8)  # 水印边距和透明度
                   .set_pos(("center")))
            result = CompositeVideoClip([video, img])

        result.write_videofile(self.dict_vedio_path['cover'], fps=25, bitrate="10000000")

    def add_music(self):
        video_clip = VideoFileClip(self.dict_vedio_path['cover'])
        # 提取视频对应的音频，并调节音量
        video_audio_clip = video_clip.audio.volumex(0.8)

        for root, dirs, files in os.walk(self.music_dir):
            bgms = files
        bgm = bgms[np.random.randint(0, len(bgms))]

        audio_clip = AudioFileClip(r'D:\project_zhoutianlian\financing_data_process\bgm\%s' % (bgm)).volumex(0.5)
        # 设置背景音乐循环，时间与视频时间一致
        audio = afx.audio_loop(audio_clip, duration=video_clip.duration)
        # 视频声音和背景音乐，音频叠加
        audio_clip_add = CompositeAudioClip([video_audio_clip, audio])

        # 视频写入背景音
        final_video = video_clip.set_audio(audio_clip_add)
        final_video.write_videofile(self.dict_vedio_path['music'], fps=28, bitrate="10000000")



if __name__ == '__main__':
    mv = MakeVedio(tag='上海人工智能', size='large', file_name='shanghaiAI', vedio_name='上海人工智能', target_company=1,
                   music='Nothing to fear')
    mv.data2bar_chart()
    mv.add_logo()
    mv.add_ending()
    mv.add_cover()
    mv.add_music()

# 依据tag热门程度批量循环生成视频
    df_tag = pd.read_csv('D:/project_zhoutianlian/financing_data_process/tags.csv', encoding='gb18030', index_col=0)
    df_tag = df_tag[df_tag['if_show'] == 1]
    df_tag = df_tag[['tag', 'file']]
    for i, r in df_tag.iterrows():
        t = r['tag']
        f = r['file']

        try:
            for s in ['large', 'median', 'small']:
                print(t, f, s)
                mv = MakeVedio(tag=t, size=s, file_name=f, vedio_name=t, target_company=0)
                mv.data2bar_chart()
                mv.add_logo()
                mv.add_ending()
                mv.add_cover()
        except:
            print('err')
