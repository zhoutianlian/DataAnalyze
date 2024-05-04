"""
当日新闻数据获取及历史数据批量写入数据库接口调用

"""
# -*- coding: utf-8 -*-：
import sys
import os
import traceback

from generate_news_origin import SaveNews

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
from flask import Flask, request
import datetime
import json
from config.log import logger
from generate.generate_news import ReadNews
import logging

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def index():
    return '首页'
"""
传入参数
日期 20200101
获取当天新闻数据
"""
@app.route('/news/financingnews', methods=["GET"])
def get_financing_news():
    response = ""
    try:
        t = int(request.args.get("date"))
        if not t:
            t = datetime.datetime.now()
            t = t.year*10000 + t.month*100 +t.day

        RN = ReadNews(update_date=t)
        ret = RN.read()
        # audio_url = RN.get_audio()
        # ret['audio_url'] = audio_url

        response = json.dumps(ret, ensure_ascii=False)
        logger(response)
        return response

    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        return response
"""
传入参数 
日期 20200101
页码 1
获取该日期向前的历史新闻数据
"""
@app.route('/news/financingnewshist', methods=["GET"])
def get_financing_news_hist():
    response = ""
    try:
        t = int(request.args.get("date"))
        p = int(request.args.get("page"))
        if not t:
            t = datetime.datetime.now()
            t = t.year*10000 + t.month*100 +t.day

        RN = ReadNews(update_date=t,page = p)
        ret = RN.hist()
        response = json.dumps(ret, ensure_ascii=False)
        return response

    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        return response

# 传入追溯天数，读取financing_event，向financing_news存入新闻数据
# 当数据断更几天的时候，手动调用，存储
@app.route('/news/makeupfinancingnews', methods=["GET"])
def makeup_financing_news():
    response = "Fail"
    try:
        N = int(request.args.get("ndays"))
        SN = SaveNews()
        SN.save(n = N)

        response = 'Success'
        logger(response)
        return response

    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        return response

if __name__ == '__main__':
    sys.argv = [sys.argv[0]]
    logging.basicConfig(level=logging.INFO,  # 控制台打印的日志级别
                        filename='config/log.txt',  # 将日志写入log.txt文件中
                        filemode='a',  ##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                        # a是追加模式，默认如果不写的话，就是追加模式
                        format=
                        '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                        # 日志格式
                        )
    app.run(host="0.0.0.0", port=7960, threaded=True)

    # RN = ReadNews(update_date=20210329, page=1)
    # ret = RN.read()
    # print(ret)


