import os
import pandas as pd
from Patenthub import patenthub_main  # 专利信息
from zhilian import test2  # 招聘信息
from liepin import test5
from qianchengwuyou import qianchengwuyou
from shunqi import shunqi_main  # 工商信息
from tools.law_req import run
from log.log import logger

# 存放文件的地址
path = r'F:\python\execl'
filenames = os.listdir(path)
for filename in filenames:
    # print(filename)
    for i in filenames:
        # print(filename)
        print(i)  ##公司名单文件
        # if i==filename:
        exec_path = 'F:\python\execl\\' + i
        f = open(exec_path, 'rb')
        data = pd.read_excel(f)  # 到此处已是循环读取某文件夹下所有excel文件，下面是在循环中对读进来的文件进行统一的重复的一致的处理
        companyName_list = data['公司名称']
        row = 0
        for companyName in companyName_list:
            patenthub_main.Spider_Main(companyName).craw()  # 获取公司专利信息
            test2.getHtml(companyName).getInfo()  # 智联招聘
            test5.getHtml(companyName).getInfo()  # 猎聘网站
            qianchengwuyou.getHtml(companyName).getInfo()  # 前程无忧网站
            shunqi_main.Spider_Main(companyName).craw()  # 顺企网站
            run(companyName)  # 司法
            row += 1
            print('Row %d : %s' % (row, companyName))
            # break
