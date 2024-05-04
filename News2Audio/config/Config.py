# -*- coding: utf-8 -*-：
import configparser
import os

config = configparser.ConfigParser()
config.read(r'setting/main.ini', encoding='utf-8')
# print(os.path.dirname(os.path.realpath(__file__)))
# print(os.path.abspath(os.path.join(os.getcwd(), "../")))
# # 创建管理对象
# conf = configparser.ConfigParser()
#
# # 读ini文件
# conf.read(cfgpath, encoding="utf-8")    # python3
#
# # conf.read(cfgpath)  # python2
#
# # 获取所有的section
# sections = conf.sections()
#
# print(sections)    # 返回list
#
# items = conf.items('email_163')
#
