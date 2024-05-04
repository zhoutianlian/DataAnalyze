# -*- coding: utf-8 -*-：
import os
from config.Config import config
from config.global_V import IP_CONFIG


class global_var:
    username = config['DEFAULT']['mongo_user']
    password = config['DEFAULT']['mongo_password']
    host = config['DEFAULT']['mongo_host']
    port = config['DEFAULT']['mongo_port']
    #java调用地址
    url = "https://www.modgo.pro/code/"
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    conf = path + "/setting/client.conf"
    # name = 'develop'
    # user = 'rdt'
    # password = 'Fu3You3R0D6t'
    # #java调用地址
    # url = "http://192.168.1.67:8011/code/"
    # img_url = "http://172.19.221.241:8888/"
    # path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    # conf = path + "/Data/fdfs_client/client_pro.conf"
    # neo_host = "172.19.221.241"
    # neo_port = "7474"
    # neo_user = "neo4j"
    # neo_pw = "FuYou123"
    # port = 27017


# 对于每个全局变量，都需要定义get_value和set_value接口
def set_name(name):
    if name == 'product':
        global_var.name = IP_CONFIG.PRODUCT
        # global_var.user = IP_CONFIG.PRODUCT
        # global_var.password = IP_CONFIG.PRODUCT
    elif name == 'test':
        global_var.name = IP_CONFIG.TEST
        global_var.user = "rdt"
        global_var.password = "rdt"
        global_var.url = "https://rdt.modgo.pro/code/"
        global_var.img_url = "http://192.168.2.108:8888/"
        global_var.conf = global_var.path + "/Data/fdfs_client/client.conf"
        global_var.neo_host = "192.168.2.105"
        global_var.neo_port = "7474"
        global_var.neo_user = "select"
        global_var.neo_pw = "select"
    elif name == 'develop':
        global_var.name = IP_CONFIG.DEVELOP
        global_var.user = "rdt"
        global_var.password = "rdt"
        global_var.url = "https://rdt.modgo.pro/code/"
        global_var.img_url = "http://192.168.2.108:8888/"
        global_var.conf = global_var.path + "/Data/fdfs_client/client_develop.conf"
        global_var.neo_host = "192.168.1.7"
        global_var.neo_port = "7474"
        global_var.neo_user = "neo4j"
        global_var.neo_pw = "python"


def get_name():

    return global_var.name


def get_user():

    return global_var.user


def get_password():

    return global_var.password


def get_port():
    return global_var.port


def get_img():
    return global_var.img_url

