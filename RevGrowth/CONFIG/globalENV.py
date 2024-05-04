# -*- coding: utf-8 -*-：
import os
from CONFIG.global_V import IP_CONFIG
from CONFIG.Config import config

class global_var:
    name = config['DEFAULT']['sql_host']
    user = config['DEFAULT']['sql_user']
    password = config['DEFAULT']['sql_password']
    url = config['DEFAULT']['DNS_url']
    # config['DEFAULT']['java_url']
    # url = "https://rdt.modgo.pro/code/"
    img_url = config['DEFAULT']['DNS_img']
    path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    conf = path + config['DEFAULT']['path']
    neo_host = config['DEFAULT']['neo_host']
    neo_port = config['DEFAULT']['neo_port']
    neo_user = config['DEFAULT']['neo_user']
    neo_pw = config['DEFAULT']['neo_pw']
    port = config['DEFAULT']['mongo_port']


# 对于每个全局变量，都需要定义get_value和set_value接口
def set_name(name):
    if name == 'product':
        global_var.name = IP_CONFIG.PRODUCT
        # global_var.user = IP_CONFIG.PRODUCT
        # global_var.password = IP_CONFIG.PRODUCT
    elif name == 'test':
        global_var.name = IP_CONFIG.TEST
        global_var.user = config['DEFAULT']['sql_user']
        global_var.password = config['DEFAULT']['sql_password']
        global_var.url = "https://rdt.modgo.pro/code/"
        global_var.img_url = "http://192.168.2.108:8888/"
        global_var.conf = global_var.path + "/Data/fdfs_client/client.conf"
        global_var.neo_host = config['DEFAULT']['neo_host']
        global_var.neo_port = config['DEFAULT']['neo_port']
        global_var.neo_user = config['DEFAULT']['neo_user']
        global_var.neo_pw = config['DEFAULT']['neo_pw']
    elif name == 'develop':
        global_var.name = IP_CONFIG.DEVELOP
        global_var.user = "rdt"
        global_var.password = "rdt"
        global_var.url = "https://rdt.modgo.pro/code/"
        global_var.img_url = "http://192.168.2.108:8888/"
        global_var.conf = global_var.path + "/Data/fdfs_client/client_develop.conf"
        global_var.neo_host = "192.168.2.105"
        global_var.neo_port = "7474"
        global_var.neo_user = "neo4j"
        global_var.neo_pw = "FuYou123"


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

