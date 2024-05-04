# -*- coding: utf-8 -*-：
import os
from pymongo import MongoClient
from config.globalENV import global_var

def get_file_id(path, name):
    path_sh = global_var.path + "/Data/fdfs_client/fdfs_upload_file "
    command = path_sh + global_var.conf + " " + path + name
    output = os.popen(command)
    file_id = output.read().strip()
    del_file(path)
    return file_id


def del_file(path):
    ls = os.listdir(path)
    for i in ls:
        c_path = os.path.join(path, i)
        if os.path.isdir(c_path):
            del_file(c_path)
        else:
            os.remove(c_path)
    os.rmdir(path)

def update_to_mongo(file_id, update_date):

    username = global_var.username
    password = global_var.password
    host = global_var.host
    port = global_var.port
    mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=modgo_quant' % \
                         (username, password, host, port)
    conn = MongoClient(mongo_uri)
    db = conn['modgo_quant']
    collection = db['financing_news']
    myquery = {'update_date':update_date}
    file_path = {'download_url': file_id}
    collection.update_one(myquery, {"$set":file_path})

