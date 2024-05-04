
from pymongo import MongoClient
import pymongo
import pandas as pd
from sqlalchemy import create_engine
from config.Config import config


def read_mongo(mongo_config,db_name, collection_name, conditions={}, query=[], sort_item='', n=0):

    mongo_uri = 'mongodb://%s:%s@%s:%s/' % mongo_config + '?authSource=%s'%(db_name)
    conn = MongoClient(mongo_uri)

    db = conn[db_name]
    collection = db[collection_name]
    if query:
        cursor = collection.find(conditions, query)
    else:
        cursor = collection.find(conditions)
    if sort_item:
        cursor = cursor.sort(sort_item, pymongo.DESCENDING)
    if n:
        cursor = cursor[:n]
    temp_list = []
    for single_item in cursor:
        temp_list.append(single_item)

    df = pd.DataFrame(temp_list)
    conn.close()
    return df
setting = dict(host=config['DEFAULT']['sql_host'] + ':%s' % (config['DEFAULT']['sql_port']),
                   user=config['DEFAULT']['sql_user'],
                   password=config['DEFAULT']['sql_password'],
                   database='modgo_quant')
def read_sql(self, setting,subset):


    engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % setting,
                           encoding='utf-8')

    sql = "select * from %s where YEAR(report_date) >= %d" % ('%s_q' % subset, 2020)
    sql_1 = "select * from %s where YEAR(report_date) >= %d" % ('%s_orig' % subset, 2020)
    try:
        df = pd.read_sql(sql, engine)
    except:
        df = pd.read_sql(sql_1, engine)
    df = df.drop_duplicates(subset=['stock_code', 'report_date'])

    del df['id']
    if 'index' in df.columns:
        del df['index']
    print(subset, len(df['stock_code'].unique()))
    return df


def save_mongo(mongo_config, df_data, db_name, tb_name, if_del=0, del_condition={},keys=[]):
    mongo_uri = 'mongodb://%s:%s@%s:%s/' % mongo_config + '?authSource=%s' % (db_name)
    conn = MongoClient(mongo_uri)

    db = conn[db_name]
    collection = db[tb_name]
    if if_del:
        if del_condition:
            collection.delete_many(del_condition)
        else:
            collection.delete_many({})
    list_data = df_data.to_dict('record')
    for i in range(len(list_data)):
        d = list_data[i]
        if '_id' in d.keys():
            del d['_id']
        print(d)
        c = []
        for k in keys:
            c.append({k: d[k]})
        condition = {'$and': c}

        result = collection.update_one(condition, {'$set': d}, upsert=True)
        print(result)