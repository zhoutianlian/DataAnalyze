
from pymongo import MongoClient
import pymongo
import pandas as pd

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