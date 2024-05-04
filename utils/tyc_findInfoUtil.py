from asyncio import ensure_future as ef, new_event_loop as nel, set_event_loop as sel, wait

from CONFIG.mongodb import connect_mongo
from log import log


def findEnterprise(enterprise_name, table_name):
    res_list = []
    try:
        db = connect_mongo('tyc_data')
        db_base = db["tyc_data"]  # 连接dbName数据库，没有则自动创建
        data = db_base[table_name]  # 使用tableName集合，没有则自动创建
        query = {"$or": [{"name": enterprise_name}, {"enterpriseName": enterprise_name}]}
        data_list = data.find(query)
        if data_list is not None:
            for data in data_list:
                data['_id'] = str(data['_id'])
                res_list.append(data)

    except Exception as e:
        log.logger("获取Mongo中tyc_data库" + table_name + "表" + "企业数据失败,原因：" + str(e))
    finally:
        db.close()
    return res_list


def EnterpriseWholeData(enterprise_name):
    res = {}
    try:
        db = connect_mongo("tyc_data")
        db_base = db["tyc_data"]
        # query = {"$or": [{"name": enterprise_name}, {"enterpriseName": enterprise_name}]}
        # 暂时删除估值内没使用的表
        collection_names = ["AL", "FJP", "TM", "Patent", "CR", "CRW", "New", "Holder",
                            "TycEnterpriseInfo"]
        # collection_names = ["AL", "FJP", "TM", "Patent", "CR", "Holder", "TycEnterpriseInfo"]
        tasks = []
        loop = nel()
        sel(loop)
        for collection_name in collection_names:
            tasks.append(ef(get_data(db_base, collection_name, enterprise_name)))
        loop.run_until_complete(wait(tasks))
        for task in tasks:
            res.update(task.result())
        loop.close()
    except Exception as e:
        log.logger("获取Mongo中所有企业数据失败,原因：" + str(e))
    finally:
        db.close()
    return res


async def get_data(db, collection_name, enterprise_name):
    res = await get_collection(db, collection_name, enterprise_name)
    return res


async def get_collection(db, collection_name, enterprise_name):
    res = {}
    query = {"enterpriseName": enterprise_name}
    condition = {"_id": 0}
    if collection_name == "TycEnterpriseInfo":
        query = {"name": enterprise_name}
        condition = {"_id": 0, "_class": 0, "businessId": 0}

    collection_res = db[collection_name].find(query, condition)
    if collection_res:
        res[collection_name] = list(collection_res)
    else:
        res[collection_name] = []
    return res
