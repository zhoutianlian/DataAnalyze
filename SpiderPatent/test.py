from pymongo import MongoClient
import pandas as pd

from CONFIG import globalENV as gl
conn = MongoClient('mongodb://hub:hubhub@%s:%s/' % (gl.get_name(), gl.get_port()))
db = conn.spider_origin  # 连接spider_origin数据库，没有则自动创建
patent_hub = db.patenthub_data  # 使用patenthub_data集合，没有则自动创建
a = patent_hub.find().batch_size(5000)
temp_list = []
count = 0
for i in a:
    if count % 1000 == 0:
        print(count)
    temp_list.append(i)
    count += 1
a = pd.DataFrame(temp_list)
name = list(a["company_name"])
print("西藏珠峰资源股份有限公司" in name)
