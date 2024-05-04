import os
import pandas as pd

# 存放文件的地址
path = r'F:\python\execl'
filenames = os.listdir(path)
# for filename in filenames:
#     for i in filenames:
#         print(i) ##公司名单文件
exec_path = 'F:\python\execl\\' + filenames[0]  # i
f = open(exec_path, 'rb')
data = pd.read_excel(f)  # 到此处已是循环读取某文件夹下所有excel文件，下面是在循环中对读进来的文件进行统一的重复的一致的处理

companyName_list = data['公司名称']
row = 0
for companyName in companyName_list:
    print(companyName)
