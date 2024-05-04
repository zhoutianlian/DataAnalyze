import pymysql
import pandas as pd
# pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
from pymongo import MongoClient
from log.config import config
import time


class Clean_Data():

    def __init__(self):
        self.username = config['DEFAULT']['mongo_user_master']
        self.password = config['DEFAULT']['mongo_password_master']
        self.host = config['DEFAULT']['mongo_host_master']
        self.port = config['DEFAULT']['mongo_port_master']
        self.loacl_user = config['DEFAULT']['mongo_user']
        self.loacl_password = config['DEFAULT']['mongo_password']
        self.loacl_host = config['DEFAULT']['mongo_host']
        self.loacl_port = config['DEFAULT']['mongo_port']

    # 链接测试数据库
    def mongo_conn_master(self, db_name):
        # 测试
        self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=%s' % \
                         (self.username, self.password, self.host, self.port, db_name)
        conn = MongoClient(self.mongo_uri)

        # 本地
        # self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=admin' % \
        #                  (self.username, self.password, self.host, self.port)
        # conn = MongoClient(self.mongo_uri)
        return conn

    # 链接本地数据库
    def mongo_conn_local(self):
        # 本地
        self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=admin' % \
                         (self.loacl_user, self.loacl_password, self.loacl_host, self.loacl_port)
        conn = MongoClient(self.mongo_uri)

        # 本地
        # self.mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=admin' % \
        #                  (self.username, self.password, self.host, self.port)
        # conn = MongoClient(self.mongo_uri)
        return conn

    # 从mongo中读取数据
    def read_mongo(self, conn, db_name, collection_name, query):
        db = conn[db_name]
        collection = db[collection_name]
        data = collection.find(query)
        df = pd.DataFrame(list(data))
        return df

    # 数据插入
    def inset_data(self, conn, db_name, collection_name, data):
        db = conn[db_name]
        collection = db[collection_name]
        collection.insert_one(data)
        print('数据插入成功')


    def data_clean(self, ):
        local_conn = self.mongo_conn_local()
        local_df = self.read_mongo(local_conn, 'spider_origin', 'ShuidiXinyong', query={})
        print('读取数据完毕，准备关闭链接')
        local_conn.close()
        conn = self.mongo_conn_master('tyc_data')
        for index, row in local_df.iterrows():

            data_dict = row.to_dict()
            name = data_dict['company_name']
            print(name)
            query = {'name': name}
            # 按条件查询被清洗数据库数据
            # df = self.read_mongo(conn=conn, db_name='tyc_data', collection_name='TycEnterpriseInfo', query=query)
            df = self.read_mongo(conn=conn, db_name='tyc_data', collection_name='SearchInfo', query=query)
            print(df.empty)
            if df.empty:
                print('未查询到数据')
                try:
                    print(data_dict['Reg_capital'])
                    Reg_capial = ''.join([n for n in data_dict['Reg_capital'] if n.isdigit()])
                    print(Reg_capial)
                    Reg_capial = int(Reg_capial)
                except:
                    print('无注册资本')
                    Reg_capial = 0
                if Reg_capial:
                    if data_dict['management_forms']:
                        data_dict['management_forms'] = data_dict['management_forms'].strip()
                    if data_dict['Staff_size'] == '企业选择不公示':
                        data_dict['Staff_size'] = '-'
                    try:
                        if data_dict['Con_capital'] == '-':
                            pass
                        else:
                            float(data_dict['Con_capital'])
                            data_dict['Con_capital'] = None
                    except:
                        if data_dict['Con_capital']:
                            Con_capial = ''.join([n for n in data_dict['Con_capital'] if n.isdigit()])
                            Con_capial = float(Con_capial)
                            if not Con_capial:
                                data_dict['Con_capital'] = None

                    # 插入数据
                    if data_dict['nip']:
                        if data_dict['nip'] != '-':
                            try:
                                data_dict['nip'] = int(data_dict['nip'].replace('人', ''))
                            except:
                                data_dict['nip'] = None
                        else:
                            data_dict['nip'] = None
                    else:
                        data_dict['nip'] = None
                    staffList = None
                    # legalPersonName = None
                    if str(data_dict['team']) != 'nan':
                        staffList = {
                            'total': len(data_dict['team']),
                        }
                        result = []
                        for people in data_dict['team']:
                            staff_dict = {
                                'name': people['name'],
                                'type': 2,
                                'typeJoin': [
                                    people['position']
                                ]
                            }
                            result.append(staff_dict)
                            # if str(people['position']) != 'nan' and people['position']:
                            #     position = people['position'].split('&')
                            #     if ('董事长' in position) or ('创始人' in position):
                            #         legalPersonName = people['name']

                        staffList['result'] = result
                    data_dict['Reg_capital'] = data_dict['Reg_capital'].replace(',', '')
                    df_dict = {
                        'regNumber': data_dict['regNumber'],
                        'historyNames': data_dict['companyname_used'],
                        'regCapital': data_dict['Reg_capital'],
                        'regStatus': data_dict['management_forms'],
                        'socialStaffNum': data_dict['nip'],
                        'industry': data_dict['industry'],
                        'name': data_dict['company_name'],
                        'isSavedOther': 0,
                        'regLocation': data_dict['Business_address'],
                        'actualCapital': data_dict['Con_capital'],
                        'estiblishTime': data_dict['establish_time'],
                        'creditCode': data_dict['Uscc'],
                        'orgNumber': data_dict['Organization_code'],
                        'taxNumber': data_dict['Uscc'],
                        'companyOrgType': data_dict['enterprises_types'],
                        'city': data_dict['Region'],
                        'regInstitute': data_dict['registration_authority'],
                        'staffNumRange': data_dict['Staff_size'],
                        # 'regLocation': data_dict['Business_address'],
                        'businessScope': data_dict['Nature_Business'],
                        # '': data_dict['company_profile']
                        'staffList': staffList,
                        'legalPersonName': data_dict['legalPersonName'],
                        'cancelDate': 0,
                        'source': 5,
                }
                #     df_dict = {
                #         'name': data_dict['company_name'],
                #         '_class': 'python',
                #         'companyType': 1,
                #         'type': 1,
                #         'regCapital': data_dict['Reg_capital'],
                #         'base': data_dict['Region'],
                #         'estiblishTime': data_dict['establish_time'],
                #         'legalPersonName': data_dict['legalPersonName'],
                #         'regStatus': data_dict['management_forms'],
                #         'creditCode': data_dict['Uscc'],
                #         'orgNumber': data_dict['Organization_code'],
                #
                #     }
                #     self.inset_data(conn=conn, db_name='tyc_data', collection_name='TycEnterpriseInfo', data=df_dict)
                    self.inset_data(conn=conn, db_name='tyc_data', collection_name='SearchInfo', data=df_dict)

            time.sleep(2)

        conn.close()


D = Clean_Data()
D.data_clean()
