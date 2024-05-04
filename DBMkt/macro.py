"""
系列宏观数据每季度变化
"""
import requests
import pandas as pd
import numpy as np
import akshare as ak
import time
from time import sleep
import tushare as ts
import datetime
from threading import Thread

# macroLeverage
from database_mkt.utils import to_sql_replace


def economic_activity_leverage(n = 5):
    cnbs = ak.macro_cnbs()
    cnbs.columns = ['update_date','resident','nonfinancial_enterprise','government','central_government',
                    'local_government','entity_enterprise','financial_enterprise_credit','financial_enterprise_debt']

    cnbs['update_date'] = cnbs['update_date'].apply(lambda x:x+'-31' if x[-2:] in ['12','03'] else x+'-30')
    print(cnbs[:n])
    cnbs['update_date'] = cnbs['update_date'].apply(pd.Timestamp)

    to_sql_replace(table='macro_leverage', data=cnbs[:n])
    return cnbs

# macroGDP
def economic_activity_gdp(n = 1):
    gdp = pd.DataFrame(columns = ['update_date','gdp'])
    data = ak.macro_china_gdp_yearly()
    gdp['update_date'] = data.index
    gdp['gdp'] = data.values
    # gdp['update_date'] = gdp['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    print(gdp[:n])
    to_sql_replace(table='macro_gdp', data=gdp[:n])
    return gdp


# macroCPI
def economic_activity_cpi(n = 1):
    cpi = pd.DataFrame(columns = ['update_date','cpi'])
    data = ak.macro_china_cpi_monthly()
    cpi['update_date'] = data.index
    cpi['cpi'] = data.values
    # cpi['update_date'] = cpi['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    print(cpi[:n])
    to_sql_replace(table='macro_cpi', data=cpi[:n])
    return cpi


# macroPPI
def economic_activity_ppi(n = 1):
    ppi = pd.DataFrame(columns = ['update_date','ppi'])
    data = ak.macro_china_ppi_yearly()
    ppi['update_date'] = data.index
    ppi['ppi'] = data.values
    # ppi['update_date'] = ppi['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_ppi', data=ppi[:n])
    return ppi


# macroExports
def economic_activity_exports(n = 1):
    exports = pd.DataFrame(columns = ['update_date','exports'])
    data = ak.macro_china_exports_yoy()
    exports['update_date'] = data.index
    exports['exports'] = data.values
    # exports['update_date'] = exports['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_exports', data=exports[:n])
    return exports


# macroImports
def economic_activity_imports(n = 1):
    imports = pd.DataFrame(columns = ['update_date','imports'])
    data = ak.macro_china_imports_yoy()
    imports['update_date'] = data.index
    imports['imports'] = data.values
    # imports['update_date'] = imports['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_exports', data=imports[:n])
    return imports


# macroTradeBalance
def economic_activity_trade_balance(n = 1):
    balance = pd.DataFrame(columns = ['update_date','trade_balance'])
    data = ak.macro_china_trade_balance()
    balance['update_date'] = data.index
    balance['trade_balance'] = data.values
    # balance['trade_balance'] = balance['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_trade_balance', data=balance[:n])
    return balance


# macroIndustrialProduction
def productivity_industrial_production(n=1):
    production = pd.DataFrame(columns = ['update_date','industrial_production'])
    data = ak.macro_china_industrial_production_yoy()
    production['update_date'] = data.index
    production['industrial_production'] = data.values
    # production['update_date'] = production['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_industrial_production', data=production[:n])
    return production



# macroPMI
def productivity_pmi(n = 1):
    pmi = pd.DataFrame(columns = ['update_date','pmi'])
    data = ak.macro_china_industrial_production_yoy()
    pmi['update_date'] = data.index
    pmi['pmi'] = data.values
    # pmi['update_date'] = pmi['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_pmi', data=pmi[:n])
    return pmi



# macroFXReserve
def financial_index_fx_reserve(n = 1):
    reserve = pd.DataFrame(columns = ['update_date','fx_reserve'])
    data = ak.macro_china_fx_reserves_yearly()
    reserve['update_date'] = data.index
    reserve['fx_reserve'] = data.values
    # reserve['update_date'] = reserve['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_fx_reserve', data=reserve[:n])
    return reserve



# macroM2
def financial_index_m2(n = 1):
    m = pd.DataFrame(columns = ['update_date','m2'])
    data = ak.macro_china_m2_yearly()
    m['update_date'] = data.index
    m['m2'] = data.values
    # m['update_date'] = m['update_date'].apply(lambda x: x.year*10000+x.month*100 + x.day)
    to_sql_replace(table='macro_m2', data=m[:n])
    return m



# macroRealEstatePrice
def financial_index_real_eastate_price(n = 1):
    m = pd.DataFrame(columns = ['update_date','m2'])
    data = ak.macro_china_new_house_price()
    data = data[['日期','城市','新建住宅价格指数-定基','新建商品住宅价格指数-定基','二手住宅价格指数-定基']]
    data.columns = ['update_date','city','residential_building','commercial_residential_building','second_hand']
    # data['update_date'] = m['update_date'].apply(lambda x: pd.to_datetime(x).year*10000+pd.to_datetime(x).month*100 + pd.to_datetime(x).day)
    to_sql_replace(table='macro_real_eastate_price', data=m[:n])
    return data



def update_macro():
    threads = list()
    threads.append(Thread(target=economic_activity_leverage,args = ()))
    threads.append(Thread(target=economic_activity_gdp, args=()))
    threads.append(Thread(target=economic_activity_cpi, args=()))
    
    # economic_activity_ppi()
    # economic_activity_exports()
    # economic_activity_imports()
    # economic_activity_trade_balance()
    # productivity_industrial_production()
    # productivity_pmi()
    # financial_index_fx_reserve()
    # financial_index_m2()
    # financial_index_real_eastate_price()

    for thread in threads:
        thread.start()
if __name__ == "__main__":
    update_macro()
