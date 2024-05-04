import requests
import numpy as np
# from config import *
import pandas as pd
import time
import datetime
from datetime import timedelta
from database_fs.CONFIG.Config import config
from database_fs.to_mysql import add_new_fs
from sqlalchemy import create_engine
from pymysql import connect

from database_mkt.utils import GetFSData



def cal_profitability():
    list_p = ['stock_code', 'report_date', 'ROE1', 'ROE2', 'ROIC1', 'ROIC2', ]

    def func(x):
        x = x.sort_values(by='report_date')
        x['ROE1'] = x['net_prft'] / x['tot_eq']
        x['ROE2'] = x['list_com_prft'] / x['tot_eq_list_com']
        x['ROIC1'] = (x['net_prft'] * (1 - x['tax'] / (x['tax'] + x['net_prft'])+x['int_cost'])
                      - x['dec_of_defrd_tax_ast'] + x['inc_of_defrd_tax_liab']) / (
                             x['tot_eq']
                             + x['borw_from_peer']
                             + x['bond_payb']
                             + x['st_loan']
                             + x['lt_loan']
                             + x['der_fin_liab']
                     )
        x['ROIC2'] = (x['net_prft'] * (1 - x['tax'] / (x['tax'] + x['net_prft'])+x['int_cost'])
                      - x['dec_of_defrd_tax_ast'] + x['inc_of_defrd_tax_liab']) / (
                            x['tot_eq']
                            + x['borw_from_peer']
                            + x['bond_payb']
                            + x['st_loan']
                            + x['lt_loan']
                            + x['der_fin_liab']
                             - x['tot_cash']
                     )

        if 'stock_code' in x.columns.tolist():
            del x['stock_code']
        if 'level_1' in x.columns.tolist():
            del x['level_1']
        return x
    gd = GetFSData()

    df = gd.merge(target=['bs_ttm', 'ins_ttm','cfs_ttm'], if_mkt=0, if_e=0)
    df = df.groupby('stock_code').apply(func)

    df = df.reset_index()

    df = df[list_p]

    return df

def cal_standardized_ratio():
    def func(x):
        x['ROE'] = x['list_com_prft']/x['tot_eq_list_com']
        x['ROE'] = x['ROE'].rolling(window=20, min_periods=1).mean()
        x['NI_std'] = x['tot_eq_list_com'] * x['ROE']
        x['MV_AMT'] = x['close'] * x['outstanding_share'] * x['amount']
        x['MV_std'] = x['MV_AMT'].rolling(window=90, min_periods=30).sum() / x['amount'].rolling(window=90, min_periods=30).sum()
        x['PE_std'] = x['MV_std']/x['NI_std']
        x['ATO'] = x['tot_oprt_incm']/x['tot_ast']
        x['ATO'] = x['ATO'].rolling(window=20, min_periods=1).mean()
        x['REV_std'] = x['tot_ast'] * x['ATO']

        if 'stock_code' in x.columns.tolist():
                del x['stock_code']
            if 'level_1' in x.columns.tolist():
                del x['level_1']
            return x
    gd = GetFSData()
    df = gd.merge(target=['bs_avg', 'ins_q',], if_mkt=1, if_e=0)
    df = df.groupby('stock_code').apply(func)




    """
    计算规范化净利润

ROE = 本Q归母净利润 / [(上Q归母权益 + 本Q归母权益)/2]
mean_ROE = 最近5年（20Q）的ROE均值(或中位数)
规范化净利润 = 每Q平均归母权益 * mean_ROE
规范化市盈率 = 下Q按交易额加权的平均市值 / 过去4Q规范化净利润合计

计算规范化营业收入

ATO (总资产周转率) = 本Q营业收入 / [(上Q总资产 + 本Q总资产)/2]
mean_ATO = 最近5年（20Q）的ATO均值(或中位数)
规范化营业收入 = 每Q平均总资产 * mean_ATO
规范化企业销售率 = (下Q按交易额加权的平均市值 + 少数股东权益 + 净债务) / 过去4Q规范化营业收入合计
    """
    """
    计算规范化EBIT(或EBITDA)

ROIC = 本QEBIT / 本Q平均投资资本面值 (带息债+权益)
mean_ROIC = 最近5年（20Q）的ROIC均值(或中位数)
规范化EBIT = 每Q平均ROIC * mean_ROIC
规范化企业息税折旧利润率 = (下Q按交易量加权的平均市值 + 少数股东权益 + 净债务) / 过去4Q规范化EBIT合计
    """


if __name__ == '__main__':
    cm = CalMultiplier()
    cm.cal_equity_multiplier()



