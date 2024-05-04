"""
读取数据库数据，计算行业数据
结合输入的企业财务及市场数据
转为模型输入项
"""
import pandas as pd
import numpy as np
from CONFIG import mongodb
from datetime import datetime
from time import time

from CONFIG.mongodb import read_mongo_columns
from tool import *
import os

class DataProcess():
    def __init__(self,data):
        self.data = data
        self.stock_code = data['stock_code']
        self.gics3 = data['gics3']
        self.gics4 = data['gics4']
        self.list_financial = ['rd_rev', 'delta_inv_rev',
                  'cff_in_a', 'cfi_in_a', 'c_fa_a',
                  'delta_advance_rev', 'delta_prepay_rev',
                  'ROA', 'DTA', 'delta_ni_rev', 'CR', 'AT', 'rev_growth',
                  'score', 'EV_factor', 'MA',
                  'delta_gics_p', 'delta_price',
                  'patent','found_time'
                  ]
        self.dict_field = {'rd_rev':0,'delta_inv_rev':1,
                 'cff_in_a':2,'cfi_in_a':3,'c_fa_a':4,
                 'delta_advance_rev':5,'delta_prepay_rev':6,
                 'ROA':7,'DTA':8,'delta_ni_rev':9,'CR':10,'AT':11,'rev_growth':12,
                 'score':13,'EV_factor':14,'MA':15,
                 'delta_gics_p':16,'delta_price':17,
              'patent':18,'found_time':19,
              'ROE_industry':20,'CR_industry':21,'AT_industry':22,'rev_growth_industry':23,
              'delta_PPI':24,'delta_GDP':25}
        self.list_financial_industry = ['roe_industry','cr_industry','at_industry','rev_growth_industry']
        self.list_macro = ['ppi', 'gdp']



    def cal_delta_gdp(self):
        df_gdp = get_mondgo('AM_origin', 'valuation_china_indicator_season',
                                  ['update_date', 'gdp_current_season'], 'delta_GDP')
        df_gdp = q2ttm_sum_macro(df_gdp, 'delta_GDP')
        df_gdp = cal_delta(df_gdp, 'delta_GDP',1)
        df_gdp = filter_outlier(df_gdp, ['delta_GDP'], scale=3)
        dict_gdp = df2dict(df_gdp)

        return dict_gdp

    def cal_delta_ppi(self):

        df_ppi = get_mondgo('AM_origin', 'valuation_china_indicator_year', ['update_date', 'ppi_pre_year1'],
                                  'delta_PPI')
        df_ppi = y2q_macro(df_ppi)
        df_ppi = cal_delta(df_ppi, 'delta_PPI',1)
        dict_ppi = df2dict(df_ppi)

        return dict_ppi

    def cal_roe_industry(self):
        df_equity = get_mondgo('AM_origin', 'valuation_list_company_fs_equity',  ['stock_code', 'update_date', 'e_totalshareholdersequity'], 'equity')
        df_equity= match_gics(df_equity, self.gics4)
        df_equity = q2ttm_avg(df_equity, 'equity')
        df_equity_industry = cal_industry_avg(df_equity, 'equity')
        df_ni = get_mondgo('AM_basement', 'is_netprofit_ttm', ['stock_code', 'update_date', 'is_netprofit_ttm'], 'NI')
        df_ni = match_gics(df_ni, self.gics4)
        df_ni_industry = cal_industry_avg(df_ni, 'NI')
        df_roe_industry = cal_is_bs(df_ni_industry, df_equity_industry, 'NI_industry', 'equity_industry', 'ROE_industry')
        df_roe_industry = df_roe_industry.drop_duplicates(subset=['update_date'], keep='first')
        df_roe_industry = filter_outlier(df_roe_industry, ['ROE_industry'], scale=3)
        dict_roe_industry = df2dict(df_roe_industry)

        return dict_roe_industry

    def cal_rev_growth_industry(self):
        df_rev = get_mondgo('AM_basement', 'is_operatingrevenue_ttm',['stock_code', 'update_date', 'is_operatingrevenue_ttm'], 'rev')
        df_rev= match_gics(df_rev, self.gics4)
        df_rev_industry = cal_industry_avg(df_rev, 'rev')
        df_rev_growth_industry = cal_delta(df_rev_industry,'rev_industry',4)
        df_rev_growth_industry.rename(columns={'rev_industry': 'rev_growth_industry'}, inplace=True)
        df_rev_growth_industry = filter_outlier(df_rev_growth_industry, ['rev_growth_industry'], scale=3)
        dict_rev_growth_industry = df2dict(df_rev_growth_industry)

        return dict_rev_growth_industry

    def cal_at_industry(self):
        df_rev = get_mondgo('AM_basement', 'is_operatingrevenue_ttm',['stock_code', 'update_date', 'is_operatingrevenue_ttm'], 'rev')
        df_rev = match_gics(df_rev, self.gics4)
        df_rev_industry = cal_industry_avg(df_rev, 'rev')
        df_asset = get_mondgo('AM_basement', 'a_totalasset', ['stock_code', 'update_date', 'a_totalasset'], 'asset')
        df_asset = q2ttm_avg(df_asset, 'asset')
        df_asset = match_gics(df_asset, self.gics4)
        df_asset_industry = cal_industry_avg(df_asset, 'asset')
        df_at_industry = cal_is_bs(df_rev_industry, df_asset_industry, 'rev_industry', 'asset_industry', 'AT_industry')
        df_at_industry = df_at_industry.drop_duplicates(subset=[ 'update_date'], keep='first')
        dict_at_industry = df2dict(df_at_industry)

        return dict_at_industry

    def cal_cr_industry(self):
        df_ca = get_mondgo('AM_origin', 'valuation_list_company_fs_currentasset',  ['stock_code', 'update_date', 'a_totalcurrentassets'], 'CA')
        df_ca = match_gics(df_ca, self.gics4)
        df_ca = q2ttm_avg(df_ca, 'CA')
        df_ca_industry = cal_industry_avg(df_ca, 'CA')
        df_cl = get_mondgo('AM_origin', 'valuation_list_company_fs_currentliability',  ['stock_code', 'update_date', 'l_totalcurrentliabilities'], 'CL')
        df_cl = q2ttm_avg(df_cl, 'CL')
        df_cl = match_gics(df_cl, self.gics4)
        df_cl_industry = cal_industry_avg(df_cl, 'CL')
        df_cr_industry = cal_bs_bs(df_ca_industry, df_cl_industry, 'CA_industry', 'CL_industry', 'CR_industry')
        df_cr_industry = filter_outlier(df_cr_industry, ['CR_industry'], scale=1)
        df_cr_industry = df_cr_industry.drop_duplicates(subset=['update_date'], keep='first')
        dict_cr_industry = df2dict(df_cr_industry)

        return dict_cr_industry


    def input_data(self):

        data_cat = np.zeros((1, 48))

        df_gics3_code = pd.read_csv(os.path.join(os.getcwd(),'static/gics3_code.csv'))
        gics3_code = df_gics3_code[df_gics3_code['gics3'] == self.gics3]['gics3_no'].values[0]

        df_gics4_code = pd.read_csv(os.path.join(os.getcwd(),'static/gics4_code.csv'))
        gics4_code = df_gics4_code[df_gics4_code['gics4'] == self.gics4]['gics4_no'].values[0]

        data_cat_g3 = data_cat + gics3_code
        data_cat_g4 = data_cat + gics4_code

        data_LSTM = np.zeros((1, 48, 26))
        for i in self.list_financial:
            for j in range(len(self.data[i])):
                data_LSTM[0, 47 - j,self.dict_field[i]] = self.data[i][j]

        dict_gdp = self.cal_delta_gdp()
        dict_ppi = self.cal_delta_ppi()
        dict_roe_industry =self.cal_roe_industry()
        dict_rev_growth_industry =self.cal_rev_growth_industry()
        dict_at_industry =self.cal_at_industry()
        dict_cr_industry = self.cal_cr_industry()


        for key, value in dict_gdp.items():
            if key<47:
                data_LSTM[0, 47 - key,self.dict_field['delta_GDP']] = value

        for key, value in dict_ppi.items():
            if key < 47:
                data_LSTM[0, 47 - key,self.dict_field['delta_PPI']] = value

        for key, value in dict_roe_industry.items():
            if key < 47:
                data_LSTM[0, 47 - key,self.dict_field['ROE_industry']] = value

        for key, value in dict_rev_growth_industry.items():
            if key < 47:
                data_LSTM[0,  47 - key,self.dict_field['rev_growth_industry']] = value

        for key, value in dict_at_industry.items():
            if key < 47:
                data_LSTM[0,  47 - key,self.dict_field['AT_industry']] = value


        for key, value in dict_cr_industry.items():
            if key < 47:
                data_LSTM[0, 47 - key,self.dict_field['CR_industry']] = value

        data_LSTM_conv = data_LSTM.reshape(1, 1, 1, 48, 26)


        return data_cat_g3,data_cat_g4,data_LSTM,data_LSTM_conv

