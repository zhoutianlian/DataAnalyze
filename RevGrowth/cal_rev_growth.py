"""
输入财务及市场数据
加载模型
输出结果
"""
import pandas as pd
import numpy as np

import os


from keras.models import load_model

from DataProcess import DataProcess


def cal_rev_growth(data):

    data_LSTM = DataProcess(data=data)
    data_cat_g3, data_cat_g4, data_LSTM, data_LSTM_conv = data_LSTM.input_data()

    LSTM = load_model(os.path.join(os.getcwd(), 'static/rev_growth_LSTM.dat'))
    y = LSTM.predict([data_cat_g3,data_cat_g4,data_LSTM,data_LSTM_conv])
    print(data_LSTM)

    y  = y[0,47,0]
    y = max(-0.8,y)
    y = min(1,y)

    return y
# 输入 1- 48 期财务数据
# 不足部分会补0
# 预测最后一期营业额增长率

if __name__ == '__main__':
    data = {"rd_rev": [0.1,0.2], # 研发费用/营业收入
     "delta_inv_rev": [0.2,0.3],# 存货变动/营业收入
     "cff_in_a": [0.03,0.04],# 融资现金流 流入/总资产
     "cfi_in_a": [0.2,0.4],# 投资现金流 流入/总资产
     "c_fa_a": [0.1,0.5], # 现金及金融资产/总资产
     "delta_advance_rev": [0.2,0.4], # 预收款变动/营业收入
     "delta_prepay_rev": [0.3,0.5], # 预付款变动/营业收入
     "ROA": [0.1,0.6],# 资产收益率
     "DTA": [0.2,0.6], # 资产负债率
     "delta_ni_rev": [0.3,0.7], # 净利润变动/营业收入
     "CR": [0.1,0.6], # 速动比率
     "AT": [0.2,0.6], # 资产周转率
     "rev_growth": [0.2,0.6], # 营业收入增长率
     "score": [0.3,0.6], # 情绪打分
     "EV_factor": [0.4,0.6], # 企业价值因素
     "MA": [0.2,0.6],
     "delta_gics_p": [0.1,0.6], # 相对于行业价格变动
     "delta_price": [0.2,0.6], # 价格变动
     "patent": [0.3,0.7], # 专利数量
     "found_time": [0.2,0.6], # 成立时间
    "stock_code":"000001.SZ",
     "gics3": 151040,
     "gics4": 15104050
     }


    y = cal_rev_growth(data)
    print(y)





