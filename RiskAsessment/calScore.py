# -*- coding: utf-8 -*-：
import pandas as pd
import numpy as np
import time, datetime

# businessInfo score
def create_df_businessInfo():
    x_bi = np.zeros(5)
    t_bi_0 = np.zeros(5)
    t_bi_1 = t_bi_0 + 1
    t_bi_2 = t_bi_0 + 2
    t_bi_3 = t_bi_0 + 3

    df_businessInfo = pd.DataFrame({'indicator': ['工商变更', '大股东变更', '股权变更', '对外投资--退出', '对外投资--新增'],
                                    'n1': x_bi, 'm1': x_bi, 'x1': x_bi, 't1': t_bi_1, 'k1': x_bi,
                                    'n2': x_bi, 'm2': x_bi, 'x2': x_bi, 't2': t_bi_2, 'k2': x_bi,
                                    'n3': x_bi, 'm3': x_bi, 'x3': x_bi, 't3': t_bi_3, 'k3': x_bi,
                                    'upper_limit': [40, 10, 10, 40, 0], 'lower_limit': [0, 0, 0, 0, -5],
                                    'score': t_bi_0})
    return df_businessInfo


def cal_gsbg(df, num=0):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_dgdbg(df, num=1):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] * df.loc[num, 'x{}'.format(i + 1)] / df.loc[
                num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_gqbg(df, num=2):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] * df.loc[num, 'k{}'.format(i + 1)] / df.loc[
                num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_tz_exit(df, alpha, miu, num=3):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


def cal_tz_add(df, alpha, miu, num=4):
    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


# devPotential score
def create_df_dev_potential():

    x_dp = np.zeros(9)
    t_dp = np.zeros(9)
    t_dp_1 = t_dp + 1
    t_dp_2 = t_dp + 2
    t_dp_3 = t_dp + 3

    # df_devPotential = pd.DataFrame(
    #     {'indicator': ['融资情况', '利好政策', '限制政策', '宏观经济增长率', '地区经济增长率', '行业经济增长率', '上市公告', '行业投资事件', '行业融资金额'],
    #      'n1': x_dp, 'm1': x_dp, 'x1': x_dp, 't1': t_dp_1, 'k1': x_dp, 'g1': x_dp,
    #      'n2': x_dp, 'm2': x_dp, 'x2': x_dp, 't2': t_dp_2, 'k2': x_dp, 'g2': x_dp,
    #      'n3': x_dp, 'm3': x_dp, 'x3': x_dp, 't3': t_dp_3, 'k3': x_dp, 'g3': x_dp,
    #      'upper_limit': [30, 0, 50, 0, 0, 0, 0, 10, 10], 'lower_limit': [-10, -5, 0, -5, -8, -10, -10, -5, -5],
    #      'score': x_dp})
    # return df_devPotential

    df_devPotential = pd.DataFrame(
        {'indicator': ['融资情况', '利好政策', '限制政策', '宏观经济增长率', '地区经济增长率', '行业经济增长率', '上市公告', '行业投资事件', '行业融资金额'],
         'upper_limit': [30, 0, 50, 0, 0, 0, 0, 10, 10], 'lower_limit': [-10, -5, 0, -5, -8, -10, -10, -5, -5],
         'score': x_dp})
    return df_devPotential


def cal_rzqk(df,data, alpha, miu, num=0):
    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + (v['m'] / alpha) * v['n'] / np.exp((year_now-i) * miu)
        #
        # for i in range(3):
        #     s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
        #         df.loc[num, 't{}'.format(i + 1)] * miu)
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_lhzc(df, num=1):
    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_xzzc(df, num=2):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_hgjjzzl(df,data, num=3):
    s = 0

    try:
        for i,v in data.iterrows:
            s = s+v['delta_gdp']
        s = s*(-5)

        #s = (df.loc[num, 'k3'] + df.loc[num, 'k2'] + df.loc[num, 'k1']) * (-5)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_dqjjzzl(df,data, num=4):
    s = 0
    try:
        s = data * (-10)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_hyjjzzl(df, num=5):

    try:
        s = (df.loc[num, 'k3'] + df.loc[num, 'k2'] + df.loc[num, 'k1']) * (-10)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_ssgg(df, num=6):

    s = 0
    try:
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = -10
    return s


def cal_hytzsl(df, num=7):

    try:
        if df.loc[num, 'n3'] != 0:
            s = -(df.loc[num, 'n3'] - ((df.loc[num, 'n1'] + df.loc[num, 'n2'] + df.loc[num, 'n3']) / 3)) / df.loc[
                num, 'n3']
        else:
            s = 0
            s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                    s < df.loc[num, 'lower_limit']) * \
                df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_hyrzje(df,data, num=8):

    try:
        avg = np.mean(data)

        #avg = ((df.loc[num, 'm1'] + df.loc[num, 'm2'] + df.loc[num, 'm3']) / 3)
        if avg != 0:
            s = -(df.loc[num, 'm3'] - avg) / df.loc[num, 'm3']
        else:
            s = 0
            s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                    s < df.loc[num, 'lower_limit']) * \
                df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def create_df_opRisk():
    x = np.zeros(16)
    t = np.zeros(16)
    t_1 = t + 1
    t_2 = t + 2
    t_3 = t + 3

    df_opRisk = pd.DataFrame(
        {'indicator': ['经营异常', '行政处罚', '严重违法', '股权出质', '股权质押', '税收违法', '动产抵押', '欠税公告', '清算信息', '土地抵押',
                       '简易注销', '行政许可', '税务评级', '抽查检查--不合格', '进出口信用', '购地信息'],
         'n1': x, 'm1': x, 't1': t_1, 'k1': x, 'g1': x,
         'n2': x, 'm2': x, 't2': t_2, 'k2': x, 'g2': x,
         'n3': x, 'm3': x, 't3': t_3, 'k3': x, 'g3': x,
         'upper_limit': [5, 5, 20, 5, 5, 10, 5, 10, 50, 10, 50, 10, 0, 10, 5, 0],
         'lower_limit': [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -20, 0, 0, -10], 'score': x})
    return df_opRisk


def cal_jyyc(df, num=0):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_xzcf(df, alpha, miu, num=1):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_yzwf(df, num=2):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = 10 * (np.log((s != 0) * s + (s == 0) * 1))
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_gqcz(df, alpha, miu, num=3):
    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_gqzy(df, num=4):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] * df.loc[num, 'k{}'.format(i + 1)] / df.loc[
                num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_sswf(df, alpha, miu, num=5):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_dcdy(df, alpha, miu, num=6):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_qsgg(df, alpha, miu, num=7):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_qsxx(df, num=8):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = 10 * s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_tddy(df, alpha, miu, num=9):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_jyzx(df, num=10):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = 10 * s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_xzxk(df, num=11):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_swpj(df, num=12):
    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'g{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_ccjc(df, num=13):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = 10 * s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_jckxx(df, num=14):

    s = 0
    try:
        for i in range(3):
            s = s + df.loc[num, 'g{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_gdxx(df, alpha, miu, num=15):

    s = 0
    try:
        for i in range(3):
            s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
                df.loc[num, 't{}'.format(i + 1)] * miu)
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


# calculate leagal risk
def create_df_legalRisk():

    x = np.zeros(10)
    t = np.zeros(10)
    t_1 = t + 1
    t_2 = t + 2
    t_3 = t + 3
    # df_legalRisk = pd.DataFrame(
    #     {'indicator': ['被告', '原告', '胜诉', '败诉', '被执行人', '失信被执行人', '民事诉讼', '刑事诉讼', '行政诉讼', '合同纠纷'],
    #      'n1': x, 't1': t_1, 'm1': x,
    #      'n2': x, 't2': t_2, 'm2': x,
    #      'n3': x, 't3': t_3, 'm3': x,
    #      'upper_limit': [5, 5, 0, 10, 10, 10, 10, 20, 10, 20], 'lower_limit': [0, -5, -5, 0, 0, 0, 0, 0, 0, 0],
    #      'score': x})
    df_legalRisk = pd.DataFrame(
        {'indicator': ['被告', '原告', '胜诉', '败诉', '被执行人', '失信被执行人', '民事诉讼', '刑事诉讼', '行政诉讼', '合同纠纷'],
         'upper_limit': [5, 5, 0, 10, 10, 10, 10, 20, 10, 20], 'lower_limit': [0, -5, -5, 0, 0, 0, 0, 0, 0, 0],
         'score': x})
    return df_legalRisk


def cal_bg(df,data, num=0):

    s = 0
    year_now = datetime.datetime.now().year
    try:

        for i,v in data.iterrows():


            s = s+v['plaintiffs'] / (year_now - i)**0.5
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_yg(df,data, num=1):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + v['defendants'] / (year_now - i) **0.5
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_ss(df,data, num=2):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_bs(df,data, num=3):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_bzxr(df,data, num=4):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                    s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_sxbzxr(df,data, num=5):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_msss(df,data, num=6):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + v['msss'] / (year_now - i) **0.5
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                    s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_xsss(df,data, num=7):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + v['xsss'] / (year_now - i) **0.5
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_xzss(df,data, num=8):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + v['xzss'] / (year_now - i) **0.5
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)] / df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_htjf(df,data, alpha, miu, num=9):

    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i, v in data.iterrows():
            s = s + v['htjf'] / (year_now - i) **0.5
        # for i in range(3):
        #     s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
        #         df.loc[num, 't{}'.format(i + 1)] * miu)
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def create_df_IP():

    x = np.zeros(6)
    t = np.zeros(6)
    t_1 = t + 1
    t_2 = t + 2
    t_3 = t + 3
    #
    # df_IP = pd.DataFrame({'indicator': ['商标信息', '专利信息', '软件著作权', '作品著作权', '知识产权合同纠纷', '无知识产权'],
    #                       'n1': x, 't1': t_1, 'm1': x,
    #                       'n2': x, 't2': t_2, 'm2': x,
    #                       'n3': x, 't3': t_3, 'm3': x,
    #                       'upper_limit': [0, 0, 0, 0, 50, 30], 'lower_limit': [-10, -10, -10, -10, 0, 0], 'score': x})
    df_IP = pd.DataFrame({'indicator': ['商标信息', '专利信息', '软件著作权', '作品著作权', '知识产权合同纠纷', '无知识产权'],
                          'upper_limit': [0, 0, 0, 0, 50, 30], 'lower_limit': [-10, -10, -10, -10, 0, 0], 'score': x})
    return df_IP


def cal_sbxx(df,data, num=0):

    s = 0
    try:
        for i,v in data.items():
            s = s+v

        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = -s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_zlxx(df, data,num=1):

    s = 0
    try:
        for i,v in data.items():
            s = s+v
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = -s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


def cal_rjzzq(df,data, num=2):

    s = 0
    try:
        for i,v in data.items():
            s = s+v
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = -s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


def cal_zpzzq(df,data, num=3):

    s = 0
    try:
        for i,v in data.items():
            s = s+v
        # for i in range(3):
        #     s = s + df.loc[num, 'n{}'.format(i + 1)]
        s = -s
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


def cal_zscqhtjf(df,data, alpha, miu, num=4):

    s = 0
    try:
        for i, v in data.items():
            s = s + v
            #for i in range(3):
            # s = s + (df.loc[num, 'm{}'.format(i + 1)] / alpha) * df.loc[num, 'n{}'.format(i + 1)] / np.exp(
            #     df.loc[num, 't{}'.format(i + 1)] * miu)
        s = 5 * np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


def cal_wzscq(df,data, num=5):

    try:
        for i, v in data.items():
            s = s + v
        s = 30*(s==0)
        # s = 30 * ((df.loc[num, 'n1'] + df.loc[num, 'n2'] + df.loc[num, 'n3']) == 0)*1
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * \
            df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * df.loc[num, 'upper_limit']
    except:
        s = 0

    return s


# calculate public opinion
def create_df_publicOpinion():

    x = np.zeros(6)
    t = np.zeros(6)
    t_1 = t + 1
    t_2 = t + 2
    t_3 = t + 3
    df_publicOpinion = pd.DataFrame(
        {'indicator': ['正面新闻--权威度', '正面新闻--热度', '正面新闻--相关性', '负面新闻--权威度', '负面新闻--热度', '负面新闻--相关性'],
         'upper_limit': [0, 0, 0, 40, 40, 20], 'lower_limit': [-10, -10, -10, 0, 0, 0], 'score': x})

    # df_publicOpinion = pd.DataFrame(
    #     {'indicator': ['正面新闻--权威度', '正面新闻--热度', '正面新闻--相关性', '负面新闻--权威度', '负面新闻--热度', '负面新闻--相关性'],
    #      'n1': x, 't1': t_1, 'g1': x,
    #      'n2': x, 't2': t_2, 'g2': x,
    #      'n3': x, 't3': t_3, 'g3': x,
    #      'upper_limit': [0, 0, 0, 40, 40, 20], 'lower_limit': [-10, -10, -10, 0, 0, 0], 'score': x})

    return df_publicOpinion

def cal_positive(df,data, num=0):
    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i,v in data.iterrows():
            s = s+v['n']/(year_now-i)**0.5
        # for i in range(3):
        #     s = s + (df.loc[num, 'n{}'.format(i + 1)])/ (df.loc[num, 't{}'.format(i + 1)] ** 0.5)
        s = -np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * \
            df.loc[num, 'upper_limit']
    except:
        s = 0
    return s


def cal_negative(df,data, num=0):
    s = 0
    year_now = datetime.datetime.now().year
    try:
        for i,v in data.iterrows():
            s = s+v['n']/(year_now-i)**0.5
        # for i in range(3):
        #     s = s + (df.loc[num, 'n{}'.format(i + 1)])/ df.loc[num, 't{}'.format(i + 1)] ** 0.5
        s = np.log((s != 0) * s + (s == 0) * 1)
        s = ((s <= df.loc[num, 'upper_limit']) & (s >= df.loc[num, 'lower_limit'])) * s + (
                s < df.loc[num, 'lower_limit']) * df.loc[num, 'lower_limit'] + (s > df.loc[num, 'upper_limit']) * \
            df.loc[num, 'upper_limit']
    except:
        s = 0
    return s
