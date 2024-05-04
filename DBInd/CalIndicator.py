import time
import datetime
from datetime import timedelta
from database_fs.CONFIG.Config import config
from database_fs.to_mysql import add_new_fs
from sqlalchemy import create_engine
from pymysql import connect

from database_fs.utils import GetFSData


def cal_equity_multiplier():
    list_em = ['stock_code', 'trade_date', 'report_date',
               'MVA', 'MVB', 'PE0A', 'PE0B', 'e_ni_parent', 'PE1A', 'PE1B', 'PCF0B', 'PEBT0B',
               'PB0A', 'PB0B', 'PTB0A', 'PTB0B', 'PFCFE0B', 'PS0B', 'PEG0A', 'PE0G0B', 'PE0G1B']

    def func(x):
        x = x.sort_values(by='trade_date')
        x['MVA'] = x['close'] * x['outstanding_share']
        x['MVB'] = x['close'] * x['outstanding_share'] + x['mino_eq']
        x['PE0A'] = x['MVA'] / x['list_com_prft']
        x['PE0B'] = x['MVB'] / x['net_prft']
        x['e_ni_parent'] = x['e_ni'] * x['list_com_prft'] / x['net_prft']
        x['PE1A'] = x['MVA'] / x['list_com_prft']
        x['PE1B'] = x['MVA'] / x['e_ni']
        x['PCF0B'] = x['MVB'] / x['net_prft'] + x['fix_ast_dep'] + x[
            'intgbl_ast_amt']
        x['PEBT0B'] = x['MVB'] / x['net_prft']
        x['PB0A'] = x['MVA'] / x['tot_eq_list_com']
        x['PB0B'] = x['MVB'] / x['tot_eq']
        x['PTB0A'] = x['MVA'] / (x['tot_eq_list_com'] - x['intgb_ast'] - x['goodwill'])
        x['PTB0B'] = x['MVB'] / (x['tot_eq'] - x['intgb_ast'] - x['goodwill'])
        x['PFCFE0B'] = x['net_cashflow_oprt_act'] - x['fin_exp']
        + (x['tot_liab'] - x['tot_cash'])
        - (x['non_cur_ast'] - x['non_cur_ast'].shift(1) - x['fix_ast_dep'])
        x['PS0B'] = x['MVB'] / x['oprt_incm']

        x['delta_ni'] = x['net_prft'].pct_change()
        x['delta_ni_parent_yoy'] = x['list_com_prft'] / x['list_com_prft'].shift(4) - 1
        x['PEG0A'] = x['MVA'] / x['list_com_prft'] / x['delta_ni_parent_yoy']
        x['PE0G0B'] = x['MVB'] / x['net_prft'] / x['delta_ni']
        x['PE0G1B'] = x['MVB'] / x['net_prft'] / x['delta_ni'] / x['e_ni_growth']
        if 'stock_code' in x.columns.tolist():
            del x['stock_code']
        if 'level_1' in x.columns.tolist():
            del x['level_1']
        return x
    gd = GetFSData()
    df = gd.merge(target=['bs_q', 'ins_ttm', 'cfs_ttm'], if_mkt=1, if_e=1)
    df = df.groupby('stock_code').apply(func)
    df = df.reset_index()
    df = df[list_em]
    return df

def cal_ic_multiplier():
    list_im = ['stock_code', 'trade_date', 'report_date',
               'BVICA', 'BVICB', 'MVICA', 'MVICB', 'MVICS0A', 'MVICS0B', 'MVICS1A', 'MVICS1B',
               'MVICDE0A', 'MVICDE0B', 'MVICEBITDA0A', 'MVICEBITDA0B', 'MVICEBIT0A', 'MVICEBIT0B',
               'MVICFCFF0A', 'MVICBVIC0B', 'MVICFCFF0A']

    def func(x):
        x = x.sort_values(by='trade_date')
        x['BVICA'] = x['tot_eq'] + x['st_loan']
        + x['non_cur_liab_one_year'] + x['non_cur_liab'] + x['bond_payb']
        x['BVICB'] = x['tot_eq'] + x['st_loan']
        + x['non_cur_liab_one_year'] + x['non_cur_liab'] + x['bond_payb']
        -x['tot_cash'] - x['trade_finast']
        x['fin_liab_mesrd_fair_val_thr_prft_loss'] - x['der_fin_ast']
        x['MVICA'] = x['close'] * x['outstanding_share'] + x['bill_payb'] + x['st_loan'] + x[
            'non_cur_liab_one_year']
        +x['non_cur_liab'] + x['bond_payb']
        x['MVICB'] = x['close'] * x['outstanding_share'] + x['bill_acnt_payb'] + x['st_loan'] + x[
            'non_cur_liab_one_year']
        +x['non_cur_liab'] + x['bond_payb']
        -x['tot_cash'] - x['trade_finast']
        x['fin_liab_mesrd_fair_val_thr_prft_loss'] - x['der_fin_ast']
        x['MVICS0A'] = x['MVICA'] / x['oprt_incm']
        x['MVICS0B'] = x['MVICB'] / x['oprt_incm']
        x['MVICS1A'] = x['MVICA'] / x['e_rev']
        x['MVICS1B'] = x['MVICB'] / x['e_rev']
        x['MVICDE0A'] = x['MVICA'] / (x['tot_prft'] + x['fin_exp']
                                      + x['fix_ast_dep'] + x['intgbl_ast_amt'] + x[
                                          'sal_payb'])
        x['MVICDE0B'] = x['MVICB'] / (x['tot_prft'] + x['fin_exp']
                                      + x['fix_ast_dep'] + x['intgbl_ast_amt'] + x[
                                          'sal_payb'])
        x['MVICEBITDA0A'] = x['MVICA'] / (x['tot_prft'] + x['fin_exp']
                                          + x['fix_ast_dep'] + x['intgbl_ast_amt'])
        x['MVICEBITDA0B'] = x['MVICB'] / (x['tot_prft'] + x['fin_exp']
                                          + x['fix_ast_dep'] + x['intgbl_ast_amt'])
        x['MVICEBIT0A'] = x['MVICA'] / (x['tot_prft'] + x['fin_exp'])
        x['MVICEBIT0B'] = x['MVICB'] / (x['tot_prft'] + x['fin_exp'])
        x['MVICDFCF0A'] = x['MVICA'] / (x['tot_prft'] + x['fin_exp'] * (
                1 - x['oprt_tax'] / (x['oprt_tax'] + x['net_prft']))
                                        + x['fix_ast_dep'] + x['intgbl_ast_amt'])
        x['MVICDFCF0B'] = x['MVICB'] / (x['tot_prft'] + x['fin_exp'] * (
                1 - x['oprt_tax'] / (x['oprt_tax'] + x['net_prft']))
                                        + x['fix_ast_dep'] + x['intgbl_ast_amt'])
        x['MVICNOPAT0A'] = x['MVICA'] / (x['tot_prft'] + x['fin_exp'] * (
                1 - x['oprt_tax'] / (x['oprt_tax'] + x['net_prft'])))

        x['MVICNOPAT0B'] = x['MVICB'] / (x['tot_prft'] + x['fin_exp'] * (
                1 - x['oprt_tax'] / (x['oprt_tax'] + x['net_prft'])))
        x['MVICBVIC0A'] = x['MVICA'] / (x['BVICA'] - x['intgb_ast'] - x['goodwill'])
        x['MVICBVIC0B'] = x['MVICB'] / (x['BVICB'] - x['intgb_ast'] - x['goodwill'])
        x['MVICFCFF0A'] = x['MVICA'] / (x['net_cashflow_oprt_act']
                                        - x['fin_exp'] * (
                                                1 - x['oprt_tax'] / (x['oprt_tax'] + x['net_prft']))
                                        - (x['non_cur_ast'] - x['non_cur_ast'].shift(1) - x[
                    'fix_ast_dep']))
        if 'stock_code' in x.columns.tolist():
            del x['stock_code']
        if 'level_1' in x.columns.tolist():
            del x['level_1']
        return x
    gd = GetFSData()
    df = gd.merge(target=['bs_q', 'ins_ttm', 'cfs_ttm'], if_mkt=1, if_e=1)
    df = df.groupby('stock_code').apply(func)

    df = df.reset_index()
    df = df[list_im]
    return df
