# -*- coding: utf-8 -*-：
from threading import Thread
import asyncio
from time import time
from RiskAssessment.calScore import *
from RiskAssessment.getInfo import *

# static parameter
alpha = 100
miu = 0.05
ex_usd = 0.75


class MyThread(Thread):
    def __init__(self, func):
        super(MyThread, self).__init__()
        self.func = func

    def run(self):
        self.result = self.func()

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


class calRisk():

    def __init__(self, name, alpha, miu, ex_usd):

        self.name = name
        self.miu = miu
        self.ex_usd = ex_usd

        reg_capital = read_mongo_limit('tyc_data', 'TycEnterpriseInfo', {'name': name},['name','regCapital'])

        if reg_capital.empty:
            self.alpha = alpha

        else:
            reg_capital = reg_capital['regCapital'].values[0]
            try:
                def func(x):
                    if len(re.findall(r"\d+\.?\d*", x)) == 0:
                        x = 5 * ('数' in x) + 100 * ('未' in x)
                    else:
                        x = float(re.findall(r"\d+\.?\d*", x)[0])
                    return x
                reg_capital_num = func(reg_capital)
                reg_capital_unit = (('美元' in reg_capital) * self.ex_usd +
                                    (not ('美元' in reg_capital)) * 1) * (
                        ('千' in reg_capital ) * 10000000 + ('百' in reg_capital) * 1000000 +
                        ('亿' in reg_capital) * 100000000 + 10000)
                reg_capital_1 = reg_capital_num * reg_capital_unit
                self.alpha = reg_capital_1
            except:
                self.alpha = alpha


    def cal_risk_businessInfo(self):
        # 6* risk class, each class has n* factor, each factor has n*sub filed of past 3 years
        # step-1: create dictionary to get the score from different class of risk
        # step-2: create dataframe with default value for calculation of each risk class
        # step-3: get the value of each sub field from mongodb
        # step-4: input the value into the dataframe for calculation
        # step-5: calculate the risk factor by sub filed data and function
        # step-6: multiply the risk factor value with corresponding weight and sum up
        df_businessInfo = create_df_businessInfo()
        df_businessInfo.loc[0, 'score'] = cal_gsbg(df_businessInfo, num=0)
        df_businessInfo.loc[1, 'score'] = cal_dgdbg(df_businessInfo, num=1)
        df_businessInfo.loc[2, 'score'] = cal_gqbg(df_businessInfo, num=2)
        df_businessInfo.loc[3, 'score'] = cal_tz_exit(df_businessInfo, self.alpha, self.miu, num=3)
        df_businessInfo.loc[4, 'score'] = cal_tz_add(df_businessInfo, self.alpha, self.miu, num=4)
        w_business_info = np.array([2, 1, 1, 1, 1])
        res = np.dot(w_business_info.T, df_businessInfo['score'].values)
        return res

    def cal_risk_devPotential(self):
        df_devPotential = create_df_dev_potential()
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        loop = asyncio.get_event_loop()

        task = [asyncio.ensure_future(get_rz_qk(self.name, self.ex_usd)),
                asyncio.ensure_future(get_hgjjzzl()),
                asyncio.ensure_future(get_dqjjzzl(self.name)),
                asyncio.ensure_future(get_industryFinance(self.name, self.ex_usd))]

        loop.run_until_complete(asyncio.wait(task))

        df_rzqk = task[0].result()
        # df_rzqk = get_rzqk(self.name, self.ex_usd)
        #df_devPotential_1 = input_rzqk(df_rzqk, df_devPotential)
        if not df_rzqk.empty:
            df_devPotential.loc[0, 'score'] = cal_rzqk(df_devPotential,df_rzqk, alpha, miu, num=0)
        df_devPotential.loc[1, 'score'] = cal_lhzc(df_devPotential, num=1)
        df_devPotential.loc[2, 'score'] = cal_xzzc(df_devPotential, num=2)

        # df_hgjjzzl = get_hgjjzzl()
        df_hgjjzzl = task[1].result()
        #df_devPotential_1 = input_hgjjzzl(df_hgjjzzl, df_devPotential)
        if not df_hgjjzzl.empty:
            df_devPotential.loc[3, 'score'] = cal_hgjjzzl(df_devPotential,df_hgjjzzl, num=3)

        g = task[2].result()
        # g = get_dqjjzzl(self.name)
        #df_devPotential_1 = input_dqjjzzl(g, df_devPotential)
        df_devPotential.loc[4, 'score'] = cal_dqjjzzl(df_devPotential,g, num=4)

        df_devPotential.loc[5, 'score'] = cal_hyjjzzl(df_devPotential, num=5)
        df_devPotential.loc[6, 'score'] = cal_ssgg(df_devPotential, num=6)
        df_devPotential.loc[7, 'score'] = cal_hytzsl(df_devPotential, num=7)

        df_industryFinance = task[3].result()
        # df_industryFinance = get_industryFinance(self.name,self.ex_usd)
        #df_devPotential_1 = input_industryFinance(df_industryFinance, df_devPotential)
        if not df_industryFinance.empty:
            df_devPotential.loc[8, 'score'] = cal_hyrzje(df_devPotential,df_industryFinance, num=8)

        w_devPotential = np.array([1, 1, 1, 1, 2, 2, 10, 1, 1])
        res = np.dot(w_devPotential.T, df_devPotential['score'].values)
        return res

    def cal_risk_op(self):
        df_opRisk = create_df_opRisk()
        df_opRisk.loc[0, 'score'] = cal_jyyc(df_opRisk, num=0)
        df_opRisk.loc[1, 'score'] = cal_xzcf(df_opRisk, self.alpha, self.miu, num=1)
        df_opRisk.loc[2, 'score'] = cal_yzwf(df_opRisk, num=2)
        df_opRisk.loc[3, 'score'] = cal_gqcz(df_opRisk, self.alpha, self.miu, num=3)
        df_opRisk.loc[4, 'score'] = cal_gqzy(df_opRisk, num=4)
        df_opRisk.loc[5, 'score'] = cal_sswf(df_opRisk, self.alpha, self.miu, num=5)
        df_opRisk.loc[6, 'score'] = cal_dcdy(df_opRisk, self.alpha, self.miu, num=6)
        df_opRisk.loc[7, 'score'] = cal_qsgg(df_opRisk, self.alpha, self.miu, num=7)
        df_opRisk.loc[8, 'score'] = cal_qsxx(df_opRisk, num=8)
        df_opRisk.loc[9, 'score'] = cal_tddy(df_opRisk, self.alpha, self.miu, num=9)
        df_opRisk.loc[10, 'score'] = cal_jyzx(df_opRisk, num=10)
        df_opRisk.loc[11, 'score'] = cal_xzxk(df_opRisk, num=11)
        df_opRisk.loc[12, 'score'] = cal_swpj(df_opRisk, num=12)
        df_opRisk.loc[13, 'score'] = cal_ccjc(df_opRisk, num=13)
        df_opRisk.loc[14, 'score'] = cal_jckxx(df_opRisk, num=14)
        df_opRisk.loc[15, 'score'] = cal_gdxx(df_opRisk, self.alpha, self.miu, num=15)
        w_opRisk = np.array([1, 2, 5, 1, 1, 2, 1, 1, 1, 5, 1, 10, 1, 1, 1, 1])
        res = np.dot(w_opRisk.T, df_opRisk['score'].values)
        return res

    def cal_risk_legal(self):
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        loop = asyncio.get_event_loop()

        task = [asyncio.ensure_future(get_leagalInfo(self.name))]

        loop.run_until_complete(asyncio.wait(task))

        df_legalRisk = create_df_legalRisk()
        df_legalInfo = task[0].result()
        #df_legalRisk_1 = input_legalInfo(df_legalInfo, df_legalRisk)
        if not df_legalInfo.empty:
            df_legalRisk.loc[0, 'score'] = cal_bg(df_legalRisk,df_legalInfo, num=0)
            df_legalRisk.loc[1, 'score'] = cal_yg(df_legalRisk,df_legalInfo, num=1)
            df_legalRisk.loc[2, 'score'] = cal_ss(df_legalRisk,df_legalInfo, num=2)
            df_legalRisk.loc[3, 'score'] = cal_bs(df_legalRisk,df_legalInfo, num=3)
            df_legalRisk.loc[4, 'score'] = cal_bzxr(df_legalRisk,df_legalInfo, num=4)
            df_legalRisk.loc[5, 'score'] = cal_sxbzxr(df_legalRisk,df_legalInfo, num=5)
            df_legalRisk.loc[6, 'score'] = cal_msss(df_legalRisk,df_legalInfo, num=6)
            df_legalRisk.loc[7, 'score'] = cal_xsss(df_legalRisk,df_legalInfo, num=7)
            df_legalRisk.loc[8, 'score'] = cal_xzss(df_legalRisk,df_legalInfo, num=8)
            df_legalRisk.loc[9, 'score'] = cal_htjf(df_legalRisk,df_legalInfo, alpha, miu, num=9)

        w_legalRisk = np.array([1, 0.5, 1, 2, 2, 5, 0.5, 1, 0.3, 1])
        res = np.dot(w_legalRisk.T, df_legalRisk['score'].values)
        return res

    def cal_risk_IP(self):
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        loop = asyncio.get_event_loop()

        task = [asyncio.ensure_future(get_tm(self.name)),
                asyncio.ensure_future(get_patent(self.name)),
                asyncio.ensure_future(get_software(self.name)),
                asyncio.ensure_future(get_work(self.name)),
                asyncio.ensure_future(get_IPCase(self.name))]
                # asyncio.ensure_future(get_noIP(self.name))]

        loop.run_until_complete(asyncio.wait(task))

        df_IP = create_df_IP()

        df_tm = task[0].result()
        #df_IP_1 = input_tm(df_tm, df_IP)
        if not df_tm.empty:
            df_IP.loc[0, 'score'] = cal_sbxx(df_IP,df_tm, num=0)

        df_patent = task[1].result()
        #df_IP_1 = input_patent(df_patent, df_IP)
        if not df_patent.empty:
            df_IP.loc[1, 'score'] = cal_zlxx(df_IP,df_patent, num=1)

        df_software = task[2].result()
        #df_IP_1 = input_software(df_software, df_IP)
        if not df_software.empty:
            df_IP.loc[2, 'score'] = cal_rjzzq(df_IP,df_software, num=2)

        df_work = task[3].result()
        #df_IP_1 = input_work(df_work, df_IP)
        if not df_work.empty:
            df_IP.loc[3, 'score'] = cal_zpzzq(df_IP,df_work, num=3)

        df_IPCase = task[4].result()
        #df_IP_1 = input_IPCase(df_IPCase, df_IP)
        if not df_IPCase.empty:
            df_IP.loc[4, 'score'] = cal_zscqhtjf(df_IP,df_IPCase, alpha, miu, num=4)
        if df_tm.empty and df_patent.empty and df_software.empty and df_work.empty:
            df_IP.loc[5, 'score'] = 30
        else:
            df_IP.loc[5, 'score'] = 0
            # df_noIP = get_noIP(name)
            # if not df_noIP.empty:
            #     df_IP.loc[5, 'score'] = cal_wzscq(df_IP,df_noIP, num=5)

        w_IP = np.array([0.2, 0.2, 0.1, 0.1, 1, 0.5])
        res = np.dot(w_IP.T, df_IP['score'].values)
        return res

    def cal_risk_public_opinion(self):
        df_public_opinion = create_df_publicOpinion()

        try:
            df_p_news, df_n_news = get_news(self.name)
        except:
            df_p_news = pd.DataFrame()
            df_n_news = pd.DataFrame()

        #df_public_opinion_1 = input_news(df_p_news, df_n_news, df_public_opinion)
        if not df_p_news.empty & df_n_news.empty:
            df_public_opinion.loc[0, 'score'] = cal_positive(df_public_opinion,df_p_news, num=0)
            df_public_opinion.loc[1, 'score'] = cal_positive(df_public_opinion,df_p_news, num=1)
            df_public_opinion.loc[2, 'score'] = cal_positive(df_public_opinion,df_p_news, num=2)
            df_public_opinion.loc[3, 'score'] = cal_negative(df_public_opinion,df_n_news, num=3)
            df_public_opinion.loc[4, 'score'] = cal_negative(df_public_opinion, df_n_news,num=4)
            df_public_opinion.loc[5, 'score'] = cal_negative(df_public_opinion,df_n_news, num=5)

        w_public_opinion = np.array([2, 2, 1, 2, 2, 1])
        res = np.dot(w_public_opinion.T, df_public_opinion['score'].values)
        return res

    def sum_up(self):

        dict_score = dict()
        thread_dict = dict()

        thread_dict["publicOpinion"] = MyThread(func=self.cal_risk_public_opinion)
        thread_dict["IP"] = MyThread(func=self.cal_risk_IP)
        thread_dict["legalRisk"] = MyThread(func=self.cal_risk_legal)
        thread_dict["opRisk"] = MyThread(func=self.cal_risk_op)
        thread_dict["devPotential"] = MyThread(func=self.cal_risk_devPotential)
        thread_dict["businessInfo"] = MyThread(func=self.cal_risk_businessInfo)

        for key, value in thread_dict.items():
            value.start()
        for key, value in thread_dict.items():
            value.join()
            dict_score[key] = value.get_result()
        print(dict_score)

        return dict_score


if __name__ == '__main__':
    list = ['上海蔚来汽车有限公司', '上海中梁地产集团有限公司', '广州亚美信息科技有限公司',
            '上海天天快递有限公司', '瑞幸咖啡（北京）有限公司', '沪江教育科技（上海）股份有限公司',
            '广东普正教育科技股份有限公司', '小米科技有限责任公司', '上海曼恒数字技术股份有限公司',
            '深圳华大基因股份有限公司', '上海证大喜马拉雅网络科技有限公司', '广州市驴迹科技有限责任公司', '金瓜子科技发展（北京）有限公司']
    name = '上海铁旗财务咨询有限公司'


    c = calRisk(name, alpha, miu, ex_usd)
    res = c.sum_up()
