from CONFIG.Config import config

class GlobalValue():
    # sam方法数据库名称
    SAM_NAME = 'var_samuelson'
    ASS_NAME = "var_asset"
    REAL_NAME = "var_real_estate"
    # 估值上限10兆亿
    MV_CILLING = 100000000000000000
    # 普通估值法大类
    METHODSUMMOD = ['abs', 'mkt', 'sam']
    # 金融估值法大类
    METHODFINMOD = ['ass', 'mkt', 'sam']
    # 房地产估值大类
    METHODREAL = ["est"]
    # 备用re取值
    REQUIRE_RETURN = [3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987]
    # 估值路径参数
    VALUROUTE_CMPLX = 30
    VALUROUTE_LITE = 20
    VALUROUTE_FIN = 20
    VALUROUTE_VI = 10
    VALUROUTE_EST = 40
    # 收入法大类
    ABS_LIST = ['rim', 'eva', 'nopat', 'apv']
    # lite风险投资模型要求回报率倍数
    MUTILPLIER = [5.6, 5, 4.2, 3.5, 3.1, 2.8, 2.2, 2.2, 2.2]
    # 公司潜力 算未来5年
    GROWTHPOTENTIALYEAR = 5


class ValuationModels():
    FIN_SAM = 1
    FIN_MKT = 2
    NOR_NOPAT = 3
    NOR_EVA = 4
    NOR_RIM = 5
    NOR_COST = 6
    NOR_MKT = 7
    NOR_SAM_LITE = 8
    NOR_SAM_STANDARD = 9
    VI_SAM = 10
    VI_NOR = 11
    NOR_APV = 12
    FIN_ASS = 13
    REAL_ESTATE = 14


class IP_CONFIG():
    TEST = config['DEFAULT']['sql_host']
    PRODUCT = config['DEFAULT']['mongo_host']
    # PRODUCT = '47.100.209.100'
    DEVELOP = '192.168.2.114'
    LOCAL = '192.168.2.11'

# class PORT_CONFIG():
#     TEST = "27017"
#     DEVELOP = "27917"
#     PRODUCT = "27017"
#     LOCAL = "27017"
