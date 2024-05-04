# 指数构建逻辑
'''
## 国家宏观趋势
长期（15年），中期（5年），短期（1-3年） 符合国家发展趋势的行业/带有契合国家发展规划属性的企业
注解：企业护城河宽 高 强，行业形成一定小趋势并已经奠定细分龙头地位；（这个不难选、都是上市条件，我们需要分别的是假还是真龙头）
融合科技属性的传统行业
互联网/科技 + 医疗/农业/金融/基建
消费升级
制造升级
科技升级
政策红利,紧紧跟随国家十四五规划和15年长期规划中的重点领域；
## 机构一致性预期
### 公募基金持仓
抱团股理性对待，考虑公募基金对于规模的限制，多将小盘股纳入思考
关注公募基金独特的，市值偏小的持仓

### 私募基金观点
关注私募发表观点中提到的个股

### 外资持仓
外资过去12M，6M，3M 持仓较高
外资持仓增长率高，3M，1M
剔除市值特别大的个股，外资也有规模限制

## 财务因子
构建方式：环比增长率qoq/同比增长率yoy/同比增长率增速yoy_qoq
数值类别：累计cum/单季q/TTM
cum - last cum + last annual report = TTM

### 高成长潜力
企业当下且未来市场很广阔
operating_revenue ttm yoy_qoq
total_profit cum yoy_qoq
net_profit cum yoy_qoq
net_profit_to_parent q yoy_qoq
net_operating_cashflow ttm yoy


### 高盈利能力
商业逻辑良好，能带来强有力的创收，资产利用效率高
roe; ttm; qoq
roa; q; yoy
net_profit_margin; ttm; qoq
gross_profit_margin; cum; yoy
operate_expense_to_gross_revenue sale_expense/ gross revenue; ttm; qoq



### 高安全边际
企业拥有充足的流动资金应对未来的风险和投资机遇
debt_to_asset yoy
current_ratio yoy
quick_ratio qoq
net_operating_cashflow_to_gross_revenue qoq

### 高运营能力
企业资源高效流动，运营周期合理
asset_turnover yoy
inventory_turnover_days qoq
account_recievable_turnover_days yoy

## 价量因子
排序相加/排序相乘（归一化）
### 动量因子
最优观察期N
sharpe ratio, N 日收益率/波动率
information ratio N 日收益率-市场日收益率 / 追踪误差 超额收益率波动率； 市场收益率为全行业个股平均日收益率
path adjusted momentum 动量  = 收盘价/N日前收盘价 -1； 动量/ N日内日收益绝对值相加


增加连续性
max return N日内最高收益率
信息离散度 information dispersion = 日收益率方向 * （观察期内正收益占比- 观察期内负收益占比）
relative strength index = 上涨日涨幅均值/ 上涨日涨幅均值 + 下跌日跌幅均值
多日动量波动率 窗口参数w 向前w日计算动量 N日平均




## 情绪因子
### 机构关注度
买入/增持评级占比 yoy
净利润调高家数
预测机构家数 yoy

### 市场关注度

'''

# 目标池筛选
'''
* 定位目标行业
** 高成长
** 政策关注

* 选择行业所对应的ETF及主动管理基金
** 业绩优异稳定，基金经理可靠

* 外资净流入


沪深300成长指数	399918
中国战略新兴产业综合指数	000891
上证沪股通指数	000159
优势资源	000145
上证中国制造2025主题指数	000161

'''




## 生物医药
"""
* 创新药

国证生物医药指数
399441.SZ

006229.OF
006113.OF
001915.OF
110023.OF
"""


## 消费升级-新能源
"""
* 新能源汽车
* 锂电池
中证新能源汽车指数	399976
150211.OF
国证新能源汽车指数	399417
160225.OF

001410.OF
002168.OF
001158.OF
"""


## 周期-能源化工
"""
* 核电
中证能源指数	000928.SH
资源50	000092.SH
240022.OF
002910.OF

"""


## 软件/互联网
"""
* 行业应用软件

中证移动互联网指数	399970.SZ
中证信息技术指数	399935.SZ
中证互联网金融指数	399805.SZ
中关村50指数	399423.SZ
上证互联网+主题指数 000162.SH
007874.OF
160626.OF
007873.OF
080012.OF
001513.OF
"""

## 新材料
"""
* 稀土永磁材料
* 电动机
中证国防安全指数	399813.SZ
003984.OF
001158.OF
"""


## 半导体
"""
* 芯片
007300.OF
008887.OF
"""


## 电子
"""
* 光伏 931151
* 智能制造 930850; 950101
中证申万电子行业投资指数	399811

中证TMT	000998.SH
001476.OF
006863.OF
004935.OF
001617.OF
"""

# 航天
'''
* 国产飞机
000535.OF
中证航空航天指数 931521; H30213
'''



# 标的筛选
## step-1
"""
公募基金/上市指数
* 追溯基金历史3年持仓，打分排名
** 计算基金收益率 * 个股权重，以个股为单位求和
** 排名后选择前20%，不超过10个
* 日行情
* 个股权重

非上市指数
* 获取最新持仓数据，手动下载

"""

## step-2 财务维度

"""
* 盈利能力
** ROE
** gross profit ratio
** 
* 偿债能力；leverage ratio 排名
* 运营能力；asset turnover 排名
* 财务数据打分，每个行业选取top20
"""


## step-3 股价回报维度
"""
* 风险调整后收益；sharpe ratio 排名
* 复合收益
* 日波动率
"""


medicine_pool  = ['006229.OF',
'006113.OF',
'001915.OF',
'110023.OF']

new_energy_pool = [
    '001410.OF',
'002168.OF',
'001158.OF',
]

new_material_pool = [
    '003984.OF',
    '001158.OF'
]

semiconductor_pool = [
    '007300.OF',
'008887.OF'
]

electronics_pool = [
    '001476.OF',
'006863.OF',
'004935.OF',
'001617.OF'
]


# stock_pool design
# columns = [stock_code, benchmark_code,theme, style, weight, analyst]
# stock_code = sz/sh + code

dict_theme = {
    'consumer_staples':['801120','801130','801170',],
    'consumer_discretionary':['801080','801110','801210','801230','801760','801880'],
    'pharmaceutical':['801150',],
    'IT':['801750','801770'],
    'Financial':['801780','801780','801200'],
    'cyclical':['801010','801020','801030','801040','801050','801140','801160','801180','801710','801720','801730','801740','801890']
}


dict_style = {
    'largegrowth':'399372',
    'largevalue':'399373',
    'midgrowth':'399374',
    'midvalue':'399375',
    'smallgrowth':'399376',
    'smallvalue':'399377',
}


# macroeconomic_policy_factor
# 符合国家十四五规划的收益行业
# 选择对应主题指数
list_consumption = ['931480','930742',
                   '931007','930654','931005','930728',
                   'H30365','H30318',
                   '930717',
                    '930648','931494','399996','930711','931584'
                   'H11052','H30141','930721']
list_medicine = ['930719','930720','931592',
                '931011','931152','931440','931484','931639']

list_emerging = ['000964','H30368','930883','000891','930884']

list_technology = ['931441','931469','H30138','930713','930850','931071',
                   '931079','931491',
                  '931483','H30597',
                  '930733','931582','930722','930712','930725','930986']

# mutual_fund_factor
mf_consumption = ['110022','002168','006796','005827',]
mf_technology = ['003984','001158','001476','000601']
mf_medicine = ['006229','006113','001915','110023']
mf_comprehensive = ['161005','163402','169101','163417','009029']
list_mf = []
for i in [mf_consumption,mf_technology,mf_medicine,mf_comprehensive]:
    list_mf.extend(i)