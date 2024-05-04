import pandas as pd
import os
import json
import datetime
# get the stock pool
# calculate the quote
# zip the quote of stock and time
# calculate indicators
# input into the dictionary
from index_quote_calculation import compare_index_benchmark
from indicator_calculation import cal_indicator, cal_distribition_market, cal_top10, cal_distribution_style, \
    cal_distribution_industry, cal_volatility


class SaveJson(object):

    def save_file(self, path, item):

        # 先将字典对象转化为可写入文本的字符串
        item = json.dumps(item)

        try:
            if not os.path.exists(path):
                with open(path, "w", encoding='utf-8') as f:
                    f.write(item + ",\n")
                    print("^_^ write success")
            else:
                with open(path, "a", encoding='utf-8') as f:
                    f.write(item + ",\n")
                    print("^_^ write success")
        except Exception as e:
            print("write error==>", e)


if __name__ == '__main__':

    df_pool = pd.read_csv(os.path.join(os.getcwd(),'static','stock_pool.csv'))
    df_pool_full = pd.read_csv(os.path.join(os.getcwd(),'static','stock_pool_full.csv'))
    df_quote = pd.read_csv(os.path.join(os.getcwd(),'static','index_quote.csv'))

    path = os.path.join(os.getcwd(),'factsheet','data','test.json')
    # t = datetime.datetime.now()
    # period = str(t.year) + '年' + str(t.month) +'月' + str(t.day) +'日'
    #
    dict_indicator = cal_indicator(df_pool_full)
    # index_makcap = "\u00a5" + '%.2f' % (dict_indicator['sum_cap']/(10**8))
    # total_makcap = "\u00a5" + '%.2f' % (dict_indicator['sum_cap_index']/(10**8))
    # pe = '%.2f' %(dict_indicator['pe_ttm'])
    # pb = '%.2f' %(dict_indicator['pb'])
    # div = '%.2f%' %(dict_indicator['div_ratio']*100)
    # roe = '%.2f' %(dict_indicator['roe']*100)
    #
    # dist_market = cal_distribition_market(df_pool_full)
    # sz = '%.2f' %(dist_market.query('trade_market == "sz"')['weight'].values[0]*100)
    # sh = '%.2f' %(dist_market.query('trade_market == "sh"')['weight'].values[0]*100)
    #
    # dist_industry = cal_distribution_industry(df_pool_full)
    # medicine = str(dist_industry.query('industry == "medicine"')['weight'].values[0])
    # semiconductor = str(dist_industry.query('industry == "semiconductor"')['weight'].values[0])
    # new_material = str(dist_industry.query('industry == "new_material"')['weight'].values[0])
    # new_energy = str(dist_industry.query('industry == "new_energy"')['weight'].values[0])
    # electronics = str(dist_industry.query('industry == "electronics"')['weight'].values[0])
    # internet_software = '0'
    # finance = '0'
    # education = '0'
    # internet_software = dist_industry.query('industry == "electronics"')['weight'].values[0]
    # finance = dist_industry.query('industry == "electronics"')['weight'].values[0]
    # education = dist_industry.query('industry == "electronics"')['weight'].values[0]

    df_top = cal_top10(df_pool_full)
    df_top['weight']  = df_top['weight']*100

#     dist_style = cal_distribution_style(df_pool_full)
#     largegrowth = '%.2f' %(dist_style.query('style == "largegrowth"')['weight'].values[0]*100)
#     largevalue = '%.2f' %(dist_style.query('style == "largevalue"')['weight'].values[0]*100)
#     midgrowth = '%.2f' %(dist_style.query('style == "midgrowth"')['weight'].values[0]*100)
#     try:
#         midvalue =  '%.2f'%(dist_style.query('style == "midvalue"')['weight'].values[0]*100)
#     except:
#         midvalue  ="0"
#     smallgrowth = '%.2f'%(dist_style.query('style == "smallgrowth"')['weight'].values[0]*100)
#     smallvalue = '%.2f'%(dist_style.query('style == "smallvalue"')['weight'].values[0]*100)
#     print(smallvalue)
#
#
#     # calculate volatility in different period
#     dict_volatility = cal_volatility(df_quote)
#
# #     # compare fuyou50 with hs300, plot line chart
#     performance = compare_index_benchmark()


    # 案例字典数据
#     item = {
#     "status": "draft",
#     "id": "Hgdl165-n",
#     "fund_name": "富由50指数每日更新",
#     "period": period,
#
#     "keyfacts_title":"指数关键信息",
#     "index_makcap_title": "指数总市值",
#     "index_makcap": index_makcap,
#
#     "total_makcap_title":"成分股总市值",
#     "total_makcap": total_makcap,
#
#     "base_currency_title": "货币",
#     "base_currency": "人民币",
#
#     "full_name_title": "指数全称",
#     "full_name": "富由核心科创50指数",
#
#     "adjust_frequency_title":"调养频率",
#     "adjust_frequency": "每季度",
#
#     "launch_price_title":"基准值",
#     "launch_price": "1000",
#
#     "n_constituents_title":"样本股数量",
#     "n_constituents": "50",
#
#     "display_launch_date_title":"发布日期",
#     "display_launch_date": "01/01/2021",
#
#     "display_base_date_title":"基准日期",
#     "display_base_date": "10/01/2020",
#
#     "issuer_title":"发行机构",
#     "issuer": "富由集团",
#
#     "objectives_title":"指数构建目的",
#     "objectives": "随着市场的认可度不断攀升加之宏观政策的红利，科创版市场的投资价值逐渐显现，中国海外上市的科技及互联网题材个股表现优异，有望在将来回归A股市场，带来良好投资机遇，本指数致力于挖掘中国市场的优质科技主题个股行业主要布局但不限于创新医药，消费升级，电子，新材料，互联网等领域，构建拥抱科技，面向未来的核心资产股票池",
#
#     "logic_title":"指数构建逻辑",
#     "logic": "富由核心科创50指数以基本面分析结合量化因子计算为技术手段，从沪深A股中，依据行业景气度，企业基本面，动量因子等多维度，精选50只中具有成长潜力的高景气度个股;覆盖科技，医药，消费，金融，周期等多 个板块，以新兴科技产业为核心 ，全面挖掘A股中超额收益的机会 ，反映A股市场最具价值的一批个股的表现。",
#
#     "rev_volatility_title":"风险及收益",
#     "volatility_1y_title":"1年年化波动率",
#     "volatility_1y": "0.2",
#
#     "volatility_3m_title":"季度波动率",
#     "volatility_3m": "0.2",
#
#     "volatility_title":"年初至今波动率",
#     "volatility": str(dict_volatility['v_1']),
#
#     "rev_1y_title":"1年年化收益率",
#     "rev_1y": str(dict_volatility['v_3']),
#
#     "rev_3m_title":"3年年化收益率",
#     "rev_3m": str(dict_volatility['v_5']),
#
#     "rev_title":"年初至今收益率",
#     "rev": "0.2",
#
#     "fundamentals_title":"基本面指标",
#     "pe_title":"滚动市盈率",
#     "pe": pe,
#
#     "pb_title":"滚动市净率",
#     "pb": pb,
#
#     "div_title":"股息率",
#     "div": div,
#
#     "roe_title":"净资产收益率",
#     "roe": roe,
#
#     "leverage_title":"杠杆率",
#     "leverage": "0.2",
#
#     "sharpe_title":"夏普比率",
#     "sharpe": "20%",
#
#     "sector_title":"风格策略分布",
#     "sector_breakdown": [
#
#         {
#             "name": "大盘成长",
#             "amount": largegrowth,
#         },
#         {
#             "name": "大盘价值",
#             "amount": largevalue,
#         },
#         {
#             "name": "中盘成长",
#             "amount": midgrowth,
#         },
#         {
#             "name": "中盘价值",
#             "amount": midvalue,
#         },
#         {
#             "name": "小盘价成长",
#             "amount": smallgrowth,
#         },
#         {
#
#             "name": "小盘价值",
#             "amount": smallvalue,
#
#         }
#
#     ],
#
#     "market_title":"风格策略分布",
#     "market_breakdown": [
#
#         {
#             "name": "深交所",
#             "amount": sz,
#         },
#         {
#             "name": "上交所",
#             "amount": sh,
#         }
#     ],
#     "performance_title":"指数走势",
#     "index_line_plot_data": str(performance),
#     "industry_distribution_title":"行业权重分布",
#     "holdings": [
#         {
#             "name": "创新医药",
#             "amount": medicine,
#         },
#         {
#             "name": "新能源",
#             "amount": new_energy,
#         },
#         {
#             "name": "新材料",
#             "amount": new_material,
#         },
#         {
#             "name": "互联网及软件服务",
#             "amount": internet_software,
#         },
#         {
#             "name": "半导体",
#             "amount": semiconductor,
#         },
#         {
#             "name": "电子",
#             "amount": electronics,
#         },
#         {
#             "name": "教育",
#             "amount": education,
#         },
#         {
#             "name": "金融",
#             "amount": finance,
#         }
#     ],
#     "top_holdings_title":"前十大持仓",
#     "top_holdings": [
#
#         {
#             "name": str(df_top.iloc[0]['name']),
#             "amount": str(df_top.iloc[0]['weight'])
#         },
#         {
#             "name": str(df_top.iloc[1]['name']),
#             "amount": str(df_top.iloc[1]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[2]['name']),
#             "amount": str(df_top.iloc[2]['weight']),
#
#         },
#         {
#
#             "name": str(df_top.iloc[3]['name']),
#             "amount": str(df_top.iloc[3]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[0]['name']),
#             "amount": str(df_top.iloc[0]['weight']),
#         },
#         {
#             "name": str(df_top.iloc[4]['name']),
#             "amount": str(df_top.iloc[4]['weight']),
#         },
#         {
#             "name": str(df_top.iloc[5]['name']),
#             "amount": str(df_top.iloc[5]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[6]['name']),
#             "amount": str(df_top.iloc[6]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[7]['name']),
#             "amount": str(df_top.iloc[7]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[8]['name']),
#             "amount": str(df_top.iloc[8]['weight']),
#
#         },
#         {
#             "name": str(df_top.iloc[9]['name']),
#             "amount": str(df_top.iloc[9]['weight']),
#
#         }
#
#     ]
#
# }
    item = {"top_holdings": [

        {
            "name": str(df_top.iloc[0]['name']),
            "amount": str(df_top.iloc[0]['weight'])
        },
        {
            "name": str(df_top.iloc[1]['name']),
            "amount": str(df_top.iloc[1]['weight']),

        },
        {
            "name": str(df_top.iloc[2]['name']),
            "amount": str(df_top.iloc[2]['weight']),

        },
        {

            "name": str(df_top.iloc[3]['name']),
            "amount": str(df_top.iloc[3]['weight']),

        },

        {
            "name": str(df_top.iloc[4]['name']),
            "amount": str(df_top.iloc[4]['weight']),
        },
        {
            "name": str(df_top.iloc[5]['name']),
            "amount": str(df_top.iloc[5]['weight']),

        },
        {
            "name": str(df_top.iloc[6]['name']),
            "amount": str(df_top.iloc[6]['weight']),

        },
        {
            "name": str(df_top.iloc[7]['name']),
            "amount": str(df_top.iloc[7]['weight']),

        },
        {
            "name": str(df_top.iloc[8]['name']),
            "amount": str(df_top.iloc[8]['weight']),

        },
        {
            "name": str(df_top.iloc[9]['name']),
            "amount": str(df_top.iloc[9]['weight']),

        }

    ]}
    print(df_top)

    s = SaveJson()

    s.save_file(path, item)
