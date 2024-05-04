# coding:utf-8
import datetime
import traceback

from CONFIG.mongodb import read_mongo_limit
from Patenthub import url_manager, html_downloader, html_parser, html_outputer
from log.log import logger


class SpiderMain(object):
    # 初始化操作
    def __init__(self, company_name):
        # 设置url管理器
        self.urls = url_manager.UrlManager()
        # 设置HTML下载器
        self.downloader = html_downloader.HtmlDownloader()
        # 设置HTML解析器
        self.parser = html_parser.HtmlParser()
        # 设置HTML输出器
        self.outputer = html_outputer.HtmlOutputer()
        self.name = company_name

    # 爬虫调度程序
    def craw(self):
        try:
            print('专利网站 %s' % self.name)
            data = read_mongo_limit("tyc_data", "Patent", {"enterpriseName": self.name},
                                    {"_id": 0, "_class": 0, "businessId": 0, "lawStatus": 0})
            if data.empty:
                page_url1 = 'https://www.patenthub.cn/s?ds=cn&dm=mix&s=score%21&q=' + self.name  # self.name
                html_content = self.downloader.download(page_url1)
                if html_content is not None:
                    str_demo = '\n'.join(html_content)
                    new_url_list = self.parser.parse(self.name, str_demo)
                    for new_url in new_url_list:
                        new_html_content = self.downloader.download(new_url)
                        if new_html_content is not None:
                            new_str_demo = '\n'.join(new_html_content)
                            new_data = self.parser._get_new_data(self.name, new_url, new_str_demo)
                            if new_data is not None:
                                self.outputer.collect_data(new_data)
                                self.outputer.output_html()
                                return self.outputer.adjust_collection()
                            else:
                                return []
                        else:
                            return []
                else:
                    return []
            else:
                print("已存在")
                res = data.to_dict("records")
                return res
        except Exception as e:
            now = datetime.datetime.now()
            logger("专利汇" + str(now) + ": " + traceback.format_exc())
            return []


if __name__ == '__main__':
    company_list = ["苏州海博智能系统有限公司", "上海市山意微电子技术有限公司", "上海齐国环境科技有限公司",
                    "上海量日光电科技有限公司", "上海统冠企业管理有限公司", "陕西永诺信息科技有限公司", "青岛云购物联科技有限公司",
                    "上海泓济环保科技股份有限公司", "昆山超绿光电有限公司", "上海挚达科技发展有限公司",
                    "上海信隆行信息科技股份有限公司", "深圳市星商电子商务有限公司", "科华控股股份有限公司",
                    "北京快友世纪科技股份有限公司", "张家港丽恒光微电子科技有限公司", "上海易维视科技股份有限公司",
                    "融智通科技（北京）股份有限公司", "无锡德思普科技有限公司", "上海趣医网络科技有限公司",
                    "浙江美工精细陶瓷科技有限公司", "上海京颐科技股份有限公司", "上海杰菲物联网科技有限公司",
                    "常州好玩点信息技术有限公司", "霓螺（宁波）信息技术有限公司", "联芸科技（杭州）有限公司",
                    "昆山育源精密机械制造有限公司", "北京快友世纪科技股份有限公司", "西安国琳实业股份有限公司",
                    "江苏浩博新材料股份有限公司", "赛特斯信息科技股份有限公司", "银联商务股份有限公司",
                    "同创和达（北京）科技有限公司", "美景天下国际科技开发（北京）有限公司", "南京华设科技股份有限公司",
                    "上海生命之果生物科技有限公司", "芜湖伯特利汽车安全系统股份有限公司", "上海同捷科技股份有限公司"]
    for i in company_list:
        a = SpiderMain(i)
        a.craw()
