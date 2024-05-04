import datetime
import traceback
import requests
import json
import time

from CONFIG.mongodb import read_mongo_limit, save_mongo_from_dict
from log.log import logger

url = "https://rmfygg.court.gov.cn/web/rmfyportal/noticeinfo?p_p_id=noticelist_WAR_rmfynoticeListportlet&p_p_lifecycle=2&p_p_resource_id=initNoticeList"
aa = [{"name": "sEcho", "value": "1"}, {"name": "iColumns", "value": "6"}, {"name": "iDisplayStart", "value": "0"},
      {"name": "iDisplayLength", "value": "15"}]


def get_all(total, data, post_data):
    demo_list = eval(post_data["_noticelist_WAR_rmfynoticeListportlet_aoData"])
    demo_list[2]["value"] = str(int(demo_list[2]["value"]) + 15)
    post_data["_noticelist_WAR_rmfynoticeListportlet_aoData"] = str(demo_list)
    time.sleep(1)
    response = requests.post(url=url, data=post_data, headers={'Accept-Encoding': 'deflate, br'})
    json_str = response.content.decode()
    json_dict = json.loads(json_str)
    data.extend(json_dict["data"])
    if total > 0:
        return get_all(total - 15, data, post_data)
    else:
        return data


# def save_mongo(data):
#     username = 'hub'
#     password = 'hubhub'
#     host = '101.132.32.7'
#     port = 27017
#     mongo_uri = 'mongodb://%s:%s@%s:%s/?authSource=admin' % (username, password, host, port)
#     conn = MongoClient(mongo_uri)
#     db = conn['spider_origin']['laws']
#     db.insert_one(data)


def run(company_name):
    print('司法网站 %s' % company_name)
    data = read_mongo_limit("spider_origin", "laws", {"name": company_name}, {"_id": 0})
    if data.empty:
        try:
            item = {"name": company_name}
            data = {"_noticelist_WAR_rmfynoticeListportlet_searchContent": company_name,
                    "_noticelist_WAR_rmfynoticeListportlet_noticeTypeVal": "全部",
                    "_noticelist_WAR_rmfynoticeListportlet_aoData": str(aa),
                    "_noticelist_WAR_rmfynoticeListportlet_IEVersion": "ie",
                    "_noticelist_WAR_rmfynoticeListportlet_flag": "click"}
            response = requests.post(url=url, data=data, headers={'Accept-Encoding': 'deflate, br'})
            time.sleep(1)
            if response.status_code in [404]:
                item["total"] = 0
                item["data"] = []
            else:
                json_str = response.content.decode()
                json_dict = json.loads(json_str)
                total = json_dict["iTotalDisplayRecords"]
                item["total"] = total
                if total > 0:
                    if total > 15:
                        item["data"] = get_all(total - 15, json_dict["data"], data)
                    else:
                        item["data"] = json_dict["data"]
            save_mongo_from_dict("spider_origin", "laws", item)
            return item
        except Exception as e:
            now = datetime.datetime.now()
            logger("司法" + str(now) + ": " + traceback.format_exc())
            return ""
    else:
        print("已存在")
        res = data.to_dict("records")
        return res


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
        run(i)
