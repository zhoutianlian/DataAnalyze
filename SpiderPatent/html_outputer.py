import pandas as pd
from pymongo import MongoClient

from CONFIG.mongodb import save_mongo, delete_collection, read_mongo_all
import CONFIG.globalENV as gl


class HtmlOutputer(object):
    def __init__(self):
        self.datas = []

    def collect_data(self, data):
        if data is None:
            return
        self.datas.append(data)

    def adjust_collection(self):
        if self.datas:
            data_df = read_mongo_all("spider_origin", "patenthub_temp")
            name_list = list(set(data_df["company_name"]))
            demo_list = []
            for i in name_list:
                df = data_df[data_df["company_name"] == i]["patent_list"]
                for sig in df:
                    patent_list = [i]
                    patent_list.extend(sig[0].values())
                    demo_list.append(patent_list)
            data_demo = pd.DataFrame(demo_list,
                                     columns=["company_name", "patent_type", "patent_detail_url", "patent_name",
                                              "patent_status", "patent_publication_number",
                                              "patent_publication_date",
                                              "patent_application_number", "patent_application_date",
                                              "patent_applicat",
                                              "patent_inventor_list", "patent_ipc_number", "patent_cpc_number",
                                              "patent_summary"])
            del data_demo["patent_detail_url"]
            del data_demo["patent_cpc_number"]
            str_list = []
            sum_list = []
            for i in data_demo["patent_publication_number"]:
                a = data_demo[data_demo["patent_publication_number"] == i]["patent_inventor_list"].values[0]
                b = data_demo[data_demo["patent_publication_number"] == i]["patent_summary"].values[0]
                a = ",".join(a)
                a = a.replace(",", ";")
                b = b.strip() if b else ""
                str_list.append(a)
                sum_list.append(b)
            data_demo["patent_inventor_list"] = str_list
            data_demo["patent_summary"] = sum_list
            data_demo.rename(columns={"company_name": "enterpriseName", "patent_type": "patentType",
                                      "patent_name": "patentName", "patent_publication_number": "applicationPublishNum",
                                      "patent_publication_date": "pubDate", "patent_application_number": "patentNum",
                                      "patent_application_date": "applicationTime",
                                      "patent_applicat": "applicantName1", "patent_inventor_list": "inventor",
                                      "patent_ipc_number": "cat",
                                      "patent_summary": "abstracts"}, inplace=True)

            save_mongo("tyc_data", "Patent", data_demo)
            delete_collection("spider_origin", "patenthub_temp")
            return self.datas
        else:
            return []

    def output_html(self):
        conn = MongoClient('mongodb://hub:hubhub@%s:%s/' % (gl.get_name(), gl.get_port()))
        db = conn.spider_origin  # 连接spider_origin数据库，没有则自动创建
        patent_hub = db.patenthub_temp  # 使用patenthub_data集合，没有则自动创建
        for data in self.datas:
            patent_hub.save(data)
        conn.close()
