# -*- coding: utf-8 -*-：
import sys
import os

from django.http import JsonResponse

from financing_news.generate_news import News

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
import getopt
import json
from flask import Flask, request
import datetime
import traceback

# from RevGrowth.DataProcess import DataProcess
# from RevGrowth.cal_rev_growth import cal_rev_growth
import gc

from RevGrowthmini.cal_growth import cal_growth_mini

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False


@app.route('/nlp', methods=['POST'])
def nlp():
    response = "[]"
    try:
        text = request.form["text"]
        indus_list = nlp_indus(text)
        response = str(indus_list)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/liner", methods=['GET'])
def liner_mv():
    response = "FAIL"
    try:
        company = request.args.get("companyName")
        res = info_value(company)
        response = str(res)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route('/zhilian', methods=['GET'])
def zhilian():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    indus_list = test2.getHtml(company).getInfo()
    return str(indus_list)


@app.route("/liepin", methods=['GET'])
def liepin():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    indus = test5.getHtml(company).getInfo()
    return str(indus)


@app.route("/qcwy", methods=['GET'])
def qcwy():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    qcwy = qianchengwuyou.getHtml(company).getInfo()
    return str(qcwy)


@app.route("/patenthub", methods=['GET'])
def patenthub():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    patent = patenthub_main.SpiderMain(company).craw()
    return str(patent)


@app.route("/shunqi", methods=['GET'])
def shunqi():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    info = shunqi_main.Spider_Main(company).craw()
    return str(info)


@app.route("/law", methods=['GET'])
def law():
    company = request.args.get("enterpriseName")
    if not company:
        return "缺少参数enterpriseName"
    law_info = run(company)
    return str(law_info)


@app.route("/companyInfo", methods=['GET'])
def company_info():
    item = dict()
    company = request.args.get("enterpriseName")
    spider = request.args.get("spider")
    tyc = request.args.get("tyc")
    if spider not in [None, "1", "0"] or tyc not in [None, "1", "0"] or not company:
        return "缺少关键参数或参数错误"
    if not spider or spider == "1":
        item["zl"] = test2.getHtml(company).getInfo()
        item["lp"] = test5.getHtml(company).getInfo()
        item["qcwy"] = qianchengwuyou.getHtml(company).getInfo()
        item["patent"] = patenthub_main.SpiderMain(company).craw()
        item["shunqi"] = shunqi_main.Spider_Main(company).craw()
        item["law"] = run(company)
    if not tyc or tyc == "1":
        item["tyc"] = findInfoUtil.EnterpriseWholeData(company)
    gc.collect()
    return str(item)


@app.route("/getEnterpriseInfo", methods=['GET'])
def get_enterprise_info():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "TycEnterpriseInfo"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseAdminLicense", methods=['GET'])
def get_enterprise_admin_license():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "AL"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseHolder", methods=['GET'])
def get_enterprise_holder():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "Holder"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseNew", methods=['GET'])
def get_enterprise_new():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "New"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseCRW", methods=['GET'])
def get_enterprise_crw():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "CRW"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseCR", methods=['GET'])
def get_enterprise_cr():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "CR"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseICP", methods=['GET'])
def get_enterprise_icp():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "ICP"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterprisePatent", methods=['GET'])
def get_enterprise_patent():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "Patent"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseTradeMark", methods=['GET'])
def get_enterprise_trademark():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "TM"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseFinancing", methods=['GET'])
def get_enterprise_financing():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "Financing"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseAppbkInfo", methods=['GET'])
def get_enterprise_app_bk_info():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "AppbkInfo"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseCompetitors", methods=['GET'])
def get_enterprise_competitors():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    table_name = "FJP"
    res = findInfoUtil.findEnterprise(enterprise_name, table_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/getEnterpriseAll", methods=['GET'])
def get_enterprise_all():
    if not request.args or 'enterpriseName' not in request.args:
        return "缺少参数enterpriseName"
    enterprise_name = request.args['enterpriseName']
    res = findInfoUtil.EnterpriseWholeData(enterprise_name)
    if res is not None:
        return json.dumps(res, ensure_ascii=False)
    else:
        return "ERROR"


@app.route("/saveValuationData", methods=['POST'])
def saveValuationData():
    if request.method == 'POST':
        all_data = request.form
        if not all_data:
            return "缺少参数"
        else:
            if 'enterprise_name' not in all_data:
                return "缺少参数enterprise_name"
            elif 'mode' not in all_data:
                return "缺少参数mode"
            elif 'industry' not in all_data:
                return "缺少参数industry"
            elif 'peer' not in all_data:
                return "缺少参数peer"
            elif 'bal' not in all_data:
                return "缺少参数bal"
            elif 'flow' not in all_data:
                return "缺少参数flow"
            elif 'hypo' not in all_data:
                return "缺少参数hypo"
            elif 'bm' not in all_data:
                return "缺少参数bm"
            elif 'type' not in all_data:
                return "缺少参数type"
            elif 'basic' not in all_data:
                return "缺少参数basic"
            return json.dumps(SaveValuationRecord.start(all_data))


@app.route("/excelValue", methods=['POST'])
def excelValue():
    if request.method == 'POST':
        response = "FAIL"
        try:
            all_data = request.get_data()
            vid = excel_run(all_data)
            if vid:
                response = str(vid)
        except Exception as e:
            now = datetime.datetime.now()
            logger(str(now) + ": " + traceback.format_exc())
        finally:
            gc.collect()
            return response
    else:
        return "请求方式异常"


@app.route("/getSentiment", methods=['POST'])
def sentiment():
    response = ""
    try:
        passage = request.form["text"]
        s = get_analysis()
        s.set_sentence(passage)
        score = round(s.get_score(), 5)
        response = str(score)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/getPic", methods=['GET'])
def get_pic():
    response = "FAIL"
    try:
        rid = request.args.get("rid")
        if not rid:
            return "缺少参数rid"
        pic_id = trans_run(int(rid))
        response = str(pic_id)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/industryKgScore", methods=['GET'])
def industryKgScore():
    response = ""
    try:
        industry = request.args.get("industry")
        score = good_park_score(eval(industry))
        response = str(score)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/findIndus", methods=["GET"])
def findIndus():
    response = ""
    try:
        industry = request.args.get("industry")
        level = request.args.get("level")
        up_down = FindIndus(industry, level).all()
        response = str(up_down)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response




@app.route("/indus", methods=["POST"])
def indus_cat():
    response = ""
    try:
        # from keras.backend import tensorflow_backend as tb
        # tb._SYMBOLIC_SCOPE.value = True
        industry = request.form["text"]
        indus = indus_business(industry)
        response = str(indus)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/risk", methods=["GET"])
def risk_score():
    response = ""
    try:
        name = request.args.get("name")
        cr = calRisk(name, 100, 0.05, 7.5)
        response = str(cr.sum_up())
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        gc.collect()
        return response


@app.route("/revgrowth", methods=["POST"])
def rev_growth():
    response = 'okay'

    try:
        data = request.get_json()
        data_LSTM = DataProcess(data)
        data_cat_g3, data_cat_g4, data_LSTM, data_LSTM_conv = data_LSTM.input_data()
        result = cal_rev_growth(data_cat_g3, data_cat_g4, data_LSTM, data_LSTM_conv)
        print(result)
        response = str(result)

    except Exception as e:
        print(e)
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())

    finally:
        gc.collect()
        return response


@app.route("/revgrowthmini", methods=["POST"])
def rev_growth_mini():
    response = ''
    try:
        rev = request.form["rev"]
        ni = request.form["ni"]
        gics4 = request.form["gics4"]
        c = cal_growth_mini(float(rev), float(ni), float(gics4))
        y = c.main()
        response = str(y)
    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        return response


@app.route("/financingnews", methods=["POST"])
def get_financing_news():
    response = ''
    try:
        t = int(request.form["base_date"])
        if t:
            y = int(t/10000)
            m = int((t - y *10000)/100)
            d = (t - y *10000 -m*100)
            N = News(today=datetime.datetime(y, m, d))
        else:
            N = News()
        ret = N.generate_news()
        # response = JsonResponse(ret, json_dumps_params={'ensure_ascii': False})
        response = json.dumps(ret,ensure_ascii=False)
        print(response)

        return response

    except Exception as e:
        now = datetime.datetime.now()
        logger(str(now) + ": " + traceback.format_exc())
    finally:
        return response



@app.route("/")
def index():
    return "首页"


if __name__ == '__main__':

    from CONFIG import globalENV as gl

    opts, args = getopt.getopt(sys.argv[1:], 'e:')
    for key, value in opts:
        if key in ['-e']:
            gl.set_name(value)

    from news_nlp.eval import nlp_indus
    from liner.value import info_value
    from Patenthub import patenthub_main  # 专利信息
    from tyc import findInfoUtil
    from zhilian import test2  # 招聘信息
    from liepin import test5
    from qianchengwuyou import qianchengwuyou
    from shunqi import shunqi_main  # 工商信息
    from tools.law_req import run
    from log.log import logger
    from ValuationRecord import SaveValuationRecord
    from sentiment.senti_analy import get_analysis
    from industry_park.good_park import good_park_score
    from trans_img.trans import run as trans_run
    from find_indus.find_up_down import FindIndus
    # from NewIndus.dasda import get_all_res
    from indusCat.main import indus_business
    from excel_value.main import run as excel_run
    from RiskAssessment.calRisk import calRisk
    from RevGrowthmini import *
    import gc

    sys.argv = [sys.argv[0]]

    app.run(host="0.0.0.0", port=5001, threaded=True)
