from flask import Flask, request
import json
from tyc import findInfoUtil

app = Flask(__name__)


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


if __name__ == "__main__":
    app.run(debug=True)
