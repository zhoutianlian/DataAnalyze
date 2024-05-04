import requests, json

def send_to_dingding(secret):
      # 导入依赖库
    headers = {'Content-Type': 'application/json'}  # 定义数据类型
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token=7ed1d806d5b9e958300e88056bdcca12a207c610dfb70a3cb58a7f5362fcac4a'
    # 定义要发送的数据
    # "at": {"atMobiles": "['"+ mobile + "']"
    data = {
        "msgtype": "text",
        "text": {"content": secret},
        "isAtAll": True}
    res = requests.post(webhook, data=json.dumps(data), headers=headers)
