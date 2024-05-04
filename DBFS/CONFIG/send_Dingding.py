import requests, json

def send_to_dingding(secret):
      # 导入依赖库
    headers = {'Content-Type': 'application/json'}  # 定义数据类型
    webhook = 'https://oapi.dingtalk.com/robot/send?access_token=287cdcaf60f05350857f69bbf1cfc8c5136484c49466060da76e3b59e23e0515'
    # 定义要发送的数据
    # "at": {"atMobiles": "['"+ mobile + "']"
    data = {
        "msgtype": "text",
        "text": {"content": secret},
        "isAtAll": True}
    res = requests.post(webhook, data=json.dumps(data), headers=headers)
