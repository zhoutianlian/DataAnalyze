import requests
import pandas as pd


url = 'http://192.168.1.109:7960/news/makeupfinancingnews'
data = {
    "ndays": 1,
}

response = requests.get(url=url, params=data).text
print(response)