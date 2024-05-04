import urllib.parse
import urllib.request
import requests
import gzip
import time


class HtmlDownloader(object):

    def download(self, url):
        html = None
        try:
            if url is None:
                return None
            now = int(time.time())
            # 伪装成浏览器访问，直接访问的话csdn会拒绝
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            # headers = {'User-Agent':user_agent}
            headers = {
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "cache-control": "max-age=0",
                # "Cookie":"Hm_lvt_819e30d55b0d1cf6f2c4563aa3c36208="+str(now)+","+str(now+2)+"; Hm_lpvt_819e30d55b0d1cf6f2c4563aa3c36208="+str(now+10),
                # "Cookie":"Hm_lvt_562e30d46b0d1cf6f2c4563aa3c66209="+str(now)+"; Hm_lpvt_562e30d46b0d1cf6f2c4563aa3c66209="+str(now+200),
                "Cookie": "T_ID=20190809172740bIRPgNUAetWzxFTAct; UM_distinctid=16c75b4842e4f-00d597d1c2a19c-f353163-1fa400-16c75b4842f20f; CNZZDATA1259408509=380217028-1565340026-https%253A%252F%252Fwww.baidu.com%252F%7C1565340026; Qs_lvt_241723=1565342860; mediav=%7B%22eid%22%3A%22521176%22%2C%22ep%22%3A%22%22%2C%22vid%22%3A%22JZcIrWyhUc%3ARKO2Y%5BdnH%22%2C%22ctn%22%3A%22%22%7D; pref='ds:cn,s:score!,dm:mix_10'; _cnzz_CV1259408509=uc%7C20190809172740bIRPgNUAetWzxFTAct%7C0%26source%7Cb64%3Ad3d3LmJhaWR1LmNvbQlodHRwczovL3d3dy5iYWlkdS5jb20vbGluaz91cmw9X3JndDRwZXltMmtxbHptY2lndGZhM2JxaWJxbHdpX29kbGlwZW4zemo5Y2xuaG4tbmppNm5fXzZzODB4MXR1YiZ3ZD0mZXFpZD1jMjQyNzg2ODAwMTQ3ZTZlMDAwMDAwMDQ1ZDRkM2M3ZgkvCTExNi4yMzUuMzguMjI4%7C0%26module%7C%2Fs%7C0%26ip%7C116.235.38.228%7C0%26level%7C0%7C0; source=deleted; l=1; s=eWAGBQpqa1NDeHo+QgsFXCBRBhQjPxENIFI1LzBfPQEcJg5dVxQkDiUMfw4ZH09JGBIFCERJTnhHFjcGDQM1SjdmfAYGWVBWFRBMQ1dBIxdGSV5eE0dUGw==; Qs_pv_241723=4213763614387693600%2C1253868074590976800%2C3291658138790616000",
            }
            # 构造请求
            req = requests.get(url, headers=headers)
            # 访问页面
            # response = urllib.request.urlopen(req)
            # python3中urllib.read返回的是bytes对象，不是string,得把它转换成string对象，用bytes.decode方法
            html = req.content.decode(encoding="utf8").split('\n')
            req.close()
        except Exception as e:
            print(e)
            return html
        return html
