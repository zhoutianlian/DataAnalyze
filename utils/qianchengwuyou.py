import requests
import time
import random
import pymongo  # 保存到MongoDB中
from lxml import etree
import pandas as pd  # 转换格式写入本地
from urllib import parse
import os


class getHtml():

    # 访问请求头部信息
    def header(self, url):
        html = None
        try:
            if url is None:
                return None
            # 获取网页头部信息(反爬 让服务器知道你是一个浏览器)
            now = int(time.time())
            # 伪装成浏览器访问，直接访问的话csdn会拒绝
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36'
            headers = {  # 从想要爬取的页面中F12——》Newwork--》requesrHeaders复制
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "max-age=0",
                "Cookie": "Hm_lvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(
                    now) + "; Hm_lpvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(now + 200),
            }
            time.sleep(random.randint(6, 15))
            # 构造请求
            html = requests.get(url, headers=headers)
            html.encoding = 'gbk'  # 转换字符格式
        except Exception as e:
            print(e)
            return html
        return html

    # 保存信息到数据库中
    def clientDB(self):
        from CONFIG import globalENV as gl
        client = pymongo.MongoClient('mongodb://hub:hubhub@%s:%s/' % (gl.get_name(), gl.get_port()))
        db = client.spider_origin  # spider_origin数据库
        p = db.qianchengwuyou  # persons集合
        return p

    def getExecl(self):
        path = r'\\192.168.2.12\数据中心\天眼查数据'
        filenames = os.listdir(path)
        for filename in filenames:
            print(filename)
            sign = '_qcwy'
            reading = 'reading'
            if sign in filename:  # 判断该文件是否被爬取过
                continue
            if reading in filename:  # 判断该文件是否被爬取过
                continue
            new_name = filename[:-5] + reading + ".xlsx"
            os.rename(path + "\\" + filename, path + "\\" + new_name)  # 修改文件名字
            data = pd.read_excel(path + "\\" + new_name)  # \\192.168.2.12\数据中心\天眼查数据
            result = data['公司名称']
            self.getInfo(result)
            new_name1 = new_name[:-12] + sign + ".xlsx"
            os.rename(path + "\\" + new_name, path + "\\" + new_name1)  # 修改文件名字

    # 读取页面地址信息
    def getInfo(self, result):
        count = 1
        for name in result:
            print('Row %d : %s' % (count, name))
            demo = {"key": name}
            demo_en = parse.urlencode(demo)
            demo_en = demo_en[4:]
            url = 'https://search.51job.com/list/000000,000000,0000,00,9,99,' + demo_en + ',2,1.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
            while True:
                try:
                    html = self.header(url)
                    etree_html = etree.HTML(html.text)
                    html.close()
                    if etree_html is not None:
                        # 根据url解析页面信息
                        div = etree_html.xpath('.//li[@class="dw_nomsg"]')
                        if div:
                            print(name + "该公司未在此平台发布招聘信息")
                            break
                        else:
                            page = etree_html.xpath('.//li[@class="bk"]/a')
                            if page:
                                urlText = etree_html.xpath('.//li[@class="bk"]/a/text()')[-1]
                                if urlText == '下一页':
                                    url = etree_html.xpath('.//li[@class="bk"]/a/@href')[
                                        -1]  # 获取下一页标签# 获取下一页链接标签 **报错**
                                    self.run(etree_html, name)
                                else:
                                    self.run(etree_html, name)
                                    break
                            else:
                                self.run(etree_html, name)
                                break
                except Exception as e:
                    print('except:', e)
                    print('finally...%s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                    time.sleep(1800)
                    break
            count += 1

    # 解析网页提取有效信息
    def run(self, etree_html, name):
        recruit_list = []  # 招聘信息
        # 根据url解析页面信息
        div_list = etree_html.xpath('.//div[@class="el"]')
        if div_list:
            for div in div_list:
                if div is None:
                    continue
                recruit = {}
                jobTitle = div.xpath('.//p/span/a/text()')  # 职位名
                if jobTitle:
                    jobTitle = "".join(jobTitle).strip()
                    companyName = div.xpath('.//span[@class="t2"]/a/@title')  # 公司名
                    companyName = "".join(companyName).strip()
                    address = div.xpath('.//span[@class="t3"]/text()')  # 工作地点
                    address = "".join(address).strip()
                    wages = div.xpath('.//span[@class="t4"]/text()')  # 薪资
                    wages = "".join(wages).strip()
                    releaseTime = div.xpath('.//span[@class="t5"]/text()')  # 发布时间
                    releaseTime = "".join(releaseTime).strip()
                    recruit['company_name'] = companyName
                    recruit['job_title'] = jobTitle
                    recruit['wages'] = wages
                    recruit['address'] = address
                    recruit['release_time'] = releaseTime
                    recruit['preservation_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
                    recruit['mainCompany'] = name
                    # 保存信息
                    recruit_list.append(recruit)
                else:
                    continue
            if recruit_list != []:
                date2 = pd.DataFrame(recruit_list, index=None)
                p = self.clientDB()
                date2 = date2.to_dict("records")
                info = p.insert_many(date2)


if __name__ == "__main__":
    getHtml().getExecl()
