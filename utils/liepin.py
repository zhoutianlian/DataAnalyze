import requests
import time
from lxml import etree
import random
import pandas as pd  # 转换格式写入本地
import pymongo  # 保存到MongoDB中
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
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
            headers = {  # 从想要爬取的页面中F12——》Newwork--》requesrHeaders复制
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Accept-Encoding": "gzip, deflate",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "cache-control": "max-age=0",
                "Cookie": "Hm_lvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(
                    now) + "; Hm_lpvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(now + 200),
            }
            time.sleep(random.randint(6, 15))
            # 构造请求
            html = requests.get(url, headers=headers)
        except Exception as e:
            print(e)
            return html
        return html

    def clientDB(self):
        from CONFIG import globalENV as gl
        client = pymongo.MongoClient('mongodb://hub:hubhub@%s:%s/' % (gl.get_name(), gl.get_port()))
        db = client.spider_origin  # spider_origin数据库
        p = db.liePin2  # persons集合
        return p

    # 获取公司招聘信息的所有页面地址
    def getExecl(self):
        # 存放文件的地址
        path = r'\\192.168.2.12\数据中心\天眼查数据'
        filenames = os.listdir(path)
        for filename in filenames:
            try:
                print(filename)
                sign = '_lp'
                reading = 'reading'
                if sign in filename:  # 判断该文件是否被爬取过
                    continue
                if reading in filename:  # 判断该文件是否被爬取过
                    continue
                new_name = filename[:-5] + reading + ".xlsx"
                os.rename(path + "\\" + filename, path + "\\" + new_name)  # 修改文件名字
                # count =1
                data = pd.read_excel(path + "\\" + new_name)
                result = data['公司名称']
                self.getInfo(result)
                new_name1 = new_name[:-12] + sign + ".xlsx"
                os.rename(path + "\\" + new_name, path + "\\" + new_name1)  # 修改文件名字
            except Exception as e:
                print('except:', e)
                print('finally...%s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                time.sleep(10)
                continue

    def getInfo(self, result):
        count = 1
        for name in result:
            print('Row %d : %s' % (count, name))
            url = 'https://www.liepin.com/zhaopin/?ckid=8a329f46fa708570&key=' + name
            # 1.在此处需要爬取第一页的招聘信息
            while True:
                try:
                    html = self.header(url)
                    etree_html = etree.HTML(html.text)
                    html.close()
                    if etree_html is not None:
                        # 根据url解析页面信息
                        div = etree_html.xpath('.//div[@class="alert alert-info sojob-no-result-alert"]')
                        if div:
                            break
                        else:
                            page = etree_html.xpath('.//div[@class="pagerbar"]/a')
                            if page:
                                pageUrl = page[-2].xpath('./@href')  # 获取下一页链接标签
                                href = "".join(pageUrl).strip()
                                if href != 'javascript:;':
                                    url = "https://www.liepin.com/" + href
                                    # 根据url解析页面信息
                                    self.run(etree_html, name)
                                else:
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
        li_list = etree_html.xpath('//ul[@class="sojob-list"]//li')  # self
        if li_list:
            for li in li_list:
                if li is None:
                    continue
                recruit = {}
                companyName = li.xpath('.//p[@class="company-name"]/a/text()')
                companyName = "".join(companyName).strip()
                jobTitle = li.xpath('.//div[@class="job-info"]/h3//text()')
                jobTitle = "".join(jobTitle).strip()
                wages = li.xpath('.//p/span[@class="text-warning"]/text()')
                wages = "".join(wages).strip().replace("\t", "")
                address = li.xpath('.//p/a[@class="area"]/text()')
                address = "".join(address).strip()
                education = li.xpath('.//p/span[@class="edu"]/text()')
                education = "".join(education).strip()
                experience = li.xpath('.//p/span[@class="edu"]/following-sibling::span[1]/text()')
                experience = "".join(experience).strip()
                industry = li.xpath('.//a[@class="industry-link"]/text()')
                industry = "".join(industry).strip()
                welfare = li.xpath('.//p[@class="temptation clearfix"]/span/text()')
                welfare = ",".join(welfare).strip()
                releaseTime = li.xpath('.//p[@class="time-info clearfix"]/time/text()')
                releaseTime = "".join(releaseTime).strip()
                mainCompany = name
                # print("保存公司招聘信息："+companyName)
                recruit['company_name'] = companyName
                recruit['job_title'] = jobTitle
                recruit['wages'] = wages
                recruit['address'] = address
                recruit['education'] = education
                recruit['experience'] = experience
                recruit['industry'] = industry
                recruit['release_time'] = releaseTime
                recruit['welfare'] = welfare
                recruit['preservation_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
                recruit['mainCompany'] = mainCompany
                # 保存信息
                recruit_list.append(recruit)
            date2 = pd.DataFrame(recruit_list, index=None)
            p = self.clientDB()
            date2 = date2.to_dict("records")
            info = p.insert_many(date2)
            # print(info)


if __name__ == "__main__":
    getHtml().getExecl()
    # print(getHtml().run())
