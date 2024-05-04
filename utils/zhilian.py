import requests
import time
import random
import pymongo
import json
import pandas as pd  # 转换格式写入本地
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
                "upgrade-Insecure-Requests": "1",
                "user-Agent": user_agent,
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-Encoding": "gzip, deflate, br",
                "accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "max-age=0",
                "cookie": "Hm_lvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(
                    now) + "; Hm_lpvt_819e30d55b0d1cf6f2c4563aa3c36208=" + str(now + 200),
            }
            time.sleep(random.randint(6, 15))
            # 构造请求
            html = requests.get(url, headers=headers)
            html.encoding = 'utf-8'  # 转换字符格式
        except Exception as e:
            print(e)
            return html
        return html

    # 保存信息到数据库中
    def clientDB(self):
        from CONFIG import globalENV as gl
        client = pymongo.MongoClient('mongodb://hub:hubhub@%s:%s/' % (gl.get_name(), gl.get_port()))
        db = client.spider_origin  # spider_origin数据库
        p = db.zhilian  # persons集合
        return p

    def getExecl(self):
        # 存放文件的地址
        path = r'\\192.168.2.12\数据中心\天眼查数据'
        filenames = os.listdir(path)
        for filename in filenames:
            try:
                print(filename)
                sign = '_zl'
                reading = 'reading'
                if sign in filename:  # 判断该文件是否被爬取过
                    continue
                if reading in filename:  # 判断该文件是否被爬取过
                    continue
                new_name = filename[:-5] + reading + ".xlsx"
                os.rename(path + "\\" + filename, path + "\\" + new_name)  # 修改文件名字
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

    # 读取页面地址信息
    def getInfo(self, result):
        row = 1
        for mainCompany in result:
            print('Row %d : %s' % (row, mainCompany))
            start = 0
            pageSize = 90
            url = 'https://fe-api.zhaopin.com/c/i/sou?start=%s&pageSize=%s&kw=%s&kt=3' % (
                start, pageSize, mainCompany)  # self.name
            while True:
                try:
                    html = self.header(url)
                    json_response = html.content.decode()  # 获取html的文本，就是一个json字符串
                    json_dict = json.loads(json_response)
                    json_list = json_dict['data']['results']
                    if json_list:
                        count = json_dict['data']['count']
                        print('数量: %s' % count)
                        while True:
                            if count >= 90:
                                self.run(json_list, mainCompany)  # 可以注释了 第一页
                                count -= 90
                                # 获取第二页信息
                                start += pageSize
                                url = 'https://fe-api.zhaopin.com/c/i/sou?start=%s&pageSize=%s&kw=%s&kt=3' % (
                                    start, pageSize, mainCompany)
                                html = self.header(url)
                                json_response = html.content.decode()  # 获取html的文本，就是一个json字符串
                                json_dict = json.loads(json_response)
                                json_list = json_dict['data']['results']
                                if json_list:  # 第二页不为空
                                    self.run(json_list, mainCompany)
                                    count -= 90
                                else:
                                    break
                            else:
                                self.run(json_list, mainCompany)
                                break
                        break
                    else:
                        print('没有发布招聘信息！！！')
                        break
                except Exception as e:
                    print('except:', e)
                    print('finally...%s' % time.strftime('%Y-%m-%d %H:%M:%S'))
                    time.sleep(1800)
                    break
            row += 1

    # 解析网页提取有效信息
    def run(self, json_list, mainCompany):
        recruit_list = []  # 招聘信息
        for list in json_list:
            recruit = {}
            jobTitle = list['jobName']  # 职位名称
            companyName = list['company']['name']  # 招聘公司名称
            companyType = list['company']['type']['name']  # 公司类型
            companySize = list['company']['size']['name']  # 公司员工人数
            address = list['city']['display']  # 公司所在城市 ['items']['name']
            releaseTime = list['updateDate']  # 职位信息修改日期
            wages = list['salary']  # 薪资
            education = list['eduLevel']['name']  # 学历
            experience = list['workingExp']['name']  # 工作年限
            welfare = list['welfare']  # 员工福利
            welfare = ",".join(welfare).strip()
            jobTypeList = list['jobType']['items']
            # jobTypes = ''
            for jobType in jobTypeList:
                jobType = jobType['name']  # 工作类型 ['name']
                recruit['jobType'] = jobType
            #     print(jobType)
            # print(
            #     jobTitle + "   " + companyName + "   " + companyType + "   " + companySize + "   " + address + "   " + releaseTime + "   " + wages + "   " + education + "   " + experience)  # +"   "+jobType+"   "+workingExp+"   "+welfare
            # print(welfare)
            # recruit['jobType'] = jobType
            recruit['company_name'] = companyName
            recruit['job_title'] = jobTitle
            recruit['wages'] = wages
            recruit['address'] = address
            recruit['release_time'] = releaseTime
            recruit['preservation_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
            recruit['mainCompany'] = mainCompany
            recruit['companyType'] = companyType
            recruit['companySize'] = companySize
            recruit['education'] = education
            recruit['experience'] = experience
            recruit['welfare'] = welfare
            # 保存信息
            recruit_list.append(recruit)
        if recruit_list != []:
            # 存储到数据库中
            date2 = pd.DataFrame(recruit_list, index=None)
            p = self.clientDB()
            date2 = date2.to_dict("records")
            info = p.insert_many(date2)


if __name__ == "__main__":
    getHtml().getExecl()
