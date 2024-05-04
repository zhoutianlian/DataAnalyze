import re
import urllib
from urllib.parse import urlparse

from bs4 import BeautifulSoup


class HtmlParser(object):

    def _get_new_urls(self, page_url, soup):
        new_urls = set()
        # /view/123.htm
        links = soup.find_all('a', href=re.compile(r'/item/.*?'))
        for link in links:
            new_url = link['href']
            new_full_url = urllib.parse.urljoin(page_url, new_url)
            new_urls.add(new_full_url)
        return new_urls

    # 获取元数据
    def _get_new_data(self, name, page_url, new_str_demo):
        if page_url is None or new_str_demo is None:
            return None

        try:
            soup = BeautifulSoup(new_str_demo, 'html.parser')
            # 新建字典
            res = {}
            # 网址
            res['url'] = page_url
            # 公司名称
            res['company_name'] = name
            # 内容块
            patent_list_node = soup.find(attrs={'class': 'ui mix-mode', 'id': 'mode_1'})
            patent_list = []
            for patent_content in patent_list_node.find_all('ul', class_='patent-bib'):
                patent = {}
                patent_title_node = patent_content.find_all('span')
                patent_url_node = patent_content.find_all('a')
                # 发明类型
                # print(patent_title_node[1].get_text())
                if patent_title_node[1] is not None:
                    patent['patent_type'] = patent_title_node[1].get_text()
                # 发明详细信息url
                # print(patent_url_node[0]['href'])
                if patent_url_node[0] is not None:
                    patent['patent_detail_url'] = patent_url_node[0]['href']
                # 发明名称
                # print(patent_title_node[3].get_text())
                if patent_title_node[3] is not None:
                    patent['patent_name'] = patent_title_node[3].get_text()
                # 发明状态
                # print(patent_url_node[1].get_text())
                if patent_url_node[1] is not None:
                    patent['patent_status'] = patent_url_node[1].get_text()
                # 公告号
                # print(patent_content.find(attrs={'data-property': 'documentNumber'}).get_text())
                if patent_content.find(attrs={'data-property': 'documentNumber'}) is not None:
                    patent['patent_publication_number'] = patent_content.find(
                        attrs={'data-property': 'documentNumber'}).get_text()
                # 公告日
                # print(patent_content.find(attrs={'data-property': 'documentDate'}).get_text())
                if patent_content.find(attrs={'data-property': 'documentDate'}) is not None:
                    patent['patent_publication_date'] = patent_content.find(
                        attrs={'data-property': 'documentDate'}).get_text()
                # 申请号
                # print(patent_content.find(attrs={'data-property': 'applicationNumber'}).get_text())
                if patent_content.find(attrs={'data-property': 'applicationNumber'}) is not None:
                    patent['patent_application_number'] = patent_content.find(
                        attrs={'data-property': 'applicationNumber'}).get_text()
                # 申请日
                # print(patent_content.find(attrs={'data-property': 'applicationDate'}).get_text())
                if patent_content.find(attrs={'data-property': 'applicationDate'}) is not None:
                    patent['patent_application_date'] = patent_content.find(
                        attrs={'data-property': 'applicationDate'}).get_text()
                # 申请人
                # print(patent_content.find(attrs={'data-property': 'applicant'}).get_text())
                patent_applicat_node = patent_content.find(attrs={'data-property': 'applicant'})
                if patent_applicat_node is not None:
                    patent['patent_applicat'] = patent_applicat_node.get_text()
                # 发明人
                # print(patent_content.find_all(attrs={'data-property': 'inventor'}))
                patent_inventor_node = patent_content.find_all(attrs={'data-property': 'inventor'})
                if patent_inventor_node is not None:
                    patent_inventor_list = []
                    for patent_inventor in patent_inventor_node:
                        patent_inventor_list.append(patent_inventor.get_text())
                    patent['patent_inventor_list'] = patent_inventor_list
                # IPC分类号
                # print(patent_content.find(attrs={'data-property': 'ipc'}).get_text())
                patent_ipc_node = patent_content.find_all(attrs={'data-property': 'ipc'})
                if patent_ipc_node is not None:
                    patent_ipc_number = ''
                    for patent_ipc in patent_ipc_node:
                        if patent_ipc_number is '':
                            patent_ipc_number = patent_ipc.get_text()
                        else:
                            patent_ipc_number = patent_ipc_number + ',' + patent_ipc.get_text()
                    patent['patent_ipc_number'] = patent_ipc_number
                # CPC分类号
                patent_cpc_node = patent_content.find_all(attrs={'data-property': 'cpc'})
                if patent_cpc_node is not None:
                    patent_cpc_number = ''
                    for patent_cpc in patent_cpc_node:
                        if patent_cpc_number is '':
                            patent_cpc_number = patent_cpc.get_text()
                        else:
                            patent_cpc_number = patent_cpc_number + ',' + patent_cpc.get_text()
                    patent['patent_cpc_number'] = patent_cpc_number
                # 摘要
                # print(patent_content.find(attrs={'data-property': 'summary'}))
                patent_summary_node = patent_content.find(attrs={'data-property': 'summary'})
                if patent_summary_node is not None:
                    patent['patent_summary'] = patent_summary_node.get_text()
                patent_list.append(patent)
            res['patent_list'] = patent_list
        except Exception as e:
            print(e)
            return None
        return res

    def parse(self, name, html_content):
        page_url = 'https://www.patenthub.cn/s?ds=cn&dm=mix&s=score%21&q=' + name
        if page_url is None or html_content is None:
            return None
        page_urls = []
        soup = BeautifulSoup(html_content, 'html.parser')
        patent_size_node = soup.find(attrs={'class': 'ui clearfix'})
        patent_size = int(patent_size_node.div.span.get_text().replace('条', ''))
        if patent_size == 0:
            return page_urls
        else:
            # https://www.patenthub.cn/s?p=1&q=上海大数文化传媒股份有限公司&dm=mix&s=score%21&ds=cn
            page_num = patent_size // 10 + 2
            for i in range(1, page_num):
                new_page_url = 'https://www.patenthub.cn/s?p=' + str(i) + '&q=' + name + '&dm=mix&s=score%21&ds=cn'
                # print(new_page_url)
                page_urls.append(new_page_url)
        return page_urls
