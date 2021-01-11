# -*- coding: utf-8 -*-
import hashlib
import json
import os
import re
import redis
import requests
import socket
import traceback
from datetime import datetime
from fake_useragent import UserAgent
from lxml import etree

socket.setdefaulttimeout(20)


class Grabber(object):

    def __init__(self, hrs_config, task_config):
        self.hrs_config = hrs_config
        self.task_config = task_config

        pool = redis.ConnectionPool(host=self.hrs_config['redis']['host'], port=int(self.hrs_config['redis']['port']),
                                    db=0,
                                    decode_responses=True)
        self.rds = redis.Redis(connection_pool=pool)
        self.job_title_hash_set = self.rds.smembers(self.task_config['redis']['job_title'])

    def grab_page(self, job_url_json):
        try:
            location = os.getcwd() + '/fake_useragent_0.1.11.json'
            UA = UserAgent(path=location)
            HEADERS = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9,gl;q=0.8",
                "Cache-Control": "max-age=0",
                "Connection": "keep-alive",
                "Cookie": "guid=0cf1d776c14f9e570418e137b73ec8eb; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; slife=lastvisit%3D020000%26%7C%26lowbrowser%3Dnot%26%7C%26; adv=adsnew%3D1%26%7C%26adsnum%3D2004282%26%7C%26adsresume%3D1%26%7C%26adsfrom%3Dhttps%253A%252F%252Fwww.baidu.com%252Fother.php%253Fsc.a000000YgEAsIbp_0IkOAfhQh8cCJRD1--q2tRyK2b1Ygijz0Obp0cbRyq-B6PxOycyx4xU7WLLaB01y6L3KWzhhW4mwv8kxF1On_IHB6e_7Ds8PaFtm317BFrTRt_jEU8Wv1MRfsc6KBaxkYnzLDe9ZmBw-33fLQPvZYg2cBHJ-xnVllaeFAj0fetHqjqpJJjTLGB5cOLTTJTZclSkixDNIQhZr.7b_NR2Ar5Od66CHnsGtVdXNdlc2D1n2xx81IZ76Y_uQQr1F_zIyT8P9MqOOgujSOODlxdlPqKMWSxKSgqjlSzOFqtZOmzUlZlS5S8QqxZtVAOtIO0hWEzxkZeMgxJNkOhzxzP7Si1xOvP5dkOz5LOSQ6HJmmlqoZHYqrVMuIo9oEvpSMG34QQQYLgFLIW2IlXk2-muCyr1FkzTf.TLFWgv-b5HDkrfK1ThPGujYknHb0THY0IAYqPH7JUvc0IgP-T-qYXgK-5H00mywxIZ-suHY10ZIEThfqPH7JUvc0ThPv5HD0IgF_gv-b5HDdnHbznjczPWT0UgNxpyfqnHczPHc3P1b0UNqGujYknjmYPjm4nfKVIZK_gv-b5HDkPHnY0ZKvgv-b5H00mLFW5HckPjb1%2526ck%253D3203.1.128.330.152.337.150.374%2526dt%253D1594970799%2526wd%253D51job%2526tpl%253Dtpl_11534_22836_18815%2526l%253D1519202267%2526us%253DlinkName%25253D%252525E6%252525A0%25252587%252525E5%25252587%25252586%252525E5%252525A4%252525B4%252525E9%25252583%252525A8-%252525E4%252525B8%252525BB%252525E6%252525A0%25252587%252525E9%252525A2%25252598%252526linkText%25253D%252525E3%25252580%25252590%252525E5%25252589%2525258D%252525E7%252525A8%2525258B%252525E6%25252597%252525A0%252525E5%252525BF%252525A751Job%252525E3%25252580%25252591-%25252520%252525E5%252525A5%252525BD%252525E5%252525B7%252525A5%252525E4%252525BD%2525259C%252525E5%252525B0%252525BD%252525E5%2525259C%252525A8%252525E5%25252589%2525258D%252525E7%252525A8%2525258B%252525E6%25252597%252525A0%252525E5%252525BF%252525A7%2521%252526linkType%25253D%26%7C%26; search=jobarea%7E%60010000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%CE%EF%C1%F7%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60040000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%CE%EF%C1%F7%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60030200%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%CE%EF%C1%F7%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60020000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%CE%EF%C1%F7%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch4%7E%60000000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%CE%EF%C1%F7%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21collapse_expansion%7E%601%7C%21",
                "Host": "jobs.51job.com",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1"
            }

            JOB_DETAIL_PAGE_STOP_WORD_LIST = self.task_config['stop_word']['job_detail_page_list'].split('\n')
            JOB_URL_STOP_WORD_LIST = self.task_config['stop_word']['job_url_list'].split('\n')

            url = job_url_json['job_url']
            city_name = job_url_json['city_name']

            for url_stop_word in JOB_URL_STOP_WORD_LIST:
                if url.find(url_stop_word) > -1:
                    return None

            HEADERS["User-Agent"] = UA.random
            response = requests.get(url=url, headers=HEADERS)
            if response.status_code == 200:
                html_text = response.text
                html_text = html_text.encode("ISO8859-1").decode("GBK")

                for page_stop_word in JOB_DETAIL_PAGE_STOP_WORD_LIST:
                    if response.text.find(page_stop_word) > -1:
                        return None

                rs = re.search("<p class=\"cname\"[\s\S]+?title=\"(.+?)\"", html_text)
                if rs:
                    title = rs.group(1)
                    title_hash = hashlib.md5(title.encode(encoding='UTF-8'))
                    title_hash_value = title_hash.hexdigest()
                    if title_hash_value in self.job_title_hash_set:
                        return None
                    else:
                        self.job_title_hash_set.add(title_hash_value)
                        self.rds.sadd(self.task_config['redis']['job_title'], title_hash_value)

                        INDUSTRY_XPATH = '/html/body/div[3]/div[2]/div[4]/div[1]/div[2]/p[3]/@title'
                        industry = etree.HTML(html_text).xpath(INDUSTRY_XPATH)[0]
                        rs = re.search("<p class=\"cname\"[\s\S]+?title=\"(.+?)\"", html_text)
                        if rs:
                            title = rs.group(1)
                            html_text = re.sub("<[\s\S]+?>", "", html_text)
                            rs_email = re.search("[a-zA-Z0-9_]+?@.+?\.(com|net|cn)", html_text)
                            if rs_email:
                                email = rs_email.group()
                                if email.find("51job") > -1:
                                    email = 'UNK'
                            else:
                                email = "UNK"
                            lines = html_text.split('\n')
                            phone, target_line = None, None
                            for line in lines:
                                line = line.strip()
                                if len(line) == 0:
                                    continue
                                rs = re.search("(联系|招聘|电话).*?(1\d{2}-\d{4}-\d{4})", line)
                                if rs:
                                    target_line = line
                                    phone = rs.group(2)
                                    break

                                rs = re.search("(1\d{2}-\d{4}-\d{4}).*?(联系|招聘|电话)", line)
                                if rs:
                                    target_line = line
                                    phone = rs.group(1)
                                    break

                                rs = re.search("(联系|招聘|电话).*?(1\d{10})", line)
                                if rs:
                                    target_line = line
                                    phone = rs.group(2)
                                    break

                                rs = re.search("(1\d{10}).*?(联系|招聘|电话)", line)
                                if rs:
                                    target_line = line
                                    phone = rs.group(1)
                                    break
                            if target_line:
                                contact_person = 'UNK'
                                rs = re.search(
                                    "(叶赫那拉|爱新觉罗|萨克达|上官|东方|东门|乌雅|乐正|亓官|令狐|仲孙|佟佳|公冶|公孙|单于|南宫|司徒|司空|司马|呼延|图门|夏侯|太叔|太史|宇文|完颜|宗政|富察|尉迟|张廖|张简|慕容|拓跋|梁丘|欧阳|段干|淳于|澹台|濮阳|申屠|百里|皇甫|章佳|端木|第五|范姜|西门|诸葛|赫连|轩辕|那拉|钟离|长孙|闻人|颛孙|马佳|鲜于|王|李|张|刘|陈|杨|黄|赵|吴|周|徐|孙|马|朱|胡|郭|何|林|高|罗|郑|梁|谢|宋|唐|许|邓|韩|冯|曹|彭|曾|肖|田|董|潘|袁|蔡|蒋|余|于|杜|叶|程|魏|苏|吕|丁|任|卢|姚|沈|钟|姜|崔|谭|陆|汪|范|廖|石|金|韦|贾|夏|付|方|邹|熊|白|孟|秦|邱|侯|江|尹|薛|闫|雷|龙|黎|史|陶|贺|毛|段|郝|顾|龚|邵|覃|武|钱|戴|严|莫|孔|向|常|汤)(先生|小姐|经理|主管|女士|老师|队长|主任|总)",
                                    target_line)
                                if rs:
                                    contact_person = rs.group()

                                return {"company_name": title,
                                        "industry": industry,
                                        "contact_person": contact_person,
                                        "phone_number": phone,
                                        "email": email,
                                        "city": city_name,
                                        "url": url,
                                        "mongo_time": datetime.now()}
        except Exception as e:
            traceback.print_exc()
        return None

    def grab_url(self):
        CITY_CODE_LIST = self.task_config.options("city")
        JOB_URL_PAGE_STOP_WORD_LIST = self.task_config['stop_word']['job_url_page_list'].split('\n')
        JOB_URL_STOP_WORD_LIST = self.task_config['stop_word']['job_url_list'].split('\n')
        START_PAGE_NO = int(self.task_config['page']['start_no'])
        END_PAGE_NO = int(self.task_config['page']['end_no'])

        location = os.getcwd() + '/fake_useragent_0.1.11.json'
        UA = UserAgent(path=location)
        HEADERS = {
            "Referer": "https://www.51job.com/",
            "Upgrade-Insecure-Requests": "1"
        }

        URL = "https://search.51job.com/list/${CITY},000000,0000,00,9,99,%2B,2,${PAGE}.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare="

        job_url_json_list = []
        for city_code in CITY_CODE_LIST:
            try:
                city_url = URL.replace("${CITY}", city_code)
                for page in range(START_PAGE_NO, END_PAGE_NO):
                    try:
                        page_url = city_url.replace("${PAGE}", str(page))
                        HEADERS["User-Agent"] = UA.random
                        response = requests.get(url=page_url, headers=HEADERS)
                        if response.status_code == 200:
                            has_stop_word = False
                            for stop_word in JOB_URL_PAGE_STOP_WORD_LIST:
                                if response.text.find(stop_word) > -1:
                                    has_stop_word = True
                                    break
                            if has_stop_word:
                                break
                            else:
                                rs = re.search("window\.__SEARCH_RESULT__ =(.+?)</script>", response.text)
                                if rs:
                                    search_result_str = rs.group(1)
                                    search_result_json = json.loads(search_result_str)
                                    engine_search_result = search_result_json["engine_search_result"]
                                    for engine_search_json in engine_search_result:
                                        has_stop_word = False
                                        for stop_word in JOB_URL_STOP_WORD_LIST:
                                            if engine_search_json['job_href'].find(stop_word) > -1:
                                                has_stop_word = True
                                                break
                                        if not has_stop_word:
                                            job_url_json_list.append({
                                                "job_url": engine_search_json['job_href'],
                                                'city_name': self.task_config['city'][city_code]})
                    except Exception as e:
                        traceback.print_exc()
            except Exception as e:
                traceback.print_exc()
        return job_url_json_list
