# -*- coding: utf-8 -*-
import json
import re
import redis
import requests
import socket
import traceback
from fake_useragent import UserAgent
from lxml import etree

socket.setdefaulttimeout(20)


class Grabber(object):

    def grab_page(self, msg, job_page_cfg):
        try:
            UA = UserAgent()
            HEADERS = {
                "Host": "www.kshr.com.cn",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
                "Accept-Encoding": "gzip, deflate",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Length": "56",
                "Origin": "http://www.kshr.com.cn",
                "Connection": "keep-alive",
                "Referer": "http://www.kshr.com.cn/template-job/job2/CompanyPosition2.aspx?comid=25868870003200&t=20201130141633",
                "Cookie": "ASP.NET_SessionId=pon0nzpty4osga1ajrzzqac3; Hm_lvt_cdb64048f95ffa796d617c888ecfb522=1606701551; Hm_lpvt_cdb64048f95ffa796d617c888ecfb522=1606703901",
                "Cache-Control": "max-age=0"
            }

            URL = "http://www.kshr.com.cn/handler/CommonDataHandler.ashx"
            forum_data = {
                'compid': '',
                'comid': '',
                'comtype': '0',
                'm': 'getcpositioncontent'
            }
            company_id = msg['company_id']
            forum_data['comid'] = str(company_id)

            HEADERS["User-Agent"] = UA.random
            response = requests.post(url=URL, data=forum_data, headers=HEADERS)
            if response.status_code == 200:
                html_text = response.text
                page_json = json.loads(html_text)
                other_parm_str = page_json['OtherParm']
                other_parm_json = json.loads(other_parm_str)[0]

                return {"company_name": other_parm_json['CompanyName'],
                     "industry": other_parm_json['Trade'],
                     "contact_person": other_parm_json['LinkMan'],
                     "phone_number": other_parm_json['Phone'],
                     "email": other_parm_json['newemail'],
                     "city": job_page_cfg['city']['city_name'],
                     "url": msg['job_url']}
        except Exception as e:
            traceback.print_exc()

        return None

    def grab_url(self, job_url_cfg):
        pool = redis.ConnectionPool(host=job_url_cfg['redis']['host'], port=int(job_url_cfg['redis']['port']), db=0,
                                    decode_responses=True)
        rds = redis.Redis(connection_pool=pool)
        job_title_hash_set = rds.smembers(job_url_cfg['redis']['job_title'])

        UA = UserAgent()
        HEADERS = {
            "Host": "www.kshr.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:83.0) Gecko/20100101 Firefox/83.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Cookie": "ASP.NET_SessionId=pon0nzpty4osga1ajrzzqac3; Hm_lvt_cdb64048f95ffa796d617c888ecfb522=1606701551; Hm_lpvt_cdb64048f95ffa796d617c888ecfb522=1606702747",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0"
        }

        URL = "http://www.kshr.com.cn/joblist.aspx?t=0.09271380908477389&currentpage=${PAGE}"

        JOB_URL_XPATH = '/html/body/form/div[4]/div/div/div[1]/div/div[${SNO}]/div/div[2]/a/@href'

        START_PAGE_NO, END_PAGE_NO = int(job_url_cfg['page']['start_no']), int(job_url_cfg['page']['end_no'])
        JOB_URL_START_SNO, JOB_URL_END_NO = int(job_url_cfg['job']['url_start_sno']), int(job_url_cfg['job']['url_end_sno'])
        PASS_WORD_LIST = job_url_cfg['pass_word']['pass_word_list'].split('\n')
        job_url_json_list = []
        for page_no in range(START_PAGE_NO, END_PAGE_NO):
            try:
                page_url = URL.replace("${PAGE}", str(page_no))
                HEADERS["User-Agent"] = UA.random
                response = requests.get(url=page_url, headers=HEADERS)
                if response.status_code == 200:
                    has_pass_word = False
                    for pass_word in PASS_WORD_LIST:
                        if response.text.find(pass_word) > -1:
                            has_pass_word = True
                            break
                    if has_pass_word:
                        for job_url_sno in range(JOB_URL_START_SNO, JOB_URL_END_NO):
                            job_url_xpath = JOB_URL_XPATH.replace("${SNO}", str(job_url_sno))
                            job_url_list = etree.HTML(response.text).xpath(job_url_xpath)
                            for job_url in job_url_list:
                                rs = re.search("(?<=comid=)\d+(?=&t=)", job_url)
                                if rs:
                                    company_id = rs.group()
                                    if company_id in job_title_hash_set:
                                        continue
                                    else:
                                        job_title_hash_set.add(company_id)
                                        rds.sadd(job_url_cfg['redis']['job_title'], company_id)
                                        job_url_json_list.append({
                                            'job_url': 'http://www.kshr.com.cn' + job_url,
                                            'company_id': company_id,
                                            'module_name': 'com.ezhiyang.www.hrs.task.kshr'})
                    else:
                        break
            except Exception as e:
                traceback.print_exc()

        return job_url_json_list


