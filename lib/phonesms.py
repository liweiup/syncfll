#!/usr/bin/python
#coding=utf-8
"""
短信预警通知
"""

import requests
import urlencode
import sys
reload(sys)
sys.setdefaultencoding('utf8')


#短信通知
def sms(phoneNumber,content):
    u=urlencode.urlencode()
    content=u.urlencode(content, 'gbk')
    url='''http://sms.xdqxcm.xd-tech.cn/esmsThirdpartySend?businessId=126&mobile=%s&content=%s'''%(phoneNumber,content)
    #print url
    r=requests.get(url)
    #print r.text
    
if __name__ == "__main__":
    #短信通知
    sms_body = "测试测试"
    phoneNumber="15138931970"
    sms(phoneNumber, sms_body)    