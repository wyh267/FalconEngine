#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import random
import urllib2
import time
import re
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool 
import urllib

# GET REQUEST
def GetRequest(url,data):
    rsp = ""
    req_header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }
    try:
        para = urllib.urlencode(data)
        request = urllib2.Request(url,para,req_header)
        response = urllib2.urlopen(request,para,5)
        rsp = response.read()
        print "Get Url Content success..."
        return rsp
    except:
        print "Error to request the url [%s] ... " % url
        print "sleeping 1 second"
        time.sleep(1)
        return ""
    
def GetRequestGET(url):
    rsp = ""
    req_header = {
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'
    }
    try:
        request = urllib2.Request(url,None,req_header)
        response = urllib2.urlopen(request,None,5)
        rsp = response.read()
        print "Get Url Content success..."
        return rsp
    except:
        print "Error to request the url [%s] ... " % url
        print "sleeping 1 second"
        time.sleep(1)
        return ""


#pool = ThreadPool(4)
#urls = [
#        'http://www.baidu.com',
#        'http://www.sina.com.cn'
#          # 等等...
#        ]
#pool.map(GetRequest,urls)
#pool.close()
#pool.join()

# test simple 
def simpleSearch(perfixi,times):
    endurls = [
            'v1/search?cid=146&name=zhizhi',
            'v1/search?cid=146&-sex=1',
            'v1/search?cid=146&id=9900992'
            ]
    urls = map(lambda x : perfix+x,endurls)
    
    pool = ThreadPool(4)
    for i in range(times):
        contents = map(GetRequestGET,urls)
        #contents = pool.map(GetRequestGET,urls)
    pool.close()
    pool.join()
    for content in contents:
        if content == "":
            return False
    return True



# test update profile
def updateProfile(perfix,update_data):
    url = perfix + 'v1/update?'
    for v in update_data:
        if GetRequest(url,v)=="":
            return False
    return True

# test insert doc
def addRecord(perfix,update_data):
    url = perfix + 'v1/update?'
    for v in update_data:
        if GetRequest(url,v)=="":
            return False
    return True


# test delete doc

# test update all fields
def updateAll(perfix,update_data):
    url = perfix + 'v1/update?'
    for v in update_data:
        if GetRequest(url,v)=="":
            return False
    return True


# after update, do the query

# test post for compute score



host = "127.0.0.1"
port = 8089
perfix = "http://127.0.0.1:8089/"


insert_data_list = [
        {"id":"9900990","mobile_phone":"13911667890","sex":"1","contact_id":"9999","last_modify_time":"2015-09-11 15:26:18","email":"123@qq.com","source":"1","address":"","is_delete":"0","industry":"","name":"吴yh","from_source":"0","cid":"1146","email_click":"","sms_click":"","age":"0","job_title":"","buy_times":"","update_time":"2015-09-11 15:28:16","email_sended":"","company":"","score":"0","create_time":"2015-09-01 17:23:48","zip":"","birth":"1900-01-01","is_customer":"0","email_view":"","sms_sended":"","email_client":"0","website":"","annual_revenue":"0"},
        {"id":"9900991","mobile_phone":"12345678901","sex":"0","contact_id":"8888","last_modify_time":"2015-09-11 15:26:18","email":"234@qq.com","source":"1","address":"","is_delete":"0","industry":"","name":"weiwei","from_source":"0","cid":"1146","email_click":"","sms_click":"","age":"0","job_title":"","buy_times":"","update_time":"2015-09-11 15:28:16","email_sended":"","company":"","score":"0","create_time":"2015-09-01 17:23:48","zip":"","birth":"1900-01-01","is_customer":"0","email_view":"","sms_sended":"","email_client":"0","website":"","annual_revenue":"0"},
        {"id":"9900992","mobile_phone":"18511234567","sex":"0","contact_id":"7777","last_modify_time":"2015-09-11 15:26:18","email":"345@qq.com","source":"1","address":"","is_delete":"0","industry":"","name":"zhizhi","from_source":"0","cid":"1146","email_click":"","sms_click":"","age":"0","job_title":"","buy_times":"","update_time":"2015-09-11 15:28:16","email_sended":"","company":"","score":"0","create_time":"2015-09-01 17:23:48","zip":"","birth":"1900-01-01","is_customer":"0","email_view":"","sms_sended":"","email_client":"0","website":"","annual_revenue":"0"}
        ]
        
update_data_list = [
        {"id":"34","mobile_phone":"123123123","last_modify_time":"2015-09-11 15:26:18"},
        {"sex":"0","source":"1","email_client":"0","age":"0","id":"36","is_delete":"0","email_view":"","email_sended":"","name":"孙建清女装","contact_id":"1007","last_modify_time":"2015-09-11 15:26:20","website":"","email_click":"","industry":"","buy_times":"","is_customer":"0","score":"0","address":"","annual_revenue":"0","birth":"1983-08-17","update_time":"2015-09-11 15:28:18","email":"sjq@qq.com","sms_click":"","sms_sended":"","company":"","zip":"","create_time":"2015-09-01 17:23:48","cid":"146","mobile_phone":"18528786554","job_title":"","from_source":"0"}
        ]


update_all_data_list = [
        {"id":"34","mobile_phone":"123123123","sex":"1","contact_id":"1008","last_modify_time":"2015-09-11 15:26:18","email":"sjq@qq.com","source":"1","address":"","is_delete":"0","industry":"","name":"吴英昊nanzhuang","from_source":"0","cid":"146","email_click":"","sms_click":"","age":"0","job_title":"","buy_times":"","update_time":"2015-09-11 15:28:16","email_sended":"","company":"","score":"0","create_time":"2015-09-01 17:23:48","zip":"","birth":"1900-01-01","is_customer":"0","email_view":"","sms_sended":"","email_client":"0","website":"","annual_revenue":"0"},
        {"sex":"0","source":"1","email_client":"0","age":"0","id":"35","is_delete":"0","email_view":"","email_sended":"","name":"zhizhi","contact_id":"1007","last_modify_time":"2015-09-11 15:26:20","website":"","email_click":"","industry":"","buy_times":"","is_customer":"0","score":"0","address":"","annual_revenue":"0","birth":"1983-08-17","update_time":"2015-09-11 15:28:18","email":"sjq@qq.com","sms_click":"11111_0;22222_1","sms_sended":"","company":"九枝兰公司","zip":"","create_time":"2015-09-01 17:23:48","cid":"146","mobile_phone":"18528786554","job_title":"","from_source":"0"}

]

if updateProfile(perfix,update_data_list) == False :
    print "updateProfile fail..."
else:
    print "updateProfile success..."


if addRecord(perfix,insert_data_list) == False :
    print "updateIvt fail..."
else:
    print "updateIvt success..."

if updateAll(perfix,update_all_data_list) == False :
    print "updateAll fail..."
else:
    print "updateAll success..."

time.sleep(3)

if simpleSearch(perfix,2) == False :
    print "Simple search fail..."
else:
    print "Simple search success..."


