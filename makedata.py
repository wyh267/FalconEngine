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
import MySQLdb
import logging

reload(sys)
sys.setdefaultencoding('utf8')


etlregex = re.compile(ur"[^\u4e00-\u9f5a0-9]")
def etl(content):
    content = etlregex.sub('',content)
    return content




logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
#                    filename='./corpus.log',
#                    filemode='w')
logger = logging.getLogger()


train_set=[]
all_corpus=[]
ip = "10.254.33.5"
port = 3306
user = "backend_reader"
passwd = "r@backend"
db = "jzl_AD_DB"

conn=MySQLdb.connect(host=ip,user=user,passwd=passwd,db=db,charset="utf8")
cursor = conn.cursor()
sql = "SELECT content_id,title,content,url FROM reco_media_content WHERE is_delete=0 AND cid=3321"
#print sql
#jieba.analyse.set_stop_words("stop_words.txt")
all_count=cursor.execute(sql)
md_count =0
#print "All Media Content Number : " + str(all_count)
sql_res=cursor.fetchall()

for info in sql_res:
    title=info[1].replace('\n',"")
    title=title.replace('\r',"")
    title=title.replace('\t',"")
    title=title.replace(' ',"")
    content=info[2].replace('\n',"")
    content=content.replace('\r',"")
    content=content.replace('\t',"")
    content=content.replace(' ',"")
    print str(info[0])+"\t" + title + "\t" + etl(content) +"\t" + info[3]