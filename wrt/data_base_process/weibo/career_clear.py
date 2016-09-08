#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json


sc = SparkContext(appName="career_clear")

def f(line,word_dict):
    ss = line.strip().split('\001')
    company = ss[2]
    for word in word_dict:
        if word in company:
            return line.strip()
    return None

s_word = '/user/wrt/career_word_1000'
rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_career/ds=20160830")
cate_dict = sc.broadcast(sc.textFile(s_word).map(lambda x: (x.strip(),0)).filter(lambda x:x!=None).collectAsMap()).value
rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile('/user/wrt/temp/weibo_career_yes')
