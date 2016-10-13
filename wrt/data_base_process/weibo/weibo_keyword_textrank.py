#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
# import jieba
import re
import jieba.analyse as ja


sc = SparkContext(appName="weibo_keyword_textrank")

pattern1 = re.compile(r"\[(.*?)\]", re.I|re.X)
pattern2 = re.compile(r"\#(.*?)\#", re.I|re.X)
pattern3 = re.compile(r"\@(.*?)\:", re.I|re.X)
pattern4 = re.compile(r"\@(.*?)\ ", re.I|re.X)
pattern5 = re.compile(r"(http://|https://)([A-Za-z0-9\./-_%\?\&=:]*)?", re.I)

# def clean_weibo(line):
# 	txt=pattern5.sub('',pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',line)))))
# 	return txt

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ss = line.strip().split("\001")
    text = pattern5.sub('',pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',valid_jsontxt(ss[3]))))))
    # text = pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',valid_jsontxt(ss[3])))))
    text = text.split("//")[0]
    if text == "" or text == "转发微博": return None
    return (ss[1],text)

def weibo_juhe(x,y):
    text_list = y
    user_weibo = "\n".join([valid_jsontxt(i) for i in text_list])
    res = ja.textrank(user_weibo, topK=20, withWeight=True,
                                 allowPOS=('an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'eng'))
    return x + "\001" + "\t".join([valid_jsontxt(i) for i in res])
    # return user_weibo



rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_text/ds=20161012/part-00000")
rdd1 = rdd.map(lambda x:f(x)).filter(lambda x:x!=None)
rdd2 = rdd1.groupByKey().mapValues(list).map(lambda (x, y): weibo_juhe(x, y))
rdd2.saveAsTextFile('/user/wrt/temp/weibo_keyword_textrank')

# hfs -rmr /user/wrt/temp/weibo_keyword_textrank
# spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 weibo_keyword_textrank.py