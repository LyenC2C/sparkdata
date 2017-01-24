#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark import SparkConf
# import jieba
import re
import jieba.analyse as ja


sc = SparkContext(appName="weibo_keyword_textrank")

pattern1 = re.compile(r"\[(.*?)\]", re.I|re.X)
pattern2 = re.compile(r"\#(.*?)\#", re.I|re.X)
pattern3 = re.compile(r"\@(.*?)\:", re.I|re.X)
pattern4 = re.compile(r"\@(.*?)\ ", re.I|re.X)
#pattern5 = re.compile(r"(http://|https://)([A-Za-z0-9\./-_%\?\&=:]*)?", re.I)
pattern5 = re.compile(r"(http://|https://)([A-Za-z0-9\./-_%\?\&=:]*)", re.I)

# def clean_weibo(line):
# 	txt=pattern5.sub('',pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',line)))))
# 	return txt

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","").replace("\t"," ")

def f(line):
    ss = line.strip().split("\001")
    #微博清洗，去掉链接，@的人，话题以及表情
    text = pattern5.sub('',pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',valid_jsontxt(ss[3]))))))
    #text = pattern4.sub('',pattern3.sub('',pattern2.sub('',pattern1.sub('',valid_jsontxt(ss[3])))))
    text = text.split("//")[0]  #去掉转发
    text = text.split("@")[0]   #将刚才未清洗干净的@去掉
    # if text.strip() == "" or text.strip() == "转发微博": return None
    if text.strip() == "转发微博": text = ""
    return (ss[1],text)

def weibo_juhe(x,y):
    text_list = y
    user_weibo = "\n".join([valid_jsontxt(i) for i in text_list])
    res = ja.textrank(user_weibo, topK=20, withWeight=True,
                             allowPOS=('an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'eng'))
    words = []
    for ln in res:
        words.append(valid_jsontxt(ln[0]) + "_" + valid_jsontxt(ln[1]))
    return valid_jsontxt(x) + "\001" + "\t".join(words)
    # return user_weibo

def weibo_textrank(line):
    ss = line.strip().split("\001")
    user_id = ss[0]
    user_weibo = ss[1]
    res = ja.textrank(user_weibo, topK=20, withWeight=True,
                             allowPOS=('an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'eng'))
    words = []
    for ln in res:
        words.append(valid_jsontxt(ln[0]) + "_" + valid_jsontxt(ln[1]))
    return valid_jsontxt(user_id) + "\001" + "\t".join(words)

def weibo_juhe2(x,y):
    user_weibo = ""
    for ln in y:
        user_weibo += valid_jsontxt(ln) + "\t"
    # text_list = y
    # user_weibo = "\t".join([valid_jsontxt(i) for i in text_list])
    # res = ja.textrank(user_weibo, topK=20, withWeight=True,
    #                          allowPOS=('an', 'i', 'j', 'l', 'n', 'nr', 'nrfg', 'ns', 'nt', 'nz', 't', 'eng'))
    # words = []
    # for ln in res:
        # words.append(valid_jsontxt(ln[0]) + "_" + valid_jsontxt(ln[1]))
    # return valid_jsontxt(x) + "\001" + "\t".join(words)
    return valid_jsontxt(x) + "\001" + user_weibo

# rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_text/ds=20161126")
# rdd = sc.textFile("/hive/warehouse/wlservice.db/t_wrt_weibo_1yi8_text_concat")
rdd = sc.textFile("/user/wrt/t_wrt_weibo_1yi8_text_concat")
rdd.repartition(8000).map(lambda x:weibo_textrank(x)).saveAsTextFile('/user/wrt/temp/weibo_keyword_textrank')
# rdd2 = rdd1.groupByKey().mapValues(list).map(lambda (x, y): weibo_juhe(x, y))
# rdd2.saveAsTextFile('/user/wrt/temp/weibo_keyword_textrank')
# rdd2 = rdd1.groupByKey().map(lambda (x, y): weibo_juhe2(x, y))
# rdd2.saveAsTextFile('/user/wrt/temp/weibo_user_weibotext')

# conf = SparkConf()
# conf.set("spark.network.timeout","2000s")
# conf.set("spark.akka.timeout","1000s")
# conf.set("spark.akka.frameSize","1000")

# hfs -rmr  /user/wrt/temp/weibo_keyword_textrank
# spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 60 weibo_keyword_textrank.py
# LOAD DATA  INPATH '/user/wrt/temp/weibo_keyword_textrank' OVERWRITE INTO TABLE t_base_weibo_user_keywords PARTITION (ds='20161025');
# pyspark --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120