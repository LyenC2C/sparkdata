#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="career_clear")

def f(line,word_dict):
    ss = line.strip().split('\001')
    company = ss[2]
    for word in word_dict:
        if word in company:
            # return line.strip()
            return None
    return line.strip()

s_word = '/user/wrt/career_word_1000'
rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_career/ds=20160830")
word_dict = sc.broadcast(sc.textFile(s_word).map(lambda x: (x.strip(),0)).filter(lambda x:x!=None).collectAsMap()).value
rdd.map(lambda x:f(x, word_dict)).filter(lambda x:x!=None).saveAsTextFile('/user/wrt/temp/weibo_career_yes')
#hfs -rmr /user/wrt/temp/weibo_career_yes
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 career_clear.py
#create table t_wrt_tmp_career_yes like wlbase_dev.t_base_weibo_career;
#LOAD DATA  INPATH '/user/wrt/temp/weibo_career_yes' OVERWRITE INTO TABLE t_wrt_tmp_career_yes PARTITION (ds='20160908');