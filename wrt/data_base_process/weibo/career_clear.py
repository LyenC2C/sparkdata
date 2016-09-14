#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="career_clear")

def f(line,word_dict):
    ss = line.strip().split('\001')
    # company = ss[2]
    department = ss[3]
    for word in word_dict:
        # if word in company:
        if word in department:
            return line.strip()
            # return None
    # return line.strip()
    ss[3] = "-"
    return "\001".join(ss)

s_word = '/user/wrt/career_word_1000'
occu_word = '/user/wrt/career_5000'
rdd = sc.textFile("/hive/warehouse/wlservice.db/t_wrt_tmp_career_yes/ds=20160908")
# rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_career/ds=20160830")

word_dict = sc.broadcast(sc.textFile(occu_word).map(lambda x: (x.strip(),0)).filter(lambda x:x!=None).collectAsMap()).value
rdd.map(lambda x:f(x, word_dict)).filter(lambda x:x!=None).saveAsTextFile('/user/wrt/temp/weibo_career_yes')
#hfs -rmr /user/wrt/temp/weibo_career_yes
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 career_clear.py
#create table t_wrt_tmp_career_yes like wlbase_dev.t_base_weibo_career;
#create table t_wrt_tmp_career_no like wlbase_dev.t_base_weibo_career;

#LOAD DATA  INPATH '/user/wrt/temp/weibo_career_yes' OVERWRITE INTO TABLE t_wrt_tmp_career_yes PARTITION (ds='20160908');
#LOAD DATA  INPATH '/user/wrt/temp/weibo_career_yes' OVERWRITE INTO TABLE t_wrt_tmp_career_no PARTITION (ds='20160908');