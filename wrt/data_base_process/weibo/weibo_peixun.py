#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="weibo_peixun")

def f(line):
    ob = json.loads(line.strip())
    uid = ob.get("uid","-")
    if uid == "-": return None
    ids = ob.get("ids",[])
    if ids == []: return None
    for id in ids:
        if id in [1306541104,1315832390,1192342862,1226048993]:
            return uid
    return None


rdd = sc.textFile("/data/develop/sinawb/rel_fri.json.20160401")
# rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_career/ds=20160830")

# word_dict = sc.broadcast(sc.textFile(occu_word).map(lambda x: (x.strip(),0)).filter(lambda x:x!=None).collectAsMap()).value
rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile('/user/wrt/temp/weibo_peixun')
#hfs -rmr /user/wrt/temp/weibo_career_yes
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 career_clear.py
#create table t_wrt_tmp_career_yes like wlbase_dev.t_base_weibo_career;
#create table t_wrt_tmp_career_no like wlbase_dev.t_base_weibo_career;

#LOAD DATA  INPATH '/user/wrt/temp/weibo_career_yes' OVERWRITE INTO TABLE t_wrt_tmp_career_yes PARTITION (ds='20160908');
#LOAD DATA  INPATH '/user/wrt/temp/weibo_career_yes' OVERWRITE INTO TABLE t_wrt_tmp_career_no PARTITION (ds='20160908');