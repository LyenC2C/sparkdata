#coding:utf-8
__author__ = 'wrt'
import sys
import time
from pyspark import SparkContext

sc = SparkContext(appName="weibo_follow")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line,v_list):
    ss = line.strip().split("\001")
    user_id = ss[0]
    follow_list = ss[1].split(",")
    keywords = []
    num = 0
    for ln in follow_list:
        ln = valid_jsontxt(ln)
        if v_list.has_key(ln):
            num += 1
            for word in ["股民","股票","炒股","财经","证券","A股","B股","港股"]:
                if word in valid_jsontxt(v_list[ln]): keywords.append(word)
    if num == 0: return None
    result = valid_jsontxt(user_id) + "\001" + ",".join(list(set(keywords))) + "\001" + valid_jsontxt(num)
    return result

def f2(line):
    ss = line.strip().split("\001")
    text = valid_jsontxt(ss[1] + ss[2])
    return [valid_jsontxt(ss[0]),text]

s_v = "/hive/warehouse/wlservice.db/t_wrt_huaxun_weibo_gupiaov/*"

rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_weibo_user_fri/ds=20161106/*")
v_list = sc.broadcast(sc.textFile(s_v).map(lambda x:f2(x)).collectAsMap()).value
rdd.map(lambda x:f(x,v_list)).filter(lambda x:x!=None).saveAsTextFile("/user/wrt/temp/weibo_follow_guopiaov")

# hfs -rmr /user/wrt/temp/weibo_follow_guopiaov
# spark-submit  --executor-memory 16G  --driver-memory 16G  --total-executor-cores 200 weibo_follow.py
#LOAD DATA INPATH '/user/wrt/temp/weibo_follow_guopiaov' OVERWRITE INTO TABLE wlservice.t_wrt_huaxun_weiboid_keywords;