#coding:utf-8
__author__ = 'wrt'
import sys
import time
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="weibo_follow")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ob = json.loads(line.strip())
    ret = ob.get("ret",False)
    if not ret: return None
    flag = ob.get("flag",Flase)
    if not flag: return None
    phone = ob.get("phone","-")
    # if phone == "-": return None
    platform = ob.get("platform","-")
    if phone == "-" or platform == "-": return None
    return (phone,platform)

def gb(x, y):
    platlist = list(set(y))
    platstr = ",".join(valid_jsontxt(ln) for ln in platlist)
    num = len(platlist)
    return valid_jsontxt(x) + "\001" + valid_jsontxt(num) + "\001" + valid_jsontxt(platstr)


s_r = "/commit/stock/20161128.stock.regist.all"
rdd = sc.textFile(s_r)
rdd.map(lambda x:f(x)).filter(lambda x:x!=None).groupByKey().mapValues(list).map(lambda (x, y): gb(x, y))\
    .saveAsTextFile("/user/wrt/temp/regist_platform")

# hfs -rmr /user/wrt/temp/regist_platform
# spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 120 weibo_follow.py
#LOAD DATA INPATH '/user/wrt/temp/weibo_follow_guopiaov' OVERWRITE INTO TABLE wlservice.t_wrt_huaxun_weiboid_keywords;