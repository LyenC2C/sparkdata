#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *


sc = SparkContext(appName="t_base_item_search")

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    text = line.strip().split("\t")[-1]
    ob = json.loads(valid_jsontxt(text))
    if type(ob) != type({}): return None
    nid = ob.get("nid","\\N")
    if nid == "-": return None
    comment_count = ob.get("comment_count","\\N")
    user_id = ob.get("user_id","\\N")
    encryptedUserId = ob.get("shopcard",{}).get("encryptedUserId","\\N")
    nick = ob.get("nick","\\N")
    result = []
    result.append(nid)
    result.append(user_id)
    result.append(comment_count)
    result.append(encryptedUserId)
    result.append(nick)
    return "\001".join([valid_jsontxt(ln) for ln in result])

# rdd1 = sc.textFile("/commit/itemsearch/*2016122*")d
# rdd2 = sc.textFile("/commit/itemsearch/*2016123*")

# rdd = rdd1.union(rdd2)
rdd = sc.textFile("/commit/itemsearch/*")
rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile("/user/wrt/temp/itemsearch_else")
# hfs -rmr /user/wrt/temp/itemsearch_20_30
# spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 120 t_wrt_item_search.py
# load data inpath "/user/wrt/temp/itemsearch_20_30" overwrite into table wlservice.t_wrt_item_search
# partition (ds = '20170104_20_30')

