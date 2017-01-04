#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

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
    nid = ob.get("nid","\\N")
    if nid == "-": return None
    comment_count = ob.get("comment_count","\\N")
    user_id = ob.get("user_id","\\N")
    encryptedUserId = ob.get("shopcard",{}).get("encryptedUserId","\\N")
    nick = ob.get("nick","\\N")
    result = []
    result.append(user_id)
    result.append(comment_count)
    result.append(encryptedUserId)
    result.append(nick)
    return "\001".join([valid_jsontxt(ln) for ln in result])

rdd1 = sc.textFile("/commit/itemsearch/*2016122*")
rdd2 = sc.textFile("/commit/itemsearch/*2016123*")
rdd = rdd1.union(rdd2)
rdd.map(lambda x:f(x)).filter(lambda x:f(x)).saveAsTextFile("/user/wrt/temp/itemsearch_20_30")

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 120 t_wrt_item_search.py

