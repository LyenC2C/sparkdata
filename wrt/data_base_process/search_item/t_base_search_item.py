#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="search_item")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    text = valid_jsontxt(line.strip())
    ob = json.loads(text)
    if type(ob) != type({}): return None
    nid = ob.get("nid","-")
    if nid == "-": return None
    comment_count = ob.get("comment_count","-")
    return (nid,comment_count)

rdd = sc.textFile("/commit/itemsearch/taobao.item.20161121")
rdd.map(lambda x:f(x)).filter(lambda x:x != None).groupByKey()\
    .map(lambda (x,y): valid_jsontxt(x) + "\001" + valid_jsontxt(y[0]))\
    .saveAsTextFile("/user/wrt/temp/search_item")
