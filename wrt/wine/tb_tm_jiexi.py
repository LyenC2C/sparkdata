#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="tb_tm_jiexi_wine")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def f(line):
    ss = line.strip().split("\t",2)
    txt = valid_jsontxt(ss[2])
    ob=json.loads(txt)
    props = ob.get("props")
    if type(props) != type([]): return None
    categoryId = valid_jsontxt(ob.get("itemInfoModel",{}).get("categoryId","-"))
    if categoryId != "50008144" and categoryId != "50013052": return None
    # for ln in props:
    #     # if valid_jsontxt("香型") in valid_jsontxt(ln["name"]):
    #     #     return None
    #     if valid_jsontxt("净含量") == valid_jsontxt(ln["name"]):
    #         return None
    return line

rdd = sc.textFile("/user/zlj/temp/zlj_wine.iteminfo.2016-03-07").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/zlj/temp/wrt_wine_tb_tm_baijiu')

# spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 tb_tm_jiexi.py