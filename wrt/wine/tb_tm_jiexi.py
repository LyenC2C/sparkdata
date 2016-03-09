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
    txt = ss[2]
    ob=json.loads(txt)
    props = ob.get("props")
    for ln in props:
        if "香型".decode('utf-8') in valid_jsontxt(ln["name"]):
            return None
    return line

rdd = sc.textFile("/user/zlj/temp/zlj_wine.iteminfo.2016-03-07").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/zlj/temp/wrt_wine_tb_tm_wuxiangxing')

# spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 tongkuan.py