# coding:utf-8
import json
from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def f(line):
    jsonStr = valid_jsontxt(line.strip()).split("\t")[2]
    ob = json.loads(jsonStr)
    flag = ob.get("flag", False)
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform is None or platform == 'None': return None
    return (valid_jsontxt(phone), valid_jsontxt(platform)) if flag else None

def createCombiner(kw):
    return set([kw])

def mergeValue(set, kw):
    set.update([kw])
    return set

def mergeCombiners(set0, set1):
    set0.update(set1)
    return set0


sc = SparkContext(appName="platform")
data = sc.textFile("/commit/credit/bank/20170213.bocom.feisichuan.171.20170213")\
         .map(lambda a: f(a))\
         .filter(lambda a: a is not None).cache()

data.combineByKey(lambda a: createCombiner(a), lambda a, b: mergeValue(a, b), lambda a, b: mergeCombiners(a, b)) \
    .map(lambda a: a[0] + "\t" + ','.join(list(a[1]))) \
    .coalesce(1).saveAsTextFile("/user/lel/results/yixin/bank20170213")

