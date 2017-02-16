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
    ob = json.loads(valid_jsontxt(line.strip()))
    flag = ob.get("flag", False)
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform is None or platform == 'None': return None
    return (valid_jsontxt(phone), valid_jsontxt(platform)) if flag else None


def distinct_oth(lakala, oth):
    return None if oth.has_key(lakala[0]) else lakala


def distinct_pre(thistime, previous):
    return None if previous.has_key(thistime[0]) else thistime


def createCombiner(kw):
    return set([kw])


def mergeValue(set, kw):
    set.update([kw])
    return set


def mergeCombiners(set0, set1):
    set0.update(set1)
    return set0


sc = SparkContext(appName="platform")
data = sc.textFile("/commit/regist/daichang/yixing.phonecheck.2017-02-08").map(lambda a: f(a)).filter(
    lambda a: a is not None).cache()

previous = sc.textFile("/user/lel/results/yixin/previous/product/*").map(lambda a: a.split("\t")).collectAsMap()
previous_b = sc.broadcast(previous)
previous_bv = previous_b.value

othersRDD = data.filter(lambda a: a[1] not in '拉卡拉') \
    .combineByKey(lambda a: createCombiner(a), lambda a, b: mergeValue(a, b), lambda a, b: mergeCombiners(a, b)) \
    .map(lambda a: distinct_pre(a, previous_bv)).filter(lambda a: a is not None)
othersRDD.coalesce(1).map(lambda a: a[0] + "\t" + ','.join(list(set(a[1])))).saveAsTextFile(
    "/user/lel/results/yixin/except_lakala20170215")

others_dis = othersRDD.collectAsMap()
others_dis_b = sc.broadcast(others_dis)
others_dis_bv = others_dis_b.value

lakalaRDD = data.filter(lambda a: a[1] in '拉卡拉') \
    .combineByKey(lambda a: createCombiner(a), lambda a, b: mergeValue(a, b), lambda a, b: mergeCombiners(a, b)) \
    .map(lambda a: distinct_pre(a, previous_bv)).filter(lambda a: a is not None).map(lambda a: distinct_oth(a, others_dis_bv)).filter(
    lambda a: a is not None)
lakalaRDD.coalesce(1).map(lambda a: a[0] + "\t" + ','.join(list(set(a[1])))).saveAsTextFile(
    "/user/lel/results/yixin/lakala20170215")

# lakalaRDD.union(othersRDD) \
#     .map(lambda a: a[0] + "\t" + ','.join(list(set(a[1])))) \
#     .coalesce(1).saveAsTextFile("/user/lel/results/yixin/multi_platform20170213")

