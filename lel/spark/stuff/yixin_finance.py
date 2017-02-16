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
    if flag:
        return (valid_jsontxt(phone), valid_jsontxt(platform))
    else:
        None


def distinct(lakala, oth):
    if oth.has_key(lakala[0]):
        return None
    else:
        return lakala


def distinct_2(thistime, lasttime, before,oth):
    phone = thistime[0]
    if lasttime.has_key(phone) or before.has_key(phone):
        return None
    else:
        return distinct(thistime,oth)


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

lasttime = sc.textFile("/user/lel/results/platform100000").map(lambda a: a.split("\t")).collectAsMap()
broadcast_2 = sc.broadcast(lasttime)
val_2 = broadcast_2.value

previous = sc.textFile("/user/lel/results/before").map(lambda a: a.split("\t")).collectAsMap()
broadcast_3 = sc.broadcast(previous)
val_3 = broadcast_3.value

lakalaRDD = data.filter(lambda a: a[1] in '拉卡拉') \
    .combineByKey(lambda a: createCombiner(a), lambda a, b: mergeValue(a, b), lambda a, b: mergeCombiners(a, b)) \
    .map(lambda a: distinct_2(a, val_2, val_3)).filter(lambda a: a is not None)
lakalaRDD.coalesce(1).saveAsTextFile("/user/lel/results/lakala20170213")

othersRDD = data.filter(lambda a: a[1] not in '拉卡拉') \
    .combineByKey(lambda a: createCombiner(a), lambda a, b: mergeValue(a, b), lambda a, b: mergeCombiners(a, b)) \
    .map(lambda a: distinct_2(a, val_2, val_3)).filter(lambda a: a is not None)
othersRDD.coalesce(1).saveAsTextFile("/user/lel/results/except_lakala20170213")

others_dis = othersRDD.collectAsMap()
broadcast = sc.broadcast(others_dis)
val = broadcast.value

lakalaRDD.map(lambda a: distinct(a, val)) \
    .filter(lambda a: a is not None) \
    .union(othersRDD) \
    .map(lambda a: a[0] + "\t" + ','.join(list(set(a[1])))) \
    .coalesce(1).saveAsTextFile("/user/lel/results/multi_platform20170213")

# .map(lambda a:a[0]+ "\t" + ','.join(list(a[1])))\
# map(lambda a:distinct(a,val_2)).filter(lambda a:a is not None)
