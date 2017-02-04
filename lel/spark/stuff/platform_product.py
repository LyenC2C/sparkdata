#coding:utf-8
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
    flag = ob.get("flag",False)
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform is None or platform == 'None': return None
    if flag:
        return (valid_jsontxt(phone),valid_jsontxt(platform))
    else:
        None

def distinct(lakala,oth):
    if oth.has_key(lakala[0]):
        return None
    else:
        return lakala

def createCombiner(kw):
    return set([kw])
def mergeValue(set,kw):
    set.update([kw])
    return set
def mergeCombiners(set0,set1):
    set0.update(set1)
    return set0


sc = SparkContext(appName="platform")
data = sc.textFile("/commit/regist/yixin.2017-01-23").map(lambda a:f(a)).filter(lambda a:a is not None).cache()
lakalaRDD = data.filter(lambda a:a[1] in '拉卡拉')\
                .combineByKey(lambda a:createCombiner(a),lambda a,b:mergeValue(a,b),lambda a,b:mergeCombiners(a,b))
othersRDD = data.filter(lambda a:a[1] not in '拉卡拉')\
                .combineByKey(lambda a:createCombiner(a),lambda a,b:mergeValue(a,b),lambda a,b:mergeCombiners(a,b))
others_dis = othersRDD.collectAsMap()
broadcast = sc.broadcast(others_dis)
val = broadcast.value


lakalaRDD.map(lambda a: distinct(a,val)).filter(lambda a:a is not None).union(othersRDD).map(lambda a:a[0]+ "\t" + ','.join(list(set(a[1])))).coalesce(1).saveAsTextFile("/user/lel/res123456")



