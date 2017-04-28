# coding:utf-8
import rapidjson as json
from operator import itemgetter
from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def process(line):
    jsonStr = valid_jsontxt(line.strip())
    ob = json.loads(jsonStr)
    flag = valid_jsontxt(ob.get("flag", "False"))
    if 'None' in flag: return None
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform is None or platform == 'None': return None
    return ((phone,platform),flag)



sc = SparkContext(appName="touzi_licai")


chengdu = sc.textFile("/commit/regist/yingxiao/touzi_licai/chengdu/*")
bsg = sc.textFile("/commit/regist/yingxiao/touzi_licai/bsg/*/*")
ningbo = sc.textFile("/commit/regist/yingxiao/touzi_licai/ningbo/*")

all = chengdu.union(bsg).union(ningbo)

all.map(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .reduceByKey(lambda a,b:"True" if "True" in [a,b] else "False") \
    .map(lambda ((a,b),c): a + "\001" +b + "\001" + c) \
    .saveAsTextFile("/user/lel/temp/yingxiao/touzi_licai/all_20170401")




