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
    if flag in 'None': return None
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform is None or platform == 'None': return None
    return ((phone,platform),flag)



sc = SparkContext(appName="multplatform")

data = sc.textFile("/commit/regist/multplatform/*") \
    .map(lambda a: process(a)) \
    .filter(lambda a: a is not None ) \
    .groupByKey().mapValues(lambda a:"True" if "True" in list(a) else "False") \
    .map(lambda ((a,b),c): a + "\001" +b + "\001" + c) \
    .saveAsTextFile("/user/lel/temp/multiplatform_jiedai")

'''
#2017401
data = sc.textFile("/commit/regist/multplatform/jiedai/bsg/*/*") \
    .map(lambda a: process(a)) \
    .filter(lambda a: a is not None ) \
    .groupByKey().mapValues(lambda a:"True" if "True" in list(a) else "False") \
    .map(lambda ((a,b),c): a + "\001" +b + "\001" + c) \
    .saveAsTextFile("/user/lel/temp/multiplatform_jiedai_2")
'''


