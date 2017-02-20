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
    flag = ob.get("ascription", "\\N")
    return flag if flag else  None


sc = SparkContext(appName="daichang")
data = sc.textFile("/commit/data_product/black_phone_sec/*") \
    .map(lambda a: process(a)) \
    .distinct()\
    .filter(lambda a: a is not None)\
    .saveAsTextFile("/user/lel/temp/black_phone_fields")
