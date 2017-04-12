# coding=utf-8
from pyspark import SparkContext
import rapidjson as json
from operator import itemgetter
import sys


lastday = sys.argv[1]


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def getJson(line):
    return json.loads(valid_jsontxt(line))




def parseJson(ob):
    if not isinstance(ob, dict): return None
    info = ob.get("data", {}).get("num_info", {})
    platform = ob.get("platform", "\\N")
    word = ob.get("word", "\\N")
    source = info.get("source", "\\N")
    amount = info.get("amount", "\\N")
    tag = info.get("tag", "\\N")
    place = info.get("place", "\\N")
    tel_co = info.get("tel_co", "\\N")
    return (word,(source, tag, amount, tel_co, place,platform,len(source)+len(tag)+len(place)+len(tel_co)))


def distinct(a,b):
    re = max(b, key=itemgetter(-1))
    return valid_jsontxt(a)+"\001"+"\001".join((valid_jsontxt(i) for i in re[:-1]))


sc = SparkContext(appName="sougou_400_800_search" + lastday)

data = sc.textFile("/commit/credit/sogou/phone.search.400.800.20170112") \
    .map(lambda a:parseJson(getJson(a)))\
    .filter(lambda a: a is not None) \
    .groupByKey()\
    .map(lambda (a, b): distinct(a,list(b))) \
    .saveAsTextFile("/user/lel/temp/sougou_400_800")
