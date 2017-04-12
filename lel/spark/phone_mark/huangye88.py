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
    name = ob.get("name","\\N")
    area = ob.get("area", "\\N")
    company = ob.get("company", "\\N")
    detail = ob.get("detail", "\\N")
    update = ob.get("update", "\\N").replace("-","")
    phone = ob.get("phone", "\\N")
    contact = ob.get("contact", "\\N")
    tiezi_id = ob.get("tiezi_id", "\\N")
    pinpai = ob.get("pinpai", "\\N")
    cate = ob.get("cate", "\\N")
    return (phone,(name, area, company,contact,pinpai,cate,tiezi_id,detail,update))


def distinct(a,b):
    re = max(b, key=itemgetter(-1))
    return valid_jsontxt(a)+"\001"+"\001".join((valid_jsontxt(i) for i in re))


sc = SparkContext(appName="huangye88" + lastday)

data = sc.textFile("/commit/credit/huangye88/daikuan.info.20170215") \
    .map(lambda a:parseJson(getJson(a)))\
    .filter(lambda a: a is not None) \
    .groupByKey()\
    .map(lambda (a, b): distinct(a,list(b))) \
    .saveAsTextFile("/user/lel/temp/huangye88_daikuan")
