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


def process(line):
    ob = json.loads(valid_jsontxt(line))
    if not isinstance(ob, dict): return None
    datas = ob.get("data", {}).get("data",[])
    result = []
    for data in datas:
        name = data.get("name", "\\N")
        numbers = data.get("number", [])
        for num in numbers:
            result.append((valid_jsontxt(num),valid_jsontxt(name)))
    return result


sc = SparkContext(appName="sougou_bankname_search" + lastday)

data = sc.textFile("/commit/credit/sogou/bankname.search.phone.20170111") \
    .flatMap(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .distinct()\
    .map(lambda (a,b): a+"\001" + b) \
    .saveAsTextFile("/user/lel/temp/sougou_bankname_search")