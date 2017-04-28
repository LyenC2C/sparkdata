# coding=utf-8
from pyspark import SparkContext
import rapidjson as json
from operator import itemgetter
import sys,re


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
    phones = ob.get("phones", "\\N")
    time_dot = ob.get("time", "\\N")
    end = time_dot.find(".")
    time = time_dot[0:end]
    name = ob.get("name", "\\N")
    addr = ob.get("addr", "\\N")
    phones_num=re.findall(r'\d+[-]?\d+',phones)
    result = []
    for phone in phones_num:
        result.append((phone,(name,addr,time)))
    return result


def distinct(a,b):
    re = max(b, key=itemgetter(-1))
    return valid_jsontxt(a)+"\001"+"\001".join((valid_jsontxt(i) for i in re))


sc = SparkContext(appName="dianhuabang" + lastday)

data = sc.textFile("/commit/credit/dianhuabang/*") \
    .flatMap(lambda a:process(a)) \
    .filter(lambda a: a is not None) \
    .groupByKey() \
    .map(lambda (a, b): distinct(a,list(b))) \
    .saveAsTextFile("/user/lel/temp/dianhuabang")
