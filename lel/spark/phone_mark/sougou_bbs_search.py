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
    jsonStr = line.split("\t")[2]
    phoneStr = line.split("\t")[0]
    start = phoneStr.find("(") + 1
    end = phoneStr.find(",")
    phone = phoneStr[start:end].replace("'","")
    return (phone,json.loads(valid_jsontxt(jsonStr)))




def parseJson(ob):
    phone = ob[0]
    data = ob[1]
    if not isinstance(data, dict): return None
    content = data.get("content", "\\N")
    cacheresult_info = data.get("cacheresult_info", "\\N")
    blog_info = data.get("blog_info", "\\N")
    date = data.get("date", "\\N")
    title = data.get("title", "\\N")
    url = data.get("url","\\N")
    time_stamp = data.get("time_stamp", "\\N")
    return (phone,content,url,cacheresult_info,blog_info,date,title,time_stamp)


sc = SparkContext(appName="sougou_bbs_search" + lastday)

data = sc.textFile("/commit/data_product/sogou_bbs/sougou.bbs.search.result.20170111") \
    .map(lambda a:parseJson(getJson(a)))\
    .filter(lambda a: a is not None) \
    .distinct().map(lambda a:"\001".join((valid_jsontxt(i) for i in a)))\
    .saveAsTextFile("/user/lel/temp/sougou_bbs_search")
