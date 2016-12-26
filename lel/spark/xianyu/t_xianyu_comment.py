# coding=utf-8
import rapidjson as json
from pyspark import SparkContext
from operator import itemgetter
import sys

lastday = sys.argv[1]


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def getJson(s):
    content = [valid_jsontxt(i) for i in (s.strip().split('\t'))]
    if len(content) == 4:
        ts = content[0]
        itemid = content[1]
        start = content[3].find("({") + 1
        js = content[3][start:-1]
        return (ts, itemid, json.loads(valid_jsontxt(js)))
    else:
        return ()


def parseJson(ob):
    if len(ob) == 0: return [None]
    ts = ob[0]
    itemid = ob[1]
    if type(ob[2]) != type({}): return [None]
    items = ob[2].get("data", {}).get("items", [])
    result = []
    if len(items) > 0 and type(items) == type([]):
        for item in items:
            lv = []
            if item.get("itemId", "") != "":
                commentId = item.get("commentId", "\\N")
                content = item.get("content", "\\N")
                reportTime = item.get("reportTime", "\\N")
                reporterName = item.get("reporterName", "\\N")
                reporterNick = item.get("reporterNick", "\\N")
                lv.append(itemid)
                lv.append(commentId)
                lv.append(content)
                lv.append(reportTime)
                lv.append(reporterName)
                lv.append(reporterNick)
                lv.append(ts)
                result.append((commentId, lv))
    return result


def distinct(arr):
    return '\001'.join([valid_jsontxt(i) for i in max(list, key=itemgetter(-1))])


sc = SparkContext(appName="xianyu_iteminfo_comment" + lastday)

data = sc.textFile("/commit/2taobao/leave_comment/*" + lastday + "/*")
re = data.flatMap(lambda a: parseJson(getJson(a))).filter(lambda a: a != None).groupByKey().map(
    lambda (a, b): distinct(list(b))).saveAsTextFile(
    "/user/lel/temp/xianyu_comment_2016")
