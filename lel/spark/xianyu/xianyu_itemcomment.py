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
        return None

def parseJson(ob):
    if ob is None: return [None]
    ts = ob[0]
    itemid = ob[1]
    if not isinstance(ob[2],dict): return [None]
    items = ob[2].get("data", {}).get("items", [])
    result = []
    if items:
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
    re = max(arr, key=itemgetter(-1))
    return '\001'.join([valid_jsontxt(i) for i in re])


sc = SparkContext(appName="xianyu_iteminfo_comment" + lastday)

data = sc.textFile("/commit/2taobao/leave_comment/*" + lastday + "/*")\
            .flatMap(lambda a: parseJson(getJson(a)))\
                .filter(lambda a: a is not None)\
                    .groupByKey().mapValues(list)\
                        .map(lambda (a,b): distinct(b))\
                            .saveAsTextFile("/user/lel/temp/xianyu_itemcomment")
