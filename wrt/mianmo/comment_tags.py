#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="comment_tags")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return

def f(line):
    ob = json.loads(valid_jsontxt(line))
    if type(ob) != type({}): return [None]
    tags = ob.get("data",{}).get("tags",[])
    if tags == []: return [None]
    item_id = ob.get("item_id","-")
    if item_id == "-": return [None]
    result = []
    for ln in tags:
        lv = []
        tag_id = ln.get("id","-")
        count = ln.get("count","-")
        tag = ln.get("tag","-")
        posi = ln.get("posi","-")
        if tag_id == "-":
            result.append(None)
        else:
            lv.append(item_id)
            lv.append(tag_id)
            lv.append(count)
            lv.append(tag)
            lv.append(posi)
            result.append("\001".join(lv))
    return result

rdd = sc.textFile("/commit/project/mianmo/mianmo.item.tags")
rdd1 = rdd.flatMap(lambda x:f(x)).filter(lambda x:x!=None)
rdd1.saveAsTextFile('/user/wrt/temp/comment_tags')

# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 comment_tags.py