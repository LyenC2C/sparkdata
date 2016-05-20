#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext

import rapidjson as json

sc = SparkContext(appName="qqweibo_tags")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f(line):
    if line == "" or line == None:
        return None
    ob = json.loads(valid_jsontxt(line))
    info = ob.get("info","-")
    if info == "-": return None
    id = valid_jsontxt(info.get("id","-"))
    tags = info.get("tags",[])
    tags_r = ""
    result = []
    for tag in tags:
        # tags_r += valid_jsontxt(str(tag.get("category","-"))) + "_" + \
        #           valid_jsontxt(str(tag.get("content","-")).replace("\n","").replace("\r","").replace("\t",""))
        category = valid_jsontxt(str(tag.get("category","-")))
        tag_r = valid_jsontxt(str(tag.get("content","-")).replace("\n","").replace("\r","").replace("\t",""))
        result.append(id + "\001" + category + "\001" + tag_r)
    return result


# rdd = sc.textFile("/commit/project/ynfqqwb")

rdd = sc.textFile("/commit/project/ynfqqwb").flatMap(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile("/user/wrt/temp/ynfqqwb_tags")