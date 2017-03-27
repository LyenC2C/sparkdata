#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="t_santai_userid_cattag")

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def get_cate_dict(line):
    ss = line.strip().split('\t')
    return (valid_jsontxt(ss[0]),valid_jsontxt(ss[1]))

def f(line,cate_dict):
    ss = line.strip.split('\001')
    user_id = valid_jsontxt(ss[2])
    root_cat_name = valid_jsontxt(ss[11])
    cattag = valid_jsontxt(cate_dict.get(root_cat_name,"-"))
    if cattag == "-":
        return None
    return (user_id,cattag)

def createCombiner(kw):
    return set([kw])

def mergeValue(set, kw):
    set.update([kw])
    return set

def mergeCombiners(set0, set1):
    set0.update(set1)
    return list(set0)

# def reduce(a,b):
#     a_set = set([a])
#     a_set.update(set([b]))
#     return a_set

s_dim = '/user/wrt/santai_cat_label'
s_record = '/hive/warehouse/wl_base.db/t_base_ec_record_dev_new/ds=true'
cate_dict = sc.broadcast(sc.textFile(s_dim)\
    .map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd_c = sc.textFile(s_record).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)
rdd_result = rdd_c.combineByKey(lambda a:createCombiner(a),lambda a,b:mergeValue(a,b),lambda a,b:mergeCombiners(a,b))\
    .map(lambda (a,b):a + "\001" + "\002".join([valid_jsontxt(l) for l in b]))
rdd_result.saveAsTextFile("/user/wrt/temp/t_santai_userid_cattag")





