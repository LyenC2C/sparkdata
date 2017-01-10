#coding:utf-8
__author__ = 'wrt'
#此脚本目的为统计5000个特征及新的占比特征的特征覆盖率，即每个特征在所有样本中非零值为多少
import sys
import rapidjson as json
import copy
from pyspark import SparkContext
from pyspark.sql import *

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def tongji(line,fname_dict):
    ss = line.strip().split("\001")
    # id = ss[0]
    features = ss[1]
    result = []
    for ln in features.split():
        feature = valid_jsontxt(ln.split(":")[0])
        f_name = fname_dict[feature]
        result.append((valid_jsontxt(f_name),1))
    return result
def dict_list(fcount_dict):
    result = []
    for key in fcount_dict:
        result.append(key + "\001" + fcount_dict[key])



rdd = sc.textFile("/hive/warehouse/wlcredit.db/t_credit_feature_merge_online/ds=20170106")
rdd_fname = sc.textFile("/hive/warehouse/wlcredit.db/t_wrt_credit_all_features_name/ds=5kfeature_addnewfeature")

fname_dict = sc.broadcast(rdd_fname.map(lambda x: (valid_jsontxt(x.split("\t")[1]),valid_jsontxt(x.split("\t")[0])))\
    .filter(lambda x:x!=None).collectAsMap()).value

rdd_fcount = rdd.flatMap(lambda x:tongji(x,fname_dict)).reduceByKey(lambda agg, obj: agg + obj)\
    .map(lambda (x,y):valid_jsontxt(x) + "\001" + valid_jsontxt(y))

rdd_fcount.saveAsTextFile("/user/wrt/temp/feature_coverage")
# fcount_list = dict_list(fcount_dict).


