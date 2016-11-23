#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="rong360_features_hebing")

def f(line):
    ss = line.strip().split("\001")
    # user_id = ss[0]
    features = ss[1].split(" ")
    return [(i.split(':')[0],"") for i in features]

s1 = "/hive/warehouse/wlservice.db/t_zlj_tmp_rong360_1w_record_level3_feature/*"
rdd = sc.textFile(s1).flatMap(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): x)
rdd.saveAsTextFile('/user/wrt/temp/rong360_3j_features')