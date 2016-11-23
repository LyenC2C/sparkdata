#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="rong360_features_hebing")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


# def feature_main():


def f_3j_reindex(line):
    ss = line.strip().split("\001")
    # user_id = ss[0]
    features = ss[1].split(" ")
    return [(valid_jsontxt(i.split(':')[0]),"") for i in features]

# a = 34
# a_list = [0]*1000000

def feature_3j(line,a):
    line.strip() + fea_3j



s_3j = "/hive/warehouse/wlservice.db/t_zlj_tmp_rong360_1w_record_level3_feature/*"
s_main = "/hive/warehouse/wlservice.db/t_rong360_model_features_new/000000_0"
# rdd = sc.textFile(s_main).flatMap()
rdd = sc.textFile(s_3j).flatMap(lambda x:f_3j_reindex(x)).groupByKey().mapValues(list).map(lambda (x, y): x)
fea_3j = rdd.collect()
rdd2 = sc.textFile(s_main)
rdd2.map(lambda x:x.strip() + "".join(fea_3j)).saveAsTextFile('/user/wrt/temp/collect_test')
# rdd.saveAsTextFile('/user/wrt/temp/rong360_3j_features')