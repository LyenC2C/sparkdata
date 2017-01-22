#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *

today = sys.argv[1]


sc = SparkContext(appName="rong360_features_hebing")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ss = line.strip().split("\001")
    phone = ss[0]
    platform = ss[1]
    reg = ss[2]
    return (phone,[platform,reg])

def f2(x,y,muti):
    result = ["-1"] * 21
    for ln in y:
        i = muti.index(valid_jsontxt(ln[0]))
        result[i] = valid_jsontxt(ln[1])
    return x + "\001" + " ".join(result)


muti = ["和信贷","借了吗","马上金融","好贷宝","缺钱么","拍拍贷","翼龙贷","搜易贷","信用钱包","君融贷","人人贷","微贷网","口碑贷","闪电借款","珠宝贷","宜贷网","招联金融","钱爸爸","招商贷","团贷网","容易贷"]

rdd = sc.textFile("/hive/warehouse/wl_base.db/t_base_muti_platform/ds=20170116")
rdd.map(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): f2(x,y,muti)).saveAsTextFile("/user/wrt/temp/muti_feature")
#load data inpath '/user/wrt/temp/muti_feature' into table wl_base.t_base_muti_platform partition(ds = 20170116);