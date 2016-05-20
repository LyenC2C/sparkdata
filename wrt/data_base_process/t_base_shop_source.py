#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="shop_source")

def f(line):
    ss = line.strip().split("\t")
    shop_id = ss[0]
    url = ss[1].split('.')
    if len(url) < 3:
        return None
    if url[1] == "taobao":
        source = "tb"
    elif url[1] == "tmall":
        if url[0] == "chaoshi":
            source = "tmcs"
        elif url[2] == "hk":
            source = "tmint"
        else:
            source = "tmall"
    else:
        source = "else"
    return shop_id + "\001" + source


rdd = sc.textFile("/commit/taobao/shop/shopinfo/shop.domain.20160421")
rdd1 = rdd.map(lambda x:f(x)).filter(lambda x:x!=None)
rdd1.saveAsTextFile('/user/wrt/temp/shop_source')

# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_shop_source.py
