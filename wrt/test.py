#coding:utf-8
__author__ = 'wrt'
import sys
from pyspark import SparkContext


sc = SparkContext(appName="outdoor_sold")

def get_id_dict(x):
    return(x.strip(),None)

def f(line,id_dict):
    ss = line.strip().split("\001")
    item_id = ss[0]
    if len(ss) != 11: return None
    if not id_dict.has_key(item_id): return None
    sold = ss[6]
    flag = ss[10]
    ds = ss[11]
    return item_id + "\001" + sold + "\001" + flag + "\001" + ds

id_dict = sc.broadcast(sc.textFile("/hive/warehouse/wlservice.db/t_lzh_outdoorid").\
    map(lambda x: (x.strip(),None)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/*/part*")\
    .map(lambda x:f(x,id_dict)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/temp/outdoor_sold_tmp')

#spark-submit  --executor-memory 16G  --driver-memory 16G  --total-executor-cores 200 test.py

