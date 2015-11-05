__author__ = 'wrt'
#coding:utf-8
from pyspark import SparkContext
import sys
from pyspark.sql import *

sc=SparkContext(appName="item_pinpai")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *
import time
hiveContext=HiveContext(sc)
import rapidjson as json
import datetime

def pinpai(line):
    ss = line.split('\001')
    return (ss[1],None)
#def f(x):

hiveContext.sql('use wlbase_dev')
rdd = hiveContext.sql('select * from t_base_ec_brand')
rdd2 = sc.textFile('/user/wrt/pinpai.info')
p_dict = rdd2.map(lambda x: pinpai(x)).collectAsMap()
broadcastVar = sc.broadcast(p_dict)
place_dict = broadcastVar.value
rdd.map(lambda x:[]).map(lambda x:f(x,place_dict))\
		.filter(lambda x:x!=None)\
			.saveAsTextFile(sys.argv[3])
sc.stop()