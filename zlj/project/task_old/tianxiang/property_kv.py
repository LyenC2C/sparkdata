#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hc = HiveContext(sc)

hc.sql('use wlservice ')


def f(x):
    ls=[]
    for k,v in x.items():
        ls.append((k,v))
    return ls


rdd=hc.sql('select * from t_tianxiang_feed_item_tmp_id_map').map(lambda x:f(x.paramap)).flatMap(lambda x:x)


rdd.map(lambda x:(x[0],1)).reduceByKey(lambda a,b:a+b).map(lambda x:x[0]+'\t'+str(x[1])).saveAsTextFile('/user/zlj/tmp/dd')




rdd=sc.textFile('/hive/warehouse/wlservice.db/t_tianxiang_feed_item_tmp_id_map').map(lambda x:x.split('\001')[-1])


