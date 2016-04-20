__author__ = 'zlj'
from pyspark import SparkContext
from pyspark.sql import *

sc=SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext=HiveContext(sc)

sc.textFile('/data/develop/ec/tb/cmt_res_tmp/res2015').repartition(250).saveAsTextFile('/user/zlj/data/res2015_re')


rdd=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev').map(lambda x:(x.split('\001')[0],1)).reduceByKey(lambda a,b:a+b)