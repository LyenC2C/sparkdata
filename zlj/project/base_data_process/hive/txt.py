__author__ = 'zlj'
from pyspark import SparkContext
from pyspark.sql import *

sc=SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext=HiveContext(sc)

sc.textFile('/data/develop/ec/tb/cmt_res_tmp/res2015').repartition(250).saveAsTextFile('/user/zlj/data/res2015_re')