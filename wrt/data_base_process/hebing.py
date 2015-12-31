__author__ = 'hadoop'

import sys
from pyspark import SparkContext
ds = sys.argv[1]
sc = SparkContext(appName="hebing item_sale_" + ds)


s = "/hive/warehouse/wlbase_dev.db/t_wrt_tmp_20151230" #+ ds
rdd = sc.textFile(s).coalesce(16)
rdd.saveAsTextFile("/user/wrt/feed_2015")

# spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
# hebing.py 20151221
