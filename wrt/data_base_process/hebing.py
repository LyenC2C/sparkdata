__author__ = 'hadoop'

import sys
from pyspark import SparkContext
ds = sys.argv[1]
sc = SparkContext(appName="hebing item_sale_" + ds)


s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + ds
rdd = sc.textFile(s).coalesce(200)
rdd.saveAsTextFile("/hive/warehouse/testhive.db/t_base_ec_item_sale_dev/ds=" + ds)

# spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
# hebing.py 20151221
