__author__ = 'wrt'
import sys
from pyspark import SparkContext
today = sys.argv[1]
sc = SparkContext(     + today)
s1 = "/commit/itemsale/ds=" + today
rdd = sc.textFile(s1)
rdd2 = rdd.filter(lambda x:len(x.split('\001')) == 11)
rdd2.coalesce(100).saveAsTextFile('/user/wrt/itemsale/ds=' + today)
fout = open("/home/wrt/sparkdata/wrt/data_base_process/sh/check_log/result_"+today,'w')
# fout.write(str(rdd.count() - rdd2.count()))