__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="item_info_new")
s1 = "/commit/iteminfo/*/*"
def f(line):
    try:
        ss = line.strip().split('\t',2)
        return (ss[1],line.strip())
    except Exception,e:
        print e
        return None
# def quchong(x,y):
#     return

rdd = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x!=None).groupByKey()\
    .mapValues(list).map(lambda (x,y):y[0])
rdd.saveAsTextFile('/user/wrt/item_info_new')