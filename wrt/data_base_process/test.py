__author__ = 'wrt'
import sys
from pyspark import SparkContext
sc = SparkContext(appName="iteminfo_history")
s1 = "/commit/iteminfoorg"
def f(line):
    try:
        ss = line.strip().split('\t')
        return (ss[1],"")
    except Exception,e:
        print e
        return None
# def quchong(x,y):
#     return x

rdd = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x!=None)
#print rdd.count()
rdd2 = rdd.groupByKey().mapValues(list).map(lambda (x,y):x)
rdd2.saveAsTextFile('/user/wrt/iteminfo_history')
