__author__ = 'wrt'
# coding:utf-8
import sys
from pyspark import SparkContext

def f(line):
    ss = line.strip().split('\t')
    return (ss[0],ss[1:])

def shai(x,y):
    lv = []
    if len(y) == 1:
        return None
    else:
        for ln in y:
            if ln != "":
                lv = [x] + ln
            return "\t".join(lv)
sc = SparkContext(appName = "shai_qqnick.py")
f1 = "/data/develop/qq/group/qq.nicks.tag"
f2 = "/user/wrt/uin_d"
rdd1 = sc.textFile(f1).map(lambda x: f(x))
rdd2 = sc.textFile(f2).map(lambda x: (x, ""))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x,y):shai(x,y)).filter(lambda x:x!=None)\
    .saveAsTextFile("/user/wrt/uin_d_nick")