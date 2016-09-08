#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="weiboid_career")

def f(line):
    ss = line.strip().split("\001")
    id = ss[0]
    return (id,ss)

def quchong(x,y):
    sch = []
    dep = []
    for ln in y:
        sch.append(ln[1])
        if dep != "-":
            dep.append(ln[2])
    if dep == []: dep.append("-")
    sch_r = "\t".join(sch)
    dep_r = '\t'.join(dep)
    return x + "\001" + sch_r + "\001" + dep_r


rdd = sc.textFile("/hive/warehouse/wlservice.db/t_wrt_tmp_career_yes/ds=20160909")
rdd1 = rdd.map(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd1.saveAsTextFile('/user/wrt/temp/weiboid_career')