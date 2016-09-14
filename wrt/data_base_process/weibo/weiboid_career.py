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
    com = []
    dep = []
    for ln in y:
        com.append(ln[2])
        if ln[3] != "-":
            dep.append(ln[3])
    if dep == []: dep.append("-")
    com = list( set(com))
    dep = list(set(dep))
    com_r = "\t".join(com)
    dep_r = '\t'.join(dep)
    return x + "\001" + com_r + "\001" + dep_r


rdd = sc.textFile("/hive/warehouse/wlservice.db/t_wrt_tmp_career_yes/ds=20160909")
rdd1 = rdd.map(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd1.saveAsTextFile('/user/wrt/temp/weiboid_career')
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 weiboid_career.py