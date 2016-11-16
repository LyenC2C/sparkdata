#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
from pyspark import SparkContext

today = sys.argv[1]
yesterday = sys.argv[2]

sc = SparkContext(appName="repair_shopitem_b_"+ today)

#此脚本用来修复shopitem的b店的销量字段，由于销量字段偶尔会null或者出现比昨天小的现象，现需要将历史数据从头到尾修复一次

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")



def f(line):
    ss = line.strip().split("\001")
    return (ss[1],ss)

def repair(x,y):
    item_list = y
    if len(item_list) == 1:
        result = item_list[0][:-1]
    if len(item_list) == 2:
        if item_list[0][-1] == today:
            t_list = item_list[0][:-1]
            y_list = item_list[1][:-1]
        elif item_list[0][-1] == yesterday:
            t_list = item_list[1][:-1]
            y_list = item_list[0][:-1]
        else:
            return None
        if (t_list[2].isdigit() == False) or (int(t_list[2]) < int(y_list[2])):
            t_list[2] = y_list[2]
        result = t_list
    return '\001'.join([valid_jsontxt(i) for i in result])

s1 = "/hive/warehouse/wlbase_dev.db/t_base_ec_shopitem_b/ds=" + today
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_shopitem_b/ds=" + yesterday
rdd1 = sc.textFile(s1).map(lambda x:f(x))
rdd2 = sc.textFile(s2).map(lambda x:f(x))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y):repair(x, y))
rdd.saveAstextFile("/user/wrt/repair_shopitem_b/repair_" + today)

#spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 repair_shopitem_b.py 20160905 20160904
