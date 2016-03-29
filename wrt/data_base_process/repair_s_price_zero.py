#coding:utf-8
__author__ = 'wrt'
import sys
from pyspark import SparkContext
sc = SparkContext(appName="repair s_price_zero")

def f(line):
    ss = line.strip().split("\001")
    if ss[3] == '0.0' or ss[3] == '0':
        ss[3] = ss[2]
    return line

s1 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/*/*"
rdd = sc.textFile(s1).map(lambda x:f(x))
rdd.saveAsTextFile()