#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
'''
错误标签过滤 以及 相似标签合并
'''


# 低频词

dic_filter={}

# 语义合并词

dic_merge={}

def clean(x):
    ls=''.split()
    for i in ls:
        if i in dic_filter:
            continue
        if dic_merge.has_key(i):
            i=dic_merge[i]


rdd=sc.textFile().map()