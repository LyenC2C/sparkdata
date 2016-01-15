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
统计 tag 频率
'''
path='/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v2'

rdd=sc.textFile(path).map(lambda x:x.split('\001')).filter(lambda x:x[0]=='35240355346')
rdd.cache()
rdd.map(lambda x:(int(x[5]),1)).reduceByKey(lambda  a,b:a+b).collect()

for i in rdd.collect():    print i[3]
imprs=rdd.filter(lambda x:len(x[-1])>0).map(lambda x:x[-1].split('|')).flatMap(lambda x:x).map(lambda x:count(x)).reduceByKey(lambda a,b:a+b)

def count(x):
    tag,impr,neg,neg_word=x.split(':')
    if tag not in u'物流 价格 服务 包装'.split():return (u'商品'+u"_"+neg,1)
    else: return (tag+u"_"+neg,1)


for i in  imprs.collect(): print i[0],i[1]