#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
#
# sc = SparkContext(appName="cmt")
# sqlContext = SQLContext(sc)
# hiveContext = HiveContext(sc)


'''
统计 tag 频率
'''
# path='/hive/warehouse/wlbase_dev.db/t_zlj_feed2015_parse_v2'
#
# rdd=sc.textFile(path).map(lambda x:x.split('\001')).filter(lambda x:x[0]=='35240355346')
# rdd.cache()
# rdd.map(lambda x:(int(x[5]),1)).reduceByKey(lambda  a,b:a+b).collect()
#
# for i in rdd.collect():    print i[3]
# imprs=rdd.filter(lambda x:len(x[-1])>0).map(lambda x:x[-1].split('|')).flatMap(lambda x:x).map(lambda x:count(x)).reduceByKey(lambda a,b:a+b)
#
# def count(x):
#     tag,impr,neg,neg_word=x.split(':')
#     if tag not in u'物流 价格 服务 包装'.split():return (u'商品'+u"_"+neg,1)
#     else: return (tag+u"_"+neg,1)
#
#
# for i in  imprs.collect(): print i[0],i[1]
#
# from pyspark.sql import *
# hc = HiveContext(sc)
# hc.sql('use wlbase_dev')
# rdd=hc.sql('select user_id ,impr_c  from t_zlj_feed2015_parse_v3 where LENGTH (impr_c)>1')
# rdd.map(lambda x:(x.user_id,len(x.impr_c.strip().split(u'|')))).reduceByKey(lambda a,b:a+b)\
#     .map(lambda x:x[1]).filter(lambda x:x<50).histogram([i for i in xrange(50)])
#
#
#
# sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_feed_parse_corpus_2015/*').coalesce(16).saveAsTextFile('/user/zlj/data/t_zlj_feed_parse_corpus_2015')


fw=open('D:\\emo_all','w')
kv=set()
for line in open('D:\\data-emo_1'):
    kv.add(line.split()[-1])
for i in kv:
    fw.write(i+'\n')