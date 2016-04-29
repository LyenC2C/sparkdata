#coding:utf-8
__author__ = 'zlj'
import sys
reload(sys)
sys.setdefaultencoding('utf8')


# 分词 建立索引
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)

stopwords=sc.textFile('/user/zlj/stopwords.txt').map(lambda x: x.strip()).collect()

broadcastVal=sc.broadcast(stopwords)
dic=broadcastVal.value

path='/hive/warehouse/wlbase_dev.db/t_base_ec_item_title_cut/ds=20151225/*'
t1=sc.textFile(path).map(lambda x: [i for  i in x.split('\001')[1].split() if  x not in dic]).flatMap(lambda x:x).map(lambda x :(x,1))\
    .reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[1]).zipWithIndex().map(lambda x:" ".join([x[0][0],str(x[0][1]),str(x[1])])).coalesce(2).saveAsTextFile('/user/zlj/data/aliwordseg_wordindex_1225')


t1.zipWithIndex().map(lambda x:" ".join([x[0][0],str(x[0][1]),str(x[1])])).coalesce(2).saveAsTextFile('/user/zlj/data/aliwordseg_wordindex_1225')


rs=sc.textFile().filter(lambda x:'' in x)

for i in rs.collect():print i
# sc.textFile('/user/zlj/tmp/sd').coalesce(2).saveAsTextFile('/user/zlj/aliwordseg_wordindex_1225')
# rdd1=hiveContext.sql(sql).map(lambda x:[x[0],1.0*x[1],1.0*x[2],1.0*x[3]])
# rdd=rdd1.map(lambda x: x[1:]).repartition(100)
#
# rdd1.map(lambda x:x[3]).histogram(100)
#
# rdd1.filter(lambda x:x[1]<30000).map(lambda x: x[1]).histogram([i*100 for i in xrange(300)])
# rdd=rdd1.filter(lambda x:x[1]<30000).map(lambda x: x[1]).histogram([i*100 for i in xrange(300)])



# sc.textFile('hive/warehouse/wlbase_dev.db/t_base_ec_item_title_cut/ds=20151225/file-cleandata').coalesce(100).saveAsTextFile('/user/zlj/tmp/data100')