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


# rdd1=sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_tmp/').map(lambda x: x.split('\001')).filter(lambda x: x[0].isdigit and x[1].isdigit()).map(lambda x:(int(x[0]),int(x[1])))
# rdd2=sc.textFile('/user/zlj/project/termweight/title_corpus_aliseg_1206_clean_index/').map(lambda x:x.split('\001'))\
#     .filter(lambda x:x[0].isdigit() and len(x[0])>1)\
#     .map(lambda x:(int(x[0]),[int(i) for i in x[1].split() if i.isdigit() and len(i)>1]))
# rdd=rdd1.join(rdd2)
# def tcount(ls):
#     lv=[]
#     for i in ls:
#         lv.extend(i)
#     s=set(lv)
#     re=[]
#     for i in s:
#         re.append(str(i)+"\002"+str(lv.count(i)))
#     return '\003'.join(re)
# rdd.map(lambda (x,y):(y[0],y[1])).groupByKey().map(lambda (x,y):str(x)+"\001"+tcount(y)).saveAsTextFile('/user/zlj/project/termweight/joininfo')





rdd=sc.textFile('/user/zlj/project/termweight/joininfo/').map(lambda x:x.split('\001')[1].split('\003'))\
    .flatMap(lambda x:x).map(lambda x:x.split('\002')).filter(lambda x: len(x[0])>0)\
    .map(lambda  x: (int(x[0]),int(x[1])))\
    .reduceByKey(lambda a,b:a+b)

indexrdd=sc.textFile('/user/zlj/project/termweight/init_word_index').map(lambda x:x.split('\001')).map(lambda x:(x[0],x[1]))

broadcastVar = sc.broadcast(indexrdd.collectAsMap())
wdic = broadcastVar.value
rs=rdd.map(lambda (x,y):str(x)+"\t"+wdic.get(str(x).decode('utf-8'),'')+"\t"+str(y))
rs.saveAsTextFile('/user/zlj/project/termweight/init_word_index_count')

