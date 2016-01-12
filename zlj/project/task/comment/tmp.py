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


path="/user/zlj/data/feed_2015_alicut_parse/parse_split_clean_cut_part-00000_0002"
path="/user/zlj/data/feed_2015_alicut_parse/"
def cut(x):
    lv=x.split('\001')
    if len(lv)==6:return lv[-1]
    else:return None

def cut_1(x):
    lv=x.split()[-1].split('|')
    rs=[]
    for i in lv:
        if ':' in i:
            rs.append(i.split(',')[-1])
    return rs

rdd=sc.textFile(path).map(lambda x: cut_1(x)).filter(lambda x:x is not None).flatMap(lambda x:x).map(lambda x:(x,1))\
    .reduceByKey(lambda a,b:a+b).map(lambda x:(x[1],x[0])).sortByKey(ascending=False,numPartitions=10)

rdd.map(lambda x:str(x[0])+'\t'+x[1]).saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_rank')

# rdd.map(lambda x:x[0]).histogram()

rdd.saveAsTextFile('/user/zlj/data/impr_list')
def clean(x):
    ls=[]
    lv=x.split('|')
    for i in lv :
        ls.extend(i.split(':'))
    return ls

rdd.map(lambda x:clean(x)).flatMap(lambda x:x).distinct(100).saveAsTextFile('/user/zlj/data/impr_list1')
rdd.distinct(10).saveAsTextFile('/user/zlj/data/impr_list')



impr_words_rdd=sc.textFile('/user/zlj/data/impr_list1').map(lambda x:(x.strip(),1)).collectAsMap()

broadcastVar = sc.broadcast(impr_words_rdd)
impr_words=broadcastVar.value

path="/hive/warehouse/wlbase_dev.db/t_zlj_t_base_ec_feed_cut_cat_feed/000000_0"
sc.textFile(path).map(lambda x:x.split('\001')).repartition(1)\
    .map(lambda x:x[0]+"\001"+"\001".join([i for i in x[1].split() if  impr_words.has_key(i)])).saveAsTextFile('/user/zlj/data/impr_list1_cat_feed_chi')
