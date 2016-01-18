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

from collections import defaultdict
f_map = defaultdict(set)
for  i in '好  很好 不错  挺好  棒'.split():f_map['good'].add(i.strip().decode('utf-8'))
for  i in '快  很快  速度  神速'.split():f_map['wuliu'].add(i.strip().decode('utf-8'))
for  i in '热情 周到 耐心  解答 回答  讲解  细心 有问必答  服务'.split():f_map['fuwu'].add(i.strip().decode('utf-8'))
def merge(k,v):
    if v in f_map['good']:
        v='好'
    if v in f_map['wuliu']:#如果是物流的数据 直接返回物流
        k='物流'
        v='快'
    if v in  f_map['taidu'] or k in f_map['taidu']:
        k='服务'
        v='好'
    return k,v

def cut_1(x):
    try:
        lv=x.split()[-1].split('|')
        rs=[]
        for i in lv:
            if ':' in i:
                k,v=i.split(',')[-1].split(':')
                k1,v1=merge(k,v)
                rs.append(k1+":"+v1)
        return rs
    except: return None

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



hc=HiveContext(sc)
rdd=hc.sql('select impr_c from t_zlj_feed2015_parse_v3 where impr_c is not NULL ')\
    .map(lambda x:x.impr_c.split('|')).flatMap(lambda x:x).map(lambda x:(x,1))


rdd.reduceByKey(lambda a,b:a+b).map(lambda x:(x[1],x[0])).sortByKey(ascending=False,numPartitions=10)\
    .map(lambda x:str(x[0])+'\t'+x[1]).saveAsTextFile('/user/zlj/data/feed_2015_alicut_parse_rank_v3')


