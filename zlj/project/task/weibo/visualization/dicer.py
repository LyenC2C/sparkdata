#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
# import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


import itertools
def f(x):
    id,ids=x.split('\001')
    rs=[]
    kv=ids.split(',')
    for i in itertools.permutations(kv, 2):
        if len('\001'.join(i))<2:
            return None
        rs.append('\001'.join([id,'\002'.join(i)]))
    return rs

sc.textFile('/hive/warehouse/wlbase_dev.db/t_zlj_visul_weibo_user_fri_bi_friends_step1/')\
    .repartition(200).map(lambda x:f(x)).filter(lambda x:x!=None).flatMap(lambda x:x)\
    .saveAsTextFile('/user/zlj/tmp/weibo_bi_fri_tmp')


sc.textFile('/user/zlj/tmp/t_base_weibo_user_fri_tel')\
    .repartition(200).map(lambda x:f(x)).filter(lambda x:x!=None).flatMap(lambda x:x)\
    .saveAsTextFile('/user/zlj/tmp/weibo_bi_fri_tel_tmp')
