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


def fun(x):
    ls=x.split('\001')
    id =ls[0]
    lable=ls[1]
    rs=[]
    for  index ,i in enumerate(ls[1:],1):
        if len(i)==11:continue
        i=-1 if i  =='\N' or i=='-' else i
        rs.append(str(index)+':'+str(round(float(i),2)) )
    rs.append(str(len(ls))+':'+id)
    return lable+' '+' '.join(rs)


def fun1(x):
    ls=x.split('\001')
    id =ls[0]
    lable=ls[1]
    rs=[]
    for  index ,i in enumerate(ls[2:],1):
        if len(i)==11:continue
        i=-1 if i  =='\N' or i=='-' else i
        rs.append(str(index)+':'+str(round(float(i),2)) )
    return lable+' '+' '.join(rs)


sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_train')\
    .map(lambda x:fun(x)).saveAsTextFile('/user/zlj/tmp/t_base_ec_record_dev_new_rong360_feature_train_svm')

sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_train')\
    .map(lambda x:fun(x)).saveAsTextFile('/user/zlj/tmp/t_base_ec_record_dev_new_rong360_feature_train_svm_idin')


sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_record_dev_new_rong360_feature_train').map(lambda x:fun1(x)).saveAsTextFile('/user/zlj/tmp/t_base_ec_record_dev_new_rong360_feature_train_svm_id')
