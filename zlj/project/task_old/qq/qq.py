__author__ = 'zlj'

path='/user/hadoop/refer/refer.20150824'

import json
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext

import rapidjson as json


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
sc=SparkContext(appName="test")


sqlContext = SQLContext(sc)
hiveContext=HiveContext(sc)

# qqlinkrdd=sqlContext.read.json(path).map(lambda  x: (x.QQ,x.TB))

def parse(line):
    ob=json.loads(valid_jsontxt(line))
    return (ob.get('QQ'),ob.get('TB'))

rdd=sc.textFile(path,100).map(lambda  x: parse(x))

schema = StructType([
           StructField("qq", StringType(), True),
           StructField("tbuid",StringType(), True)])

qqlinkdf=hiveContext.createDataFrame(rdd,schema)
hiveContext.registerDataFrameAsTable(qqlinkdf,'qqlink')


hiveContext.sql("use wlbase_dev")
hiveContext.sql("create table t_zlj_data_link as select * from qqlink where qq is not null and tbuid is not null ")


# ƒÍ¡‰
path='/user/hadoop/qq/info/qq_age.0611'

rdd_age=sc.textFile(path).map(lambda  x: (x.split()[0],x.split()[1]))
schema = StructType([
           StructField("qq", StringType(), True),
           StructField("age",StringType(), True)])
qqlinkdf=hiveContext.createDataFrame(rdd_age,schema)
hiveContext.registerDataFrameAsTable(rdd_age,'rdd_age')


hiveContext.sql("use wlbase_dev")
hiveContext.sql("create table t_zlj_qq_age as select * from rdd_age  ")



sc.textFile('/data/develop/ec/tb/user/userinfo.20160429.format/').\
    map(lambda x:int(x.split('\001')[2])).sum()

sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_user_info_s_tbuserinfo/000012_0').\
    map(lambda x:x.split('\001')).take(10)