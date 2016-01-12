#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext(appName="parse_comment")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def getfield(x):
    lv=x.split()
    rs=[]
    if len(lv)!=5: return None
    else:
        item_id,feed_id,user_id,feed,impr=lv
        for i in impr.split('|'):
            if ":" in i:
                rs.append(i.split(',')[-1])
    return [item_id,feed_id,user_id,feed,impr,'|'.join(rs)]





def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
        return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    else: return res
path='/user/zlj/temp/part-00015_parse'

path='/user/zlj/data/feed2015_parse-v1/*_parse'
schema1 = StructType([
    StructField("item_id", StringType(), True),
    StructField("feed_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("feed", StringType(), True),
    StructField("impr", StringType(), True),
    StructField("impr_c", StringType(), True)
    ])

# '' is None
#
# for i in ls:
#     if i is None: print 'dsfd'
#     for j in i:
#         if j is None:print i

hiveContext.sql('use wlbase_dev')
rdd=sc.textFile(path).map(lambda x:getfield(x)).filter(lambda x:x is not None)
df=hiveContext.createDataFrame(rdd,schema1)
hiveContext.registerDataFrameAsTable(df,'temp_zlj')
hiveContext.sql('drop table  if EXISTS t_zlj_feed2015_parse_v1')
hiveContext.sql('create table t_zlj_feed2015_parse_v1 as select * from temp_zlj')



# rds=rdd.filter(lambda x:x is not  None).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)\
#     .map(lambda x:x[0]+"\t"+str(x[1]))
# rds.saveAsTextFile(path+'count')