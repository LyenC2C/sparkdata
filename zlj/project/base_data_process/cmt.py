#coding:utf-8
__author__ = 'zlj'
# import json
from pyspark.sql import *
from pyspark import SparkContext
import sys
from pyspark.sql import *
from pyspark.sql.types import *
sc=SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext=HiveContext(sc)
import time
import rapidjson as json
import datetime
# path='/data/develop/ec/tb/cmt/*'
# path='/data/develop/ec/tb/cmt/item.tmall.2.output.aa.2015-09-24'
# path='/data/develop/ec/tb/cmt/item.tmall.output.head.7M.2015-09-24'
# path='/data/develop/ec/tb/cmt/item.tmall.2.output.aa.2015-09-24'
path=sys.argv[1]


# t=sc.parallelize(s)

# s=''
# s.isdigit()
def parse(line):
    id ,a,n, tx=line.split('\t')
    index=tx.find('2(')
    text=tx[index+2:-1]
    # s= json.loads(text)
    data=json.loads(text)['data']
    list=[]
    list.append(id)
    list.append(str(data.get('total')))
    list.append(str(data.get('totalPage')))
    list.append(str(data.get('feedAllCount')))
    list.append(str(data.get('feedGoodCount')))
    list.append(str(data.get('feedNormalCount')))
    list.append(str(data.get('feedBadCount')))
    list.append(str(data.get('feedAppendCount')))
    list.append(str(data.get('feedPicCount')))
    return '\001'.join(list)

def parse_cmt(line):
    id ,a,n, tx=line.split('\t')
    index=tx.find('2(')
    text=tx[index+2:-1]
    ob=json.loads(text.encode('utf-8'))
    data=ob['data']
    list=[]
    for value in data['rateList']:
        l=[]
        l.append(value.get('auctionNumId','-'))
        l.append(value.get('id','-'))
        l.append(value.get('userId','-'))
        # l.append(data.get('userStar'))
        l.append(value.get('feedback','-').replace('\001','').replace('\n',''))
        date=value.get('feedbackDate','-')
        l.append(date)
        l.append(value.get('annoy','-'))
        l.append(str(time.mktime(datetime.datetime.now().timetuple())))
        # l.append(date.replace('-',''))
        list.append("\001".join(l))
        # list.append(l)
    return list

# path='/user/zlj/data/tmp/'
rdd=sc.textFile(path,100).filter(lambda  x: 'SUCCESS' in x)\
    .map(lambda  x: parse_cmt(x))\
    .flatMap(lambda  x: x)\
    .saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_item_tag_dev/ds=20000103')


#
#
schema = StructType([
    StructField("item_id",StringType(), True),
    StructField("feed_id",StringType(), True),
    StructField("user_id",StringType(), True),
    StructField("content",StringType(), True),
    StructField("f_date",StringType(), True),
    StructField("annoy",StringType(), True),
    StructField("ts",StringType(), True),
     StructField("ds",StringType(), True)]
)


df=hiveContext.createDataFrame(rdd,schema)
hiveContext.sql('use wlbase_dev')
hiveContext.sql('SET hive.exec.dynamic.partition=true ')
hiveContext.sql('SET hive.exec.dynamic.partition.mode=nonstrict ')
hiveContext.sql('SET hive.exec.max.dynamic.partitions.pernode = 1000 ')
hiveContext.sql('SET hive.exec.max.dynamic.partitions=1000 ')
hiveContext.sql('set hive.exec.reducers.bytes.per.reducer=500000000 ')

dfw=DataFrameWriter(df)
dfw.partitionBy().insertInto('table')
dfw.saveAsTable()
#
# # dfw.partitionBy('ds').insertInto('t_base_ec_item_feed_dev_test')
# hiveContext.registerDataFrameAsTable(df,'userbuy')
# # rdd1=hiveContext.sql('select ds from userbuy group by ds')
#
# result_rdd=hiveContext.sql('select * from userbuy where ds>20150801 ')
# dfw=DataFrameWriter(result_rdd)
# dfw.partitionBy('ds').insertInto('t_base_ec_item_feed_dev_test')

#

#
# hiveContext.sql('use wlbase_dev')
# hiveContext.sql("INSERT overwrite TABLE t_base_ec_item_feed_dev_test PARTITION ( ds ) select *,regexp_replace(f_date,'-','') as ds from userbuy ")


    # .saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_item_tag_dev/ds=20000102')