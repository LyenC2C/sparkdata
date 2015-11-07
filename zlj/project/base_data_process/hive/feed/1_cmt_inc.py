#coding:utf-8
__author__ = 'zlj'
# import json
from pyspark import SparkContext
import sys
from pyspark.sql import *

sc=SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *
import time
hiveContext=HiveContext(sc)
import rapidjson as json
import datetime

# path='/data/develop/ec/tb/cmt/*'
# path='/data/develop/ec/tb/cmt/item.tmall.2.output.aa.2015-09-24'
# path='/data/develop/ec/tb/cmt/item.tmall.output.head.7M.2015-09-24'
# path='/data/develop/ec/tb/cmt/item.tmall.2.output.aa.2015-09-24'
path=sys.argv[1]


# t=sc.parallelize(s)


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
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content


def parse_cmt(line_s):
    line=valid_jsontxt(line_s)
    # ts =line[:line.find('\t')]
    ts=str(time.mktime(datetime.datetime.now().timetuple()))
    json_txt=line.strip()[line.find('2(')+2:-1]
    ob = json.loads(valid_jsontxt(json_txt))
    if type(ob) == type({}) and ob["data"].has_key("rateList"):
        data=ob['data']
        list=[]
        for value in data['rateList']:
            l=[]
            l.append(value.get('auctionNumId','-'))
            l.append(value.get('auctionTitle','-'))
            l.append(value.get('id','-'))
            l.append(value.get('userId','-'))
            # l.append(data.get('userStar'))
            feedback=value.get('feedback','-').replace('\001','').replace('\n','')
            l.append(valid_jsontxt(feedback))
            date=value.get('feedbackDate','-')
            l.append(date)
            l.append(value.get('annoy','-'))
            l.append(ts)
            l.append(date.replace('-',''))
            # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
            list.append("\001".join(l))
        return list
    # except:
    #     return None



def parse_cmt_test(line_s):
    line=valid_jsontxt(line_s)
    ts =line[:line.find('\t')]
    json_txt=line.strip()[line.find('2(')+2:-1]
    ob = json.loads(valid_jsontxt(json_txt))
    if type(ob) == type({}) and ob["data"].has_key("rateList"):
        data=ob['data']
        list=[]
        for value in data['rateList']:
            l=[]
            l.append(value.get('auctionNumId','-'))
            auctionTitle=value.get('auctionTitle','-').replace('\001','').replace('\n','')
            l.append(valid_jsontxt(auctionTitle))
            l.append(value.get('id','-'))
            l.append(value.get('userId','-'))
            # l.append(data.get('userStar'))
            feedback=value.get('feedback','-').replace('\001','').replace('\n','')
            l.append(valid_jsontxt(feedback))
            date=value.get('feedbackDate','-')
            l.append(date)
            l.append(value.get('annoy','-'))
            l.append(ts)
            l.append(date.replace('-',''))
            # l.append(str(time.mktime(datetime.datetime.now().timetuple())))
            # list.append("\001".join(l))
            list.append(l)
        return list

rdd=sc.textFile(path,120).filter(lambda  x: 'SUCCESS' in x)\
    .map(lambda  x: parse_cmt(x))\
    .filter(lambda x:x is not None)\
    .flatMap(lambda  x: x)\
    .saveAsTextFile('/user/zlj/data/all_cmt/1')


#
#
schema = StructType([
    StructField("item_id",StringType(), True),
    StructField("item_title",StringType(), True),
    StructField("item_id",StringType(), True),
    StructField("feed_id",StringType(), True),
    StructField("user_id",StringType(), True),
    StructField("content",StringType(), True),
    StructField("f_date",StringType(), True),
    StructField("annoy",StringType(), True),
    StructField("ts",StringType(), True)]
)

# rdd=sc.textFile('/user/zlj/data/all_cmt/1/part-00000').map(lambda x:x.split('\001')[:-1]).filter(lambda  x:len(x)==8)

rdd=sc.textFile('/user/zlj/data/all_cmt/1').map(lambda x:x.split('\001')[:-1]).filter(lambda x:len(x)==8)

hiveContext.sql('use wlbase_dev')
hiveContext.sql("SET hive.exec.dynamic.partition=true")

d1=datetime.datetime(2013, 1, 1)
d2=datetime.datetime(2013, 12, 31)

rdd1=rdd.filter(lambda x: '2013' in x[-3])

for i in xrange((d2-d1).days):
    d0=d1 + datetime.timedelta(days =i)
    ds1=d0.strftime("%Y-%m-%d")
    ds2=d0.strftime("%Y%m%d")
    data=rdd1.filter(lambda x: ds1 in x[-3]).repartition(10)
    df=hiveContext.createDataFrame(data,schema)
    hiveContext.registerDataFrameAsTable(df,'data')
    hiveContext.sql('insert overwrite table t_base_ec_item_feed_dev_zlj  PARTITION(ds='+ds2+') select * from data')


d1=datetime.datetime(2014, 1, 1)
d2=datetime.datetime(2014, 12, 31)
rdd1=rdd.filter(lambda x: '2014' in x[-3])

for i in xrange((d2-d1).days):
    d0=d1 + datetime.timedelta(days =i)
    ds1=d0.strftime("%Y-%m-%d")
    ds2=d0.strftime("%Y%m%d")
    # print ds1,ds2
    data=rdd1.filter(lambda x: ds1 in x[-3]).repartition(10)
    df=hiveContext.createDataFrame(data,schema)
    hiveContext.registerDataFrameAsTable(df,'data')
    hiveContext.sql('insert overwrite table t_base_ec_item_feed_dev_zlj  PARTITION(ds='+ds2+') select * from data')

d1=datetime.datetime(2015, 1, 1)
d2=datetime.datetime(2015, 10, 31)
rdd1=rdd.filter(lambda x: '2015' in x[-3])

for i in xrange((d2-d1).days):
    d0=d1 + datetime.timedelta(days =i)
    ds1=d0.strftime("%Y-%m-%d")
    ds2=d0.strftime("%Y%m%d")
    data=rdd1.filter(lambda x: ds1 in x[-3]).repartition(10)
    df=hiveContext.createDataFrame(data,schema)
    hiveContext.registerDataFrameAsTable(df,'data')
    hiveContext.sql('insert overwrite table t_base_ec_item_feed_dev_zlj  PARTITION(ds='+ds2+') select * from data')



sc.textFile('/data/develop/ec/tb/cmt_res_tmp/res2015').repartition(250).saveAsTextFile('/user/zlj/data/res2014_re')
#
#
# df=hiveContext.createDataFrame(rdd,schema)
# hiveContext.sql('use wlbase_dev')
# hiveContext.sql('SET hive.exec.dynamic.partition=true ')
# hiveContext.sql('SET hive.exec.dynamic.partition.mode=nonstrict ')
# hiveContext.sql('SET hive.exec.max.dynamic.partitions.pernode = 1000 ')
# hiveContext.sql('SET hive.exec.max.dynamic.partitions=1000 ')
# hiveContext.sql('set hive.exec.reducers.bytes.per.reducer=500000000 ')
#
# dfw=DataFrameWriter(df)
# dfw.partitionBy().insertInto('table')
# dfw.saveAsTable()
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