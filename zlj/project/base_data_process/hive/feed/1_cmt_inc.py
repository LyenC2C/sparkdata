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
            # list.append("\001".join(l))
            list.append(l)
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
    .flatMap(lambda  x: (x[0],x))\
    # .saveAsTextFile('/user/zlj/data/all_cmt/1')


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


ds_1=''
feed_count_rdd=hiveContext.sql('select * from t_zlj_ec_item_feed_count where ds=%s '%ds_1).map(lambda x: (x.item_id,x.feed_ids))


def fun(x,y):
    feedids=y[0]
    itemfeed=y[1]
    feed_id=itemfeed[2]
    t1=(1,[x,feedids])
    t2=(0,itemfeed)
    result=[]
    if feed_id in feedids:
        result.append(t1)

    else:
        lv=sorted([int(i) for i in feedids.split('_')],reverse=True)
        ts=[ str(value) for index,value in enumerate(lv) if index<10]
        t1=(1,[x,'_'.join(ts)])
        result.append(t1)
        result.append(t2)
    return result

allrdd=rdd.union(feed_count_rdd).groupByKey().map(lambda (x ,y): fun(x,y)).flatMap(lambda x: x)



countrdd=allrdd.filter(lambda x:x[0]==0).map(lambda x: x[1]) #[x,feedids]

def save_count(countrdd,ds):
    schema = StructType([
	StructField("item_id",StringType(), True),
	StructField("feedids",StringType(), True)
	])

    df=hiveContext.createDataFrame(countrdd,schema)
    hiveContext.registerDataFrameAsTable(df,'tmptable')

    # hiveContext.sql('select * from tmptable limit 10').collect()
    sql='''
     insert overwrite table t_zlj_ec_item_feed_count partition(ds=%s)
            select *,'%s' as ds from tmptable
    '''
    hiveContext.sql(sql%(ds,ds))


def save_item_feed(item_feedrdd,ds):
    df=hiveContext.createDataFrame(item_feedrdd,schema)
    hiveContext.registerDataFrameAsTable(df,'tmptable')
    hiveContext.sql('SET hive.exec.dynamic.partition=true')
    hiveContext.sql('SET hive.exec.dynamic.partition.mode=nonstrict')
    hiveContext.sql('SET hive.exec.max.dynamic.partitions.pernode = 1000')
    hiveContext.sql('SET hive.exec.max.dynamic.partitions=2000')
    hiveContext.sql('set hive.exec.reducers.bytes.per.reducer=500000000')

    sql='''
    INSERT INTO TABLE t_base_ec_item_feed_dev PARTITION (ds )
    select
    * from tmptable
    '''
    hiveContext.sql(sql)






# resultrdd=allrdd.filter(lambda x:x[0]==1).map(lambda x: x[0]) #itemfeed list













# rdd=sc.textFile('/user/zlj/data/all_cmt/1/part-00000').map(lambda x:x.split('\001')[:-1]).filter(lambda  x:len(x)==8)

# rdd=sc.textFile('/user/zlj/data/all_cmt/1').map(lambda x:x.split('\001')[:-1]).filter(lambda x:len(x)==8)







# sc.textFile('/data/develop/ec/tb/cmt_res_tmp/res2015').repartition(250).saveAsTextFile('/user/zlj/data/res2014_re')
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