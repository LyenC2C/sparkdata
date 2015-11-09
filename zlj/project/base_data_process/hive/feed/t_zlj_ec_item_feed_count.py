__author__ = 'zlj'


from pyspark import SparkContext
from pyspark.sql import *

sc=SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
from pyspark.sql.types import *

hiveContext=HiveContext(sc)


# 最大的10个feed id

# rdd=hiveContext.sql('select item_id,feed_id from t_base_ec_item_feed_dev where ds>20130102 and ds<20151103').map(lambda x:(x.item_id,x.feed_id))\
#     .groupByKey()


# rdd=hiveContext.sql('select item_id,feed_id from t_base_ec_item_feed_dev where ds>20151001').map(lambda x:(x.item_id,x.feed_id))\
#     .groupByKey()
def fun1(x):
    v=x.split('\001')
    if(len(x)>8):
        return (v[0],v[2])
    else: return None

sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/*',100).map(lambda x: '\t'.join(fun1(x))).saveAsTextFile('/user/zlj/data/t_zlj_ec_item_feed_count')
rdd=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/*').map(lambda x: fun1(x)).filter(lambda x: x!=None).groupByKey()

def fun(item,feed_ids):
    s=[ int(i) for i in list(feed_ids)]
    # s=list(feed_ids)
    lv=sorted(s,key=lambda t : t,reverse=True)
    ts=[ str(value) for index,value in enumerate(lv) if index<10]
    return (item,'_'.join(ts))

rdd1=rdd.map(lambda (x,y):fun(x,y)).coalesce(100)


schema = StructType([
	StructField("item_id",StringType(), True),
	StructField("feedids",StringType(), True)
	])

df=hiveContext.createDataFrame(rdd1,schema)
hiveContext.registerDataFrameAsTable(df,'tmptable')

# hiveContext.sql('select * from tmptable limit 10').collect()
sql='''
 insert overwrite table t_zlj_ec_item_feed_count partition(ds=%s)
        select *,'%s' as ds from tmptable
'''
ds='20151101'

hiveContext.sql(sql%(ds,ds))



