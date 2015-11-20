__author__ = 'wrt'

#coding:utf-8
import sys
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
sc = SparkContext(appName="spark item_sale")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
def f(line):
    ss = line.strip().split('\001')
    flag = '0'
    ss.append(flag)
    ss[2] = float(ss[2])
    ss[3] = float(ss[3])
    ss[5] = int(ss[5])
    ss[6] = int(ss[6])
    ss[7] = int(ss[7])
    return ss
def quchong(x):
    max = 0
    item_list = x[1:]
    if len(item_list) == 1:
        item_list[9] = '1'
    else:
        for ln in item_list:
            if ln[8] > max:
                max = ln[8]
                x[1] = item_list
    return x

schema = StructType([
    StructField("item_id",StringType(), True),
	StructField("item_title",StringType(), True),
	StructField("r_price",FloatType(), True),
    StructField("s_price",FloatType(), True),
    StructField("bc_type",StringType(), True),
    StructField("quantity",IntegerType(), True),
    StructField("total_sold",IntegerType(), True),
    StructField("order_cost",IntegerType(), True),
    StructField("shop_id",StringType(), True),
    StructField("ts",StringType(), True),
    StructField("flag",StringType(),True)
	])

hiveContext.sql('use testhive')
s1 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + sys.argv[1] #today
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + sys.argv[2] #yesterday
rdd1 = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd2 = sc.textFile(s2).map(lambda x:f(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd = rdd1.union(rdd2).groupByKey().map(lambda x:quchong(x)).map(lambda x:[x[0]] + x[1:])
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.registerDataFrameAsTable(df, 'data')
#st = s.find('2015')
#ds2 = s[st:st+4] + s[st+5:st+7] + s[st+8:st+10]
#l = len(s1)
#ds1 = s1[l-8:]
hiveContext.sql('insert overwrite table t_base_ec_item_sale_dev PARTITION(ds=' + s1 + ') select * from data')
		#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()
#spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80 t_wrt_base_ec_item_sale.py
#/commit/shopitem/20151116/*6