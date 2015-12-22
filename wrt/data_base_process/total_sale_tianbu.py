__author__ = 'wrt'

#coding:utf-8
import sys
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
sc = SparkContext(appName="spark item_sale")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)
def f1(line):
    ss = line.strip().split('\001')
    flag = '0'
    ss.append(flag)
    ss[2] = float(ss[2])
    ss[3] = float(ss[3])
    ss[5] = int(ss[5])
    ss[6] = int(ss[6])
    ss[7] = int(ss[7])
    return ss
def f2(line):
    ss = line.strip().split('\001')
    ss[2] = float(ss[2])
    ss[3] = float(ss[3])
    ss[5] = int(ss[5])
    ss[6] = int(ss[6])
    ss[7] = int(ss[7])
    ss[10] = '0'
    return ss
def quchong(x,y):
    max = 0
    item_list = y
    if len(item_list) == 1:
        ln = item_list[0]
        ln[9] = '1'
        y = ln
    else:
        for ln in item_list:
            if int(ln[8]) > max:
                max = int(ln[8])
                y = ln
    return [x] + y

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
ds = sys.argv[1]
s1 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + ds #today
s2 = "/hive/warehouse/testhive.db/t_base_ec_item_sale_dev/ds=" + sys.argv[2] #yesterday
rdd1 = sc.textFile(s1).map(lambda x:f1(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd2 = sc.textFile(s2).map(lambda x:f2(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x,y):quchong(x,y))
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.registerDataFrameAsTable(df, 'data')
#st = s.find('2015')
#ds2 = s[st:st+4] + s[st+5:st+7] + s[st+8:st+10]
#l = len(s1)
#ds1 = s1[l-8:]
hiveContext.sql('insert overwrite table t_base_ec_item_sale_dev PARTITION(ds=' + ds + ') select * from data')
		#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()
#spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80 t_wrt_base_ec_item_sale.py
#/commit/shopitem/20151116/*6