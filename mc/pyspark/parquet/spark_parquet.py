#coding:utf-8
#入库数据到parquet存储格式表的处理
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
import json
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
sc = SparkContext(appName="operation_parquet", conf=conf)
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

# Datatype: "DataType", "NullType", "StringType", "BinaryType", "BooleanType", "DateType",
#     "TimestampType", "DecimalType", "DoubleType", "FloatType", "ByteType", "IntegerType",
#     "LongType", "ShortType", "ArrayType", "MapType", "StructField", "StructType"]
#
# BINARY -> STRING
# BOOLEAN -> BOOLEAN
# DOUBLE -> DOUBLE
# FLOAT -> FLOAT
# INT32 -> INT
# INT64 -> BIGINT
# INT96 -> TIMESTAMP

myschema=StructType([StructField('item_id', StringType(), True),\
                     StructField('title', StringType(), True),\
                     StructField('cat_id', StringType(), True),\
                     StructField('cat_name', StringType(), True),\
                     StructField('root_cat_id', StringType(), True),\
                     StructField('root_cat_name', StringType(), True),\
                     StructField('brand_id', StringType(), True),\
                     StructField('brand_name', StringType(), True),\
                     StructField('bc_type', StringType(), True),\
                     StructField('price', StringType(), True),\
                     StructField('price_zone', StringType(), True),\
                     StructField('is_online', LongType(), True),\
                     StructField('off_time', StringType(), True),\
                     StructField('favor', LongType(), True),\
                     StructField('seller_id', StringType(), True),\
                     StructField('shop_id', StringType(), True),\
                     StructField('location', StringType(), True),\
                     StructField('paramap', MapType(StringType(),StringType(),True), True),\
                     StructField('sku', MapType(StringType(),StringType(),True), True),\
                     StructField('ts', StringType(), True)
                     ])

rdd = sc.textFile('/hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=20170405/000000_0').map(lambda x:tuple(x.split("\001")))
df=sqlContext.createDataFrame(rdd, myschema)
df.write.parquet("/hive/warehouse/wl_base.db/t_base_ec_item_dev_new2/ds=20170405")

#这种方式导入的bigint和map字段有问题，其他正常