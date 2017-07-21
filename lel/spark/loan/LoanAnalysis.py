# coding=utf-8
import rapidjson as json
from pyspark.sql import SparkSession
from pyspark.sql import UDFRegistration
from pyspark.sql import types
import sys

loanPath = "hdfs://master:9000/data/wolong/loan"
mapDetailsPath = "hdfs://master:9000/data/wolong/fraud"
multiPlatformPath = "hdfs://master:9000/data/wolong/wolong_jiedai.json"

spark = SparkSession \
    .builder() \
    .appName("Loan_cacsi") \
    .master("local[*]") \
    .getOrCreate()


def split(data):
    tmp = data.split("\t")
    return (tmp[0], tmp[1])


mapInfo = spark.sparkContext.textFile(mapDetailsPath).map(lambda a: split(a)).collectAsMap()
broadcast = spark.sparkContext.broadcast(mapInfo)
bv = broadcast.value


def mapping(data):
    if data is None:
        res = None
    else:
        res = ":".join([bv.get(k) for k in data if bv.get(k) is not None])
    return res

spark.udf.register("mapping", mapping)

spark.sql(
    "select phone,zm_credit_score,fraud_apply_score,mapping(fraud_apply_verify),mapping(fraud_focus_list) from loan").show

spark.sql("select business_focus_list from loan where business_focus_list is not null").show()

spark.sql("select ")
