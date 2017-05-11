from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql.types import *

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")
schemaString = "phone id idd"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.


sc = SparkContext(appName="fri_mining")
wbid = sc.textFile("/trans/lel/shen/shen_other_channel.teltbsnwb").map(lambda a: a.split("\001"))
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(wbid, schema).rdd
df.write.parquet("/user/lel/results/test_parquet_1")


