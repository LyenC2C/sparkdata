#coding:utf-8
import sys
import time
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *


sc = SparkContext(appName="spark brandinfo")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
def f(line):
	line_s = valid_jsontxt(line)
	ob = json.loads(line_s)
	popularity = str(ob.get("popularity",0))
	brandName = ob.get("brandName","-")
	brandId = str(ob.get("brandId",0))
	brand_list = []
	brand_list.append(brandId)
	brand_list.append(brandName)
	brand_list.append(popularity)
	return brand_list
schema = StructType([
	StructField("brand_id",StringType(), True),
	StructField("brand_name",StringType(), True),
	StructField("stars",IntegerType(), True)
	])

if sys.argv[1] == '-h':
    comment = '产品信息格式化为hive数据格式'
    print comment
    print 'argvs: \n argv[1]:brand info file or dir input\n'
rdd = sc.textFile(sys.argv[1]).map(lambda x: f(x)).filter(lambda x: x != None)
df = hiveContext.createDataFrame(rdd , schema)
hiveContext.sql('use wlbase_dev')
hiveContext.registerDataFrameAsTable(df,'data')
ds2 = '20151103'
hiveContext.sql('insert overwrite table t_base_ec_brand  PARTITION(ds='+ds2+') select * from data')
sc.stop()