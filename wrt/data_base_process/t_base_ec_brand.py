#coding:utf-8
import sys
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import *

import rapidjson  as json

sc = SparkContext(appName="spark brandinfo")
sqlContext = SQLContext(sc)
hiveContext= HiveContext(sc)

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
	brand_list.append(popularity)
	brand_list.append(brandName)
	return brand_list
schema = StructType([
	StructField("brandId",StringType(), True),
	StructField("brandName",StringType(), True),
	StructField("popularity",IntegerType(), True)
	])

if sys.argv[1] == '-h':
		comment = '产品信息格式化为hive数据格式'
		print comment
		print 'argvs:\n argv[1]:brand info file or dir input\n '

sc.textFile

rdd = sc.textFile(sys.argv[1]).map(lambda x:f(x)).filter(lambda x:x!=None)

df = hiveContext.createDataFrame(rdd,schema)
hiveContext.registerDataFrameAsTable(df,'data')

ds2='20151103'
hiveContext.sql('insert overwrite table t_base_ec_brand  PARTITION(ds='+ds2+') select * from data')
sc.stop()