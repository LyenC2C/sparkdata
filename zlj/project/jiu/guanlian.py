__author__ = 'zlj'
import numpy as np
from pyspark.mllib.clustering import *

from pyspark.sql.types import  *
from pyspark.sql import *
import json

from pyspark import SparkContext

sc = SparkContext(appName="test")

sqlContext = SQLContext(sc)


buy=sqlContext.read.json("/user/hadoop/service/jiu/jiu.sp.item.2.cmt.t.cat.dec.uid.sq.his.nw")
buy.printSchema()

item_path="/user/hadoop/service/jiu/spread.iteminfo.dir/"
schema = StructType([
           StructField("itemId", StringType(), True),
           StructField("title", StringType(), True),
           StructField("categoryId", StringType(), True),
           StructField("brandId", StringType(), True),
           StructField("BC_type", StringType(), True),
           StructField("price", StringType(), True),
           StructField("brand", StringType(), True)])
itemdf=sc.textFile(item_path).filter(lambda x: len(x.split('\t'))==7).map(lambda  x:tuple(x.split('\t')))
item=sqlContext.createDataFrame(itemdf,schema)
item.printSchema
item.map(lambda x:json.dumps(x.asDict(),ensure_ascii=False).encode("utf-8")).saveAsTextFile('/user/zlj/data/jiu/json_spread_iteminfo_dir/')

sqlContext.registerDataFrameAsTable(buy,'buy')
sqlContext.registerDataFrameAsTable(item,'item')


querydd=sqlContext.sql('select uid,categoryId,count(1) buy_times from buy a join item b  on a.item_id=b.itemId group by uid,categoryId')

sqlContext.registerDataFrameAsTable(querydd,"querydd")
Person = Row('brandId', 'topbrandName')
pp=sc.textFile('/user/hadoop/taobao/category/category.final').map(lambda  x:Person(x.split()[0],x.split()[-1]))
pprdd=sqlContext.createDataFrame(pp)

sqlContext.registerDataFrameAsTable(pprdd,"pp")


resultrdd=sqlContext.sql('select  uid,categoryId,topbrandName,buy_times from querydd a join pp b on a.categoryId=b.brandId')
resultrdd.map(lambda x:str(x.uid)+" "+str(x.categoryId)+" "+x.topbrandName.encode('utf-8')+" "+str(x.buy_times)).saveAsTextFile('/user/zlj/data/user_pre_cat')
