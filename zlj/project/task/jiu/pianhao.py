__author__ = 'zlj'

from pyspark.sql import *

from pyspark.sql.types import *
from pyspark import SparkContext
sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)


schemaString = "itemId title categoryId brandId BC_type price brand"
ts=Row('itemId','title','categoryId' ,'brandId', 'BC_type','price brand')
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
item.printSchema()

buy_his_path="/user/hadoop/service/jiu/jiu.sp.item.2.cmt.t.cat.dec.uid.sq.his.nw"

buyrdd=sqlContext.read.json(buy_his_path)
buyrdd.printSchema()
buyrdd.columns
# root
#  |-- content: string (nullable = true)
#  |-- datefeed: string (nullable = true)
#  |-- datenow: string (nullable = true)
#  |-- datestr: string (nullable = true)
#  |-- datetype: string (nullable = true)
#  |-- feed_id: string (nullable = true)
#  |-- imgurl: string (nullable = true)
#  |-- item_id: string (nullable = true)
#  |-- item_url: string (nullable = true)
#  |-- page: long (nullable = true)
#  |-- price: string (nullable = true)
#  |-- title: string (nullable = true)
#  |-- ucode: string (nullable = true)
#  |-- uid: string (nullable = true)

sqlContext.registerDataFrameAsTable(item,"item")
sqlContext.registerDataFrameAsTable(buyrdd,"buy")

queryrdd=sqlContext.sql('select b.price as buy_price,b.uid,a.* from item a join buy b on a.itemId =b.item_id')
# [price: string, uid: string, itemId: string, title: string, categoryId: string, brandId: string, BC_type: string, price: string, brand: string]

queryrdd.cache()
queryrdd.unpersist()
queryrdd.printSchema()
sqlContext.registerDataFrameAsTable(queryrdd,"userbuy")
def groupby_join(v):
    s=[]
    for item in v:
        s.append("_".join([str(i) for i in item]))
    return ";".join(s)
topk=5


# Æ«ºÃÀàÄ¿

catrdd=sqlContext.sql('select  uid,categoryId,count(1) num from userbuy group by uid,categoryId').map(lambda  x:(x[0],(x[1],x[2])))
rankrdd=catrdd.groupByKey().flatMap(lambda  x: ([(x[0], i[0],i[1],index )for index, i in enumerate(sorted(x[1],key=lambda t : t[1],reverse=True))  if index<topk]))
rankrdd.map(lambda  x: (x[0],(x[1],x[2],x[3]))).groupByKey().map(lambda  x: x[0]+" "+groupby_join(x[1])).saveAsTextFile("/user/zlj/data/temp/jiu/pianhao/cat_pianhao1")
# rankrdd.map(lambda  x:" ".join([str(i) for i in x])).saveAsTextFile("/user/zlj/data/temp/jiu/pianhao/cat_pianhao1")


#price
# sqlContext.sql('select * from userbuy limit 1').collect()
avgprice_rdd=sqlContext.sql('select  uid,avg(buy_price) avp from userbuy group by uid').map(lambda  x:x[1]).histogram([i*10 for i in xrange(500)])
avgprice_rdd





tt=[(1,(2,3)),(1,(2,5)),(1,(3,5))]
trdd=sc.parallelize(tt)
sd=trdd.groupByKey().flatMap(lambda  x: ([(x[0], i[0],i[1],index )for index, i in enumerate(sorted(x[1],reverse=True))  if index<topk]))

sd.groupBy(lambda  x:x[0]).map(lambda x: x[0]+" ".join([str(i) for i in x[1]]))


# sd.groupBy(lambda  x:x[0]).map(lambda x: (x[0],f(x[1]))).take(2)
# sd.groupBy(lambda  x:x[0]).mapValues(tuple).map(lambda  x: x[0]+" ".join([str(i) for i in x[1]])).collect()

