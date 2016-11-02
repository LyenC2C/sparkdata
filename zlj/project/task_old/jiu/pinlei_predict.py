__author__ = 'zlj'

'''
品类预测

'''


from pyspark.sql import SQLContext
from pyspark import SparkContext



sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)

# df = sqlContext.read.json("examples/src/main/resources/people.json")
# df.columns
# sqlrdd=sqlContext.jsonFile()
# sqlrdd.registerAsTable()
#
# df.select("name")


rdd=sqlContext.sql('select  count(1)  from item where BC_type="B" and brandId="4536492"  group '
                   'by shopId')


rdd=sqlContext.sql('select  itemId  from item where BC_type="B" and brandId="4536492" ')

rdd.map(lambda  x: x.itemId).saveAsTextFile("/user/zlj/data/temp/wuliangye_tmail_itemid")
# root
#  |-- BC_type: string (nullable = true)
#  |-- brandId: string (nullable = true)
#  |-- categoryId: string (nullable = true)
#  |-- itemId: string (nullable = true)
#  |-- nick: string (nullable = true)
#  |-- price: string (nullable = true)
#  |-- sellerId: string (nullable = true)
#  |-- shopId: string (nullable = true)
#  |-- shopTitle: string (nullable = true)
#  |-- title: string (nullable = true)


save_path="/user/zlj/data/iteminfo5"
df = sqlContext.read.json(save_path)
df.printSchema()
df.registerTempTable('item')

rdd=sqlContext.sql('select  title   from item where brandId="4536492"  ').map(lambda x:(x.title,''))



rdd=sqlContext.sql('select  itemId   from item where brandId="4101168" and BC_type="B"').map(lambda x:(x.itemId,''))
jiupinrdd=sc.textFile("/user/zlj/data/temp/iteminfo_jiupin").map(lambda x :x.split(',')).map(lambda x:(x[0],x))

sq=jiupinrdd.join(rdd)

sq.map(lambda x: ",".join(x[1][0])).saveAsTextFile("/user/zlj/data/temp/wuliangye_tmall_814")
jiupinrdd.join()
# info=df.select('BC_type','brandId','title','price').where(df.BC_type=='B',df.brandId='4536492')


#五粮液

sqlContext.sql('select  itemId,title   from item where brandId="4536492" ').map(lambda  x:x.itemId+","+x.title).saveAsTextFile("/user/zlj/data/temp/wuliangye_all")
#result
resultrdd=sc.textFile('/user/zlj/data/temp/wuliangye/predict_id_brand').map(lambda  x:x.split(','))
itemrdd=sqlContext.sql('select  *   from item where brandId="4536492" ').map(lambda x:(x.itemId,x))
sq=itemrdd.join(resultrdd)
sq.map(lambda x: ",".join(x[1][0])+","+x[1][1]).saveAsTextFile("/user/zlj/data/temp/wuliangye/predict_result_join")

sc.textFile("/user/zlj/data/temp/user/zlj/data/temp/wuliangye_all_result_join/").sample(False,0.08,79).saveAsTextFile("/user/zlj/data/temp/wuliangye_all_result_join_sample")


# 茅台-
sqlContext.sql('select  itemId,title   from item where brandId="4101168" ').map(lambda  x:x.itemId+","+x.title).saveAsTextFile("/user/zlj/data/temp/maotai_all")
#predict
rdd=sqlContext.sql('select  itemId,title   from item where brandId="4101168" ').map(lambda  x:x.itemId+","+x.title)
rdd.saveAsTextFile("/user/zlj/data/temp/maotai_all_predict")


rdd=sqlContext.sql('select  itemId   from item where brandId="4101168" and BC_type="B"').map(lambda x:(x.itemId,''))
sq=jiupinrdd.join(rdd)
sq.map(lambda x: ",".join(x[1][0])).saveAsTextFile("/user/zlj/data/temp/wuliangye_tmall_814")


#泸州老窖
sqlContext.sql('select  itemId,title   from item where brandId="4537002" ').map(lambda  x:x.itemId+","+x.title).saveAsTextFile("/user/zlj/data/temp/luzhou_predict")


rdd=sqlContext.sql('select  itemId   from item where brandId="4537002" and BC_type="B"').map(lambda x:(x.itemId,''))

sq=jiupinrdd.join(rdd)
sq.map(lambda x: ",".join(x[1][0])).saveAsTextFile("/user/zlj/data/temp/luzhou_tmall_1997")