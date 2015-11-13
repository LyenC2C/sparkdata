# coding:utf-8
__author__ = 'zlj'

# 用户偏好 价格
# 先将商品价格 聚类划分价格区间，然后将用户购买记录对应上去，最后去分数最大的价格阶段
# 最后结果还没做映射



from pyspark.sql import *
from pyspark.mllib.clustering import *
from pyspark.sql.types import *
from pyspark import SparkContext

from numpy import array

sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)

from pyspark.sql import HiveContext

hiveContext = HiveContext(sc)
hiveContext.sql('use wlbase_dev')


# hiveContext.sql('select * from t_zlj_ec_userbuy limit 10').collect()
# rdd=hiveContext.sql('select cast(price as int) price from wlbase_dev.t_base_ec_item_dev where ds=20151030 and cast(price as int)>0').map(lambda x:x.price)
# rdd1=hiveContext.sql('select user_id,avg(price) avg_price from   t_zlj_ec_userbuy  where user_id rlike   "^\\\\d+$" group by user_id    HAVING  avg(price)>0 ').map(lambda x:[x.user_id,x.avg_price])

rdd1=hiveContext.sql('select user_id,avg_price from t_zlj_ec_perfer_priceavg limit 10000 ').map(lambda x:[x.user_id,x.avg_price])
rdd=rdd1.map(lambda x: x[1]).repartition(100)

# rdd.filter(lambda  x:x<50000).histogram(1000)


data=rdd.filter(lambda x:x<30000).map(lambda x:array(x))
model = KMeans.train( data, 5, maxIterations=20, runs=30, initializationMode="random",seed=50, initializationSteps=5, epsilon=1e-4)


model.centers=sorted(model.centers)



# [array([ 947.35742005]), array([ 278.74380204]), array([ 2558.86612172]), array([ 41.31785555]), array([ 6119.56633409])]

# rdd2=sqlContext.sql('select user_id,price from wlbase_dev.t_zlj_ec_userbuy_1 where price>0')
userlevel_rdd=rdd1.map(lambda  x: (x[0],x[1],model.predict(x[1])))


# rdd2.map(lambda  x: (x.user_id,x.price))
schema = StructType([
           StructField("uid", StringType(), True),
           StructField("avg_price", IntegerType(), True),
           StructField("ulevel",IntegerType(), True)])
df=sqlContext.createDataFrame(userlevel_rdd,schema)
sqlContext.registerDataFrameAsTable(df,'userlevel')
# df.cache()

# hiveContext.sql('use wlbase_dev')
#
#
# result=sqlContext.sql('select  uid,ulevel,num, row_number()  OVER (PARTITION BY uid ORDER BY num desc) as rn '
#                'from (select uid,ulevel,count(1) as num from userlevel group by uid, ulevel) t ')


# 保存
hiveContext.registerDataFrameAsTable(df,'sals')
hiveContext.sql('drop table if EXISTS t_zlj_perfer_user_level ')
hiveContext.sql('create table wlbase_dev.t_zlj_perfer_user_level as select * from sals')

# sqlContext.sql('select uid,ulevel,count(1) as num from userlevel group by uid, ulevel').take(1)

# pricerdd=hiveContext.sql('select * from t_zlj_ec_userbuy_1 where price<0')
# pricerdd=hiveContext.sql('select * from t_base_ec_shop_dev where ds=20151023 limit 10')