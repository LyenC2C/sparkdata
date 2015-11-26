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

sc=SparkContext(appName="user_price")
sqlContext = SQLContext(sc)

from pyspark.sql import HiveContext

hiveContext = HiveContext(sc)
hiveContext.sql('use wlbase_dev')

sql='''

select user_id,buytimes,sum_price,avg_price from t_zlj_ec_perfer_priceavg limit 199
'''

rdd1=hiveContext.sql(sql).map(lambda x:[x[0],1.0*x[1],1.0*x[2],1.0*x[3]])
rdd=rdd1.map(lambda x: x[1:]).repartition(100)

data=rdd.filter(lambda x:x[-1]<30000).map(lambda x:array(x))
model = KMeans.train( data, 5, maxIterations=20, runs=50, initializationMode="random",seed=50, initializationSteps=5, epsilon=1e-4)

model.centers=sorted(model.centers,key=lambda t:t[-2])

userlevel_rdd=rdd1.map(lambda  x: (x[0],x[1],model.predict(array(x[1:]))))


# rdd1.map(lambda  x: model.predict(array([x[1]]))).take(10)
schema = StructType([
           StructField("uid", StringType(), True),
           StructField("buytimes", IntegerType(), True),
           StructField("sum_price", FloatType(), True),
           StructField("avg_price", FloatType(), True),
           StructField("ulevel",IntegerType(), True)])

df=hiveContext.createDataFrame(userlevel_rdd,schema)
# sqlContext.registerDataFrameAsTable(df,'userlevel')

# 保存
hiveContext.registerDataFrameAsTable(df,'userlevel')
hiveContext.sql('drop table if EXISTS t_zlj_perfer_user_level_mult ')
hiveContext.sql('create table t_zlj_perfer_user_level_mult as select * from userlevel')
