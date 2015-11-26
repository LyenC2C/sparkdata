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

rdd1=hiveContext.sql('select user_id,avg_price from t_zlj_ec_perfer_priceavg ').map(lambda x:[x.user_id,x.avg_price])
rdd=rdd1.map(lambda x: x[1]).repartition(100)

data=rdd.filter(lambda x:x<30000).map(lambda x:array(x))
model = KMeans.train( data, 5, maxIterations=20, runs=50, initializationMode="random",seed=50, initializationSteps=5, epsilon=1e-4)
model.centers=sorted(model.centers)

userlevel_rdd=rdd1.map(lambda  x: (x[0],x[1],model.predict([x[1]])))

schema = StructType([
           StructField("uid", StringType(), True),
           StructField("avg_price", FloatType(), True),
           StructField("ulevel",IntegerType(), True)])
df=hiveContext.createDataFrame(userlevel_rdd,schema)
# sqlContext.registerDataFrameAsTable(df,'userlevel')

# 保存
hiveContext.registerDataFrameAsTable(df,'userlevel')
hiveContext.sql('drop table if EXISTS t_zlj_perfer_user_level ')
hiveContext.sql('create table wlbase_dev.t_zlj_perfer_user_level as select * from userlevel')
