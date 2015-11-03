__author__ = 'zlj'

# �û�ƫ�� �۸�
# �Ƚ���Ʒ�۸� ���໮�ּ۸����䣬Ȼ���û������¼��Ӧ��ȥ�����ȥ�������ļ۸�׶�
# �������û��ӳ��



from numpy import array
import json
from pyspark.sql import *
import numpy as np
from pyspark.mllib.clustering import *
from pyspark.sql.types import *
from pyspark import SparkContext

sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)

from pyspark.sql import HiveContext

hiveContext = HiveContext(sc)
hiveContext.sql('use wlbase_dev')


hiveContext.sql('drop table IF EXISTS  t_zlj_ec_perfer_priceavg')

hiveContext.sql('create table t_zlj_ec_perfer_priceavg as '
                'select  user_id ,count(1)  buytimes,sum(price) as sum_price, avg(price) as avg_price '
                'from  t_zlj_ec_userbuy group by user_id    HAVING  avg(price)>0 ')





rdd=hiveContext.sql('select cast(price as int) price from wlbase_dev.t_base_ec_item_dev where ds=20151030 and cast(price as int)>0').map(lambda x:x.price)

rdd.filter(lambda  x:x<50000).histogram(1000)


data=rdd.filter(lambda x:x<10000).map(lambda x:array(x))
model = KMeans.train(
    data, 5, maxIterations=20, runs=30, initializationMode="random",
    seed=50, initializationSteps=5, epsilon=1e-4)



model.centers
# [array([ 947.35742005]), array([ 278.74380204]), array([ 2558.86612172]), array([ 41.31785555]), array([ 6119.56633409])]

rdd2=sqlContext.sql('select user_id,price from wlbase_dev.t_zlj_ec_userbuy_1 where price>0')
userlevel_rdd=rdd2.map(lambda  x: (x.user_id,model.predict(array([x.price]))))


# rdd2.map(lambda  x: (x.user_id,x.price))
schema = StructType([
           StructField("uid", StringType(), True),
           StructField("ulevel",IntegerType(), True)])
df=sqlContext.createDataFrame(userlevel_rdd,schema)
sqlContext.registerDataFrameAsTable(df,'userlevel')
df.cache()

hiveContext.sql('use wlbase_dev')


result=sqlContext.sql('select  uid,ulevel,num, row_number()  OVER (PARTITION BY uid ORDER BY num desc) as rn '
               'from (select uid,ulevel,count(1) as num from userlevel group by uid, ulevel) t ')


# ����
hiveContext.registerDataFrameAsTable(result,'sals')
hiveContext.sql('create table wlbase_dev.t_zlj_perfer_user_levelqq as select * from sals')

# sqlContext.sql('select uid,ulevel,count(1) as num from userlevel group by uid, ulevel').take(1)

# pricerdd=hiveContext.sql('select * from t_zlj_ec_userbuy_1 where price<0')
# pricerdd=hiveContext.sql('select * from t_base_ec_shop_dev where ds=20151023 limit 10')