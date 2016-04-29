__author__ = 'zlj'
from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="cmt")

from pyspark.sql.types import *

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

rdd = sc.textFile('/user/hadoop/qq/info/qq_age.0611', 100).map(lambda x: x.split())
rdd.groupByKey().count()
rdd.filter(lambda x: '469330328' in x[0]).collect()
rdd1 = rdd.map(lambda x: x.split()[0]).distinct()
schema = StructType([
    StructField("qq", StringType(), True),
    StructField("age", StringType(), True)]
)

hiveContext.sql('use wlbase_dev')
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.registerDataFrameAsTable(df, 'qqage')
hiveContext.sql('select qq ,count(1) from qqage group by qq HAVING  COUNT(1)>1 limit 10').collect()

s = hiveContext.table('t_base_q_user_dev')

sql = '''
Insert overwrite table t_base_q_user_dev_zlj  PARTITION(ds=20151103)
select uin ,birthday, phone, gender_id, college, lnick, loc_id, loc,
 h_loc_id, h_loc, personal, shengxiao, gender, occupation, constel, blood, url, homepage, nick, email, uin2, mobile, ts,
case when t2.age<150  and t2.age >5 then t2.age  else cast(t1.age as int) end age

from
t1
left  join

 t2     on t1.qq=t2.uin
'''

hiveContext.sql(' Insert overwrite table t_base_q_user_dev_zlj  PARTITION(ds=20151103) '
                'select birthday, phone, gender_id, college, uin, lnick, loc_id, loc, h_loc_id, h_loc, personal, shengxiao, gender, occupation, constel, blood, url, homepage, nick, email, uin2, mobile, ts'
                ','
                ' case when t2.age<150  and t2.age >5 then t2.age  else cast(t1.age as int) end age'
                ' from  qqage  t1  join'
                '(select *,(2015-year(birthday)) age  from   t_base_q_user_dev )t2  on t1.qq=t2.uin')

# sc.textFile('/data/develop/ec/tb/cmt_res_tmp/res2014').repartition(250).saveAsTextFile('/data/develop/ec/tb/cmt_res_tmp/res2014_re')

# 数据去重
rdd = hiveContext.sql('select * from t_base_q_user_dev').map(lambda x: (x.uin, x)).groupByKey()

rdd = sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_q_user_dev_zlj/ds=20151103').map(lambda x: x.split('\001')).map(
    lambda x: (x[4], x)).groupByKey()

rdd.map(lambda x: [i for i in x[1]][0]).saveAsTextFile('/user/zlj/data/qq_age')
