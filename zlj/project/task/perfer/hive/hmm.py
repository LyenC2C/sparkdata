# encoding: utf-8
__author__ = 'zlj'

from pyspark import SparkContext
from pyspark.sql import *

sc = SparkContext(appName="hmm")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)




hiveContext.sql('use wlbase_dev')

hiveContext.sql('drop table if EXISTS t_zlj_userbuy_item_hmm ')
sqlString='''

create table  t_zlj_userbuy_item_hmm

AS

select
user_id, concat_ws('_', collect_set(hmm))

from
(
select item_id,hmm from t_zlj_item_hmm
)t1
join

(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>20150101
and  LENGTH (user_id)>0

)t2

on t1.item_id=t2.item_id
group by user_id

'''

hiveContext.sql(sqlString)