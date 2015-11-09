__author__ = 'zlj'


from pyspark.sql import *
from pyspark import SparkContext

import  sys

'''
合并所有标签
'''



# /data/develop/ec/tb/iteminfo/jiu.iteminfo


sc=SparkContext(appName="merge_perfer")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def mergeinfo(info):
    dic={}
    value=''
    for item in info:
        dic[item[0]]=item[1]
    value+=dic.get('dim')
    value+='_'+dic.get('brand')
    value+='_'+dic.get('car')
    value+='_'+dic.get('house')
    return value






hiveContext.sql('use wlbase_dev')

dim_limit=5

sql_dim='''
SELECT
 user_id ,concat_ws('|', collect_set(v)) as diminfos
FROM
(
SELECT /*+ mapjoin(t2)*/
 t1.user_id, concat_ws('_',t2.cate_id,cate_name,cast(f as String),cast(rn as String)) as v
FROM t_base_ec_dim t1 join  t_zlj_ec_perfer_dim t2 on t1.root_cat_id=t2.cate_id
where rn <%s
)
t
group by user_id ;
'''
rdd_dim=hiveContext.sql(sql_dim%(dim_limit)).map(lambda x:(x.user_id,('dim',x.diminfo)))


brand_limit=5
sql_brand='''
SELECT
 user_id ,concat_ws('|', collect_set(brandinfo)) as brandinfos
FROM
(
SELECT /*+ mapjoin(t2)*/
 t1.user_id, concat_ws('_',t2.brand_id,brand_name,cast(rn as String)) as brandinfo

FROM t_base_ec_brand t2 join t_zlj_ec_perfer_brand t1 on t1.brand_id=t2.brand_id

where t1.rn <%s
)
t
group by user_id ;

'''
rdd_brand=hiveContext.sql(sql_brand%brand_limit).map(lambda x:(x.user_id,('brand',x.brandinfo)))


sql_car='''
select user_id,tag
from
t_zlj_ec_perfer_house
;

'''
rdd_car=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('car',x.tag)))

rdd=rdd_dim.union(rdd_brand).union(rdd_car)

rdd.groupByKey(100).map(lambda (x,y):(x,mergeinfo(y))).saveAsTextFile(sys.argv[1])

