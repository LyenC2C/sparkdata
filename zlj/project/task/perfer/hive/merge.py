#coding:utf-8
__author__ = 'zlj'


from pyspark.sql import *
from pyspark import SparkContext

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

def dim():
    sql_dim='''
    SELECT
     user_id ,concat_ws('|', collect_set(v)) as diminfos
    FROM
    (
    SELECT /*+ mapjoin(t2)*/
     t1.user_id, concat_ws('_',,cast(t2.cate_id as String),cate_name,cast(f as String),cast(rn as String)) as v
    FROM t_base_ec_dim t2 join  t_zlj_ec_perfer_dim t1 on t1.root_cat_id=t2.cate_id
    where rn <%s
    )
    t group by user_id ;
    '''
    rdd_dim=hiveContext.sql(sql_dim%(dim_limit)).map(lambda x:(x.user_id,('dim',x.diminfo)))
    return rdd_dim


def brand():
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
    return rdd_brand


def brandtag():
    sql_brandtag='''

    SELECT
          user_id,
          concat_ws('|', collect_set(brandtag)) AS brandtags
        FROM
          (
            SELECT
              t2.user_id,
              concat_ws('_', brand_tag, brand_level) AS brandtag
            FROM
              (
                SELECT
                  brand_level,
                  brand_tag
                FROM t_wrt_item_tag_level
              ) t1
              JOIN t_zlj_ec_perfer_brand t2

                ON (t2.rn < 5 AND t1.brand_id = t2.brand_id)
          ) t3
        GROUP BY user_id;

    '''
    rdd_brandtag=hiveContext.sql(sql_brandtag).map(lambda x:(x.user_id,('brandtag',x.tag)))
    return rdd_brandtag


def price():
    return ''

def shop():
    sql_shop='''
    SELECT
     user_id ,concat_ws('|', collect_set(v)) as shopinfos
    FROM
    (
    SELECT /*+ mapjoin(t2)*/
     t1.user_id, concat_ws('_',t2.shop_id,shop_name,cast(f as String),cast(rn as String)) as v
    FROM t_base_ec_shop  t2 join  t_zlj_ec_perfer_dim t1 on t1.root_cat_id=t2.cate_id
    where rn <5
    )
    t
    group by user_id ;
    '''
    rdd_shop=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('shop',x.tag)))
    return rdd_shop

def car():
    sql_car='''
    select user_id,tag
    from
    t_zlj_ec_perfer_house
    ;
    '''
    rdd_car=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('car',x.tag)))
    return rdd_car

def house():
    sql_car='''
    select user_id,tag
    from
    t_zlj_ec_perfer_house
    ;

    '''
    rdd=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('house',x.tag)))
    return rdd


def qq():
    return ''


# def freq():
#


# rdd=rdd_dim.union(rdd_brand).union(rdd_car)
#
# rdd.groupByKey(100).map(lambda (x,y):(x,mergeinfo(y))).saveAsTextFile(sys.argv[1])

