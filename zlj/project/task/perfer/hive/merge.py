#coding:utf-8
__author__ = 'zlj'


from pyspark.sql import *
from pyspark import SparkContext

'''
合并所有标签
spark-submit  --total-executor-cores  100   --executor-memory  20g  --driver-memory 20g   merge.py  -merge
'''



# /data/develop/ec/tb/iteminfo/jiu.iteminfo
sc = SparkContext(appName="merge")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

from pyspark.sql.types import *

import  sys


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content

def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x
dim_limit=10
brand_limit=10
shop_limit=10
brandtag_limit=10
def dim():
    sql_dim='''
    SELECT
     user_id ,concat_ws('|', collect_set(v)) as diminfos
    FROM
    (
    SELECT
     user_id, concat_ws('_',cast(root_cat_id as String),root_cat_name,cast(rn as String),cast(f as String)) as v
    FROM t_zlj_ec_perfer_dim

    where rn <%s

    )
    t group by user_id

    '''
    rdd_dim=hiveContext.sql(sql_dim%(dim_limit)).map(lambda x:(x.user_id,('dim',x.diminfos)))
    return rdd_dim


def brand():

    sql_brand='''
    SELECT
     user_id ,concat_ws('|', collect_set(brandinfo)) as brandinfos
    FROM
    (
    SELECT
     user_id, concat_ws('_',brand_id,brand_name,cast(rn as String),cast(f as String)) as brandinfo
    FROM t_zlj_ec_perfer_brand
    where rn <%s

    )
    t
    group by user_id

    '''
    rdd_brand=hiveContext.sql(sql_brand%brand_limit).map(lambda x:(x.user_id,('brand',x.brandinfos)))
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
                  brand_id,
                  brand_level,
                  brand_tag
                FROM t_wrt_item_tag_level

              ) t1
              JOIN t_zlj_ec_perfer_brand t2

                ON (t2.rn <%s AND t1.brand_id = t2.brand_id)
          ) t3
        GROUP BY user_id

    '''
    rdd_brandtag=hiveContext.sql(sql_brandtag%brandtag_limit).map(lambda x:(x.user_id,('brandtag',x.brandtags)))
    return rdd_brandtag


def price():
    sql_price='''
    select
    uid,buytimes, sum_price,avg_price,ulevel
    from
    t_zlj_perfer_user_level_mult

    '''
    rdd_price=hiveContext.sql(sql_price).map(lambda x:(x.uid,('price_level',"_".join(str(i) for i in x[1:]))))
    return rdd_price
    # return ''

def shop():
    sql_shop='''
    SELECT
     user_id ,concat_ws('|', collect_set(v)) as shopinfos
    FROM
    (
    SELECT
     user_id, concat_ws('_',shop_id,shop_name,cast(rn as String),cast(f as String)) as v
    FROM  t_zlj_ec_perfer_shop
    where rn <%s
    )
    t
    group by user_id
    '''
    rdd_shop=hiveContext.sql(sql_shop%shop_limit).map(lambda x:(x.user_id,('shop',x.shopinfos)))
    return rdd_shop

def car():
    sql_car='''
    select
    user_id,tag
    from
    t_zlj_ec_perfer_car

    '''
    rdd_car=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('car',x.tag)))
    return rdd_car

def house():
    sql_car='''
    select
    user_id,tag
    from
    t_zlj_ec_perfer_house
    '''
    rdd=hiveContext.sql(sql_car).map(lambda x:(x.user_id,('house',x.tag)))
    return rdd



def qq():
    sql_qq='''
        SELECT
    t4.user_id,
    t3.uin,
    t3.birthday,
    t3.phone,
    t3.gender_id,
    t3.college,
    t3.lnick,
    t3.loc_id,
    t3.loc,
    t3.h_loc_id,
    t3.h_loc,
    t3.personal,
    t3.shengxiao,
    t3.gender,
    t3.occupation,
    t3.constel,
    t3.blood,
    t3.url,
    t3.homepage,
    t3.nick,
    t3.email,
    t3.uin2,
    t3.mobile,
    t3.ts,
    t3.age
    FROM
      (
        SELECT
          t2.* ,
          t1.tbuid,
          t1.qq
        FROM
          t_zlj_data_link t1
          JOIN
          t_base_qq_user_dev t2
            ON (LENGTH(t2.uin)>0 AND LENGTH(t1.qq)>0 AND t1.qq = t2.uin)

      ) t3
      JOIN
      (
      select user_id
      from
      t_zlj_ec_userbuy
      where length(user_id)>0
      group by user_id
      )
      t4
        ON ( length(t3.tbuid)>0 and t3.tbuid= t4.user_id)
    '''
    rdd=hiveContext.sql(sql_qq).map(lambda x:[x.user_id,x.uin  ,
                                                                x.birthday  ,
                                                                x.phone     ,
                                                                x.gender_id ,
                                                                x.college   ,
                                                                x.lnick     ,
                                                                x.loc_id    ,
                                                                x.loc       ,
                                                                x.h_loc_id  ,
                                                                x.h_loc     ,
                                                                x.personal  ,
                                                                x.shengxiao ,
                                                                x.gender    ,
                                                                x.occupation,
                                                                x.constel   ,
                                                                x.blood     ,
                                                                x.url       ,
                                                                x.homepage  ,
                                                                x.nick      ,
                                                                x.email     ,
                                                                x.uin2      ,
                                                                x.mobile    ,
                                                                x.ts        ,
                                                                x.age ]) \
        .map(lambda x:(x[0],('qq','\003'.join([f_coding(str(valid_jsontxt(i))) for i in x[1:]]))))
    return rdd


def hmm_tag():
    sql_tag='''
    select
    user_id,tfidftags
    from
    t_zlj_userbuy_item_tfidf_tagbrand_weight_2015_v4
    '''
    rdd=hiveContext.sql(sql_tag).map(lambda x:(x.user_id,('tfidftags',x.tfidftags)))
    return rdd

def cat_tags():
    sql_tag='''
    select
    user_id,cat_tags
    from
    t_zlj_userbuy_item_cattags
    '''
    rdd=hiveContext.sql(sql_tag).map(lambda x:(x.user_id,('cat_tags',x.cat_tags)))
    return rdd
schema1 = StructType([
    StructField("uid", StringType(), True),
    StructField("dim", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("brandtag", StringType(), True),
    StructField("shop", StringType(), True),
    StructField("price_level", StringType(), True),
    StructField("car", StringType(), True),
    StructField("house", StringType(), True),
    StructField("tfidftags", StringType(), True),
    StructField("cat_tags", StringType(), True),
    StructField("qq", StringType(), True)
        ])
def mergeinfo(uid,info):
    m={}
    lv=[]
    lv.append(uid)
    for value in info:
        m[value[0]]=value[1]
    # if 'dim' in key:
    lv.append(m.get('dim',''))
    lv.append(m.get('brand',''))
    lv.append(m.get('brandtag',''))
    lv.append(m.get('shop',''))
    lv.append(m.get('price_level',''))
    lv.append(m.get('car',''))
    lv.append(m.get('house',''))
    lv.append(m.get('tfidftags',''))
    lv.append(m.get('cat_tags',''))
    lv.append(m.get('qq',''))
    return lv



if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if sys.argv[1] == '-h':
        comment = '-合并所有标签 \n\
				  '
        print comment
        print '-merge argvs:\n argv[1]:file or dir input\n argv[2]:ds  \n'
        print '-inc      argvs:\n argv[1]:file or dir input\n argv[2]:ds_1  \n argv[3] ds\n'
    elif sys.argv[1]=='-test':
        rdd_qq=qq()
        rdd_qq.saveAsTextFile('/user/zlj/data/temp/qq')
    elif sys.argv[1]=='-merge':
        rdd_dim=dim()
        rdd_brand=brand()
        rdd_brandtag=brandtag()
        rdd_price=price()
        rdd_shop=shop()
        rdd_car=car()
        rdd_house=house()
        rdd_tag=hmm_tag()
        rdd_cat_tags=cat_tags()
        # rdd_qq=qq()
        rdd=rdd_dim.union(rdd_brand).union(rdd_brandtag).union(rdd_price).union(rdd_shop).union(rdd_car)\
            .union(rdd_house).union(rdd_tag).union(rdd_cat_tags)\
            # .union(rdd_qq)
        # rdd=rdd_dim.union(rdd_brand)
        rdd1=rdd.groupByKey().map(lambda (x,y): mergeinfo(x,y)).coalesce(1000)
        ddf=hiveContext.createDataFrame(rdd1,schema1)
        hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        hiveContext.sql('drop table if exists t_zlj_user_tag_join')
        hiveContext.sql('''
        create table  t_zlj_user_tag_join
        as
        select * from  tmptable
        ''')

# ''.strip()
# def freq():
#
# rdd=rdd_dim.union(rdd_brand).union(rdd_car)
#
# rdd.groupByKey(100).map(lambda (x,y):(x,mergeinfo(y))).saveAsTextFile(sys.argv[1])

