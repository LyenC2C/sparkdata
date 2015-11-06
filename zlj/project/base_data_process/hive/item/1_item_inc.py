#coding:utf-8
__author__ = 'zlj'

# import json
from pyspark.sql import *
import sys

import rapidjson as json


# /data/develop/ec/tb/iteminfo/jiu.iteminfo

from pyspark import SparkContext
sc=SparkContext(appName="test")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
# path='/data/develop/ec/tb/iteminfo/jiu.iteminfo'
# path='/data/develop/ec/tb/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo'


path=sys.argv[1]
def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=0
        if '-' in tmp:     v=float(tmp.split('-')[0])
        else :             v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
# s=''
# s.split()
def parse(line_s):
    line=valid_jsontxt(line_s)
    ts,id,txt=line.split('\t')
    ob=json.loads(txt)
    itemInfoModel=ob['itemInfoModel']
    item_id=itemInfoModel.get('itemId','-')
    title=itemInfoModel.get('title','-')
    categoryId=itemInfoModel.get('categoryId','-')
    cat_name='-'
    root_cat_id='-'
    root_cat_name='-'
    favor=itemInfoModel.get('favcount','-')
    trackParams=ob['trackParams']
    BC_type=trackParams.get('BC_type','-')
    brandId=trackParams.get('brandId','-')
    brand_name='-'
    props=ob.get('props',[])
    for v in props:
        if valid_jsontxt('品牌') in valid_jsontxt(v['name']):
            brand_name=v['value']
    value=parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price=value[0]
    price_zone=value[1]
    is_online=1
    off_time='-'
    seller=ob['seller']
    seller_id=seller.get('userNumId','-')
    shopId=seller.get('shopId','-')
    # ts=time.time()
    list=[]
    list.append(item_id)
    list.append(title)
    list.append(categoryId)
    list.append(cat_name)
    list.append(root_cat_id)
    list.append(root_cat_name)
    list.append(brandId)
    list.append(brand_name)
    list.append(BC_type)
    list.append(str(price))
    list.append((price_zone))
    list.append(str(is_online))
    list.append(off_time)
    list.append(favor)
    list.append(seller_id)
    list.append(shopId)
    list.append(str(ts))
    strlist=[]
    # for i in list:
    #     if len(i)==0: strlist.append('-')
    #     else : strlist.append(i)
    # return "\001".join(strlist)
    return (item_id,list)



rdd=sc.textFile(path,100).map(lambda  x: parse(x)).groupByKey().map(lambda x:[i for  i in  x[1] ][0])\
    .repartition(100)

rdd=sc.textFile('/data/develop/ec/tb/iteminfo_new/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01',100)\
    .map(lambda  x: parse(x)).groupByKey().map(lambda x:[i for  i in  x[1] ][0]).map(lambda x: (x[0],x))\
    .repartition(100)

hiveContext.sql('use wlbase_dev')
df=hiveContext.sql('select * from t_base_ec_item_dev where ds=20151101')
schema1=df.schema

rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.ts]))\
    .repartition(100)


def fun(x,y):
    item_id=x
    return sorted(y,key=lambda t : t[-1],reverse=True)[0]


rdd2=rdd1.union(rdd).groupByKey()
rdd3=rdd2.map(lambda x,y:fun(x,y)).repartition(20)

# .saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_item_dev/ds=20150101')

# sc.textFile(path,100).map(lambda  x: parse_shop(x)).saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_shop_dev/ds=20150101')

# sc.textFile('/hive/external/wlbase_dev/t_base_ec_shop_dev/ds=20150101').filter(lambda  x: '64661829' in x).take(10)