#coding:utf-8
__author__ = 'zlj'

# import json
from pyspark.sql import *
import sys
from pyspark import SparkContext
import  time

import rapidjson as json










# /data/develop/ec/tb/iteminfo/jiu.iteminfo


sc=SparkContext(appName="shop-inc")

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content

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

def  parse_shop(line,flag):
    ob=json.loads(valid_jsontxt(line))
    itemInfoModel=ob['itemInfoModel']
    location=valid_jsontxt(itemInfoModel.get('location','-'))
    seller = ob["seller"]
    evaluateInfo = seller.get("evaluateInfo",[])
    shopId=seller.get("shopId","-")
    seller_id=seller.get("userNumId","-")
    seller_name=seller.get("nick","-")
    credit=seller.get("creditLevel","-")
    starts = seller.get("starts","--")
    trackParams=ob['trackParams']
    BC_type=trackParams.get('BC_type','-')
    item_count=seller.get('actionUnits',[])[0].get('value','0')
    fansCount = seller.get("fansCount","--")
    goodRatePercentage = seller.get("goodRatePercentage","--")
    # nick= seller.get("nick","--").encode('utf-8')
    weitaoId = seller.get("weitaoId","--")
    # userNumId = seller.get("userNumId","--")
    # shopTitle = seller.get("shopTitle","--").encode('utf-8')
    shopTitle = seller.get("shopTitle","--")
    desc_score=evaluateInfo[0].get("score")
    service_score=evaluateInfo[1].get("score")
    wuliu_score=evaluateInfo[2].get("score")
    star='99'
    list=[]
    list.append(shopId)
    list.append(seller_id)
    list.append(shopTitle)
    list.append(seller_name)
    if 'B' in BC_type:
        list.append(star)
    else: list.append('0')
    list.append(credit)
    list.append(starts)
    list.append(BC_type)
    list.append(int(item_count))
    list.append(int(fansCount))
    list.append(float(goodRatePercentage.replace('%','')))
    list.append(weitaoId)
    list.append(float(desc_score))
    list.append(float(service_score))
    list.append(float(wuliu_score))
    list.append(location)
    list.append(str(time.time()))
    if flag=='insert':
        # for i in list:
        #     if len(i)==0: strlist.append('-')
        #     else : strlist.append(i)
        # return "\001".join(strlist)
        return list
    elif flag=='inc':
        return (shopId,list)
# rdd=sc.textFile('/data/develop/ec/tb/iteminfo_new/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01',100)\
def fun(y):
    return sorted(y,key=lambda t : t[-1],reverse=True)[0]
def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x

def fun1(x,ds):
    x.append(ds)
    return [f_coding(i) for i in x]

if __name__ == "__main__":
    hiveContext.sql('use wlbase_dev')
    if sys.argv[1] == '-h':
       comment = '-店铺 \n\
			  '
       print comment
       print '-insert argvs:\n argv[1]:file or dir input\n argv[2]:ds  \n'
       print '-inc      argvs:\n argv[1]:file or dir input\n argv[2]:ds_1  \n argv[3] ds\n '
    elif sys.argv[1]=='-insert':
        filepath=sys.argv[2]
        ds=sys.argv[3]
        rdd=sc.textFile(filepath,100)\
            .map(lambda x:parse_shop(x,'insert'))\
            .map(lambda x:fun1(x,ds))
        df=hiveContext.sql('select * from t_base_ec_shop_dev limit 1')
        schema1=df.schema
        ddf=hiveContext.createDataFrame(rdd,schema1)
        hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        sql='''
        insert overwrite table t_base_ec_shop_dev partition(ds=%s)
        select * from tmptable
        '''
        hiveContext.sql(sql%(ds))
    elif sys.argv[1]=='-inc':
        filepath=sys.argv[2]
        ds_1=sys.argv[3]
        ds=sys.argv[4]
        rdd=sc.textFile(filepath,100)\
            .map(lambda x:parse_shop(x,'inc'))
        hiveContext.sql('use wlbase_dev')
        df=hiveContext.sql('select * from t_base_ec_shop_dev where ds=%s'%ds_1)
        schema1=df.schema
        rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,
                                         x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.ts]))
        rdd2=rdd1.union(rdd).groupByKey()
        rdd3=rdd2.map(lambda (x,y):fun(y)).coalesce(40)
        ddf=hiveContext.createDataFrame(rdd3.map(lambda x:fun1(x,ds)),schema1)
        hiveContext.registerDataFrameAsTable(ddf,'tmptable')
        sql='''
        insert overwrite table t_base_ec_item_dev partition(ds=%s)
        select * from tmptable
        '''
        hiveContext.sql(sql%(ds))



# hiveContext.sql('use wlbase_dev')
# ds='20151104'
# df=hiveContext.sql('select * from t_base_ec_item_dev where ds=%s'%ds)
# schema1=df.schema
#
# rdd1=df.map(lambda x:(x.item_id,[x.item_id,x.title,x.cat_id,x.cat_name,x.root_cat_id,x.root_cat_name,x.brand_id,x.brand_name,x.bc_type,x.price,x.price_zone,x.is_online,x.off_time,x.favor,x.seller_id,x.shop_id,x.ts]))
#
#
# rdd3.map(lambda x:"\001".join([str(valid_jsontxt(i)) for i in x])).saveAsTextFile("/data/develop/ec/tb/iteminfo_tmp/1101.dir")

    # return x



# sql='''
# insert overwrite table t_base_ec_item_dev partition(ds=%s)
# select item_id,title,cat_id,cat_name,root_cat_id,root_cat_name,brand_id,brand_name,bc_type,price,price_zone,is_online,off_time,favor,seller_id,shop_id,ts from testtable
# '''

