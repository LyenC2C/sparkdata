__author__ = 'zlj'


from pyspark.sql import *
import numpy as np
from pyspark.mllib.clustering import *
from pyspark.sql.types import *


import rapidjson as json
import  time

import datetime


# /data/develop/ec/tb/iteminfo/jiu.iteminfo

from pyspark import SparkContext
sc=SparkContext(appName="test")
sqlContext = SQLContext(sc)

# path='/data/develop/ec/tb/iteminfo/jiu.iteminfo'
path='/data/develop/ec/tb/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo'

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

def parse(line_s):
    line=valid_jsontxt(valid_jsontxt)
    ts,id,txt=line.split('\t')
    s=json.loads(txt)
    itemInfoModel=s['itemInfoModel']
    item_id=itemInfoModel.get('itemId','-')
    title=itemInfoModel.get('title','-')
    categoryId=itemInfoModel.get('categoryId','-')
    cat_name='-'
    root_cat_id='-'
    root_cat_name='-'
    favor=itemInfoModel.get('favcount','-')
    trackParams=s['trackParams']
    BC_type=trackParams.get('BC_type','-')
    brandId=trackParams.get('brandId','-')
    brand_name='-'
    # price_zone=s['defDyn']['itemInfoModel']['priceUnits'][0]['price']
    # price='-'
    # if '-' not in price_zone:
    #     price=price_zone
    value=parse_price(s['apiStack']['itemInfoModel']['priceUnits'])
    price=value[0]
    price_zone=value[1]
    is_online=1
    off_time='-'
    seller=s['seller']
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
    for i in list:
        if len(i)==0: strlist.append('-')
        else : strlist.append(i)
    return "\001".join(strlist)


def  parse_shop(line):
    ob=json.loads(line)
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
    list.append(item_count)
    list.append(fansCount)
    list.append(goodRatePercentage.replace('%',''))
    list.append(weitaoId)
    list.append(desc_score)
    list.append(service_score)
    list.append(wuliu_score)
    list.append(str(time.time()))
    strlist=[]
    for i in list:
        if len(i)==0: strlist.append('-')
        else : strlist.append(i)
    return "\001".join(strlist)
    # return '\001'.join(list)


sc.textFile(path,100).map(lambda  x: parse(x))\
    .saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_item_dev/ds=20150101')




# sc.textFile(path,100).map(lambda  x: parse_shop(x)).saveAsTextFile('/hive/external/wlbase_dev/t_base_ec_shop_dev/ds=20150101')

# sc.textFile('/hive/external/wlbase_dev/t_base_ec_shop_dev/ds=20150101').filter(lambda  x: '64661829' in x).take(10)