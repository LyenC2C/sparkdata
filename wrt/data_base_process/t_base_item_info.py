#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import zlib
import base64
import time
import rapidjson as json
from pyspark import SparkContext


sc = SparkContext(appName="t_base_item_info")

def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=""
        if '-' in tmp:     v=tmp.split('-')[0]
        else :             v=tmp
        if v.isdigit():
            v = float()
        else:
            v = 0.0
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3],ss[8]])


def f(line,cate_dict):
    ss = line.strip().split("\001")
    item_id = ss[0]
    is_online = ss[1] #0没有1上架2下架
    ts = ss[2]
    data_flag = ss[3] #历史状态，0没有1上架2下架（当天新商品没有的时候，他保持不变）
    data_ts = ss[4] #历史状态时间
    if (ss[5]) == "": return None
    line = decompress(ss[5])
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    location = valid_jsontxt(itemInfoModel.get('location','-'))
    # item_id = itemInfoModel.get('itemId','-')
    title = itemInfoModel.get('title','-').replace("\n","")
    favor = itemInfoModel.get('favcount','0')
    categoryId = itemInfoModel.get('categoryId','-')
    root_cat_id = cate_dict.get(categoryId,["-","-","-"])[1]
    cat_name = cate_dict.get(categoryId,["-","-","-"])[0]
    root_cat_name = cate_dict.get(categoryId,["-","-","-"])[2]
    trackParams = ob.get('trackParams',{})
    BC_type = trackParams.get('BC_type','-')
    brandId = trackParams.get('brandId','-')
    # brand_name = brand_dict.get(brandId,"-")
    brand_name = "-"
    # item_info = "-"
    props=ob.get('props',[])
    item_info_list = []
    for v in props:
        item_info_list.append(valid_jsontxt(v.get('name',"-")).replace(":","").replace(",","") \
                     +":" + valid_jsontxt(v.get('value',"-")).replace(":","").replace(",",""))
        if valid_jsontxt('品牌') in valid_jsontxt(v.get('name',"-")) and brand_name == "-" :
            brand_name = v['value']
    item_info = ",".join(item_info_list)
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    price_zone = value[1]
    seller = ob.get('seller',{})
    seller_id = seller.get('userNumId','-')
    shopId = seller.get('shopId','-')
    off_time = "-"
    if is_online == 0 and data_flag == 2: off_time = data_ts #如果已下架，显示下架时间，未下架，显示“-”
    sku_info = "-"
    # skuProps = ob.get("apiStack",{}).get("skuModel",{}).get("","-")

    # if skuProps != "-":
    result = []
    result.append(item_id)
    result.append(title)
    result.append(categoryId)
    result.append(cat_name)
    result.append(root_cat_id)
    result.append(root_cat_name)
    result.append(brandId)
    result.append(brand_name)
    result.append(BC_type)
    result.append(str(price))
    result.append((price_zone))
    result.append((is_online))
    result.append(off_time)
    result.append(int(favor))
    result.append(seller_id)
    result.append(shopId)
    result.append(location)
    result.append(item_info)
    result.append(sku_info)
    result.append(ts)
    # return (item_id,result)
    return "\001".join([str(valid_jsontxt(i)) for i in result])

def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)

    # result = []

s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_house/part*"
s_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20151023/1073988839"
cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)
# rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/iteminfo_test')


# spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 t_base_item_info.py
#LOAD DATA  INPATH '/user/wrt/temp/iteminfo_test' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds='20160421');