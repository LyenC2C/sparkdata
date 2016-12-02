#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import zlib
import base64
import time
import rapidjson as json


def parse_price(price_dic):
    min = 1000000000.0
    price = '0'
    price_range = '-'
    for value in price_dic:
        tmp=value['price']
        v = ""
        if '-' in tmp:     v=tmp.split('-')[0]
        else:             v=tmp
        if v.replace('.',"").isdigit():
            v = float(v)
        else:
            v = 0.1
        if min > v:
            min = v
            price = v
            if '-' in tmp: price_range = tmp
    return [price,price_range]

def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         return content.encode("utf-8")
#     else:
#         return content

def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3],ss[8]])


def f(line,cate_dict):
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    item_id = ss[1]
    # is_online = ss[1] #0没有1上架2下架
    is_online = "-"
    ts = ss[0]
    # data_flag = ss[3] #历史状态，0没有1上架2下架（当天新商品没有的时候，他保持不变）
    data_flag = "-"
    # data_ts = ss[4] #历史状态时间
    data_ts = "-"
    # if (ss[5]) == "": return None
    # line = decompress(ss[5])
    # ss = line.strip().split("\t",2)
    # if len(ss) != 3: return None
    # txt = valid_jsontxt(ss[2])
    ob = json.loads(ss[2])
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
    if BC_type != 'B' and BC_type != 'C': BC_type = "-"
    brandId = trackParams.get('brandId','-')
    # brand_name = brand_dict.get(brandId,"-")
    brand_name = "-"
    # item_info = "-"
    props = ob.get('props',[])
    item_info_list = []
    for v in props:
        item_info_list.append(valid_jsontxt(v.get('name',"-")).replace(":","").replace(",","") \
                     + ":" + valid_jsontxt(v.get('value',"-")).replace(":","").replace(",",""))
        if valid_jsontxt('品牌') == valid_jsontxt(v.get('name',"-")) and brand_name == "-" :
            brand_name = v.get('value',"-")
    item_info = ",".join(item_info_list)
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    price_zone = value[1]
    seller = ob.get('seller',{})
    seller_id = seller.get('userNumId','-')
    shopId = seller.get('shopId','-')
    off_time = "-"
    # if is_online <> '1' and data_flag == '2': off_time = data_ts #如果已下架，显示下架时间，未下架，显示“-”
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
    result.append(price_zone)
    result.append(str(is_online))
    result.append(off_time) #老表里放的是“-1”，新表里放的是“-”
    result.append(str(favor))
    result.append(seller_id)
    result.append(shopId)
    result.append(location)
    result.append(item_info)
    result.append(sku_info)
    result.append(ts)
    return (item_id,result)
    # return "\001".join([str(valid_jsontxt(i)) for i in result])

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
        lv.append(valid_jsontxt(ln))
    return "\001".join(lv)


    # result = []
if __name__ == "__main__":
    if sys.argv[1] == "-local":
        cate_dict = {}
        for line in open("../../public/data/cate_dim"):
            ss = line.strip().split("\001")
            cate_dict[ss[0]] = [ss[1],ss[3],ss[8]]
        for line in sys.stdin:
            print f(line,cate_dict)
    if sys.argv[1] == "-spark":
        # s_last = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_dev_new/ds=" + last_day
        from pyspark import SparkContext
        sc = SparkContext(appName="t_base_item_info")
        # s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_house/part*"
        s = "/commit/iteminfo/house_tmp/*/*"
        s_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20161122/000000_"
        cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
        rdd_c = sc.textFile(s).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)
        rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
        # rdd_last = sc.textFile(s_last).map()
        rdd.saveAsTextFile('/user/wrt/temp/iteminfo_tmp')

# hfs -rmr /user/wrt/temp/iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 100 t_base_item_info.py -spark
#LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds='20160606');
# status_flag,data_flag：
# 0,0（根本就没有此商品，内容都没有，也就没有了入库的必要）
# 0,1（因为有可能是本次采集没采到，所以不能认为是下架，data_ts记录了过去采到时候的时间戳）
# 0,2（过去就下架了，现在没采到或者不存在此商品，即下架，data_ts即为下架时间，只要后面此商品不再上架，那么data_ts和data_flag就不会变）
# 1,1（上架）
# 2,2（采到的时候刚好下架，所以商品即为下架，同0,2）