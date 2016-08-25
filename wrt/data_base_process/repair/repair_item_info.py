#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
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
            v = 0.0
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
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
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
    txt = valid_jsontxt(line.strip())
    ob = json.loads(txt)
    if type(ob) == type(1.0): return None
    if ob.has_key("ret") and "ERRCODE_QUERY_DETAIL_FAIL::宝贝不存在" in ob["ret"]:
        return None
    data = ob.get('data',"-")
    apiStack = data.get("apiStack",[])
    if apiStack == []:
        return None
    value_json = apiStack[0].get("value")
    value_ob = json.loads(valid_jsontxt(value_json))
    if value_ob["data"]["itemControl"]["unitControl"].has_key("offShelfUrl"):
        is_online = 2
    else:
        is_online = 1
    if data == "-": return None
    itemInfoModel = data.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    location = valid_jsontxt(itemInfoModel.get('location','-'))
    item_id = itemInfoModel.get('itemId','-')
    if item_id == "-": return None
    title = itemInfoModel.get('title','-').replace("\n","")
    favor = itemInfoModel.get('favcount','0')
    categoryId = itemInfoModel.get('categoryId','-')
    if categoryId != '121954006' and categoryId != '122326002' and categoryId != '50012481':
        return None
    root_cat_id = cate_dict.get(categoryId,["-","-","-"])[1]
    cat_name = cate_dict.get(categoryId,["-","-","-"])[0]
    root_cat_name = cate_dict.get(categoryId,["-","-","-"])[2]
    trackParams = data.get('trackParams',{})
    BC_type = trackParams.get('BC_type','-')
    if BC_type != 'B' and BC_type != 'C': BC_type = "-"
    brandId = trackParams.get('brandId','-')
    # brand_name = brand_dict.get(brandId,"-")
    brand_name = "-"
    # item_info = "-"
    props = data.get('props',[])
    item_info_list = []
    for v in props:
        item_info_list.append(valid_jsontxt(v.get('name',"-")).replace(":","").replace(",","") \
                     +":" + valid_jsontxt(v.get('value',"-")).replace(":","").replace(",",""))
        if valid_jsontxt('品牌') in valid_jsontxt(v.get('name',"-")) and brand_name == "-" :
            brand_name = v.get('value',"-")
    item_info = ",".join(item_info_list)
    value = parse_price(value_ob["data"]["itemInfoModel"]["priceUnits"])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    price_zone = value[1]
    seller = data.get('seller',{})
    seller_id = seller.get('userNumId','-')
    shopId = seller.get('shopId','-')
    ts = "1471104000"
    off_time = "-"
    if is_online == '2' : off_time = ts #如果已下架，显示下架时间，未下架，显示“-”,此处为修复程序，下架时间暂时为入库时间，后面会join之前的数据产生准确的下架时间
    sku_info = "-"
    # skuProps = data.get("apiStack",{}).get("skuModel",{}).get("","-")
    # if skuProps != "-":
    # is_online = "-"
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
    result.append(off_time)
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
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)


    # result = []
if __name__ == "__main__":
    from pyspark import SparkContext
    sc = SparkContext(appName="repair_item_info")
    s = "/commit/iteminfo/20160814/*"
    s_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20151023/1073988839"
    cate_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
    rdd = sc.textFile(s).map(lambda x: f(x,cate_dict)).filter(lambda x:x!=None)\
        .groupByKey().mapValues(list).map(lambda (x,y):"\001".join([valid_jsontxt(i) for i in y[0]]))
    # rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
    rdd.saveAsTextFile('/user/wrt/temp/repair_iteminfo_tmp')

# hfs -rmr user/wrt/temp/znk_iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  repair_item_info.py
# LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds='20160606');
# status_flag,data_flag：
# 0,0（根本就没有此商品，内容都没有，也就没有了入库的必要）
# 0,1（因为有可能是本次采集没采到，所以不能认为是下架，但是不入库）
# 0,2（过去就下架了，现在没采到或者不存在此商品，即下架，但是不入库）
# 1,1（上架）
# 2,2（采到的时候刚好下架，所以商品即为下架，data_ts即为下架时间，只要后面此商品不再上架，那么data_ts和data_flag就不会变）