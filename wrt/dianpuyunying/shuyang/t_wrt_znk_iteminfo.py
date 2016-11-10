#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
import rapidjson as json
# import json
from pyspark import SparkContext
now_day = sys.argv[1]
last_day = sys.argv[2]
sc = SparkContext(appName="shuyang_iteminfo_"+now_day)


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
    return (valid_jsontxt(ss[0]),valid_jsontxt(ss[1]))

def ps(title):
    # for ln in ["片","枚"]:
    result = ""
    title = title.decode("utf-8")
    for i in range(len(title)):
        if "片" == title[i].encode("utf-8"):
            j = i -1
            while (j >=0 and title[j].isdigit()):
                result = title[j] + result
                j -= 1
            if result != "": return result
        if "枚" == title[i].encode("utf-8"):
            j = i -1
            while (j >=0 and title[j].isdigit()):
                result = title[j] + result
                j -= 1
            if result != "": return result
    return "-"

def tp(title):
    for ln in ["拉拉裤","纸尿裤","护理垫","纸尿片"]:
        if ln in title:
            return "成人"+ln
    return "-"


def f1(line,brand_dict):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) == type(1.0): return None
    data = ob.get('data',"-")
    if data == "-": return None
    itemInfoModel = data.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    # location = valid_jsontxt(itemInfoModel.get('location','-'))
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
    brand_name = "-"
    props = data.get('props',[])
    item_info_list = []
    for v in props:
        item_info_list.append(valid_jsontxt(v.get('name',"-")).replace(":","").replace(",","") \
                     +":" + valid_jsontxt(v.get('value',"-")).replace(":","").replace(",",""))
        if valid_jsontxt('品牌') in valid_jsontxt(v.get('name',"-")) and brand_name == "-" :
            brand_name = v.get('value',"-")
    item_info = ",".join(item_info_list)
    apiStack = data.get("apiStack",[])
    if apiStack == []:
        price = 0.0
        price_zone = '-'
    else:
        value_json = apiStack[0].get("value")
        value_ob = json.loads(valid_jsontxt(value_json))
        value = parse_price(value_ob["data"]["itemInfoModel"]["priceUnits"])
        price = value[0]
        if int(price) > 160000:
            price = 1.0
        price_zone = value[1]
    seller = data.get('seller',{})
    seller_id = seller.get('userNumId','-')
    shopId = seller.get('shopId','-')
    off_time = "-"
    # if is_online <> '1' and data_flag == '2': off_time = data_ts #如果已下架，显示下架时间，未下架，显示“-”
    sku_info = "-"
    # skuProps = data.get("apiStack",{}).get("skuModel",{}).get("","-")
    # if skuProps != "-":
    ts = "-"
    is_online = "-"
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

def f2(line,brand_dict):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    data = ob.get('data',"-")
    if data == "-": return None
    itemInfoModel = data.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    item_id = itemInfoModel.get('itemId','-')
    categoryId = itemInfoModel.get('categoryId','-')
    if not categoryId in ["121954006","122326002","50012481"]: return None #不属于纸尿裤的直接舍弃掉
    title = itemInfoModel.get('title','-').replace("\n","")
    picsPath = itemInfoModel.get('picsPath',[])
    if picsPath == []: picurl = "-"
    else: picurl = picsPath[0]
    trackParams = data.get('trackParams',{})
    # brandId = trackParams.get('brandId','-')
    # brand_name = brand_dict.get(brandId,"new_brand") #每次入库都要人工查看一下是否产生新的brandid。
    props = data.get('props',[])
    item_count = "-"
    item_size = "-"
    item_type = "-"
    #brand_name = "-"
    brand = "-"
    for v in props:
        if valid_jsontxt('尺码') == valid_jsontxt(v.get('name','-')): item_size = v.get("value","-")
        if valid_jsontxt('成人纸尿裤护理品') == valid_jsontxt(v.get('name','-')): item_type = v.get("value","-")
        if valid_jsontxt('包装数量(片)') == valid_jsontxt(v.get('name','-')): item_count = v.get("value",'-')
        if valid_jsontxt('品牌') == valid_jsontxt(v.get('name','-')): brand = valid_jsontxt(v.get("value",'-'))
    if brand == '-': return None
    brand_name = brand_dict.get(brand,brand)
    # item_info = ",".join(item_info_list)
    if not item_size in ['L','XL','M','S']: return None #大部分商品皆有尺码，没有尺码的直接舍弃掉即可
    if not item_count.isdigit():
        item_count = ps(title) #先判断是否为数字，如果不是就从title中解析
    if item_count == "-": return None #解析失败返回“-”，那么最终舍弃这个商品
    for znk_type in ["拉拉裤","纸尿裤","护理垫","纸尿片"]: #同意成人xxx的形式
        if znk_type in item_type:
            item_type = "成人" + znk_type
    if not item_type in ["成人拉拉裤","成人纸尿裤","成人护理垫","成人纸尿片"]: item_type = tp(title) #统一后依然不在标准数据范围就解析title
    if item_type == "-": return None #解析失败返回“-”，那么最终舍弃这个商品
    #value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    #price = value[0]
    apiStack = data.get("apiStack",[])
    if apiStack == []:
        return None
    value_json = valid_jsontxt(apiStack[0].get("value",""))
    if value_json == "": return None
    value_ob = json.loads(value_json)
    if type(value_ob) != type({}): return None
    value = parse_price(value_ob["data"]["itemInfoModel"]["priceUnits"])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    result = []
    result.append(item_id)
    result.append(title)
    result.append(brand_name)
    result.append(item_size)
    result.append(item_type)
    result.append(item_count)
    result.append(str(price))
    result.append(str(picurl))
    return (item_id,result)
    #return "\001".join([str(valid_jsontxt(i)) for i in result])

def f3(line):
    ss = line.strip().split("\001")
    ss.append(last_day) #强行加入一个字段使得和今天数据区分开
    return (ss[0],ss)

def twodays(x,y):   #同一个item_id下进行groupby后的结果
    item_list = y
    if len(item_list) == 1: #只有一个商品
        if len(item_list[0]) == 9:
            yes_item = item_list[0] #此商品为昨日商品（昨日商品多个ds字段），今日商品需要复制昨日商品
            result = yes_item[:-1]   #记得将最后的ds去掉，不要复制进来
        if len(item_list[0]) == 8:
            tod_item = item_list[0] #此商品为今日商品
            result = tod_item #使用默认值即可
    if len(item_list) == 2: #有两个商品，一个是昨日，一个是今日
        #判断今日和昨日的位置并分别命名赋值
        if len(item_list[0]) == 9:
            tod_item = item_list[1]
        if len(item_list[0]) == 8:
            tod_item = item_list[0]
        result = tod_item
    return "\001".join([str(valid_jsontxt(i)) for i in result])

    # result = []

#s = "/commit/tb_tmp/iteminfo/diapers.iteminfo.cb"
s1 = "/commit/tb_tmp/iteminfo/" + now_day + "/*"
s2 = "/hive/warehouse/wlservice.db/t_wrt_znk_iteminfo_new/ds=" +last_day
s_dim = "/hive/warehouse/wlservice.db/t_wrt_znk_brand_brandname/znk_brand_brandname"
brand_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd_now_c = sc.textFile(s1).map(lambda x: f2(x, brand_dict)).filter(lambda x:x!=None)
rdd_now = rdd_now_c.groupByKey().mapValues(list).map(lambda (x,y):(x,y[0]))
rdd_last = sc.textFile(s2).map(lambda x:f3(x))
rdd = rdd_now.union(rdd_last).groupByKey().mapValues(list).map(lambda (x, y):twodays(x, y)) #两天数据合并
rdd.saveAsTextFile('/user/wrt/temp/znk_iteminfo_tmp')
# hfs -rmr /user/wrt/temp/znk_iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  t_wrt_znk_iteminfo.py 20160913 20160912
# LOAD DATA  INPATH '/user/wrt/temp/znk_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_znk_iteminfo_new PARTITION (ds='20160919');
