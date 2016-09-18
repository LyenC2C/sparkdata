#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
import rapidjson as json
from pyspark import SparkContext
last_day = sys.argv[1]
sc = SparkContext(appName="t_base_item_info")


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
    return (ss[0],ss[1])

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
    txt = valid_jsontxt(line)
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
    txt = valid_jsontxt(line)
    ob = json.loads(txt)
    if type(ob) == type(1.0): return None
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
    brandId = trackParams.get('brandId','-')
    brand_name = brand_dict.get(brandId,"new_brand") #每次入库都要人工查看一下是否产生新的brandid。
    props = ob.get('props',[])
    for v in props:
        if valid_jsontxt('尺码') == valid_jsontxt(v.get('name','-')): item_size = v.get("value","-")
        if valid_jsontxt('成人纸尿裤护理品') == valid_jsontxt(v.get('name','-')): item_type = v.get("value","-")
        if valid_jsontxt('包装数量(片)') == valid_jsontxt(v.get('name','-')): item_count = v.get("value",'-')
    # item_info = ",".join(item_info_list)
    if not item_size in ['L','XL','M','S']: return None #大部分商品皆有尺码，没有尺码的直接舍弃掉即可
    if not item_count.isdigit(): item_count = ps(title) #先判断是否为数字，如果不是就从title中解析
    if item_count == "-": return None #解析失败返回“-”，那么最终舍弃这个商品
    for type in ["拉拉裤","纸尿裤","护理垫","纸尿片"]: #同意成人xxx的形式
        if type in item_type:
            item_type = "成人" + type
    if not item_type in ["成人拉拉裤","成人纸尿裤","成人护理垫","成人纸尿片"]: item_type = tp(title) #统一后依然不在标准数据范围就解析title
    if item_type == "-": return None #解析失败返回“-”，那么最终舍弃这个商品
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    result = []
    result.append(item_id)
    result.append(title)
    result.append(brand_name)
    result.appned(item_size)
    result.appned(item_type)
    result.appned(item_count)
    result.append(str(price))
    result.append(str(picurl))
    return (item_id,result)
    # return "\001".join([str(valid_jsontxt(i)) for i in result])

def f3(line):
    ss = line.strip().split("\001")
    ss.append(yesterday)
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

# s = "/commit/tb_tmp/iteminfo/diapers.iteminfo.cb"
s1 = ""
s2 = "/hive/warehouse/wlservice.db/t_wrt_znk_iteminfo_new/ds=" +last_day
s_dim = "/hive/warehouse/wlservice.db/t_wrt_znk_brandid_name/brandid_name"
brand_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd_now = sc.textFile(s).map(lambda x: f2(x, brand_dict)).filter(lambda x:x!=None)\
    .groupByKey().mapValues(list).map(lambda (x,y):(x,y[0]))
rdd_last = sc.textFile(s).map(lambda x:f3(x))
rdd = rdd_now.union(rdd_last).groupByKey().mapValues(list).map(lambda (x, y):twodays(x, y)) #两天数据合并
# rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/znk_iteminfo_tmp')

# hfs -rmr user/wrt/temp/znk_iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  t_wrt_znk_iteminfo.py
# LOAD DATA  INPATH '/user/wrt/temp/znk_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_znk_iteminfo PARTITION (ds='20160825');
# status_flag,data_flag：
# 0,0（根本就没有此商品，内容都没有，也就没有了入库的必要）
# 0,1（因为有可能是本次采集没采到，所以不能认为是下架，data_ts记录了过去采到时候的时间戳）
# 0,2（过去就下架了，现在没采到或者不存在此商品，即下架，data_ts即为下架时间，只要后面此商品不再上架，那么data_ts和data_flag就不会变）
# 1,1（上架）
# 2,2（采到的时候刚好下架，所以商品即为下架，同0,2）