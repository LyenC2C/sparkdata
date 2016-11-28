#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

yes_day = sys.argv[1]

sc = SparkContext(appName="ppzs_itemid_info_" + yes_day)

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def get_brand(line):
    brand_id = line.strip()
    return (brand_id,"1")

def get_cate_dict(line):
    ss = line.strip().split("\001")
    return (ss[0],[ss[1],ss[3],ss[8]])

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

def f(line,brand_dict,cate_dict):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    categoryId = itemInfoModel.get('categoryId','-')
    root_cat_id = cate_dict.get(categoryId,["-","-","-"])[1]
    if not root_cat_id in ["50010788","1801"]: return None #不属于化妆品
    title = itemInfoModel.get('title','-').replace("\n","")
    picsPath = itemInfoModel.get('picsPath',[])
    if picsPath == []: picurl = "-"
    else: picurl = picsPath[0]
    trackParams = ob.get('trackParams',{})
    brandId = valid_jsontxt(trackParams.get('brandId','-'))
    # if brandId == "-": return None
    if not brand_dict.has_key(brandId): return None
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    if int(price) > 160000:
        price = 1.0
    result = []
    result.append(item_id)
    result.append(brandId)
    result.append(title)
    result.append(picurl)
    result.append(price)
    return (item_id,result)


def f_old(line,brand_dict,cate_dict):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    data = ob.get('data',"-")
    if data == "-": return None
    itemInfoModel = data.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    categoryId = itemInfoModel.get('categoryId','-')
    root_cat_id = cate_dict.get(categoryId,["-","-","-"])[1]
    if not root_cat_id in ["50010788","1801"]: return None #不属于化妆品
    title = itemInfoModel.get('title','-').replace("\n","")
    picsPath = itemInfoModel.get('picsPath',[])
    if picsPath == []: picurl = "-"
    else: picurl = picsPath[0]
    trackParams = data.get('trackParams',{})
    brandId = valid_jsontxt(trackParams.get('brandId','-'))
    # if brandId == "-": return None
    if not brand_dict.has_key(brandId): return None
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
    result.append(brandId)
    result.append(title)
    result.append(picurl)
    result.append(price)
    return (item_id,result)


b_dim = "/hive/warehouse/wlservice.db/ppzs_brandid_72ge"
s = "/commit/tb_tmp/iteminfo/" + yes_day
# s = "/commit/tb_tmp/iteminfo/20161120/*2016-11-21"
c_dim = "/hive/warehouse/wlbase_dev.db/t_base_ec_dim/ds=20161122/000000_0"
brand_dict = sc.broadcast(sc.textFile(b_dim).map(lambda x: get_brand(x)).filter(lambda x:x!=None).collectAsMap()).value
cate_dict = sc.broadcast(sc.textFile(c_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:f_old(x,brand_dict,cate_dict)).filter(lambda x:x!=None)
rdd.groupByKey().mapValues(list).map(lambda (x,y): "\001".join([str(valid_jsontxt(i)) for i in y[0]]))\
    .saveAsTextFile("/user/wrt/temp/ppzs_itemid_info")


# hfs -rmr /user/wrt/temp/ppzs_itemid_info
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  ppzs_item_info.py
# LOAD DATA  INPATH '/user/wrt/temp/ppzs_itemid_info' OVERWRITE INTO TABLE wlservice.ppzs_itemid_info PARTITION (ds='20161108');