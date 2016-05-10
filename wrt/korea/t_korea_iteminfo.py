#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="k_korea_iteminfo")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=0
        if '-' in tmp:     v=tmp.split('-')[0]
        else :             v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def get_cate_dict(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    return (ob["sub_category_id"],[ob["sub_category_name"],ob["category_name"]])

def get_country_dict(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    return (ob["brand"],ob["country_name"])

def f(line,cate_dict,get_country_dict):
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    ts = ss[0]
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    seller = ob.get('seller',{})
    rateInfo = ob.get('rateInfo',{})
    rateCounts = rateInfo.get('rateCounts',"-")
    if itemInfoModel == "-": return None
    location = valid_jsontxt(itemInfoModel.get('location','-'))
    shopId = seller.get('shopId','-')
    seller_id = seller.get('userNumId','-')
    categoryId = itemInfoModel.get('categoryId','-')
    title = itemInfoModel.get('title','-').replace("\n","").replace("\r","")
    cate_name = cate_dict.get(categoryId,["-","-"])[0]
    cate_root_name = cate_dict.get(categoryId,["-","-"])[1]
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    price_zone = value[1]
    trackParams = ob.get('trackParams',{})
    BC_type = trackParams.get('BC_type','-')
    # if BC_type == 'C': return None
    brandId = trackParams.get('brandId','-')
    brand_name = '-'
    props=ob.get('props',[])
    for v in props:
        if valid_jsontxt('品牌') in valid_jsontxt(v['name']):
            brand_name = v['value']
    country = get_country_dict.get(valid_jsontxt(brand_name),"-")
    result = []
    # result.append(item_id)
    item_count = "-"
    result.append(title)
    result.append(categoryId)
    result.append(cate_name)
    result.append(cate_root_name)
    result.append(BC_type)
    result.append(brandId)
    result.append(brand_name)
    result.append(str(price))
    result.append((price_zone))
    result.append(rateCounts)
    result.append(seller_id)
    result.append(shopId)
    result.append(country)
    result.append(item_count)
    result.append(location)
    result.append(ts)
    return (item_id,result)

def quchong(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    result = [x] + y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)


s = "/commit/project/hanguo3/han.tbtm.iteminfo"
s_dim = "/commit/project/tmallint/dim.subcat.final.json"
b_country = "/commit/project/tmallint/brand.json"
rdd1 = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap())
cate_dict = rdd1.value
rdd2 = sc.broadcast(sc.textFile(b_country).map(lambda x: get_country_dict(x)).filter(lambda x:x!=None).collectAsMap())
country_dict = rdd2.value
rdd_c = sc.textFile(s).map(lambda x: f(x,cate_dict,country_dict)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/t_korea_iteminfo')


#spark-submit  --executor-memory 3G  --driver-memory 5G  --total-executor-cores 40 t_korea_iteminfo.py

