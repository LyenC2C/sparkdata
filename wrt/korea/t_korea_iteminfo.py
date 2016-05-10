#coding:utf-8
__author__ = 'wrt'
import sys
import re
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

# def get_country_dict(line):
#     ob = json.loads(valid_jsontxt(line.strip()))
#     return (ob["brand"],ob["country_name"])

def get_laiyuan_dict(line):
    ss = line.strip().split("\t")
    return (ss[0],ss[1])

def f(line,cate_dict,laiyuan_dict):
    ss = line.strip().split("\t",2)
    if len(ss) != 3: return None
    ts = ss[0]
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
    seller = ob.get('seller',{})
    rateInfo = ob.get('rateInfo',{})
    rateCounts = rateInfo.get('rateCounts',"-")
    location = valid_jsontxt(itemInfoModel.get('location','-'))
    shopId = seller.get('shopId','-')
    seller_id = seller.get('userNumId','-')
    categoryId = itemInfoModel.get('categoryId','-')
    title = valid_jsontxt(itemInfoModel.get('title','-').replace("\n","").replace("\r",""))
    cate_name = valid_jsontxt(cate_dict.get(categoryId,["-","-"])[0])
    if cate_name == "-": return None
    if cate_name in ["护垫","产妇卫生巾"]:
        cate_name = "卫生巾"
    if cate_name == "原汁机":
        cate_name = "榨汁机"
    if cate_name == "洗护套装":
        cate_name = "洗发水"
    if cate_name == "电煲/电锅类配件":
        cate_name = "电饭煲"
    if cate_name == "纸尿裤/拉拉裤/纸尿片":
        cate_name = "纸尿片"
    # cate_name = "面部护理套装"
    # cate_root_name = cate_dict.get(categoryId,["-","-"])[1]
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    price_zone = value[1]
    trackParams = ob.get('trackParams',{})
    laiyuan = laiyuan_dict.get(shopId,"-")
    # if BC_type == 'C': return None
    # brandId = trackParams.get('brandId','-')
    brand_name = '-'
    props=ob.get('props',[])
    for v in props:
        if valid_jsontxt('品牌') in valid_jsontxt(v['name']):
            brand_name = v['value']
    # country = get_country_dict.get(valid_jsontxt(brand_name),"-")
    result = []
    # result.append(item_id)
    # item_count = "-"
    # if cate_name in ["榨汁机","电饭煲"]:
    item_count = "1"
    if cate_name == "卫生巾" or cate_name == "纸尿片":
        if "包" in title:
            # tt = title.decode("utf-8")
            i = title.find("包")
            if i >= 0:
                i = i-1
                item_count = ""
                while(title[i].isdigit() and i > 0):
                    item_count = title[i] + item_count
                    if i > 0: i = i - 1
                    else: break
            if item_count == "": item_count = '1'
        elif "片*" in title or "p*" in title or "P*" in title or "片x" in title or "片X" in title:
            for ln in ["片*","p*","P*","片x","片X"]:
                i = title.find(ln)
                if i <= 0: continue
                else:
                    i = i + len(ln)
                    if i >= len(title):continue
                    item_count = ""
                    while(title[i].isdigit()):
                        item_count = item_count + title[i]
                        if i < len(title) - 1: i += 1
                        else: break
                    if item_count == "": item_count = "1"
                    break
    if cate_name == "洗发水":
        if "瓶" in title or "支" in title:
            # tt = title.decode("utf-8")
            for ln in ["瓶","支"]:
                i = title.find(ln)
                if i < 0: continue
                else:
                    i = i - 1
                    item_count = ""
                    while(title[i].isdigit()):
                        item_count = title[i] + item_count
                        if i > 0: i = i - 1
                        else: break
                    if item_count == "": item_count = '1'
                    break
        elif "*" in title or "X" in title or "x" in title:
            for ln in ["*","X","x"]:
                i = title.find(ln)
                if i <= 0: continue
                else:
                    i = i + 1
                    if i >= len(title):continue
                    item_count = ""
                    while(title[i].isdigit()):
                        item_count = item_count + title[i]
                        if i < len(title) - 1: i += 1
                        else: break
                    if item_count == "": item_count = "1"
                    break
    if cate_name == "面膜":
        if "片" in title:
            # tt = title.decode("utf-8")
            i = title.find("片")
            if i >= 0:
                i = i-1
                item_count = ""
                while(title[i].isdigit() and i > 0):
                    item_count = title[i] + item_count
                    if i > 0: i = i - 1
                    else: break
            if item_count == "": item_count = '-'
        else:
            item_count = "-"
    result.append(title)
    # result.append(categoryId)
    result.append(cate_name)
    # result.append(cate_root_name)
    result.append(laiyuan)
    # result.append(brandId)
    result.append(brand_name)
    result.append(str(price))
    result.append((price_zone))
    result.append(rateCounts)
    # result.append(seller_id)
    # result.append(shopId)
    # result.append(country)
    result.append(item_count)
    # result.append(location)
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
s2 = "/commit/project/hanguo3/han.tbtm.iteminfo.patch"
s_dim = "/commit/project/tmallint/dim.subcat.final.json"
b_country = "/commit/project/tmallint/brand.json"
laiyuan_dim = "/commit/project/hanguo3/han.tbtm.iteminfo.shoptype"
rdd1 = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_cate_dict(x)).filter(lambda x:x!=None).collectAsMap())
cate_dict = rdd1.value
# rdd2 = sc.broadcast(sc.textFile(b_country).map(lambda x: get_country_dict(x)).filter(lambda x:x!=None).collectAsMap())
# country_dict = rdd2.value
rdd3 = sc.broadcast(sc.textFile(laiyuan_dim).map(lambda x: get_laiyuan_dict(x)).filter(lambda x:x!=None).collectAsMap())
laiyuan_dict = rdd3.value
rdd_c = sc.textFile(s).map(lambda x: f(x,cate_dict,laiyuan_dict)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/t_korea_iteminfo_patch')


#spark-submit  --executor-memory 3G  --driver-memory 5G  --total-executor-cores 40 t_korea_iteminfo.py

