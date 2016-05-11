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

def get_sku_dict(x):
    ob = json.loads(valid_jsontxt(x))
    product = ob.get("product","-")
    if product == "-": return [None]
    item_id = str(product.get("item_id","-"))
    if item_id == "-": return [None]
    skus = product.get("colorSize",[])
    if skus == []: return [N    one]
    result = []
    for sku in skus:
        skuid = str(sku.get("SkuId"))
        result.append((skuid,item_id))
    return result

def f(x,sku_dict):
    result = []
    ob = json.loads(valid_jsontxt(x))
    skuid = ob.get("item_id","-")
    item_id = sku_dict.get(skuid,skuid)
    title = ob.get("title","-")
    key = valid_jsontxt(ob.get("key","-"))
    cate_name = "-"
    brand_name = "-"
    if "润膏" in key:
        cate_name = "洗发水"
        brand_name = "润膏"
    if "雪花秀" in key:
        cate_name = "面部护理套装"
        brand_name = "雪花秀"
    if "MEDIHEAL" in key:
        cate_name = "面膜"
        brand_name = "MEDIHEAL"
    if "兰芝" in key:
        cate_name = "BB霜"
        brand_name = "兰芝"
    if "贵爱娘" in key:
        cate_name = "卫生巾"
        brand_name = "贵爱娘"
    if "White" in key:
        cate_name = "卫生巾"
        brand_name = "White"
    if "BOSOMI" in key:
        cate_name = "纸尿片"
        brand_name = "BOSOMI"
    if "好奇" in key:
        cate_name = "纸尿片"
        brand_name = "好奇"
    if "惠人" in key:
        cate_name = "榨汁机"
        brand_name = "惠人"
    if "福库" in key:
        cate_name = "电饭煲"
        brand_name = "福库"
    if "兰芝" in key:
        cate_name = "面部护理套装"
        brand_name = "兰芝"
    laiyuan = "yh"
    price_zone = "-"
    price = ob.get("price","-")
    rateCounts = ob.get("comment_count","-").split()[0]
    item_count = "1"
    ts = "-"
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
    result.append(skuid)
    result.append(title)
    result.append(cate_name)
    result.append(laiyuan)
    result.append(brand_name)
    result.append(str(price))
    result.append(item_id)# result.append((price_zone))
    result.append(rateCounts)
    result.append(item_count)
    result.append(ts)
    # return "\001".join([str(valid_jsontxt(i)) for i in result])
    return (item_id,result)

def quchong(x,y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[7]) > max:
                max = int(ln[7])
                y = ln
    return "\001".join([str(valid_jsontxt(i)) for i in y])

s_jd = "/commit/project/hanguo3/han.jingdong.iteminfo.search"
s_ymt = "/commit/project/hanguo3/han.yangmatou*"
s_yh = "/commit/project/hanguo3/yunhou.han.key.3.json"
s_dim = "/commit/project/hanguo3/han.jingdong.iteminfo"
rdd1 = sc.broadcast(sc.textFile(s_dim).flatMap(lambda x: y(x)).filter(lambda x:x!=None).collectAsMap())
sku_dict = rdd1.value
rdd_c = sc.textFile(s_jd).map(lambda x: f(x)).filter(lambda x: x != None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y))
rdd.saveAsTextFile('/user/wrt/temp/t_korea_iteminfo_yh')
