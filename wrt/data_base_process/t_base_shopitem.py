#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="t_base_item_info")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ss = line.strip().split("\t",3)
    shop_id = ss[1]
    ts = ss[0]
    text = line.strip()
    star = text.find("({")
    # if star == -1: return [None]
    # else:
    star += 1
    end = text.rfind("})") + 1
    ob = json.loads(valid_jsontxt(text[star:end]))
    shopTitle = ob.get("shopTitle","-")
    item_count = ob.get("totalResults","-")
    itemsArray = ob.get("itemsArray",[])
    result = []
    for item in itemsArray:
        lv = []
        auctionId = item.get("auctionId","-")
        title = item.get("title","-")
        picUrl = item.get("picUrl","-")
        sold = item.get("sold","-")
        reservePrice = item.get("reservePrice","-")
        salePrice = item.get("salePrice","-")
        auctionType = item.get("auctionType","-")
        quantity = item.get("quantity","-")
        totalSoldQuantity = item.get("totalSoldQuantity","-")
        orderCost = item.get("orderCost","-")
        bonusAmount = item.get("bonusAmount","-")
        onSale = item.get("onSale","-")
        online_days = "1"
        lv.append(valid_jsontxt(shop_id))
        lv.append(valid_jsontxt(shopTitle))
        lv.append(valid_jsontxt(item_count))
        lv.append(valid_jsontxt(auctionId))
        lv.append(valid_jsontxt(title))
        lv.append(valid_jsontxt(picUrl))
        lv.append(valid_jsontxt(sold))
        lv.append(valid_jsontxt(reservePrice))
        lv.append(valid_jsontxt(salePrice))
        lv.append(valid_jsontxt(auctionType))
        lv.append(valid_jsontxt(quantity))
        lv.append(valid_jsontxt(totalSoldQuantity))
        lv.append(valid_jsontxt(orderCost))
        lv.append(valid_jsontxt(bonusAmount))
        lv.append(valid_jsontxt(onSale))
        lv.append(valid_jsontxt(online_days))
        lv.append(ts)
        result.append(item_id,lv)
    return result

def quchong(x,y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[-1]) > max:
            max = int(ln[-1])
            y = ln
    return "\001".join(y)

s = "/commit/tb_shopitem.json"
rdd1_c = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None)
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y))
rdd1.saveAsTextFile('/user/wrt/shopitem_tmp')

# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_shopitem.py