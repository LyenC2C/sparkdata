#coding:utf-8
__author__ = 'wrt'

import sys
import json
from pyspark import SparkContext

sc = SparkContext(appName="check_sale_price")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def float_k(x):
    if x == "":
        return 0.0
    else:
        return float(x)

def parse_price(price_dic):
    min = 1000000000.0
    price = '0'
    price_range = '-'
    for value in price_dic:
        if value['name'].encode('utf-8') != '价格': continue
        tmp = value['price']
        v = 0
        if '-' in tmp:     v=float(tmp.split('-')[0])
        else:             v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def f1(line):
    ss = line.strip().split('\t',3)
    ts = ss[0]
    result = []
    zhengwen = ss[3]
    ob = json.loads(valid_jsontxt(zhengwen))
    if type(ob) != type({}):
        return [None]
    if not ob.has_key("itemsArray"):
        return [None]
    if ob["totalResults"] == 0:
        return [None]
    itemsArray = ob["itemsArray"]
    # shop_id = ob.get("shopId","-")
    for item in itemsArray:
        if item.has_key("salePrice"): continue
        lv = []
        item_id = item.get("auctionId","-")
        r_price = item.get("reservePrice",0.0)
        lv.append(str(valid_jsontxt(item_id)))
        lv.append(float_k(r_price))
        result.append(lv)
    return result
def f2(line):
    try:
        lis=line.split('\t')
        if len(lis)!=3: return None
        ts = lis[0]
        txt = lis[2]
        ob=json.loads(valid_jsontxt(txt))
        if type(ob) != type({}): return None
        itemInfoModel= ob.get('itemInfoModel',"-")
        if itemInfoModel == '-': return None
        item_id = valid_jsontxt(itemInfoModel.get('itemId','-'))
        if item_id == '-': return None
        # if len(ob['apiStack']['itemInfoModel']['priceUnits']) != 1: return None
        # if not ob['apiStack']['itemInfoModel']['priceUnits'][0].has_key("value")
        # price = valid_jsontxt(ob['apiStack']['itemInfoModel']['priceUnits'][0]['price'])
        value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
        price = value[0]
        return (item_id,[price,'i'])
    except Exception,e:
        print valid_jsontxt(line)
        return None


def quchong(x, y):
    y = y[0]
        # except Exception,e:
        #     # print valid_jsontxt(ln[8])
        #     x = "000000"
        #     y = ln
        #     # y = item_list[0]
        #     break
    return (x, y)

def bidui(x,y):
    if len(y) == 2:
        if y[0][0] != y[1][0]:
            if y[0][1] == 'r':
                return str(x) + "\001" + str(y[0][0]) + "\001" + str(y[1][0])
            else:
                return str(x) + "\001" + str(y[1][0]) + "\001" + str(y[0][0])
        else:
            return "hehe"
    else:
        return  None

s1 = "/commit/shopitem2/20160225"
s2 = "/commit/iteminfo/20160225"

rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x!=None).map(lambda x:(x[0],[x[1],'r']))
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y))
rdd2_c = sc.textFile(s2).map(lambda x: f2(x)).filter(lambda x:x!=None)
rdd2 = rdd2_c.groupByKey().mapValues(list).map(lambda (x,y):quchong(x, y))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y): bidui(x, y)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/check_sale_price_3')

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 check_sale_price.py