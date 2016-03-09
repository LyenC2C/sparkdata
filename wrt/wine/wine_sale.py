#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="wine_sale")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def set_null(x):
    if x.strip() == "":
        return "-"
def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    item_id = valid_jsontxt(ob.get("auctionId","-"))
    if item_id == "-" or item_id.strip() == "": return None
    title = set_null(valid_jsontxt(ob.get("title","-")))
    sold = set_null(valid_jsontxt(ob.get("sold","-")))
    reservePrice = set_null(valid_jsontxt(ob.get("reservePrice","-")))
    shop_id = set_null(valid_jsontxt(ob.get("shop_id","-")))
    salePrice = set_null(valid_jsontxt(ob.get("salePrice","-")))
    if salePrice == "-" or sold == "-":
        month_sales = "-"
    else:
        try:
            month_sales = valid_jsontxt(str(float(salePrice) * float(sold)))
        except Exception,e:
            print "hahaha" + ss[0]
            return None
    # result = []
    return ((item_id,[title,sold,reservePrice,salePrice,month_sales,shop_id]))
    # picUrl = ob.get("")
def quchong(x, y):
    # max = 0
    # item_list = y
    # for ln in item_list:
    #   try:
        # if int(ln[8]) > max:
        #         max = int(ln[8])
        # y = ln
        # except Exception,e:
        #     # print valid_jsontxt(ln[8])
        #     x = "000000"
        #     y = ln
        #     # y = item_list[0]
        #     break
    # flag = '0'
    # y.append(flag)
    result = [x] + y[0]
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)

rdd_c = sc.textFile("/commit/project/wine/wine_shopid.0309.shopitem.2016-03-09").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x, y))
rdd.saveAsTextFile('/user/wrt/wine_sale_tmp')

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 wine_sale.py

