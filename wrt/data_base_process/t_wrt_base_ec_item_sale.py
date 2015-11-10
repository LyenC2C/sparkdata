__author__ = 'wrt'

#coding:utf-8
import sys
import rapidjson as json
import time
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
sc = SparkContext(appName="spark item_sale")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
	if type(content) == type(u""):
		return content.encode("utf-8")
    	else :
        		return content

def f(line):
    ss = line.strip().split('\t')
    zhengwen = ""
    ts = ss[0]
    for ln in ss[2:]:
        zhengwen += ln
    l = len(zhengwen)
    result = []
    ob = json.loads(valid_jsontxt(zhengwen[zhengwen.find("({") + 1:l-1]))
    if type(ob) == type(0.1):
        return [None]
    if not ob.has_key("data"):
        return [None]
    if not ob["data"].has_key("totalResults"):
        return [None]
    if not ob["data"].has_key("itemsArray"):
        return [None]
    if ob["data"]["totalResults"] == 0:
        return [None]
    itemsArray = ob["data"]["itemsArray"]
    shop_id = ob["data"].get("shopId","-")
    for item in itemsArray:
        lv = []
        item_id = item.get("auctionId","-")
        item_title = item.get("title","-")
        r_price = item.get("reservePrice","-")
        s_price = item.get("salePrice","-")
        bc_type = item.get("auctionType","-")
        quantity = item.get("quantity","-")
        total_sold = item.get("totalSoldQuantity","-")
        order_cost = item.get("orderCost","-")
        lv.append(item_id)
        lv.append(item_title)
        lv.append(r_price)
        lv.append(s_price)
        lv.append(bc_type)
        lv.append(quantity)
        lv.append(total_sold)
        lv.append(order_cost)
        lv.append(shop_id)
        lv.append(ts)
        result.append('\001'.join([valid_jsontxt(i) for i in lv]))
        #result.append(item_id + '\001' + r_price + '\001' + s_price + '\001' + bc_type + '\001' + quantity + '\001' + total_sold + '\001' + order_cost + '\001' + shop_id + '\001' + ts)
    return result
schema = StructType([
    StructField("item_id",StringType(), True),
	StructField("item_title",StringType(), True),
	StructField("r_price",FloatType(), True),
    StructField("s_price",FloatType(), True),
    StructField("bc_type",StringType(), True),
    StructField("quantity",IntegerType(), True),
    StructField("total_sold",IntegerType(), True),
    StructField("order_cost",IntegerType(), True),
    StructField("shop_id",StringType(), True),
    StructField("ts",StringType(), True)
	])

rdd = sc.textFile("/commit/shopitem/tmall.shop.2.item.2015-10-27").flatMap(lambda x:f(x))\
    .filter(lambda x:x!=None)
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.sql('use wlbase_dev')
hiveContext.registerDataFrameAsTable(df, 'data')
ds2 = '20151027'
hiveContext.sql('insert overwrite table t_base_ec_item_sale PARTITION(ds='+ds2+') select * from data')
		#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()