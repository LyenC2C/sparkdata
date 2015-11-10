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
    else:
        return content
def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x
def int_k(x):
    if x == "":
        return 0
    else:
        return int(x)
def float_k(x):
    if x == "":
        return 0.0
    else:
        return float(x)
def f(line):
    ss = line.strip().split('\t')
    zhengwen = ""
    ts = ss[0]
    for ln in ss[3:]:
        zhengwen += ln
    l = len(zhengwen)
    result = []
    ob = json.loads(valid_jsontxt(zhengwen[zhengwen.find("({") + 1:l-1]))
    if type(ob) != type({}):
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
        r_price = item.get("reservePrice",0.0)
        s_price = item.get("salePrice",0.0)
        bc_type = item.get("auctionType","-")
        quantity = item.get("quantity",0)
        total_sold = item.get("totalSoldQuantity",0)
        order_cost = item.get("orderCost",0)
        lv.append(valid_jsontxt(item_id))
        lv.append(f_coding(item_title))
        lv.append(float_k(r_price))
        lv.append(float_k(s_price))
        lv.append(valid_jsontxt(bc_type))
        lv.append(0)
        lv.append(0)
        lv.append(0)
        lv.append(valid_jsontxt(shop_id))
        lv.append(ts)
        result.append(lv)
        # result.append('\001'.join([valid_jsontxt(i) for i in lv]))
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

hiveContext.sql('use wlbase_dev')
s = sys.argv[1]
rdd = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None)
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.registerDataFrameAsTable(df, 'data')
st = s.find('2015')
ds2 = s[st:st+4] + s[st+5:st+7] + s[st+8:st+10]
hiveContext.sql('insert overwrite table t_base_ec_item_sale_dev PARTITION(ds=' + ds2 + ') select * from data')
		#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()