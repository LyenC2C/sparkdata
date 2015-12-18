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
def get_bctype_dict(x):
    ss = x.split('\001')
    return (ss[0],ss[7])
def f1(bctype_dict, line):
    try:
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
        bc_type = bctype_dict.get(shop_id,"-")
        for item in itemsArray:
            lv = []
            item_id = item.get("auctionId","-")
            item_title = item.get("title","-")
            r_price = item.get("reservePrice",0.0)
            s_price = item.get("salePrice",0.0)
            quantity = item.get("quantity",0)
            total_sold = item.get("totalSoldQuantity",0)
            order_cost = item.get("orderCost",0)
            #flag = "0"
            lv.append(valid_jsontxt(item_id))
            lv.append(f_coding(item_title))
            lv.append(float_k(r_price))
            lv.append(float_k(s_price))
            lv.append(valid_jsontxt(bc_type))
            lv.append(int_k(quantity))
            lv.append(int_k(total_sold))
            lv.append(int_k(order_cost))
            lv.append(valid_jsontxt(shop_id))
            lv.append(ts)
            result.append(lv)
        return result
    except Exception,e:
		print e,valid_jsontxt(line)
		return [None]
def f2(line):
    ss = line.strip().split('\001')
    ss[2] = float(ss[2])
    ss[3] = float(ss[3])
    ss[5] = int(ss[5])
    ss[6] = int(ss[6])
    ss[7] = int(ss[7])
    ss[10] = '0'
    if ss[4] != "B" or ss[4] !="C":
        ss[4] = '-'
    return ss
def quchong_1(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        if int(ln[8]) > max:
            max = int(ln[8])
            y = ln
    flag = '0'
    y.append(flag)
    return (x, y)
def quchong_2(x, y):
    max = 0
    item_list = y
    if len(item_list) == 1:
        ln = item_list[0]
        ln[9] = '1'
        y = ln
    else:
        for ln in item_list:
            if int(ln[8]) > max:
                max = int(ln[8])
                y = ln
    return [x] + y

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
    StructField("ts",StringType(), True),
    StructField("flag",StringType(),True)
	])

hiveContext.sql('use wlbase_dev')
ds = sys.argv[1]
s = "/hive/warehouse/wlbase_dev.db/t_base_ec_shop_dev/ds=20151217" #+ ds #today's t_base_ec_shop_dev
s1 = "/commit/shopitem/" + ds #today
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + sys.argv[2] #yesterday
bctype_dict = sc.broadcast(sc.textFile(s).map(lambda x: get_bctype_dict(x)).filter(lambda x:x!=None).collectAsMap())
rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(bctype_dict.value, x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong_1(x, y))
rdd2 = sc.textFile(s2).map(lambda x:f2(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y):quchong_2(x, y))
df = hiveContext.createDataFrame(rdd, schema)
hiveContext.registerDataFrameAsTable(df, 'data')
#st = s.find('2015')
#ds2 = s[st:st+4] + s[st+5:st+7] + s[st+8:st+10]
#l = len(s1)
#ds1 = s1[l-8:]
hiveContext.sql('insert overwrite table t_base_ec_item_sale_dev PARTITION(ds=' + ds + ') select * from data')
#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()
#spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80 t_wrt_base_ec_item_sale.py