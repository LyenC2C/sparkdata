__author__ = 'wrt'

#coding:utf-8
import sys
import rapidjson as json
import time
from pyspark import SparkContext
yesterday = sys.argv[1]
today = sys.argv[2]
iteminfo_day = sys.argv[3]
sc = SparkContext(appName="spark item_sale_new_" + today)
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
# def get_bctype_dict(x):
#     ss = x.split('\001')
#     return (ss[0],ss[7]+'\001'+ss[])
def f1(line):
    try:
        ss = line.strip().split('\t',3)
        #zhengwen = ""
        ts = ss[0]
        # for ln in ss[3:]:
        #     zhengwen += ln
        # l = len(zhengwen)
        result = []
        zhengwen = ss[3]
        ob = json.loads(valid_jsontxt(zhengwen))
        # ob = json.loads(valid_jsontxt(zhengwen[zhengwen.find("({") + 1:l-1]))
        if type(ob) != type({}):
            return [None]
        # if not ob.has_key("data"):
        #     return [None]
        # if not ob["data"].has_key("totalResults"):
        #     return [None]
        if not ob.has_key("itemsArray"):
            return [None]
        if ob["totalResults"] == 0:
            return [None]
        itemsArray = ob["itemsArray"]
        shop_id = ob.get("shopId","-")
        #s_price = bctype_dict.get(shop_id,)
        #bc_type = bctype_dict.get(shop_id,"-")
        bc_type = "-"
        # s_price = 0.0
        order_cost = 0
        for item in itemsArray:
            lv = []
            item_id = item.get("auctionId","-")
            item_title = item.get("title","-")
            r_price = item.get("reservePrice",0.0)
            s_price = r_price
            #s_price = item.get("salePrice",0.0)
            quantity = item.get("quantity",0)
            total_sold = item.get("totalSoldQuantity",0)
            #order_cost = item.get("orderCost",0)
            #flag = "0"
            lv.append(str(valid_jsontxt(item_id)))
            lv.append(f_coding(item_title))
            lv.append(float_k(r_price))
            lv.append(float_k(s_price))
            lv.append(valid_jsontxt(bc_type))
            lv.append(int_k(quantity))
            lv.append(int_k(total_sold))
            lv.append(int_k(order_cost))
            lv.append(valid_jsontxt(shop_id))
            lv.append(int_k(ts))
            result.append(lv)
        return result
    except Exception,e:
        #print e,valid_jsontxt(line)
        return [None]

def f2(line):
    ss = line.strip().split('\001')
    # ss[9] = float(ss[9])
    item_id = ss[0]
    s_price = float(ss[9])
    bc_type = valid_jsontxt(ss[8])
    lv = []
    lv.append(item_id)
    lv.append(s_price)
    lv.append(bc_type)
    #return [item_id,s_price,bc_type]
    # return lv
    return lv

def f3(line):
    ss = line.strip().split('\001')
    ss[2] = float(ss[2])
    ss[3] = float(ss[3])
    ss[5] = int(ss[5])
    ss[6] = int(ss[6])
    ss[7] = int(ss[7])
    ss[10] = '0'
    if ss[4] != "B" and ss[4] != "C":
        ss[4] = '-'
    return ss

def quchong_1(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        # try:
        if int(ln[8]) > max:
                max = int(ln[8])
                y = ln
        # except Exception,e:
        #     # print valid_jsontxt(ln[8])
        #     x = "000000"
        #     y = ln
        #     # y = item_list[0]
        #     break
    flag = '0'
    y.append(flag)
    return (x, y)

def quchong_2(x, y):
    item_list = y
    #lv = []
    if len(item_list) == 2:
        if len(item_list[0]) == 2:
            item_list[1][2] = item_list[0][0]
            item_list[1][3] = item_list[0][1]
            y = item_list[1]
        if len(item_list[1]) == 2:
            item_list[0][2] = item_list[1][0]
            item_list[0][3] = item_list[1][1]
            y = item_list[0]
        # result = [x] + y
        # lv = []
        # for ln in result:
        #     lv.append(str(valid_jsontxt(ln)))
        # return "\001".join(lv)
        #return str(x) + "\001" + str(len(item_list)) + '\001' + str(type(y)) + "\001" + str(len(lv))
        return (x, y)
    elif len(item_list[0]) > 2:
        y = item_list[0]
        # result = [x] + y
        # lv = []
        # for ln in result:
        #     lv.append(str(valid_jsontxt(ln)))
        # # return str(type(x)) + "\001" + str(len(item_list)) + "\001" + str(len(item_list[0]))
        # return "\001".join(lv)
        return (x, y)
    else:
        return None

def quchong_3(x, y):
    max = 0
    item_list = y
    if len(item_list) == 1:
        ln = item_list[0]
        ln[9] = '1' #flag
        y = ln
    else:
        for ln in item_list:
            if int(ln[8]) > max:
                max = int(ln[8])
                y = ln
    result = [x] + y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)

#s = "/hive/warehouse/wlbase_dev.db/t_base_ec_shop_dev/ds=" + iteminfo_day #today's t_base_ec_shop_dev
s1 = "/commit/shopitem2/" + today #today
# s1 = "/commit/shopitem2/20160112/shop.item.crawler171.2016-01-12"
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=" + iteminfo_day #today's t_base_ec_item_dev
s3 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + yesterday #yesterday

# bctype_dict = sc.broadcast(sc.textFile(s).map(lambda x: get_bctype_dict(x)).filter(lambda x:x!=None).collectAsMap())
rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong_1(x, y))
rdd2 = sc.textFile(s2).map(lambda x: f2(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd3 = sc.textFile(s3).map(lambda x: f3(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y): quchong_2(x, y)).filter(lambda x:x!=None)
# rdd.saveAsTextFile('/user/wrt/sale_tmp')
rdd_final = rdd.union(rdd3).groupByKey().mapValues(list).map(lambda (x, y):quchong_3(x, y)).coalesce(200)
rdd_final.saveAsTextFile('/user/wrt/sale_tmp')
#st = s.find('2015')
#ds2 = s[st:st+4] + s[st+5:st+7] + s[st+8:st+10]
#l = len(s1)
#ds1 = s1[l-8:]
#.saveAsTextFile("/user/wrt/item_sale")
sc.stop()
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 t_wrt_base_ec_item_sale.py