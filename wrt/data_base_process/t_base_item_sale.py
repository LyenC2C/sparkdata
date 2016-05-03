#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import zlib
import base64
import time
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="t_base_item_sale")

yesterday = sys.argv[1]
today = sys.argv[2]
iteminfo_day = sys.argv[3]

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f1(line):
    ss = line.strip().split("\t",1)
    if len(ss) != 2: return [None]
    ts = ss[0]
    zhengwen = ss[1]
    # l = len(zhengwen)
    star = zhengwen.find("({") + 1
    end = zhengwen.rfind("})") + 1
    # end = l-2
    text = zhengwen[star:end]
    text2 = text.replace(",]","]")
    text3 = valid_jsontxt(text2)
    ob = json.loads(text3)
    if type(ob) !=  type({}):
        return [None]
    auctions = ob["auctions"]
    result = []
    for auction in auctions:
        lv = []
        item_id = auction["aid"]
        amount = auction["amount"]
        total = auction["total"]
        qu = auction["qu"]
        st = auction["st"]
        inSale = auction["inSale"]
        start = auction["start"]
        cp_flag = '2' #复制flag,初始值为2
        price = "-"
        lv.append(item_id)
        lv.append(price)
        lv.append(amount)
        lv.append(total)
        lv.append(qu)
        lv.append(st)
        lv.append(inSale)
        lv.append(start)
        lv.append(cp_flag)
        lv.append(ts)
        result.append(lv)
    return result

def f2(line):
    ss = line.strip().split('\001')
    # ss[9] = float(ss[9])
    item_id = ss[0]
    price = ss[9]
    # bc_type = valid_jsontxt(ss[8])
    lv = []
    lv.append(item_id)
    lv.append(price)
    #return [item_id,s_price,bc_type]
    # return lv
    return lv
def f3(line):
    ss = line.strip().split('\001')
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
    # flag = '2'
    # y.append(flag)
    return (x, y)

def quchong_2(x, y):
    item_list = y
    #lv = []
    if len(item_list) == 2:
        if len(item_list[0]) == 2:
            item_list[1][1] = item_list[0][1]
            y = item_list[1]
        if len(item_list[1]) == 2:
            item_list[0][1] = item_list[1][1]
            y = item_list[0]
        # result = [x] + y
        # lv = []
        # for ln in result:
        #     lv.append(str(valid_jsontxt(ln)))
        # return "\001".join(lv)
        #return str(x) + "\001" + str(len(item_list)) + '\001' + str(type(y)) + "\001" + str(len(lv))
        return (x, y)
        # return "\001".join(y)
    elif len(item_list[0]) > 2:
        y = item_list[0]
        # result = [x] + y
        # lv = []
        # for ln in result:
        #     lv.append(str(valid_jsontxt(ln)))
        # # return str(type(x)) + "\001" + str(len(item_list)) + "\001" + str(len(item_list[0]))
        # return "\001".join(lv)
        # return "\001".join(y)
        return (x, y)
    else:
        return None

def quchong_3(x, y):
    max = 0
    item_list = y
    if len(item_list) == 1:
        ln = item_list[0]
        if ln[8] == '2': ln[8] = '0'#flag
        else: ln[8] = '1'
        y = ln
    else:
        for ln in item_list:
            if int(ln[9]) > max:
                max = int(ln[9])
                y = ln
    result = y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)


s1 = "/commit/itemsold/" + today
s2 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_dev_new/ds=" + iteminfo_day
s3 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sold_dev/ds=" + yesterday

rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x))
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong_1(x, y))
rdd2 = sc.textFile(s2).map(lambda x: f2(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x))
rdd3 = sc.textFile(s3).map(lambda x: f3(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x))
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x, y): quchong_2(x, y)).filter(lambda x:x!=None)
rdd_final = rdd.union(rdd3).groupByKey().mapValues(list).map(lambda (x, y):quchong_3(x, y)).coalesce(200)
rdd_final.saveAsTextFile('/user/wrt/sale_tmp')

# spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 t_base_item_sale.py 20160424 20160425 20160424
#LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='20160424');