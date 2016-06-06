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
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.akka.frameSize","70")
sc = SparkContext(appName="outdoor_sold_now",conf = conf)



def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f1(line):
    ss = line.strip().split("\t",1)
    if len(ss) != 2: return [None]
    zhengwen = ss[1]
    # if zhengwen == "<!DOCTYPE html>": return [None]
    # l = len(zhengwen)
    star = zhengwen.find("({")# + 1
    if star == -1: return [None]
    else: star += 1
    end = zhengwen.rfind("})") + 1
    # end = l-2
    text = zhengwen[star:end]
    text2 = text.replace(",]","]")
    text3 = valid_jsontxt(text2)
    ob = json.loads(text3)
    if type(ob) !=  type({}):
        return [None]
    if not ob.has_key("auctions"): return [None]
    auctions = ob["auctions"]
    result = []
    for auction in auctions:
        lv = []
        item_id = auction["aid"]
        total = auction["total"]
        lv.append(item_id)
        lv.append(total)
        result.append(lv)
    return result


rdd = sc.textFile('/commit/project/lzhoutdoor/lzh.outdoorid.amount').flatMap(lambda x:f1(x)).filter(lambda x:x!=None)\
    .map(lambda x:"\t".join(x))
rdd.saveAsTextFile('/user/wrt/outdoor_sold_now')

"""
hfs -rmr /user/wrt/sale_tmp
spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 t_base_item_sale.py 20160428 20160429 20160424
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='20160428');
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE wlservice.t_wrt_sold_tmp PARTITION (ds='20160428');
insert into table t_base_ec_item_sold_dev PARTITION (ds='20160428')
select
t2.item_id,
t2.price,
t2.amount,
t2.total,
t2.qu,
t2.st,
t2.insale,
t2.start,
t2.cp_flag,
t2.ts
from
(select * from t_base_ec_item_sold_dev where ds = 20160427)t1
join
(select * from wlservice.t_wrt_sold_tmp where ds = 20160428)t2
on
t1.item_id = t2.item_id
"""