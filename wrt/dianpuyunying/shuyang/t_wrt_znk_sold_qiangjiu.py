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

sc = SparkContext(appName="znk_aid")

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
    star = zhengwen.find("({")# + 1
    if star == -1: return [None]
    else: star += 1
    end = zhengwen.rfind("})") + 1
    text = zhengwen[star:end]
    text2 = text.replace(",]","]")
    text3 = valid_jsontxt(text2)
    if text3 == '': return [None]
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
        lv.append(ts)
        result.append(lv)
    return result

def quchong_1(x, y):
    max = 0
    item_list = y
    for ln in item_list:
        # try:
        if int(ln[-1]) > max:
                max = int(ln[-1])
                y = ln
    return "\001".join(valid_jsontxt(i) for i in y)

s1 = "/commit/itemsold/20161113/10.2.4.175_002590aded80.znk.2016-11-13.complete"
rdd1_c = sc.textFile(s1).flatMap(lambda x:f1(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x))
rdd1 = rdd1_c.groupByKey().mapValues(list).map(lambda (x, y):quchong_1(x, y))
rdd1.saveAsTextFile("/user/wrt/temp/znk_sold_aid")

#hfs -rmr /user/wrt/temp/znk_sold_aid
#spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 t_wrt_znk_sold_qiangjiu.py

# drop table wlservice.t_wrt_znk_sold_aid;

# create table wlservice.t_wrt_znk_sold_aid
# (
# item_id string,
# sold string,
# ts string
# )
# COMMENT '销量抢救表'
# ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
# stored as textfile ;
#LOAD DATA  INPATH '/user/wrt/temp/znk_sold_aid' OVERWRITE INTO TABLE t_wrt_znk_sold_aid;


# insert overwrite table t_wrt_znk_development_data partition(ds = 20161114)
# select
# t1.feed_id,
# t1.user_id,
# t1.item_id,
# t1.dsn,
# t1.title,
# t1.brand_name,
# t1.item_size,
# t1.item_type,
# t1.item_count,
# t1.price,
# t1.pic_url,
# case when t2.sold is null then t1.sold else t2.sold end as sold
# from
# (select * from t_wrt_znk_development_data where ds = 20161113)t1
# left join
# t_wrt_znk_sold_aid t2
# on
# t1.item_id = t2.item_id