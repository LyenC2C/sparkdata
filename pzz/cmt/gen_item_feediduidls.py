#coding:utf-8
import sys
from pyspark import *

def clean(x):
    ls = x.strip().split("\001")
    itemid = ls[0]
    feedid = ls[2]
    uid = ls[3]
    try:
        int(itemid)
        int(feedid)
        int(uid)
    except:
        return None
    if len(itemid) <= 5 or len(feedid) <= 5 or  len(uid) <= 1:
        return None
    return [itemid,[feedid,uid]]

def uniq_gen(x,y):
    feediddic = {}
    feedls = []
    for each in y:
        [feedid,uid] = each
        if feediddic.has_key(feedid):
            pass
        else:
            feediddic[feedid] = None
            feedls.append(feedid+'\003'+uid)
    return x+'\001'+'\002'.join(feedls)


if __name__ == '__main__':
    tilldate=sys.argv[1] #till date
    conf = SparkConf()
    conf.set("spark.network.timeout","2000s")
    conf.set("spark.akka.timeout","1000s")
    conf.set("spark.akka.frameSize","1000")
    sc = SparkContext(appName="gen itemid feediduidls "+tilldate,conf=conf)
    rdd = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev_new/ds=*/*")
    rdd1 = rdd.map(lambda x:clean(x))\
        .filter(lambda x:x!=None)\
        .groupByKey()\
        .map(lambda (x,y):uniq_gen(x,y))
    rdd1.saveAsTextFile("/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls"+tilldate)