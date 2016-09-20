#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="t_znk_record")

now_day = sys.argv[1]

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    line = valid_jsontxt(line)
    ls = line.strip().split("\t")
    if len(ls) != 5:
        return None
    json_txt = ls[4][10:-1]
    ob = json.loads(json_txt)
    if type(ob) != type({}): return [None]
    rateList = ob.get("rateDetail",{}).get("rateList",[])
    if rateList == []:return [None]
    result = []
    for rate in rateList:
        # lv = []
        # lv.append("1")
        # try:
        lv = []
        tradeEndTime = float(str(rate['tradeEndTime'])[-3:])
        tradetime = time.strftime("%Y%m%d" ,time.gmtime(tradeEndTime))
        feed_id = rate['id']
        lv.append(valid_jsontxt(feed_id))
        lv.append(valid_jsontxt(tradetime))
        result.append((feed_id,lv))
        # except:
        #     continue
    return result


s = "/commit/tb_tmp/order_time/" + now_day
rdd = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None)\
    .groupByKey().mapValues(list).map(lambda (x,y):"\001".join(y[0]))
rdd.saveAsTextFile('/user/wrt/temp/znk_ordertime_tmp')

#hfs -rmr /user/wrt/temp/znk_ordertime_tmp
#spark-submit  --executor-memory 4G  --driver-memory 5G  --total-executor-cores 80  t_wrt_znk_ordertime.py 20160920