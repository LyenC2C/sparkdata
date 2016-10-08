#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import time
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="repair_item_info")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def change_fmt(jsn):
    try:
        dat=jsn.get('data',{})
        api=dat.get('apiStack',[])
        vl=api[0].get('value','')
        jsnld=json.loads(vl)
        api_dat=jsnld.get('data',{})
        dat['apiStack']=api_dat
        return dat
    except Exception, e:
        pass
    try:
        ret=jsn.get('ret',[])
        ret_in=str(ret[0])
        if 'ERRCODE_QUERY_DETAIL_FAIL' in ret_in:
            jsn['ret']=str(ret_in)
        return jsn
    except Exception, e:
        print e
        return False

def f(line):
    sp = line.split('\t')
    jsn = json.loads(valid_jsontxt(sp[2]))
    new_jsn = change_fmt(jsn)
    if new_jsn:
        jsndp = json.dumps(new_jsn)
        newsave = valid_jsontxt(sp[0]) + '\t' + valid_jsontxt(sp[1]) + '\t' + valid_jsontxt(jsndp)
        return newsave
    else:
        return None

# today = sys.argv[1]
s1 = "/commit/iteminfo/201610*" #+ today
rdd = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x != None)
rdd.saveAsTextFile('/user/wrt/temp/repair_iteminfo_10_tmp')

# hfs -rmr /user/wrt/temp/repair_iteminfo_tmp
# spark-submit  --executor-memory 10G  --driver-memory 8G  --total-executor-cores 120  repair_iteminfo_new_old.py