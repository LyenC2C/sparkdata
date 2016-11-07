#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ss = line.strip().split("\t")
    if len(ss) != 3: return None
    item_id = ss[1]
    txt = valid_jsontxt(ss[2])
    ob = json.loads(txt)
    if type(ob) != type({}): return None
    data = ob.get('data',"-")
    if data == "-": return None
    # itemInfoModel = data.get('itemInfoModel',"-")
    trackParams = data.get('trackParams',{})
    brandId = valid_jsontxt(trackParams.get('brandId','-'))
    if brandId == "-": return None
    return (item_id,brandId)


rdd = sc.textFile("/commit/credit/taobao/20161104.yichang.iteminfo.complete").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.groupByKey().mapValues(list).map(lambda (x,y):valid_jsontxt(x) + "\001" + valid_jsontxt(y[0]))\
    .saveAsTextFile("/user/wrt/temp/ppzs_itemid_brandid")

hfs -rmr /user/wrt/temp/ppzs_itemid_brandid
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80  ppzs_itemid_brandid.py