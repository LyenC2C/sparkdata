#codint:utf-8
__author__ = 'hadoop'

import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="t_korea_itemsale")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return

def f(line):
    st = line.find("(")
    ed = line.find(")")
    txt = line[st+1:ed].replace(",]","]")
    ob= json.loads(valid_jsontxt(txt))
    if type(ob) != type({}): return None
    auctions = ob.get["auctions","-"]
    if auctions == "-": return None
    result = []
    for auction in auctions:
        lv = []
        lv.append(auction.get("aid",'-'))
        lv.append(auction.get("amount",'-'))
        lv.append(auction.get("total",'-'))
        lv.append(auction.get("qu",'-'))
        lv.append(auction.get("st",'-'))
        lv.append(auction.get("inSale",'-'))
        lv.append(auction.get("start",'-'))
        result.append(lv)
    return result

def quchong(x,y):
    y = y[0]
    result = [x] + y
    return "\001".join(result)



s = "/commit/project/tmallint/sold.item.source.20160401"
rdd = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd_f = rdd.map(lambda (x,y):quchong(x,y))
rdd_f.saveAsTextFile('/user/wrt/temp/t_korea_itemsale')

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 t_korea_itemsale.py