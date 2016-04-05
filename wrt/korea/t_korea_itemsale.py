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
    txt = valid_jsontxt(line[st+1:ed]).replace(",]","]")
    ob= json.loads(txt)
    # if type(ob) != type({}): return [None]
    auctions = ob.get("auctions")
    # if auctions == "-": return [None]
    result = []
    for auction in auctions:
        lv = []
        lv.append(valid_jsontxt(auction.get("aid",'-')))
        lv.append(valid_jsontxt(auction.get("amount",'-')))
        lv.append(valid_jsontxt(auction.get("total",'-')))
        lv.append(valid_jsontxt(auction.get("qu",'-')))
        lv.append(valid_jsontxt(auction.get("st",'-')))
        lv.append(valid_jsontxt(auction.get("inSale",'-')))
        lv.append(valid_jsontxt(auction.get("start",'-')))
        # return "\001".join(lv)
        # result.append("\001".join(lv))
        result.append(lv)
    return result

def quchong(x,y):
    result = [x] + y[0]
    lv = []
    # lv.append(x)
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)



s = "/commit/project/tmallint/sold.item.source.20160401"
rdd = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x[1:]))
rdd_f = rdd.groupByKey().mapValues(list).map(lambda (x,y):quchong(x,y))
rdd_f.saveAsTextFile('/user/wrt/temp/t_korea_itemsale')

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 t_korea_itemsale.py