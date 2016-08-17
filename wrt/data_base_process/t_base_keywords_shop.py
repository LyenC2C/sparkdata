#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="t_base_kewords_shop")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(x):
    text = line.strip()
    ob = json.loads(valid_jsontxt(text))
    shop = ob.get("shop",{})
    shop_id = shop.get("id","-")
    if shop_id == "-": return None
    isMall = shop.get("isMall",False)
    if isMall: bc_type = 'B'
    else: bc_type = 'C'
    totalSold = shop.get("totalSold","0")
    # return shop_id + "\001" + bc_type + "\001" + totalSold
    return (shop_id,(shop_id,bc_type,totalSold))

# def quchong(x,y):
#     return "\001".join(y[0])


sc = "/commit/tb_shop_search.160714.clean.all.sq"
rdd_c = sc.textFile(sc).map(lambda x:f(x)).filter(lambda x:x != None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):"\001".join(y[0]))
rdd.saveAsTextFile('/user/wrt/keywords_shop_tmp')

#hfs -rmr /user/wrt/keywords_shop_tmp
#spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 t_base_keywordsd_shop.py
