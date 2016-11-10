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

def f(line):
    text = line.strip()
    ob = json.loads(valid_jsontxt(text))
    shop = ob.get("shop",{})
    shop_id = valid_jsontxt(shop.get("id","-"))
    if shop_id == "-": return None
    isMall = shop.get("isMall",False)
    if isMall: bc_type = 'B'
    else: bc_type = 'C'
    bc_type = valid_jsontxt(bc_type)
    totalSold = valid_jsontxt(shop.get("totalSold","0"))
    # return shop_id + "\001" + bc_type + "\001" + totalSold
    return (shop_id,(shop_id,bc_type,totalSold))

# def quchong(x,y):
#     return "\001".join(y[0])


s1 = "/commit/tb_shop_search.160714.clean.all.sq"
rdd_c = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x != None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):"\001".join(y[0]))
rdd.saveAsTextFile('/user/wrt/keywords_shop_tmp')

#hfs -rmr /user/wrt/keywords_shop_tmp
#spark-submit  --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120 t_base_keywords_shop.py
#LOAD DATA  INPATH '/user/wrt/keywords_shop_tmp' OVERWRITE INTO TABLE t_base_keywords_shop PARTITION (ds='20160714');
#
