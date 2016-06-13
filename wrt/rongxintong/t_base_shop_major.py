#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext
import rapidjson as json

sc = SparkContext(appName="qqweibo_tags")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def f(line):
    ob = json.loads(valid_jsontxt(line))
    if type(ob) != type({}): return None
    shop_id = ob.get("shop_id","").strip()
    if shop_id == "": return None
    main_cat_name = ob.get("main_cat_name","-")
    return shop_id + "\001" + valid_jsontxt(main_cat_name)

rdd = sc.textFile("/commit/project/sichuanseller/sichuan.sellerid.info").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/temp/shop_major_tmp')