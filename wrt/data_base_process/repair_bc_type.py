__author__ = 'hadoop'
#coding:utf-8
import sys
from pyspark import SparkContext
sc = SparkContext(appName="spark repair_bc_type")
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def get_bctype_dict(x):
    ss = x.split('\001')
    return (ss[0],ss[7])
def f1(bctype_dict, x):
    ss = x.split('\001')
    shop_id = ss[8]
    ss[4] = "0"
    if bctype_dict.has_key(shop_id):
        ss[4] = bctype_dict[shop_id]
    return "\001".join(ss)
def f2(bctype_dict, x):
    ss = x.split('\001')
    shop_id = ss[0]
    ss[14] = "0"
    if bctype_dict.has_key(shop_id):
        ss[14] = bctype_dict[shop_id]
    return "\001".join(ss)
ds = sys.argv[1]
s = "/hive/warehouse/wlbase_dev.db/t_base_ec_shop_dev/ds=20151215"
s1 = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=" + ds #today
s2 = "/hive/warehouse/wlbase_dev.db/t_zlj_base_ec_item_sale_dev_day/ds=" + ds #today
bctype_dict = sc.broadcast(sc.textFile(s).map(lambda x: get_bctype_dict(x)).collectAsMap())
rdd1 = sc.textFile(s1).map(lambda x:f1(bctype_dict.value, x))
rdd2 = sc.textFile(s2).map(lambda x:f2(bctype_dict.value, x))
rdd1.saveAsTextFile("/hive/warehouse/wlbase_dev.db/t_base_ec_item_sale_dev/ds=1111")
rdd2.saveAsTextFile("/hive/warehouse/wlbase_dev.db/t_zlj_base_ec_item_sale_dev_day/ds=1111")
sc.stop()