__author__ = 'wrt'
import sys
from pyspark import SparkContext
sc = SparkContext(appName="spark tmall_item_info")

def get_id_dict(x):
    return ()


id_dict = sc.broadcast(sc.textFile("/user/wrt/tmallint.shop.item.2015-12-15.dec.itemid").map(lambda x:get_id_dict(x)))

