#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
import time
import rapidjson as json

sc = SparkContext(appName="cmt")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def decode_props_map(item_info_json):
    props = item_info_json['skuModel']['skuProps']
    props_value_list = []
    for prop in props:
        props_id = prop['propId']
        props_name = prop['propName']
        values = prop['values']
        for value in values:
            value_id = value['valueId']
            value_name = value['name']
            props_value_list.append((props_id,props_name,value_id,value_name))


rdd=sc.textFile('/hive/warehouse/wlbase_dev.db/t_base_ec_item_house').map(lambda x:x.split('\001')[-1])

import zlib
import base64
import time

def compress(data):
    compressed = zlib.compress(data)
    out = base64.b64encode(compressed)
    return out


def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data


def  parse_try(info):
    ts=decompress(info).split('\t')[-1]
    if 'skuProps' not in ts:return None
    x=json.loads(ts)
    if type(x)==type(0.0):return None
    try:
        return decode_props_map(x)
    except:return None

rs=rdd.filter(lambda x:len(x)>0).map(lambda x:parse_try(x))
rs.filter(lambda x:x is not None).map(lambda x:'\t'.join(x)).saveAsTextFile('/user/zlj/tmp/skuinfo')