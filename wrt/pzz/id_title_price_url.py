#coding:utf-8
__author__ = 'wrt'
import sys
import copy
import math
import zlib
import base64
import time
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="id_title_price_url")

def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=""
        if '-' in tmp:     v=tmp.split('-')[0]
        else :             v=tmp
        if v.replace('.',"").isdigit():
            v = float(v)
        else:
            v = 0.0
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         return content.encode("utf-8")
#     else:
#         return content

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "").replace("\\","")

def f(line):
    ss = line.strip().split("\001")
    item_id = ss[0]
    # is_online = ss[1] #0没有1上架2下架
    # ts = ss[2]
    # data_flag = ss[3] #历史状态，0没有1上架2下架（当天新商品没有的时候，他保持不变）
    # data_ts = ss[4] #历史状态时间
    if (ss[5]) == "": return None
    line = decompress(ss[5])
    ss = line.strip().split('\t',2)
    ob = json.loads(ss[2])
    result = []
    # item_id = valid_jsontxt(ss[1])
    title = ob.get("itemInfoModel").get("title")
    value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
    price = value[0]
    picurl_list = ob.get("itemInfoModel",{}).get("picsPath",[])
    if type(picurl_list) != type([]): picurl_y = "-"
    elif len(picurl_list) == 0: picurl_y = "-"
    else: picurl_y = picurl_list[0]
    picurl = picurl_y.replace("img.alicdn.com","gw.alicdn.com")
    result.append(item_id)
    result.append(title)
    result.append(price)
    result.append(picurl)
    return "\001".join([str(valid_jsontxt(i)) for i in result])


s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_house/part*"
rdd = sc.textFile(s).map(lambda x: f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/id_title_price_url_new_0422')

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 id_title_price_url.py
