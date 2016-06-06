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


sc = SparkContext(appName="t_base_shop_info" )

# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         return content.encode("utf-8")
#     else:
#         return content
def decompress(out):
    decode = base64.b64decode(out)
    data = zlib.decompress(decode)
    return data

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","")

def f(line):
    ss = line.strip().split("\001")
    ts = ss[2]
    if (ss[5]) == "": return None
    text = decompress(ss[5])
    ss = text.strip().split("\t",2)
    if len(ss) != 3: return None
    ob = json.loads(ss[2])
    location = valid_jsontxt(itemInfoModel.get('location', '-'))
    seller = ob.get("seller",[])
    if seller == []: return None
    evaluateInfo = seller.get("evaluateInfo", [])
    shopId = seller.get("shopId", "-")
    seller_id = seller.get("userNumId", "-")
    seller_name = seller.get("nick", "-")
    credit = seller.get("creditLevel", "-")
    starts = seller.get("starts", "-")
    trackParams = ob['trackParams']
    BC_type = trackParams.get('BC_type', '-')
    item_count = '0'
    for item in seller.get('actionUnits', []):
        if item.has_key('track'):
            if item['track'] == 'Button-AllItem':
                item_count = item.get('value', '0')
    fansCount = seller.get("fansCount", "0")
    try:
        t = seller.get("goodRatePercentage", "0.0").replace('%', '')
        if (t.replace('.', '').isdigit()):
            goodRatePercentage = float(t)
        else:
            goodRatePercentage = 0.0
    except Exception, e:
        print e, line
        return None
    weitaoId = seller.get("weitaoId", "-")
    shopTitle = seller.get("shopTitle", "--")
    desc_score = evaluateInfo[0].get("score", '0.0')
    service_score = evaluateInfo[1].get("score", '0.0')
    wuliu_score = evaluateInfo[2].get("score", '0.0')
    star = '99'
    list = []
    list.append(shopId)
    list.append(seller_id)
    list.append(shopTitle)
    list.append(seller_name)
    if 'B' in BC_type:
        list.append(star)
    else:
        list.append('0')
    list.append(credit)
    list.append(starts)
    list.append(BC_type)
    list.append(int(item_count))
    list.append(int(fansCount))
    list.append(goodRatePercentage)
    list.append(weitaoId)
    list.append(float(desc_score))
    list.append(float(service_score))
    list.append(float(wuliu_score))
    list.append(location)
    list.append(ts)
    return (shopId, list)

def quchong(x,y):
    result = y[0]
    return "\001".join([str(valid_jsontxt(i)) for i in result])

s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_house/part*"
rdd_c = sc.textFile(s).map(lambda x: f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x,y))
rdd.saveAsTextFile('/user/wrt/temp/iteminfo_tmp')

#spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_shop_info.py