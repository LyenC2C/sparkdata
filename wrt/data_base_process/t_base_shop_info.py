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
    itemInfoModel = ob.get('itemInfoModel',"-")
    if itemInfoModel == "-": return None
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
    if not fansCount.isdigit(): fansCount = "0"
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
    if len(evaluateInfo) < 3: evaluateInfo =[{},{},{}]
    desc_score = evaluateInfo[0].get("score", '0.0').strip()
    desc_highGap = evaluateInfo[0].get("highGap", '0.0').strip()
    service_score = evaluateInfo[1].get("score", '0.0').strip()
    service_highGap = evaluateInfo[1].get("highGap", '0.0').strip()
    wuliu_score = evaluateInfo[2].get("score", '0.0').strip()
    wuliu_highGap = evaluateInfo[2].get("highGap", '0.0').strip()
    if not desc_score.replace(".","").isdigit(): desc_score = '0.0'
    if not service_score.replace(".","").isdigit(): service_score = '0.0'
    if not wuliu_score.replace(".","").isdigit(): wuliu_score = '0.0'
    if not desc_highGap.replace(".","").replace("-","").isdigit(): desc_highGap = '0.0'
    if not service_highGap.replace(".","").replace("-","").isdigit(): service_highGap = '0.0'
    if not wuliu_highGap.replace(".","").replace("-","").isdigit(): wuliu_highGap = '0.0'
    is_online = "0"
    shop_type = "-"
    shop_certifi = '-'
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
    list.append(item_count)
    list.append(fansCount)
    list.append(str(goodRatePercentage))
    list.append(weitaoId)
    list.append(desc_score)
    list.append(service_score)
    list.append(wuliu_score)
    list.append(location)
    list.append(ts)
    list.append(desc_highGap)
    list.append(service_highGap)
    list.append(wuliu_highGap)
    list.append(is_online)
    list.append(shop_type)
    list.append(shop_certifi)
    return (shopId, list)

def quchong(x,y):
    result = y[0]
    return "\001".join([str(valid_jsontxt(i)) for i in result])

s = "/hive/warehouse/wlbase_dev.db/t_base_ec_item_house/part*"
rdd_c = sc.textFile(s).map(lambda x: f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y):quchong(x,y))
rdd.saveAsTextFile('/user/wrt/temp/shopinfo_tmp')

#spark-submit  --executor-memory 18G  --driver-memory 18G  --total-executor-cores 240 t_base_shop_info.py
#LOAD DATA  INPATH '/user/wrt/temp/shopinfo_tmp_000' OVERWRITE INTO TABLE t_base_ec_shop_dev_new PARTITION (ds='20160613');