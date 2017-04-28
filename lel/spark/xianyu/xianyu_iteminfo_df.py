# coding=utf-8
import rapidjson as json
from urlparse import urlparse
from operator import itemgetter
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark import HiveContext
from pyspark.sql.types import *
import sys

lastday = sys.argv[1]
last_2day = sys.argv[2]

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8").replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")
    else:
        res = content
    return res


def transform_df_fileds(content):
    if isinstance(content, unicode):
        res = content.encode("utf-8").decode("latin-1").encode("iso-8859-1").decode("utf-8")
    elif isinstance(content, str):
        res = content.decode("latin-1").encode("iso-8859-1").decode("utf-8")
    else:
        res = content
    return res

def getJson(s):
    content = s.strip().split('\t')
    if len(content) == 3:
        start = content[2].find("({") + 1
        js = content[2][start:-1]
        return (content[0], content[1], json.loads(valid_jsontxt(js)))
    else:
        return None


def parseJson(ob):
    if ob is None: return None
    ts = ob[0]
    itemid = ob[1]
    if not isinstance(ob[2], dict): return None
    item = ob[2].get("data", {}).get("item", {})
    if item.get("id") == '': return None
    area = item.get("area", None)
    phone = item.get("phone", None)
    contacts = item.get("contacts", None)
    postPrice = item.get("postPrice", 0.0)
    auctionType = item.get("auctionType", None)
    categoryId = item.get("categoryId", None)
    province = item.get("province", None)
    city = item.get("city", None)
    description = item.get("description", None)
    detailFrom = item.get("detailFrom", None)
    favorNum = item.get("favorNum", 0)
    firstModified = item.get("firstModified", None)
    firstModifiedDiff = item.get("firstModifiedDiff", None)
    t_from = item.get("from", None)
    gps = item.get("gps", None)
    commentNum = item.get('commentNum', 0)
    offline = item.get("offline", None)
    originalPrice = item.get("originalPrice", 0.0)
    price = item.get("price", 0.0)
    title = item.get("title", None)
    userNick = item.get("userNick", None)
    categoryName = item.get("categoryName", None)
    fishPoolId = item.get("fishPoolId", None)
    fishpoolName = item.get("fishpoolName", None)
    barDO = item.get("barDO", {})
    bar = barDO.get("bar", None)
    if bar is not None:
        if '.' in barDO.get("barInfo", None):
            if barDO.get("barInfo", None) is not None:
                barInfo = barDO.get("barInfo", None).split('.', 2)[1]
            else:
                barInfo = None
        else:
            barInfo = barDO.get("barInfo", None)
    else:
        barInfo = None
    xyAbbr = item.get("xianyuAbbr", {})
    abbr = xyAbbr.get("abbr", None)
    officialTagList = xyAbbr.get("officialTagList", [])
    kv = {}
    validate = {}
    flag = '未'
    for officialTag in officialTagList:
        comment = valid_jsontxt(officialTag.get("comment", None))
        if '实人认证' in comment:
            if flag in comment:
                validate.setdefault("实人认证", "0")
            else:
                validate.setdefault("实人认证", "1")
        elif '芝麻信用' in comment:
            if flag in comment:
                validate.setdefault("芝麻信用", "0")
            else:
                validate.setdefault("芝麻信用", "1")
        url = officialTag.get("link", None)
        if '?' in url:
            params = urlparse(url).query
            for key_value in params.split('&'):
                key = key_value.split('=')[0]
                value = key_value.split('=')[1]
                kv.setdefault(key, value)
    userid = kv.get("userId", None)
    if not itemid.isdigit(): return None
    result = [valid_jsontxt(itemid), \
              valid_jsontxt(userid), \
              valid_jsontxt(phone), \
              valid_jsontxt(contacts), \
              valid_jsontxt(title), \
              valid_jsontxt(province), \
              valid_jsontxt(city), \
              valid_jsontxt(area), \
              valid_jsontxt(auctionType), \
              valid_jsontxt(description), \
              valid_jsontxt(detailFrom), \
              int(favorNum), \
              int(commentNum), \
              valid_jsontxt(firstModified), \
              valid_jsontxt(firstModifiedDiff), \
              valid_jsontxt(t_from), \
              valid_jsontxt(gps), \
              valid_jsontxt(offline), \
              float(originalPrice), \
              float(price), \
              float(postPrice), \
              valid_jsontxt(userNick), \
              valid_jsontxt(categoryId), \
              valid_jsontxt(categoryName), \
              valid_jsontxt(fishPoolId), \
              valid_jsontxt(fishpoolName), \
              valid_jsontxt(bar), \
              valid_jsontxt(barInfo), \
              valid_jsontxt(abbr), \
              valid_jsontxt(validate.get("实人认证", None)), \
              valid_jsontxt(validate.get("芝麻信用", None)), \
              ts]
    return (itemid, result)


sc = SparkContext(appName="xianyu_iteminfo")

data = sc.textFile("/commit/2taobao/iteminfo/*" + lastday + "/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .reduceByKey(lambda a, b: max([a, b], key=itemgetter(-1))) \
    .map(lambda (a, b): b) \
    .map(lambda a: [transform_df_fileds(i) for i in a])


schema = StructType([StructField('itemid', StringType(), True), \
                     StructField('userid', StringType(), True), \
                     StructField('phone', StringType(), True), \
                     StructField('contacts', StringType(), True), \
                     StructField('title', StringType(), True), \
                     StructField('province', StringType(), True), \
                     StructField('city', StringType(), True), \
                     StructField('area', StringType(), True), \
                     StructField('auctionType', StringType(), True), \
                     StructField('description', StringType(), True), \
                     StructField('detailFrom', StringType(), True), \
                     StructField('favorNum', IntegerType(), True), \
                     StructField('commentNum', IntegerType(), True), \
                     StructField('firstModified', StringType(), True), \
                     StructField('firstModifiedDiff', StringType(), True), \
                     StructField('t_from', StringType(), True), \
                     StructField('gps', StringType(), True), \
                     StructField('offline', StringType(), True), \
                     StructField('originalPrice', FloatType(), True), \
                     StructField('price', FloatType(), True), \
                     StructField('postPrice', FloatType(), True), \
                     StructField('userNick', StringType(), True), \
                     StructField('categoryId', StringType(), True), \
                     StructField('categoryName', StringType(), True), \
                     StructField('fishPoolId', StringType(), True), \
                     StructField('fishpoolName', StringType(), True), \
                     StructField('bar', StringType(), True), \
                     StructField('barInfo', StringType(), True), \
                     StructField('abbr', StringType(), True), \
                     StructField('shiren', StringType(), True), \
                     StructField('zhima', StringType(), True), \
                     StructField('ts', StringType(), True)
                     ])
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(data, schema)
df.registerTempTable("xianyu_iteminfo")
sqlContext.sql("insert OVERWRITE table  wl_base.`t_base_ec_xianyu_iteminfo_parquet` PARTITION(ds = '"+lastday+"') "
               "select "
               "case when t1.itemid is null then t2.itemid else t1.itemid end, "
               "case when t1.itemid is null then t2.userid else t1.userid end, "
               "case when t1.itemid is null then t2.phone else t1.phone end, "
               "case when t1.itemid is null then t2.contacts else t1.contacts end, "
               "case when t1.itemid is null then t2.title else t1.title end, "
               "case when t1.itemid is null then t2.province else t1.province end, "
               "case when t1.itemid is null then t2.city else t1.city end, "
               "case when t1.itemid is null then t2.area else t1.area end, "
               "case when t1.itemid is null then t2.auctionType else t1.auctionType end, "
               "case when t1.itemid is null then t2.description else t1.description end, "
               "case when t1.itemid is null then t2.detailFrom else t1.detailFrom end, "
               "case when t1.itemid is null then t2.favorNum else t1.favorNum end, "
               "case when t1.itemid is null then t2.commentNum else t1.commentNum end, "
               "case when t1.itemid is null then t2.firstModified else t1.firstModified end, "
               "case when t1.itemid is null then t2.firstModifiedDiff else t1.firstModifiedDiff end, "
               "case when t1.itemid is null then t2.t_from else t1.t_from end, "
               "case when t1.itemid is null then t2.gps else t1.gps end, "
               "case when t1.itemid is null then t2.offline else t1.offline end, "
               "case when t1.itemid is null then t2.originalPrice else t1.originalPrice end, "
               "case when t1.itemid is null then t2.price else t1.price end, "
               "case when t1.itemid is null then t2.postprice else t1.postprice end, "
               "case when t1.itemid is null then t2.userNick else t1.userNick end, "
               "case when t1.itemid is null then t2.categoryid else t1.categoryid end, "
               "case when t1.itemid is null then t2.categoryName else t1.categoryName end, "
               "case when t1.itemid is null then t2.fishPoolid else t1.fishPoolid end, "
               "case when t1.itemid is null then t2.fishpoolName else t1.fishpoolName end, "
               "case when t1.itemid is null then t2.bar else t1.bar end, "
               "case when t1.itemid is null then t2.barInfo else t1.barInfo end, "
               "case when t1.itemid is null then t2.abbr else t1.abbr end, "
               "case when t1.itemid is null then t2.zhima else t1.zhima end, "
               "case when t1.itemid is null then t2.shiren else t1.shiren end, "
               "case when t1.itemid is null then t2.ts else t1.ts end "
               "from "
               "(select * from  wl_base.`t_base_ec_xianyu_iteminfo_parquet` where ds = '"+last_2day+"')t1 "
               "full outer JOIN "
               "(select * from xianyu_iteminfo)t2 "
               "ON t1.itemid = t2.itemid")
