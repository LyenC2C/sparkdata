from pyspark import SparkContext
from operator import itemgetter

'''
product to wxc:

闲鱼用户最新用户id,返回 item_id\001user_id
每个userid对应最新的itemid
'''


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def splitAndMatch(data):
    lines = data.split('\001')
    itemid = valid_jsontxt(lines[0])
    userid = valid_jsontxt(lines[1])
    ts = valid_jsontxt(lines[31])
    if userid_value.has_key(valid_jsontxt(userid)):
        return (userid, (itemid, ts))
    else:
        return None


def distinct(datalist):
    return max(datalist, key=itemgetter(-1))


sc = SparkContext(appName="xianyu_iteminfo")
line = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_xianyu_iteminfo/ds=20170103/00*")
userid = sc.textFile("/user/lel/datas/20161223.zhima.userid")
broadcast = sc.broadcast(userid.map(lambda a: (valid_jsontxt(a), "")).collectAsMap())
userid_value = broadcast.value

line.map(lambda a: splitAndMatch(a))\
    .filter(lambda a: a != None)\
    .groupByKey()\
    .mapValues(lambda a: distinct(list(a)))\
    .map(lambda (a, b): a + '\001' + b[0])\
    .coalesce(1)\
    .saveAsTextFile("/user/lel/results/xianyu_itemid_userid_20170103")
