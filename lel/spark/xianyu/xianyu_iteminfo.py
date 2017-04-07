# coding=utf-8
import rapidjson as json
import sys
from urlparse import urlparse
from operator import itemgetter
from pyspark import SparkContext

lastday = sys.argv[1]

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def getJson(s):
    content = s.strip().split('\t')
    if len(content) == 3:
        start = content[2].find("({") + 1
        js = content[2][start:-1]
        return (content[0], content[1], json.loads(valid_jsontxt(js)))
    else:
        return None

def parseJson(ob):
    if ob is None:return None
    result = []
    ts = ob[0]
    itemid = ob[1]
    if not isinstance(ob[2],dict): return None
    item = ob[2].get("data", {}).get("item", {})
    if item.get("id") == '':return None
    area = item.get("area", "\\N")
    phone = item.get("phone", "\\N")
    contacts = item.get("contacts", "\\N")
    postPrice = item.get("postPrice", 0.0)
    auctionType = item.get("auctionType", "\\N")
    categoryId = item.get("categoryId", "\\N")
    province = item.get("province", "\\N")
    city = item.get("city", "\\N")
    description = item.get("description", "\\N")
    detailFrom = item.get("detailFrom", "\\N")
    favorNum = item.get("favorNum", 0)
    firstModified = item.get("firstModified", "\\N")
    firstModifiedDiff = item.get("firstModifiedDiff", "\\N")
    t_from = item.get("from", "\\N")
    gps = item.get("gps", "\\N")
    commentNum = item.get('commentNum', 0)
    offline = item.get("offline", "\\N")
    originalPrice = item.get("originalPrice", 0.0)
    price = item.get("price", 0.0)
    title = item.get("title", "\\N")
    userNick = item.get("userNick", "\\N")
    categoryName = item.get("categoryName", "\\N")
    fishPoolId = item.get("fishPoolId", "\\N")
    fishpoolName = item.get("fishpoolName", "\\N")
    barDO = item.get("barDO", {})
    bar = barDO.get("bar", "\\N")
    if '.' in barDO.get("barInfo", "\\N"):
        barInfo = barDO.get("barInfo", "\\N").split('.', 2)[1]
    else:
        barInfo = barDO.get("barInfo", "\\N")
    xyAbbr = item.get("xianyuAbbr", {})
    abbr = xyAbbr.get("abbr", "\\N")
    officialTagList = xyAbbr.get("officialTagList", [])
    kv = {}
    validate = {}
    flag = '未'
    for officialTag in officialTagList:
        comment = valid_jsontxt(officialTag.get("comment", "\\N"))
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
        url = officialTag.get("link", "\\N")
        if '?' in url:
            params = urlparse(url).query
            for key_value in params.split('&'):
                key = key_value.split('=')[0]
                value = key_value.split('=')[1]
                kv.setdefault(key, value)
    userid = kv.get("userId", "\\N")
    result.append(itemid)
    result.append(userid)
    result.append(phone)
    result.append(contacts)
    result.append(title)
    result.append(province)
    result.append(city)
    result.append(area)
    result.append(auctionType)
    result.append(description)
    result.append(detailFrom)
    result.append(favorNum)
    result.append(commentNum)
    result.append(firstModified)
    result.append(firstModifiedDiff)
    result.append(t_from)
    result.append(gps)
    result.append(offline)
    result.append(originalPrice)
    result.append(price)
    result.append(postPrice)
    result.append(userNick)
    result.append(categoryId)
    result.append(categoryName)
    result.append(fishPoolId)
    result.append(fishpoolName)
    result.append(bar)
    result.append(barInfo)
    result.append(abbr)
    result.append(validate.get("实人认证", "\\N"))
    result.append(validate.get("芝麻信用", "\\N"))
    result.append(ts)  # timestamp
    return (itemid, [valid_jsontxt(i) for i in result])

def distinct(list):
    return '\001'.join(max(list, key=itemgetter(-1)))
def distinct_2(list):
    return '\001'.join(list)
def distinct_3(list):
    return '\001'.join(list[0])

sc = SparkContext(appName="xianyu_iteminfo" + lastday)

data = sc.textFile("/commit/2taobao/iteminfo/*" + lastday + "/*")\
            .map(lambda a: parseJson(getJson(a)))\
                .filter(lambda x: x is not None)\
                    .groupByKey().mapValues(list)\
                        .map(lambda a: distinct(a[1]))\
                           .saveAsTextFile("/user/lel/temp/xianyu_iteminfo")
'''

data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .repartition(5) \
    .groupByKey().mapValues(list) \
    .map(lambda a: distinct(a[1])) \
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_groupby1")

data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .repartition(5) \
    .reduceByKey(lambda a,b:max([a,b], key=itemgetter(-1))) \
    .map(lambda (a,b): distinct_2(b)) \
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_reduce1")
'''
'''

def distinct_2(list):
    return '\001'.join(list)
def distinct_3(list):
    return '\001'.join(list[0])

data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .groupByKey().mapValues(list) \
    .map(lambda a: distinct(a[1])) \
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_groupby")
data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .reduceByKey(lambda a,b:max([a,b], key=itemgetter(-1)))\
    .map(lambda (a,b): distinct_2(b))\
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_reduce")

data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .reduceByKey(lambda a,b:max([a,b], key=itemgetter(-1)))\
    .map(lambda (a,b): distinct_2(b))\
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_reduce")

def createCombiner(x):
    return [x]
def mergeValue(xs, x):
    xs.append(x)
    return [max(xs, key=itemgetter(-1))]
def mergeCombiners(a, b):
    a.extend(b)
    return [max(a, key=itemgetter(-1))]
data = sc.textFile("/commit/2taobao/test_shuffle/*") \
    .map(lambda a: parseJson(getJson(a))) \
    .filter(lambda x: x is not None) \
    .combineByKey(lambda a: createCombiner(a),lambda a,b:mergeValue(a,b),lambda a,b:mergeCombiners(a,b))\
    .map(lambda (a,b): distinct_3(b)) \
    .saveAsTextFile("/user/lel/temp/xianyu_iteminfo_test_combine_4")

'''

