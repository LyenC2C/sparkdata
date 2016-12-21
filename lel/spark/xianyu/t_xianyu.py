# coding=utf-8
import json
import sys
from urlparse import urlparse
from operator import itemgetter
from pyspark import SparkContext

today = sys.argv[1]


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



def parseJson(ob):
    result = []
    ts = ob[0]
    id = ob[1]
    if type(ob[2]) != type({}): return None
    item = ob[2].get("data", {}).get("item", {})
    if item.get("id") == '':
        return None
    area = item.get("area", "-")
    auctionType = item.get("auctionType", "-")
    categoryId = item.get("categoryId", "-")
    province = item.get("province", "-")
    city = item.get("city", "-")
    description = item.get("description", "-")
    detailFrom = item.get("detailFrom", "-")
    favorNum = item.get("favorNum", "-")
    firstModified = item.get("firstModified", "-")
    firstModifiedDiff = item.get("firstModifiedDiff", "-")
    t_from = item.get("from", "-")
    gps = item.get("gps", "-")
    commentNum = item.get('commentNum', '0')
    offline = item.get("offline", "-")
    originalPrice = item.get("originalPrice", "-")
    price = item.get("price", "-")
    title = item.get("title", "-")
    userNick = item.get("userNick", "-")
    categoryName = item.get("categoryName", "-")
    fishPoolId = item.get("fishPoolId", "-")
    fishpoolName = item.get("fishpoolName", "-")
    barDO = item.get("barDO", {})
    bar = barDO.get("bar", "-")
    if '.' in barDO.get("barInfo", "-"):
        barInfo = barDO.get("barInfo", "-").split('.', 2)[1]
    else:
        barInfo = barDO.get("barInfo", "-")
    xyAbbr = item.get("xianyuAbbr", {})
    abbr = xyAbbr.get("abbr", "-")
    officialTagList = xyAbbr.get("officialTagList", [])
    kv = {}
    validate = {}
    flag = '未'
    for officialTag in officialTagList:
        comment = valid_jsontxt(officialTag.get("comment", "-"))
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
        url = officialTag.get("link", "-")
        if '?' in url:
            params = urlparse(url).query
            for key_value in params.split('&'):
                key = key_value.split('=')[0]
                value = key_value.split('=')[1]
                kv.setdefault(key, value)
    userId = kv.get("userId", "-")
    result.append(id)
    result.append(userId)
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
    result.append(userNick)
    result.append(categoryId)
    result.append(categoryName)
    result.append(fishPoolId)
    result.append(fishpoolName)
    result.append(bar)
    result.append(barInfo)
    result.append(abbr)
    result.append(validate.get("实人认证", "-"))
    result.append(validate.get("芝麻信用", "-"))
    result.append(ts)  # timestamp
    return (id, [valid_jsontxt(i) for i in result])


def distinct(list):
    return '\001'.join(max(list, key=itemgetter(-1)))


sc = SparkContext(appName="xianyu_iteminfo" + today)
data = sc.textFile("/commit/2taobao/iteminfo/*" + today + "/*")
data.map(lambda a: parseJson(getJson(a))).filter(lambda x: x != None).groupByKey().mapValues(list).map(
    lambda a: distinct(a[1])).saveAsTextFile("/user/lel/temp/xianyu_2016")
