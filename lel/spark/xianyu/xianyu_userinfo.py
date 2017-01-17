# coding=utf-8
__author__ = 'lel'

from pyspark import SparkContext
import rapidjson as json
from operator import itemgetter

sc = SparkContext(appName="t_xianyu_userinfo")


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
        if start == -1: return None
        js = content[2][start:-2]
        ts = content[0]
        return (valid_jsontxt(ts), json.loads(valid_jsontxt(js)))
    else:
        return None


def parseJson(data):
    if data is None: return None
    result = []
    ts = data[0]
    ob = data[1]
    if not isinstance(ob,dict): return None
    idleItemSearch = ob.get("idleItemSearch@2", {}).get("data", {})
    totalCount = idleItemSearch.get("totalCount", "\\N")
    userPersonalInfo = ob.get("userPersonalInfo@1", {}).get("data", {})
    if not userPersonalInfo: return None
    userId = userPersonalInfo.get("userId", "\\N")
    if userId == "\\N": return None
    gender = userPersonalInfo.get("gender", "\\N")
    idleUserId = userPersonalInfo.get("idleUserId", "\\N")
    nick = userPersonalInfo.get("nick", "\\N")  # 闲鱼nick
    tradeCount = userPersonalInfo.get("tradeCount", 0)
    tradeIncome = userPersonalInfo.get("tradeIncome", 0.0)
    userNick = userPersonalInfo.get("userNick", "\\N")  # 淘宝nick
    constellation = userPersonalInfo.get("constellation", "\\N")
    birthday = userPersonalInfo.get("birthday", "\\N")
    city = userPersonalInfo.get("city", "\\N")
    infoPercent = userPersonalInfo.get("infoPercent", "\\N")
    signature = userPersonalInfo.get("signature", "\\N")
    constellation = '\\N' if len(constellation) < 1 else constellation
    birthday = '\\N' if len(birthday) < 1 else birthday
    city = '\\N' if len(city) < 1 else city
    result.append(valid_jsontxt(userId))
    result.append(valid_jsontxt(totalCount))
    result.append(valid_jsontxt(gender))
    result.append(valid_jsontxt(idleUserId))
    result.append(valid_jsontxt(nick))
    result.append(valid_jsontxt(tradeCount))
    result.append(valid_jsontxt(tradeIncome))
    result.append(valid_jsontxt(userNick))
    result.append(valid_jsontxt(constellation))
    result.append(valid_jsontxt(birthday))
    result.append(valid_jsontxt(city))
    result.append(valid_jsontxt(infoPercent))
    result.append(valid_jsontxt(signature))
    result.append(valid_jsontxt(ts))
    return (userNick, result)


def distinct(list):
    return '\001'.join(max(list, key=itemgetter(-1)))


path = "/commit/2taobao/userinfo_by_nick/*/*"

rdd = sc.textFile(path).map(lambda x: parseJson(getJson(x))) \
    .filter(lambda x: x is not None) \
    .groupByKey().mapValues(lambda a: distinct(list(a))) \
    .repartition(100) \
    .saveAsTextFile('/user/lel/temp/xianyu_userinfo')
