#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext

import rapidjson as json

sc = SparkContext(appName="t_xianyu_userinfo")

def valid_jsontxt(content):
    res = str(content)
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(x):
    result = []
    text = line.strip()
    star = text.find("({") + 1
    if star == -1: return [None]
    else: star += 1
    end = text.rfind("})") + 1
    ob = json.loads(valid_jsontxt(text[star:end]))
    if type(ob) != type({}): return [None]
    items = ob.get("idleItemSearch@2",{}).get("data",{}).get("items",[])
    lv = []
    for item in items:
        area = item.get("area","-")
        auctionType = item.get("auctionType","-")
        categoryId = item.get("categoryId","-")
        province = item.get("province","-")
        city = item.get("city","-")
        description = item.get("description","-")
        detailFrom = item.get("detailFrom","-")
        favorNum = item.get("favorNum","-")
        firstModified = item.get("firstModified","-")
        frm = item.get("from","-")
        gps = item.get("gps","-")
        id = item.get("id","-")
        offline = item.get("offline","-")
        originalPrice = item.get("originalPrice","-")
        price = item.get("price","-")
        title = item.get("title","-")
        userId = item.get("userId","-")
        userNick = item.get("userNick","-")
        lv.append(valid_jsontxt(id))
        lv.append(valid_jsontxt(title))
        lv.append(valid_jsontxt(categoryId))
        lv.append(valid_jsontxt(province))
        lv.append(valid_jsontxt(city))
        lv.append(valid_jsontxt(area))
        lv.append(valid_jsontxt(auctionType))
        lv.append(valid_jsontxt(description))
        lv.append(valid_jsontxt(detailFrom))
        lv.append(valid_jsontxt(favorNum))
        lv.append(valid_jsontxt(firstModified))
        lv.append(valid_jsontxt(frm))
        lv.append(valid_jsontxt(offline))
        lv.append(valid_jsontxt(originalPrice))
        lv.append(valid_jsontxt(price))
        lv.append(valid_jsontxt(userId))
        lv.append(valid_jsontxt(userNick))
        result.append("\001".join(lv))
    return result



s = "/commit/160719.userinfo"
rdd = sc.textFile(s).flatMap(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile('/user/wrt/temp/xianyu_iteminfo_tmp')