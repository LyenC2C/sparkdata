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


def feed(comment_dic):
    ''
def f_1(line):
    result = []
    text = line.strip()
    star = text.find("({")+1
    ob = json.loads(valid_jsontxt(line[star:-2]))
    if type(ob)== type(1.0): return [None]
    items = ob.get("idleItemSearch@2",{}).get("data",{}).get("items",[])
    for item in items:
        subTags=item.get('subTags',{})
        if len(subTags)>1:
            result.append(subTags)
    return result


def f(line):
    result = []
    text = line.strip()
    star = text.find("({")+1
    ob = json.loads(valid_jsontxt(line[star:-2]))
    if type(ob)== type(1.0): return [None]
    items = ob.get("idleItemSearch@2",{}).get("data",{}).get("items",[])
    for item in items:
        lv = []
        area = item.get("area","-")
        auctionType = item.get("auctionType","-")
        categoryId = item.get("categoryId","-")
        province = item.get("province","-")
        city = item.get("city","-")
        description = item.get("description","-")
        detailFrom = item.get("detailFrom","-")
        favorNum = item.get("favorNum","-")
        firstModified = item.get("firstModified","-")
        t_from = item.get("from","-")
        gps = item.get("gps","-")
        id = item.get("id","-")
        commentNum=item.get('commentNum','0')
        offline = item.get("offline","-")
        originalPrice = item.get("originalPrice","-")
        price = item.get("price","-")
        title = item.get("title","-")
        userId = item.get("userId","-")
        userNick = item.get("userNick","-")
        community_name='-'
        subTags=item.get('subTags',{})
        for kv in subTags:
            if int(kv.get('type','0'))==3:
                community_name=kv.get('name','-')
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
        lv.append(valid_jsontxt(commentNum))
        lv.append(valid_jsontxt(firstModified))
        lv.append(valid_jsontxt(t_from))
        lv.append(valid_jsontxt(gps))
        lv.append(valid_jsontxt(offline))
        lv.append(valid_jsontxt(originalPrice))
        lv.append(valid_jsontxt(price))
        lv.append(valid_jsontxt(userId))
        lv.append(valid_jsontxt(userNick))
        lv.append(valid_jsontxt(community_name))
        result.append(lv)
    return result


def f_try(line):
    try:
        return f(line)
    except:return [None]

# s = "/commit/160719.userinfo"
s = "/commit/taobao_xianyu_back/"

# rdd = sc.textFile(s).flatMap(lambda x:f_1(x)).filter(lambda x:x!=None)

rdd = sc.textFile(s).filter(lambda x:len(x)>10).flatMap(lambda x:f_try(x)).filter(lambda x:x!=None).map(lambda x:(x[0],x)).groupByKey()\
    .map(lambda (x,y):list(y)[0]).map(lambda x:'\001'.join(x))
rdd.repartition(100).saveAsTextFile('/user/zlj/temp/xianyu_iteminfo_tmp')

