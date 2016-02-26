__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="title_price_picurl")
s1 = "/user/wrt/item_info_new"
s2 = "/comment/iteminfoorg"
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content
def parse_price(price_dic):
    min=1000000000.0
    price='0'
    price_range='-'
    for value in price_dic:
        tmp=value['price']
        v=0
        if '-' in tmp:
            v=(float(tmp.split('-')[0])+float(tmp.split('-')[1]))/2.0
        else :
            v=float(tmp)
        if min>v:
            min=v
            price=v
            if '-' in tmp: price_range=tmp
    return [price,price_range]

def f_new(line):
    try:
        ss = line.strip().split('\t',2)
        ob = json.loads(valid_jsontxt(ss[2]))
        result = []
        item_id = valid_jsontxt(ss[1])
        title = ob.get("itemInfoModel").get("title")
        value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
        price = value[0]
        picurl_list = ob.get("itemInfoModel").get("picsPath")
        picurl_y = picurl_list[0]
        picurl = picurl_y.replace("img.alicdn.com","gw.alicdn.com")
        result.append(title)
        result.append(price)
        result.append(picurl)
        result.append("new")
        return (item_id,result)
    except Exception,e:
        print e
        return None
def f_old(line):
    try:
        ss = line.strip().split('\t',2)
        ob = json.loads(valid_jsontxt(ss[2]))
        result = []
        item_id = valid_jsontxt(ss[1])
        title = ob.get("itemInfoModel").get("title")
        value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
        price = value[0]
        picurl_list = ob.get("itemInfoModel").get("picsPath")
        picurl_y = picurl_list[0]
        picurl = picurl_y.replace("img.alicdn.com","gw.alicdn.com")
        result.append(title)
        result.append(price)
        result.append(picurl)
        result.append("old")
        return (item_id,result)
    except Exception,e:
        print e
        return None
def quchong(x,y):
    # if len(y) == 1:
    # if len(y) != 1:
    # y = y[0]
    info_list = y
    for ln in info_list:
        y = ln
        if ln[3] == "new": break
    result = [x] + y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)
    # return x

rdd1 = sc.textFile(s1).map(lambda x: f_new(x)).filter(lambda x:x!=None)
rdd2 = sc.textFile(s2).map(lambda x: f_old(x)).filter(lambda x:x!=None)
#print rdd.count()
rdd = rdd1.union(rdd2).groupByKey().mapValues(list).map(lambda (x,y):quchong(x,y))
rdd.saveAsTextFile('/user/wrt/id_title_price_url')
