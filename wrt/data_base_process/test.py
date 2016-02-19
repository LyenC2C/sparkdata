__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="title_price_picurl")
s1 = "/commit/iteminfoorg/item.info.crawler171.2016-01-04"
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

def f(line):
    try:
        ss = line.strip().split('\t',2)
        ob = json.loads(valid_jsontxt(ss[2]))
        result = []
        item_id = valid_jsontxt(ss[1])
        title = ob.get("itemInfoModel").get("title")
        value = parse_price(ob['apiStack']['itemInfoModel']['priceUnits'])
        price = value[0]
        picurl_list = ob.get("itemInfoModel").get("picsPath")
        picurl = picurl_list[0]
        picurl.decode('utf-8').replace("img.alicdn.com","gw.alicdn.com").encode('utf-8')
        result.append(title)
        result.append(price)
        result.append(picurl)
        return (item_id,result)

    except Exception,e:
        print e
        return None
def quchong(x,y):
    # if len(y) == 1:
    # if len(y) != 1:
    y = y[0]
    result = [x] + y
    lv = []
    for ln in result:
        lv.append(str(valid_jsontxt(ln)))
    return "\001".join(lv)
    # return x

rdd = sc.textFile(s1).map(lambda x:f(x)).filter(lambda x:x!=None)
#print rdd.count()
rdd2 = rdd.groupByKey().mapValues(list).map(lambda (x,y):quchong(x,y))
rdd2.saveAsTextFile('/user/wrt/title_price_url')
