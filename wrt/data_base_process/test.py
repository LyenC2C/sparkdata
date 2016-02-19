__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="iteminfo_history")
s1 = "/commit/iteminfoorg"
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def f(line):
    try:
        ss = line.strip().split('\t',2)
        ob = json.loads(valid_jsontxt(ss[2]))
        result = []
        item_id = valid_jsontxt(ss[1])
        title = ob.get("itemInfoModel").get("title")
        price = ob.get("itemInfoModel").get("price")
        picurl_list = ob.get("itemInfoModel").get("picsPath")
        picurl = picurl_list[0]
        picurl.replace("img.alicdn.com","gw.alicdn.com")
        result.append(title)
        result.append(price)
        result.append(picurl)
        return (item_id,result)

    except Exception,e:
        print e
        return None
def quchong(x,y):
    # if len(y) == 1:
    if len(y) != 1:
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
