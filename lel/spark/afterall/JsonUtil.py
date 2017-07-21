import json as json
from pyspark import SparkContext
from pyspark.sql.functions import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def parseJson(js):
    ob = json.loads(js)
    uid = ob.get("uid", None)
    orders = ob.get("orders", None)
    res = []
    for order in orders:
        orderInfo = []
        [orderInfo.append(valid_jsontxt(order.get(k, None))) for k in order.keys() if "goodsInfo" not in k]
        goods = order.get("goodsInfo", None)
        for good in goods:
            goodInfo = [valid_jsontxt(uid)]
            [goodInfo.append(valid_jsontxt(good.get(m, None))) for m in good.keys()]
            goodInfo.append("\001".join(orderInfo))
            res.append("\001".join(goodInfo))
    return res


def f(x):
    print(x)


sc = SparkContext(appName="income_analysis")
data = sc.textFile("/home/lyen/data/weibo.json")
data.flatMap(lambda js: parseJson(js)).saveAsTextFile("hdfs://master:9000/data/taobao")
