#coding:utf-8
from pyspark import SparkContext

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

sc = SparkContext(appName="daily")
data = sc.textFile("/home/lyen/0215detail.xlsx").take(1)

for i in data:
    print data

