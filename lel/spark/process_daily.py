#coding:utf-8
from pyspark import SparkContext
#
# def valid_jsontxt(content):
#     if type(content) == type(u""):
#         res = content.encode("utf-8")
#     else:
#         res = str(content)
#     return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")
#
sc = SparkContext(appName="daily")
# data = sc.textFile("/home/lyen/0215detail.xlsx").take(1)
# sc = SparkContext(appName="daily")
# c_ab = sc.parallelize([("a","b"),("b","a"),("a","b"),("f","d"),("a","b"),("b","a"),("a","b"),("f","d"),("a","f"),("c","d")]).sample(withReplacement=False,fraction=0.25)

import datetime,time



def date_to_ts(date_str):
    if date_str in "\\N": return "\\N"
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
        t = d.timetuple()
        ts = str(int(time.mktime(t)))
        return ts
    except ValueError as e:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        t = d.timetuple()
        ts = str(int(time.mktime(t)))
        return ts
