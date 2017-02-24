# coding:utf-8
import json
from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


#
# def process(line):
#     ob = json.loads(valid_jsontxt(line))
#     if not isinstance(ob, dict): return None
#     datas = ob.get("data", {}).get("data",[])
#     result = []
#     for data in datas:
#         name = data.get("name", "\\N")
#         numbers = data.get("number", [])
#         for num in numbers:
#             result.append((valid_jsontxt(num),valid_jsontxt(name)))
#     return result
sc = SparkContext(appName="sougou_bankname_search")
#
# data = sc.textFile("/home/lyen/json.json") \
#     .flatMap(lambda a: process(a)) \
#     .filter(lambda a: a is not None) \
#     .distinct() \
#     .map(lambda (a,b): a+"\001" + b).collect()
# for i in data:
#     print i
data1 = sc.parallelize([(1, 2)])
data2 = sc.parallelize([(1, 3)])
data3 = sc.parallelize([(1, (4, 5, 6,"\\N"))])
# [a[0], a[1][0][0], a[1][0][1], a[1][1][0], a[1][1][1], a[1][1][2], a[1][1][3]])

# res = data1.join(data2).join(data3).map(
#     lambda (a, ((b, c), (d, e, f, g))): "\001".join(valid_jsontxt(i) for i in [a, b, c, d, e, f, g])).collect()
# for i in res:
#     print res


