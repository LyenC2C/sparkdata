#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext

sc = SparkContext(appName="t_hx_rong360_sogou_tag")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    phone = ob.get("phone","-")
    platform = ob.get("platform","-")
    data = str(ob.get("data","-"))
    ret = ob.get("ret","-")
    msg = ob.get("msg","-")
    result.append(phone)
    result.append(platform)
    result.append(data)
    result.append(ret)
    result.append(msg)
    return "\001".join([valid_jsontxt(i) for i in result])

sc.textFile("/commit/credit/sogou").map(lambda x:f(x)).saveAsTextFile("/user/wrt/temp/t_hx_rong360_sogou_tag")





