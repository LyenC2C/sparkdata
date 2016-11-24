#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="phone_tag_sogou")

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
    data = ob.get("data",{})
    source = data.get("source","-")
    amount = ob.get("amount","-")
    tag = ob.get("tag","-")
    place = ob.get("place","-")
    tel_co = ob.get("tel_co","-")
    result = []
    result.append(phone)
    result.append(platform)
    result.append(source)
    result.append(amount)
    result.append(tag)
    result.append(place)
    result.append(tel_co)
    return "\001".join([valid_jsontxt(i) for i in result])


sc.textFile("/commit/credit/sogou").map(lambda x:f(x)).saveAsTextFile("/user/wrt/temp/t_credit_phone_tag_sogou")
