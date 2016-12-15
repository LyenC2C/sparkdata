#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
import time
sc = SparkContext(appName="phone_tag_sogou")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def get_dict(line):
    ss = line.strip().split("\t")
    if ss[1] == "1": ss[1] = "诈骗"
    if ss[1] == "2": ss[1] = "中介"
    if ss[1] == "3": ss[1] = "快递"
    if ss[1] == "4": ss[1] = "外卖"
    if ss[1] == "5": ss[1] = "其他"
    return (ss[0],ss[1])


def f(line,l_dict):
    ss = line.strip().split("\001")
    phone = ss[0]
    source = ss[2]
    amount = ss[3]
    tag = ss[4]
    label = l_dict.get(tag,"其他")
    place = ss[5]
    tel_co = ss[6]
    result = []
    result.append(phone)
    result.append(source)
    result.append(amount)
    result.append(tag)
    result.append(label)
    result.append(place)
    result.append(tel_co)
    return "\001".join([valid_jsontxt(i) for i in result])

s_dim = "/user/wrt/total_mark.mark"
l_dict = sc.broadcast(sc.textFile(s_dim).map(lambda x: get_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
sc.textFile("/hive/warehouse/wlcredit.db/t_credit_phone_tag_sogou").map(lambda x:f(x,l_dict))\
    .saveAsTextFile("/user/wrt/temp/phone_sogou_label")

