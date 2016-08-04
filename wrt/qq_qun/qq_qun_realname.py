#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json

sc = SparkContext(appName="t_base_shopitem")

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

s = "/user/zlj/nlp/oper_group_info_name"

def f(x):
    ss = x.split("\t")
    if len(ss) == 1: return x
    qq = ss[0]
    text = ss[1]
    name_list = text.split("_")
    if len(name_list) == 1: return qq + "\t" + text
    l = ([(x,name_list.count(x)) for x in set(name_list)])
    l.sort(key = lambda k:k[1],reverse=True)
    name = l[0][0]
    return qq + "\t" + name

rdd = sc.textFile("s").map(lambda x:f(x)).saveAsTextFile('/user/wrt/temp/qq_real_name')