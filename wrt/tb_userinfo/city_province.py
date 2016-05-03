#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext

sc = SparkContext(appName="city_province")

def get_p_dict(line):
    ss = line.strip().split("\t")
    return (ss[0],ss[1])
def f(line,p_dict):
    ss = line.strip().split("\001")
    ss[6] = p_dict.get(ss[6],"") + ss[6]
    return "\001".join(ss)

s_p = '/user/wrt/city_pro'
rdd = sc.textFile("/data/develop/ec/tb/user/userinfo.20160429.format")
p_dict = sc.broadcast(sc.textFile().map(lambda x: get_p_dict(x)).filter(lambda x:x!=None).collectAsMap()).value
rdd.map(lambda x:f(x,p_dict)).saveAsTextFile("/user/wrt/temp/tb_userinfo")