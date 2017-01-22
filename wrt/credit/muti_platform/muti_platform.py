#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *

today = sys.argv[1]


sc = SparkContext(appName="rong360_features_hebing")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    flag = ob.get("flag",False)
    if flag == None: return None
    platform = valid_jsontxt(ob.get("platform"))
    phone = valid_jsontxt(ob.get("phone"))
    if not phone.isdigit(): return None
    if platform == None or platform == 'None': return None
    # if flag: return (phone,[platform,1])
    # if not flag: return (phone,[platform,0])
    if flag:
        return phone + "\001" + platform + "\001" + "1"
    if not flag: return phone + "\001" + platform + "\001" + "0"

# def f(line):
#     ob = json.loads(valid_jsontxt(line.strip()))
#     platform = ob.get("platform")
#     return (valid_jsontxt(platform),"1")
# rdd.map(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): x)

# def f2(x,y,muti):
#     result = [-1] * 21
#     for ln in y:
#         i = muti.index(ln)
#         result[i] = 1

# def f(line):
#     ob = json.loads(valid_jsontxt(line.strip()))
#     platform = ob.get("platform")
#     if platform != None: return None
#     else: return line



rdd.map(lambda x:f(x)).groupByKey().mapValues(list).map(lambda (x, y): x)


muti = ["和信贷","借了吗","马上金融","好贷宝","缺钱么","拍拍贷","翼龙贷","搜易贷","信用钱包","君融贷","人人贷","微贷网","口碑贷","闪电借款","珠宝贷","宜贷网","招联金融","钱爸爸","招商贷","团贷网","容易贷"]
rdd = sc.textFile("/commit/regist/multplatform/phone.check.20170111")
rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile("/user/wrt/temp/muti_platform")
# rdd.map(lambda x:f(x)).filter(lambda x:x!=None).groupByKey().mapValues(list).map(lambda (x, y): f2(x,y,muti))
#load data inpath '/user/wrt/temp/muti_platform' into table wl_base.t_base_muti_platform partition;


