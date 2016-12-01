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

def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    phone = ob.get("phone","-")
    platform = ob.get("platform","-")
    data = ob.get("data",{})
    source = data.get("source","-")
    amount = data.get("amount","-")
    tag = data.get("tag","-")
    place = data.get("place","-")
    tel_co = data.get("tel_co","-")
    ts = int(time.time())
    result = []
    result.append(phone)
    result.append(platform)
    result.append(source)
    result.append(amount)
    result.append(tag)
    result.append(place)
    result.append(tel_co)
    result.append(ts)
    return "\001".join([valid_jsontxt(i) for i in result])


sc.textFile("/commit/credit/sogou").map(lambda x:f(x)).saveAsTextFile("/user/wrt/temp/t_credit_phone_tag_sogou")

#spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 t_credit_phone_tag_sogou.py
#LOAD DATA INPATH '/user/wrt/temp/t_credit_phone_tag_sogou' INTO TABLE wlcredit.t_credit_phone_tag_sogou;
