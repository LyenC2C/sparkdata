#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext
sc = SparkContext(appName="t_base_shopitem")

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")


def f(line):
    ob = json.loads(valid_jsontxt(line.strip()))
    if type(ob) != type({}): return None
    year = ob.get("year","-")
    blacklist_info = ob.get("blacklist_info",{})
    if blacklist_info == {}: return None
    phone = blacklist_info.get("phone","-")
    user_id_number = blacklist_info.get("user_id_number","-")
    uname = blacklist_info.get("uname","-")
    real_name = blacklist_info.get("real_name","-")
    borrow_list = blacklist_info.get("borrow_list","-")
    result = []
    result.append(uname)
    result.append(real_name)
    result.append(phone)
    result.append(user_id_number)
    result.append(borrow_list)
    result.append(year)
    return (uname,result)

rdd_c = sc.textFile("/commit/credit/ppd/financial_blacklist_ppd_user_info").map(lambda x:f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): valid_jsontxt(i) for i in y[0])
rdd.saveAsTextFile('/user/wrt/temp/ppd_info_tmp')

# hfs -rmr /user/wrt/temp/iteminfo_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_ppd_info.py