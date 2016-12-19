#coding:utf-8
__author__ = 'wrt'

import sys
import rapidjson as json
from pyspark import SparkContext
now_day = sys.argv[1]
sc = SparkContext(appName="t_base_ppd_listinfo_" + now_day)


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
    year = ob.get("year","-") #此人被拍拍贷公布的年份
    # blacklist_info = ob.get("blacklist_info",{})
    # if blacklist_info == {}: return None
    user_phone = ob.get("user_phone",'-')
    # phone = ob.get("phone","-")
    id_card = ob.get("id_card","-")
    user_name = ob.get("user_name","-")
    real_name = ob.get("real_name","-")
    borrow_list = ob.get("borrow_list",[])
    borrow_money = ob.get("borrow_money",'-')
    if borrow_list == []: return None
    result = []
    for borrow in borrow_list:
        lv = []
        borrow_date = borrow.get("borrow_date","-").replace("-","")
        borrow_nper = borrow.get("borrow_nper","-")
        list_number = borrow.get("list_number","-")
        time_out_interest = borrow.get("time_out_interest","-")
        time_out_days = borrow.get("time_out_days","-")
        lv.append(list_number)
        lv.append(user_name)
        lv.append(real_name)
        lv.append(user_phone)
        lv.append(id_card)
        lv.append(year)
        lv.append(borrow_money)
        lv.append(borrow_date)
        lv.append(borrow_nper)
        lv.append(time_out_interest)
        lv.append(time_out_days)
        result.append((list_number,lv))
    return result

rdd_c = sc.textFile("/commit/credit/ppd/ppdai.blacklist.user." + now_day).flatMap(lambda x:f(x)).filter(lambda x:x!=None)
rdd = rdd_c.groupByKey().mapValues(list).map(lambda (x, y): "\001".join([valid_jsontxt(i) for i in y[0]]))
rdd.saveAsTextFile('/user/wrt/temp/ppd_info_tmp')

# hfs -rmr /user/wrt/temp/ppd_info_tmp
# spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 t_base_ppd_info.py
# LOAD DATA  INPATH '/user/wrt/temp/ppd_info_tmp' OVERWRITE INTO TABLE t_base_ppd_listinfo PARTITION (ds='20161115');