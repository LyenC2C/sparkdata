#coding=utf-8
__author__ = 'wrt'
from pyspark import SparkContext

import rapidjson as json

sc = SparkContext(appName="qqweibo_user_info")

def valid_jsontxt(content):
    res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    # return res.replace("\\n", " ").replace("\n"," ").replace("\u0001"," ").replace("\001", "").replace("\\r", "")
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

#
def f(line):
    if line == "" or line == None:
        return None
    ob = json.loads(valid_jsontxt(line))
    info = ob.get("info","-")
    if info == "-": return None
    id = info.get("id","-")
    company = info.get("company",[])
    com_dict = {}
    for com in company:
        com_startYear = str(com.get("startYear","-"))
        com_endYear = com.get("endYear","-")
        com_comName = com.get("comName","-")
        com_depName = com.get("depName","-")
        if com_startYear.isdigit(): index = float(com_startYear) #按照入职年份排序来入表
        else: index = 0.0
        if com_dict.has_key(index): index = index + 0.1 #避免同下标覆盖
        com_dict[index] = [com_startYear,com_endYear,com_comName,com_depName]
    com_list = sorted(com_dict.iteritems(), key = lambda d:d[0], reverse = True)
    lv = []
    for ln in com_list: #排好序后的前三位
        lv.append('\002'.join(ln[1]))
    company_res = '\003'.join(lv)
    return [id,company_res]
    # return "\001".join(result)


# s_occu = "/commit/qqweibo/userinfo/map/occu.map"
# s = "/commit/qqweibo/userinfo/qqweibo_user*"
s = "/commit/qqweibo/userinfo/new-all"
# occu_dict = sc.broadcast(sc.textFile(s_occu).map(lambda x: (x.split("\t")[0],x.split("\t")[1]))\
#     .filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:f(x)).filter(lambda x:x!=None)\
    .map(lambda x:(x[0],x)).groupByKey().map(lambda (x,y):'\001'.join(list(y)[0]))
rdd.saveAsTextFile("/user/wrt/temp/qqweibo_user_company")

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 120 t_qqweibo_company.py

#LOAD DATA  INPATH '/user/wrt/temp/qqweibo_user_school' OVERWRITE INTO TABLE t_qqweibo_user_company


#