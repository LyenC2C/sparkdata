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

def get_dict(x):
    ss = x.split("\t")
    return (ss[0],ss[1])
def f(line,occu_dict):
    if line == "" or line == None:
        return None
    ob = json.loads(valid_jsontxt(line))
    info = ob.get("info","-")
    if info == "-": return None
    id = info.get("id","-")
    school = info.get("school",[])
    sch_dict = {}
    background_list = ["博士","硕士","大学","高中","初中","小学","-"]
    for sch in school:
        # schoolId = sch.get("schoolId",[])
        # index = sch.get("index")
        year = valid_jsontxt(str(sch.get("year","-")))
        background = valid_jsontxt(str(sch.get("background","-")))
        if background not in background_list: background = "-"
        index = background_list.index(background) #讲学历大小按照顺序排列好，作为下标
        department = valid_jsontxt(str(sch.get("department","-")))
        school = valid_jsontxt(str(sch.get("school","-")))
        if sch_dict.has_key(index): index = index + 0.1 #处理相同下标，避免字典覆盖
        sch_dict[index] = [year,background,school,department] #排序学历，高的优先输出
    sch_list = sorted(sch_dict.iteritems(), key = lambda d:d[0], reverse = False)
    school_res = ""
    lv = []
    for ln in sch_list: #排好序后的前三位
        # for ls in ln[1]: #year,background,school,department
        lv.append('\002'.join(ln[1]))
    school_res = '\003'.join(lv)
    return [id,school_res]
    # return "\001".join(result)


# s_occu = "/commit/qqweibo/userinfo/map/occu.map"
# s = "/commit/qqweibo/userinfo/qqweibo_user*"
s = "/commit/qqweibo/userinfo/new-all/qqweibo_user.p2.177.json"
# occu_dict = sc.broadcast(sc.textFile(s_occu).map(lambda x: (x.split("\t")[0],x.split("\t")[1]))\
#     .filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:f(x)).filter(lambda x:x!=None)\
    .map(lambda x:(x[0],x)).groupByKey().map(lambda (x,y):'\001'.join(list(y)[0]))
rdd.saveAsTextFile("/user/wrt/temp/qqweibo_user_school")

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 120 t_qqweibo_user_info.py

#LOAD DATA  INPATH '/user/wrt/temp/qqweibo_user' OVERWRITE INTO TABLE t_qqweibo_user_info


#