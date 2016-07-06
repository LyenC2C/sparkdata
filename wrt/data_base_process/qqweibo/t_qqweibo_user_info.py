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
    result = []
    # text = line.replace("\\n", "").replace("\\r", "").replace("\\t", "").replace("\u0001", "")
    # try:
    #     ob = json.loads(valid_jsontxt(line))
    # except:
    #     print valid_jsontxt(line)
    #     return None
    if line == "" or line == None:
        return None
    ob = json.loads(valid_jsontxt(line))
    info = ob.get("info","-")
    if info == "-": return None
    id = info.get("id","-")
    certificationInfo = ob.get("certificationInfo","-") #认证信息
    faceUrl = info.get("faceUrl","-")
    gender = info.get("gender","-") #性别，1是男，2是女，0好像也是男
    isVIP = info.get("isVIP","-") #微博vip？
    isWbStar = info.get("isWbStar","-") #微博之星
    auth = info.get("auth","-") #微博认证等级 ，1是个人，2是机构
    nickName = info.get("nickName","-").replace("\n","").replace("\r","").replace("\t","")
    occupation_id = info.get("occupation","-") #映射
    occupation = occu_dict.get(occupation_id,"-")
    personal = info.get("personal","-").replace("\n","").replace("\r","").replace("\t","")
    regTime = info.get("regTime","-")
    hometown = info.get("hometown",{})
    h_nation = hometown.get("province","-")
    h_countryName = hometown.get("countryName","-")
    h_province_ = hometown.get("province","-")
    h_provinceName = hometown.get("provinceName","-")
    h_city = hometown.get("","-")
    h_cityName = hometown.get("","-")
    location = info.get("location",{})
    l_nation = location.get("province","-")
    l_countryName = location.get("countryName","-")
    l_province_ = location.get("province","-")
    l_provinceName = location.get("provinceName","-")
    l_city = location.get("city","-")
    l_cityName = location.get("cityName","-")
    birthday = info.get("birthday",{})
    b_year = birthday.get("year","-")
    b_month = birthday.get("month","-")
    b_day = birthday.get("day","-")
    b_star = birthday.get("star","-")
    # b_starId = birthday.get("starId","-")
    tags = info.get("tags",[])
    tags_r = ""
    for tag in tags:
        tags_r += valid_jsontxt(str(tag.get("category","-"))) + "_" + \
                  valid_jsontxt(str(tag.get("content","-")))
    result.append(valid_jsontxt(str(id)))
    result.append(valid_jsontxt(str(certificationInfo)))
    result.append(valid_jsontxt(str(faceUrl)))
    result.append(valid_jsontxt(str(gender)))
    result.append(valid_jsontxt(str(isVIP)))
    result.append(valid_jsontxt(str(isWbStar)))
    result.append(valid_jsontxt(str(auth)))
    result.append(valid_jsontxt(str(nickName)))
    # result.append(valid_jsontxt(str(occupation_id)))
    result.append(valid_jsontxt(str(occupation)))
    result.append(valid_jsontxt(str(personal)))
    result.append(valid_jsontxt(str(regTime)))
    result.append(valid_jsontxt(str(h_nation)))
    result.append(valid_jsontxt(str(h_countryName)))
    result.append(valid_jsontxt(str(h_province_)))
    result.append(valid_jsontxt(str(h_provinceName)))
    result.append(valid_jsontxt(str(h_city)))
    result.append(valid_jsontxt(str(h_cityName)))
    result.append(valid_jsontxt(str(l_nation)))
    result.append(valid_jsontxt(str(l_countryName)))
    result.append(valid_jsontxt(str(l_province_)))
    result.append(valid_jsontxt(str(l_provinceName)))
    result.append(valid_jsontxt(str(l_city)))
    result.append(valid_jsontxt(str(l_cityName)))
    result.append(valid_jsontxt(str(tags_r)))
    result.append(valid_jsontxt(str(b_year)))
    result.append(valid_jsontxt(str(b_month)))
    result.append(valid_jsontxt(str(b_day)))
    result.append(valid_jsontxt(str(b_star)))
    company = info.get("company",[])
    com_dict = {}
    com_repeat = []
    for com in company:
        com_startYear = str(com.get("startYear","-"))
        com_endYear = com.get("endYear","-")
        com_comName = com.get("comName","-")
        com_depName = com.get("depName","-")
        if com_startYear.isdigit(): index = float(com_startYear) #按照入职年份排序来入表
        else: index = 0.0
        #if com_dict.has_key(index): index = index + 0.1
        while(com_dict.has_key(index)): index = index + 0.1# #避免同下标覆盖
        com_res = [com_startYear,com_endYear,com_comName,com_depName]
        if not com_res in com_repeat: #去掉重复的工作公司
            com_dict[index] = com_res
            com_repeat.append(com_res)
    com_list = sorted(com_dict.iteritems(), key = lambda d:d[0], reverse = True)
    i = 0
    for ln in com_list[:3]: #排好序后的前三位
        i += 1
        for ls in ln[1]: #com_startYear,com_endYear,com_comName,com_depName
            result.append(valid_jsontxt(str(ls)))
    for i in range(3-i): #不足3个的补齐“-”
        for ls in range(4):
            result.append("-")
    school = info.get("school",[])
    background_list = ["博士","硕士","大学","高中","初中","小学","-"]
    sch_dict = {}
    sch_repeat = []
    for sch in school:
        # schoolId = sch.get("schoolId",[])
        # index = sch.get("index")
        year = sch.get("year","-")
        background = valid_jsontxt(str(sch.get("background","-")))
        if background not in background_list: background = "-"
        index = background_list.index(background) #讲学历大小按照顺序排列好，作为下标
        department = sch.get("department","-")
        school = str(sch.get("school","-"))
        #if sch_dict.has_key(index): index = index + 0.1
        while(sch_dict.has_key(index)): index = index + 0.1#处理相同下标，避免字典覆盖
        sch_res = [year,background,school,department]#+valid_jsontxt(background)+valid_jsontxt(school)+valid_jsontxt(department)
        if not sch_res in sch_repeat: #去掉重复的学历学校
            sch_dict[index] = sch_res #排序学历，高的优先输出
            sch_repeat.append(sch_res)
    sch_list = sorted(sch_dict.iteritems(), key = lambda d:d[0], reverse = False)
    i = 0
    for ln in sch_list[:3]: #排好序后的前三位
        i += 1
        for ls in ln[1]: #year,background,school,department
            result.append(valid_jsontxt(str(ls)))
    for i in range(3-i): #不足3个的补齐“-”
        for ls in range(4):
            result.append("-")
    return result
    # return "\001".join(result)


s_occu = "/commit/qqweibo/userinfo/map/occu.map"
# s = "/commit/qqweibo/userinfo/qqweibo_user*"
s = "/commit/qqweibo/userinfo/new-all/*"
occu_dict = sc.broadcast(sc.textFile(s_occu).map(lambda x: (x.split("\t")[0],x.split("\t")[1]))\
    .filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:f(x,occu_dict)).filter(lambda x:x!=None)\
    .map(lambda x:(x[0],x)).groupByKey().map(lambda (x,y):'\001'.join(list(y)[0]))
rdd.saveAsTextFile("/user/wrt/temp/qqweibo_user")

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 120 t_qqweibo_user_info.py

#LOAD DATA  INPATH '/user/wrt/temp/qqweibo_user' OVERWRITE INTO TABLE t_qqweibo_user_info


#