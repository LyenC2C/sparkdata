#coding=utf-8
__author__ = 'wrt'
import sys
from pyspark import SparkContext

sc = SparkContext(appName="qqweibo_user_info")

def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content


def f(line,occu_dict):
    result = []
    ob = loads.loads(valid_jsontxt(line))
    info = ob.get("info","-")
    if info == "-": return None
    id = info.get("id","-")
    certificationInfo = ob.get("certificationInfo","-") #认证信息
    faceUrl = info.get("faceUrl","-")
    gender = info.get("gender","-") #性别，1是男，2是女，0好像也是男
    isVIP = info.get("isVIP","-") #微博vip？
    isWbStar = info.get("isWbStar","-") #微博之星
    auth = info.get("auth","-") #微博认证等级 ，1是个人，2是机构
    nickName = info.get("nickName","-")
    occupation_id = info.get("occupation","-") #映射
    occupation = occu_dict.get(occupation_id,"-")
    personal = info.get("personal","-")
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
    birthday = info.get("birthday","{}")
    b_year = birthday.get("year","-")
    b_month = birthday.get("month","-")
    b_day = birthday.get("day","-")
    b_star = birthday.get("star","-")
    # b_starId = birthday.get("starId","-")
    tags = info.get("tags",[])
    tags_r = ""
    for tag in tags:
        tags_r += tag.get("category","-") + "_" + tag.get("content","-")
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
    for com in company:
        com_startYear = com.get("startYear","-")
        com_endYear = com.get("endYear","-")
        com_comName = com.get("comName","-")
        com_depName = com.get("depName","-")
        index = company.get("index")
        com_dict[index] = [com_startYear,com_endYear,com_comName,com_depName]
    com_list = sorted(com_dict.iteritems(), key = lambda d:d[0], reverse = True)
    i = 0
    for ln in com_list[:3]: #排好序后的前三位
        i += 1
        for ls in ln[1]: #com_startYear,com_endYear,com_comName,com_depName
            result.append(valid_jsontxt(ls))
    for i in range(3-i): #不足3个的补齐“-”
        for ls in range(4):
            result.append("-")
    school = info.get("school",[])
    sch_dict = {}
    for sch in school:
        # schoolId = sch.get("schoolId",[])
        index = sch.get("index")
        year = sch.get("year","-")
        background = sch.get("background","-")
        department = sch.get("department","-")
        school = sch.get("school","-")
        sch_dict[index] = [year,background,school,department]
    sch_list = sorted(sch_dict.iteritems(), key = lambda d:d[0], reverse = True)
    i = 0
    for ln in sch_list[:3]: #排好序后的前三位
        i += 1
        for ls in ln[1]: #year,background,school,department
            result.append(valid_jsontxt(ls))
    for i in range(3-i): #不足3个的补齐“-”
        for ls in range(4):
            result.append("-")
    return "\001".join(result)


s_occu = "/commit/qqweibo/userinfo/map/all.school.map"
s = "/commit/qqweibo/userinfo/qqweibo_user.all.179.json"
occu_dict = sc.broadcast(sc.textFile(s_occu).map(lambda x: (x.split("\t")[0],x.split("\t")[1]))\
    .filter(lambda x:x!=None).collectAsMap()).value
rdd = sc.textFile(s).map(lambda x:f(x)).filter(lambda x:x!=None)
rdd.saveAsTextFile("/user/wrt/temp/qqweibo_user")

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 40 t_qqweibo_user_info.py