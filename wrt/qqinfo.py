# coding:utf-8
import sys
import time
# import json
import rapidjson  as json
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

sc = SparkContext(appName="qqinfo_age")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def f_coding(x):
    if type(x) == type(""):
        return x.decode("utf-8")
    else:
        return x


def birth(x):
    if x == "":
        return 0
    else:
        return x


def trans(x):
    if x == "" or x == "-":
        return "-"
    else:
        return x


def dis(x):
    if x == "":
        return "0"
    else:
        return x


def sex(x):
    if x != 1 and x != 2:
        return ""
    else:
        return x


def cal_sheng(x):
    x = (x - 3) % 12
    if x == 0:
        x = 12
    return x


def cal_constel(month, day):
    n = (12, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
    d = ((1, 20), (2, 19), (3, 21), (4, 21), (5, 21), (6, 22), (7, 23), (8, 23), (9, 23), (10, 23), (11, 23), (12, 23))
    return n[len(filter(lambda y: y <= (month, day), d)) % 12]


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content


def f(line, place_dict):
    line_s = valid_jsontxt(line.replace("\\n", "").replace("\\r", "").replace("\\t", "").replace("\u0001", ""))
    ob = json.loads(line_s)
    if type(ob) == type(1.0):
        return None
    sheng_dict = {1: "鼠", 2: "牛", 3: "虎", 4: "兔", 5: "龙", 6: "蛇", 7: "马", 8: "羊", 9: "猴", 10: "鸡", 11: "狗", 12: "猪"}
    constel_dict = {1: "水瓶座", 2: "双鱼座", 3: "白羊座", 4: "金牛座", 5: "双子座", 6: "巨蟹座", 7: "狮子座", 8: "处女座", 9: "天秤座", 10: "天蝎座",
                    11: "射手座", 12: "摩羯座"}
    blood_dict = {1: "A型", 2: "B型", 3: "O型", 4: "AB型"}
    if ob.has_key("birthday"):
        year = ob["birthday"].get("year", 0)
        month = ob["birthday"].get("month", 0)
        day = ob["birthday"].get("day", 0)
        birthday = str(birth(year)) + "-" + str(birth(month)) + "-" + str(birth(day))
    else:
        year = 0
        month = 0
        day = 0
        birthday = "0-0-0"
    phone = trans(ob.get("phone", "-"))
    gender_id = str(sex(ob.get("gender_id", "")))
    college = trans(ob.get("college", "-"))
    uin = str(ob.get("uin", ""))
    lnick = trans(ob.get("lnick", "-"))
    country_id = dis(ob.get("country_id", "0"))
    province_id = "-" + dis(ob.get("province_id", "0"))
    city_id = "-" + dis(ob.get("city_id", "0"))
    zone_id = "-" + dis(ob.get("zone_id", "0"))
    loc_id = country_id + province_id + city_id + zone_id
    loc = place_dict.get(loc_id, "-")
    h_country = dis(ob.get("h_country", "0"))
    h_province = "-" + dis(ob.get("h_province", "0"))
    h_city = "-" + dis(ob.get("h_city", "0"))
    h_zone = "-" + dis(ob.get("h_zone", "0"))
    h_loc_id = h_country + h_province + h_city + h_zone
    h_loc = place_dict.get(h_loc_id, "-")
    personal = trans(ob.get("personal", "-"))
    shengxiao = sheng_dict.get(trans(ob.get("shengxiao", "-")), "-")
    if shengxiao == "-":
        if year != 0:
            shengxiao = sheng_dict.get(cal_sheng(year), "-")
    constel = constel_dict.get(trans(ob.get("constel", "-")), "-")
    if constel == "-":
        if month != 0 and day != 0:
            constel = constel_dict.get(cal_constel(month, day), "-")
    gender = str(sex(ob.get("gender", "")))
    occupation = trans(ob.get("occupation", "-"))
    blood = blood_dict.get(trans(ob.get("blood", "-")), "-")
    url = trans(ob.get("url", "-"))
    homepage = trans(ob.get("homepage", "-"))
    nick = trans(ob.get("nick", "-"))
    email = trans(ob.get("email", "-"))
    uin2 = trans(ob.get("uin2", "-"))
    mobile = trans(ob.get("mobile", "-"))
    ts = str(int(time.time()))
    lv = []
    lv.append(uin)
    lv.append(birthday)
    lv.append(phone)
    lv.append(gender_id)
    lv.append(college)
    lv.append(lnick)
    lv.append(loc_id)
    lv.append(loc)
    lv.append(h_loc_id)
    lv.append(h_loc)
    lv.append(personal)
    lv.append(shengxiao)
    lv.append(gender)
    lv.append(occupation)
    lv.append(constel)
    lv.append(blood)
    lv.append(url)
    lv.append(homepage)
    lv.append(nick)
    lv.append(email)
    lv.append(uin2)
    lv.append(mobile)
    lv.append(ts)
    lv.append(0)
    return lv
# return '\001'.join([ valid_jsontxt(i) for i in lv])
# return birthday + '\001' + phone + '\001' + gender_id + '\001' + college + '\001' + uin + '\001' + lnick + '\001' + loc_id + '\001' + loc + '\001' + h_loc_id + '\001' + h_loc + '\001' +\
#    personal + '\001' + shengxiao + '\001' + gender + '\001' + occupation + '\001' + constel + '\001' + blood + '\001' + url + '\001' + homepage + '\001' + nick + '\001' +\
# 		email + '\001' + uin2 + '\001' + mobile + '\001' + ts
if sys.argv[1] == '-h':
    comment = 'qq用户信息格式化为hive数据格式'
    print comment
    print 'argvs:\n argv[1]:qq file or dir input\n argv[2]:district_dict file or dir input\n argv[3]:dir output'

rdd = sc.textFile(sys.argv[1])
rdd2 = sc.textFile(sys.argv[2])
p_dict = rdd2.map(lambda x: x.split('\t')).collectAsMap()
broadcastVar = sc.broadcast(p_dict)
place_dict = broadcastVar.value
rdd_info = rdd.map(lambda x: f(x, place_dict)).filter(lambda x: x != None)
rdd_age = sc.textFile('/user/hadoop/qq/info/qq_age.0611').map(lambda x: x.split('\t')).map(lambda x:[x[0],int(x[1])])
schema1 = StructType([
    StructField("uin", StringType(), True),
    StructField("birthday", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("gender_id", StringType(), True),
    StructField("college", StringType(), True),
    StructField("lnick", StringType(), True),
    StructField("loc_id", StringType(), True),
    StructField("loc", StringType(), True),
    StructField("h_loc_id", StringType(), True),
    StructField("h_loc", StringType(), True),
    StructField("personal", StringType(), True),
    StructField("shengxiao", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("constel", StringType(), True),
    StructField("blood", StringType(), True),
    StructField("url", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("nick", StringType(), True),
    StructField("email", StringType(), True),
    StructField("uin2", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("age", IntegerType(), True)
])
schema2 = StructType([
    StructField("qq", StringType(), True),
    StructField("age", IntegerType(), True)
])

df1 = hiveContext.createDataFrame(rdd_info, schema1)
hiveContext.registerDataFrameAsTable(df1, 'qqinfo')
df2 = hiveContext.createDataFrame(rdd_age, schema2)
hiveContext.registerDataFrameAsTable(df2, 'qqage')
hiveContext.sql('use wlbase_dev')

sql = '''
Insert overwrite table t_base_q_user_dev  PARTITION(ds=20151112)
                select uin ,birthday, phone, gender_id, college, lnick, loc_id, loc,
                h_loc_id, h_loc, personal, shengxiao, gender, occupation, constel, blood, url, homepage, nick, email, uin2, mobile, ts,

                case when qqinfo_age< 150 and qqinfo_age >5 then qqinfo_age else   t2.age end age
                from

                (select *,(2015-year(birthday)) as qqinfo_age
                 from
                qqinfo
                where uin is not null)t1

                 left join
                 qqage t2
                on t1.uin=t2.qq

'''
hiveContext.sql(sql)
sc.stop()
