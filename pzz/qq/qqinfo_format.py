__author__ = 'steven'
#coding:utf-8
import  json
import sys,time


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
    line_s = valid_jsontxt(line.strip().replace("\\n", "").replace("\\r", "").replace("\\t", "").replace("\u0001", ""))
    #print line_s
    ob = json.loads(line_s)
    #print json.dumps(ob)
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
    age = 2015 - year
    #print uin
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
    lv.append(uin.decode("utf-8"))
    lv.append(str(age))
    #lv.append(birthday)
    #lv.append(phone.decode("utf-8"))
    #lv.append(gender_id)
    lv.append(f_coding(college))
    #lv.append(f_coding(lnick))
    #lv.append(loc_id)
    lv.append(f_coding(loc))
    #lv.append(h_loc_id)
    lv.append(f_coding(h_loc))
    #lv.append(f_coding(personal))
    lv.append(f_coding(shengxiao))
    lv.append(gender.decode("utf-8"))
    lv.append(f_coding(occupation))
    lv.append(f_coding(constel))
    lv.append(f_coding(blood))
    #lv.append(url)
    #lv.append(f_coding(homepage))
    #lv.append(f_coding(nick))
    #lv.append(f_coding(email))
    #lv.append(uin2)
    #lv.append(mobile)
    #lv.append(ts)
    #lv.append(0)
    return lv

if __name__ == '__main__':
    loc_dic = {}
    for line in open("./loc.map.final"):
        ls = line.split("\t")
        loc_dic[ls[0]] = ls[1].strip()

    for line in sys.stdin:
        res = f(line,loc_dic)
        #print res
        print "\t".join(res).encode("utf-8")