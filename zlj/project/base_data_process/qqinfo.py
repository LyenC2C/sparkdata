#coding:utf-8
import sys,json
import time
from pyspark import SparkContext

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
def f(line,place_dict):
	line = line.replace("\\n","").replace("\\r","").replace("\\t","").replace("\u0001","")
	ob = json.loads(line)
	sheng_dict = {1:"鼠",2:"牛",3:"虎",4:"兔",5:"龙",6:"蛇",7:"马",8:"羊",9:"猴",10:"鸡",11:"狗",12:"猪"}
	constel_dict = {1:"水瓶座",2:"双鱼座",3:"白羊座",4:"金牛座",5:"双子座",6:"巨蟹座",7:"狮子座",8:"处女座",9:"天秤座",10:"天蝎座",11:"射手座",12:"摩羯座"}
	blood_dict = {1:"A型",2:"B型",3:"O型",4:"AB型"}
	if ob.has_key("birthday"):
		birthday = str(birth(ob["birthday"].get("year",0))) + "-" + str(birth(ob["birthday"].get("month",0)))  + "-" +str(birth(ob["birthday"].get("day",0)))
	else:
		birthday = "0-0-0"
	phone = trans(ob.get("phone","-")).encode('utf-8')
	gender_id = str(sex(ob.get("gender_id","")))
	college = trans(ob.get("college","-")).encode('utf-8')
	uin = str(ob.get("uin",""))
	lnick = trans(ob.get("lnick","-")).encode('utf-8')
	country_id = dis(ob.get("country_id","0")).encode('utf-8')
	province_id = "-" + dis(ob.get("province_id","0")).encode('utf-8')
	city_id = "-" + dis(ob.get("city_id","0")).encode('utf-8')
	zone_id ="-" + dis(ob.get("zone_id","0")).encode('utf-8')
	loc_id = country_id + province_id + city_id + zone_id
	loc = place_dict.get(loc_id,"-").encode('utf-8')
	h_country = dis(ob.get("h_country","0")).encode('utf-8')
	h_province = "-" + dis(ob.get("h_province","0")).encode('utf-8')
	h_city = "-" + dis(ob.get("h_city","0")).encode('utf-8')
	h_zone ="-" + dis(ob.get("h_zone","0")).encode('utf-8')
	h_loc_id = h_country + h_province + h_city + h_zone
	h_loc = place_dict.get(h_loc_id,"-").encode('utf-8')
	personal = trans(ob.get("personal","-")).encode('utf-8')
	shengxiao = sheng_dict.get(trans(ob.get("shengxiao","-")),"-")
	gender = str(sex(ob.get("gender","")))
	occupation = trans(ob.get("occupation","-")).encode('utf-8')
	constel = constel_dict.get(trans(ob.get("constel","-")),"-")
	blood = blood_dict.get(trans(ob.get("blood","-")),"-")
	url =trans(ob.get("url","-")).encode('utf-8')
	homepage = trans(ob.get("homepage","-")).encode('utf-8')
	nick = trans(ob.get("nick","-")).encode('utf-8')
	email = trans(ob.get("email","-")).encode('utf-8')
	uin2 = trans(ob.get("uin2","-")).encode('utf-8')
	mobile = trans(ob.get("mobile","-")).encode('utf-8')
	ts =  str(int(time.time()))
	return birthday + '\001' + phone + '\001' + gender_id + '\001' + college + '\001' + uin + '\001' + lnick + '\001' + loc_id + '\001' + loc + '\001' + h_loc_id + '\001' + h_loc + '\001' +\
			personal + '\001' + shengxiao + '\001' + gender + '\001' + occupation + '\001' + constel + '\001' + blood + '\001' + url + '\001' + homepage + '\001' + nick + '\001' +\
					email + '\001' + uin2 + '\001' + mobile + '\001' + ts

if __name__ == "__main__":
	if sys.argv[1] == '-h':
		comment = 'qq用户信息格式化为hive数据格式'
		print comment
		print 'argvs:\n argv[1]:qq file or dir input\n argv[2]:district_dict file or dir input\n argv[3]:dir output'
	sc = SparkContext(appName="spark qqinfo")
	rdd = sc.textFile(sys.argv[1])
	rdd2 = sc.textFile(sys.argv[2])
	p_dict = rdd2.map(lambda x:x.split('\t')).collectAsMap()
	broadcastVar = 	sc.broadcast(p_dict)
	place_dict = broadcastVar.value
	rdd.map(lambda x:f(x,place_dict))\
		.filter(lambda x:x!=None)\
			.saveAsTextFile(sys.argv[3])
	sc.stop()