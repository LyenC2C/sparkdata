# coding=utf-8
from pyspark import SparkContext
import rapidjson as json
from operator import itemgetter
import sys


lastday = sys.argv[1]


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def getJson(line):
    return json.loads(valid_jsontxt(line))

def parseJson(ob):
    if not isinstance(ob, dict): return None
    company = ob.get("company","\\N")
    auth = ob.get("auth", "\\N")
    sex = ob.get("sex", "\\N")
    phone = ob.get("phone", "\\N")
    if not phone.replace('-','').isdigit(): return None
    if phone.startswith("400") or phone.startswith("800"):
        code = ''
        p = phone.replace('-','')
    elif phone.startswith("86"):
        code = ''
        p = phone[2:]
    elif '-' in phone:
        code = phone.split('-')[0]
        p = phone.split('-')[1]
    else:
        code = ''
        p = phone
    birth_day = ob.get("birth_day", "\\N").replace("-","")
    jifen = ob.get("jifen", "\\N")
    row = ob.get("row", "\\N")
    name = ob.get("name", "\\N")
    area = ob.get("area", "\\N")
    register_time = ob.get("register_time", "\\N")
    last_log_in = ob.get("last_log_in", "\\N")
    return (code+p,code,p,company,auth,sex,birth_day,jifen,row,name,area,register_time,last_log_in)

sc = SparkContext(appName="huangye88" + lastday)

data = sc.textFile("/commit/credit/huangye88/20170220.huangye88.userinfo.all.20170220") \
    .map(lambda a:parseJson(getJson(a)))\
    .filter(lambda a: a is not None) \
    .distinct()\
    .map(lambda a: "\001".join((valid_jsontxt(i) for i in a))) \
    .saveAsTextFile("/user/lel/temp/huangye88_userinfo_3")
