# coding=utf-8
from pyspark import SparkContext
import rapidjson as json
import sys,re


lastday = sys.argv[1]


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def process(line):
    ob = json.loads(valid_jsontxt(line))
    if not isinstance(ob, dict): return [None]
    phones = ob.get("phones", "\\N")
    cate = ob.get("cate","\\N")
    cid = ob.get("cid","\\N")
    city_name = ob.get("city_name","\\N")
    name = ob.get("name", "\\N")
    addr = ob.get("addr", "\\N")
    phones_num=re.findall(r'\d+[-]?\d+',phones)
    result = []
    if phones_num:
        for phone in phones_num:
            if '-' in phone:
                acode = phone.split('-')[0]
                phone_num = phone.split('-')[1]
                phone = phone.replace('-','')
                result.append((phone,acode,phone_num,name,addr,cate,cid,city_name))
            else:
                acode = ''
                phone_num = phone
                result.append((phone,acode,phone_num,name,addr,cate,cid,city_name))
    else: return [None]
    return result



sc = SparkContext(appName="dianhuabang" + lastday)

data = sc.textFile("/commit/credit/dianhuabang/dianhua_bang.0317") \
    .flatMap(lambda a:process(a)) \
    .filter(lambda a:a is not None)\
    .distinct()\
    .map(lambda a: "\001".join([valid_jsontxt(i) for i in a]))\
    .saveAsTextFile("/user/lel/temp/dianhuabang")
