    # coding=utf-8
from pyspark import SparkContext
import rapidjson as json
import re
from operator import itemgetter
import sys

lastday = sys.argv[1]


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def replace(s):
    return s.replace(' ', '').replace('（', '(').replace('）',')')\
            .replace('－', '-').replace('--', '-').replace('--', '-')\
            .replace('[','').replace(']','').replace('、','/')
def process(line):
    ob = json.loads(valid_jsontxt(line))
    if not isinstance(ob, dict): return None
    result = []
    province_name = ob.get("province", {}).get("province_name", "\\N")
    city_name = ob.get("city", {}).get("city_name", "\\N")
    wd_name = ob.get("blank_list", {}).get("wy_name", "\\N")
    wd_phone = replace(ob.get("blank_list", {}).get("wd_phone", "\\N"))
    if wd_phone not in "\\N":
        numbers = re.findall(r"\d+[-|(]?\d+[-|/|)]?\d+", wd_phone)
        for phone in numbers:
            if phone.startswith('86') or phone.startswith("(86"):
                phone_sub = re.findall(r"\d+?\d*", phone)
                if len(phone_sub) == 2:
                    code = phone_sub[0].replace('86', '0086')
                    p = phone_sub[1].replace('-','')
                    result.append((code+p,code,p,wd_name))
                if len(phone_sub) == 3:
                    code =phone_sub[0].replace('86', '0086')+phone_sub[1]
                    p = phone_sub[2]
                    result.append((code+p,code,p,wd_name))
            elif '/' in phone:
                if '-' in phone:
                    tmp = phone.split("-")
                    code = tmp[0]
                    phones = tmp[1].split('/')
                    for p in phones:
                        result.append((code+p,code,p,wd_name))
                else:
                    phones = phone.split('/')
                    for p in phones:
                        result.append((p,'',p,wd_name))
            elif '-' in phone:
                if len(phone) >20:
                    code = phone.split("-")[0]
                    phones = phone.split(code+'-')
                    for p in phones:
                        result.append((code+p,code,p,wd_name))
                else:
                    code = phone.split("-")[0]
                    p = phone.split("-")[1]
                    result.append((code+p,code,p,wd_name))
            elif phone.startswith("400") or phone.startswith("800"):
                 p = phone.replace('-','')
                 result.append((p,'',p,wd_name))
            else:
                result.append((p,'',p,wd_name))
    blank_name = ob.get("blank_name", "\\N")
    blank_phone = replace(ob.get("blank_phone", "\\N"))
    if blank_phone not in "\\N":
        numbers = re.findall(r"\d+[-|(]?\d+[-|/|)]?\d+", blank_phone)
        for phone in numbers:
            if phone.startswith('86') or phone.startswith("(86"):
                phone_sub = re.findall(r"\d+?\d*", phone)
                if len(phone_sub) == 2:
                    code = phone_sub[0].replace('86', '0086')
                    p = phone_sub[1].replace('-','')
                    result.append((code+p,code,p,blank_name))
                if len(phone_sub) == 3:
                    code =phone_sub[0].replace('86', '0086')+phone_sub[1]
                    p = phone_sub[2]
                    result.append((code+p,code,p,blank_name))
            elif '/' in phone:
                if '-' in phone:
                    tmp = phone.split("-")
                    code = tmp[0]
                    phones = tmp[1].split('/')
                    for p in phones:
                        result.append((code+p,code,p,blank_name))
                else:
                    phones = phone.split('/')
                    for p in phones:
                        result.append((p,'',p,blank_name))
            elif '-' in phone:
                if len(phone) >20:
                    code = phone.split("-")[0]
                    phones = phone.split(code+'-')
                    for p in phones:
                        result.append((code+p,code,p,blank_name))
                else:
                    code = phone.split("-")[0]
                    p = phone.split("-")[1]
                    result.append((code+p,code,p,blank_name))
            elif phone.startswith("400") or phone.startswith("800"):
                p = phone.replace('-','')
                result.append((p,'',p,blank_name))
            else:
                result.append((p,'',p,blank_name))
    return result


sc = SparkContext(appName="bankinfo" + lastday)

data = sc.textFile("/commit/data_product/bank_info/20170217.bank.infos.all") \
    .flatMap(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .distinct() \
    .map(lambda (a, b): a + "\001" + b).coalesce(1) \
    .saveAsTextFile("/user/lel/temp/bankinfo")
