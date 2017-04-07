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
    return s.replace(' ', '').replace('（', '(').replace('）',')').replace('－', '-').replace('--', '').replace('--', '').replace('-', '')

def process(line):
    ob = json.loads(valid_jsontxt(line))
    if not isinstance(ob, dict): return None
    result = []
    province_name = ob.get("province", {}).get("province_name", "\\N")
    city_name = ob.get("city", {}).get("city_name", "\\N")
    wd_name = ob.get("blank_list", {}).get("wy_name", "\\N")
    wd_phone = replace(ob.get("blank_list", {}).get("wd_phone", "\\N"))
    if wd_phone not in "\\N":
        numbers = re.findall(r"[(]?\d+[)]?\d*", wd_phone)
        for phone in numbers:
            if phone.startswith('('):
                phone_sub = re.findall(r"\d+?\d*", phone)
                if phone_sub[0].startswith('86') and '：' not in phone_sub[0]:
                    phone = phone_sub[0].replace('86', '') + phone_sub[1]
                else:
                    phone = phone_sub[0] + phone_sub[1]
            if len(phone) > 5:
                result.append((phone, wd_name))
    blank_name = ob.get("blank_name", "\\N")
    blank_phone = replace(ob.get("blank_phone", "\\N"))
    if blank_phone not in "\\N":
        numbers = re.findall(r"[(]?\d+[)]?\d*", blank_phone)
        for phone in numbers:
            if phone.startswith('('):
                phone_sub = re.findall(r"\d+?\d*", phone)
                if phone_sub[0].startswith('86') and '：' not in phone_sub[0]:
                    phone = phone_sub[0].replace('86', '') + phone_sub[1]
                else:
                    phone = phone_sub[0] + phone_sub[1]
            if len(phone) > 5:
                result.append((phone, blank_name))
    return result


sc = SparkContext(appName="bankinfo" + lastday)

data = sc.textFile("/commit/data_product/bank_info/20170217.bank.infos.all") \
    .flatMap(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .distinct() \
    .map(lambda (a, b): a + "\001" + b).coalesce(1) \
    .saveAsTextFile("/user/lel/temp/bankinfo")
