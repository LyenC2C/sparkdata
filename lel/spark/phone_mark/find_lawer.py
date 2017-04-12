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


def process(line):
    jsonStr = line
    data = json.loads(valid_jsontxt(jsonStr))
    result = []
    if not isinstance(data, dict): return [None]
    lawer_info_url = data.get("lawer_info_url", "\\N")
    practice_company = data.get("practice_company", "\\N")
    lawer_reduce = data.get("lawer_reduce", "\\N")
    # phones_dim = filter(lambda a: len(a) == 11 or ('-' in a and (len(a) >= 12 and len(a) <=13)),re.findall(r"\d+[-]?\d*",lawer_reduce))
    phones_dim = filter(lambda a: len(a) >= 11 ,re.findall(r"\d{3,4}[-]?\d{7,8}",lawer_reduce))
    lawer_office = data.get("lawer_office", "\\N")
    lvshi_info_add = data.get("lvshi_info_add", "\\N")
    professional_qualifications = data.get("professional_qualifications", "\\N")
    lawer_phone = data.get("lawer_phone", "\\N")
    lawer_e_mail = data.get("lawer_e_mail", "\\N")
    lawer_name = data.get("lawer_name", "\\N")
    lawer_qq = data.get("lawer_qq", "\\N")
    lawer_telphone = data.get("lawer_telphone", "\\N")
    year_of_practice = data.get("year_of_practice", "\\N")
    img_url = data.get("img_url", "\\N")
    if phones_dim and lawer_phone == '':
        for phone_dim in phones_dim:

            result.append((phone_dim, lawer_name, practice_company, lawer_office, lvshi_info_add, lawer_telphone,
                       professional_qualifications, lawer_reduce, lawer_info_url, lawer_e_mail, lawer_qq, year_of_practice, img_url))
    else:
        result.append((lawer_phone, lawer_name, practice_company, lawer_office, lvshi_info_add, lawer_telphone,
                       professional_qualifications, lawer_reduce, lawer_info_url, lawer_e_mail, lawer_qq, year_of_practice, img_url))
    return result


sc = SparkContext(appName="find_lawer_search" + lastday)

data = sc.textFile("/commit/credit/findlaw/lawer.info.20170217") \
    .flatMap(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .distinct().map(lambda a: "\001".join((valid_jsontxt(i) for i in a))) \
    .saveAsTextFile("/user/lel/temp/find_lawer_search")
