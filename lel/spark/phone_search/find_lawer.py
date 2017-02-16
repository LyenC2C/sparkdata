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


def process(line):
    lines = line.split("\t")
    intro_page = lines[0]
    timpstamp = lines[1]
    jsonStr = lines[2]
    data = json.loads(valid_jsontxt(jsonStr))
    if not isinstance(data, dict): return None
    lawer_info_url = data.get("lawer_info_url", "\\N")
    practice_company = data.get("practice_company", "\\N")
    lawer_reduce = data.get("lawer_reduce", "\\N")
    lawer_office = data.get("lawer_office", "\\N")
    lvshi_info_add = data.get("lvshi_info_add", "\\N")
    professional_qualifications = data.get("professional_qualifications", "\\N")
    lawer_phone_place = data.get("lawer_phone", "\\N")
    lawer_e_mail = data.get("lawer_e_mail", "\\N")
    lawer_name = data.get("lawer_name", "\\N")
    lawer_qq = data.get("lawer_qq", "\\N")
    lawer_telphone = data.get("lawer_telphone", "\\N")
    year_of_practice = data.get("year_of_practice", "\\N")
    img_url = data.get("img_url", "\\N")
    return (
        lawer_telphone, lawer_name, practice_company, lawer_office, lvshi_info_add, lawer_phone_place,
        professional_qualifications, lawer_reduce, lawer_info_url, lawer_e_mail, lawer_qq, year_of_practice,
        intro_page, img_url, timpstamp)


sc = SparkContext(appName="find_lawer_search" + lastday)

data = sc.textFile("/commit/credit/findlaw/*") \
    .map(lambda a: process(a)) \
    .filter(lambda a: a is not None) \
    .distinct().map(lambda a: "\001".join((valid_jsontxt(i) for i in a))) \
    .saveAsTextFile("/user/lel/temp/find_lawer_search")
