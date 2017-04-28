from pyspark import SparkContext
import rapidjson as json


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def getJson(s):
    return json.loads(valid_jsontxt(s))


def parseJson(data):
    if not isinstance(data, dict): return None
    uuid = data.get("uuid", "\\N")
    employment_info = data.get("employment_info", "\\N")
    return (uuid, employment_info)


sc = SparkContext(appName="weibo_user_employment")
sc. setLogLevel("")

data = sc.textFile("/commit/weibo/industry_category/*") \
         .map(lambda a: parseJson(getJson(a))) \
         .filter(lambda a: a is not None) \
         .distinct() \
         .map(lambda a:a[0] + "\001" + a[1])\
         .saveAsTextFile("/user/lel/temp/weibo_user_employment")
