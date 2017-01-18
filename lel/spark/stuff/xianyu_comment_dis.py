from pyspark import SparkContext
from operator import itemgetter

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

sc = SparkContext(appName="xianyu_comment_dis")
data = sc.textFile("/hive/warehouse/wlbase_dev.db/t_base_ec_xianyu_itemcomment/ds=20170116")

def getfields(lines):
    fields = valid_jsontxt(lines.split("\001"))
    itemid = valid_jsontxt(fields[0])
    commentid = valid_jsontxt(fields[1])
    content = valid_jsontxt(fields[2])
    reporttime =valid_jsontxt(fields[3])
    reportername = valid_jsontxt(fields[4])
    reporternick = valid_jsontxt(fields[5])
    ts = fields[6]
    return (commentid,(itemid,commentid,content,reporttime,reportername,reporternick,ts))

data.map(lambda a:getfields(a))\
    .groupByKey()\
    .mapValues(lambda a:max(list(a), key=itemgetter(-1)))\
    .map(lambda (a,b):b)\
    .saveAsTextFile("/user/lel/temp/xianyu_comment_dis")

# 0 	26680328648 	772489424