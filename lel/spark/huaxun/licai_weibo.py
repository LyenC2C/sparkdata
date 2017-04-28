# coding:utf-8
from pyspark import SparkContext


def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

sc = SparkContext(appName="touzi_licai")

id_map = sc.textFile("/hive/warehouse/wl_service.db/t_lel_huaxun_licai_weibo_licaiidmiddle_20170330").map(lambda a:(a,None)).collectAsMap()
bc_id_map = sc.broadcast(id_map)
bc_id_v_map = bc_id_map.value
def split(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    loc = data[2]
    return ((id,loc),ids)

def filter(a,b):
    if bc_id_v_map.has_key(b):
        return "\001".join([a[0],a[1]])

fir = sc.textFile("/hive/warehouse/wl_service.db/t_lel_huaxun_licai_weibo_idmiddle_20170330")\
    .map(lambda a:split(a))\
    .flatMapValues(lambda a:a.split(","))\
    .map(lambda (a,b):filter(a,b)).filter(lambda a:a is not None).distinct().saveAsTextFile("/user/lel/temp/results/huaxun_weibo")





