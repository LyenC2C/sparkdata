#coding:utf-8
__author__ = 'wrt'
import sys
import rapidjson as json
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *

table = sys.argv[1] #t_wrt_weibo_invest_dav
table_put = sys.argv[2] #t_wrt_weibo_wbid_isinvest
sc = SparkContext(appName="dav_fans_" + table)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def split(fields):
    data = fields.split("\001")
    id = data[0]
    ids = data[1]
    return (id,ids)

def filter(a,b,bc_id_v_map):
    if bc_id_v_map.has_key(b):
        # return "\001".join([a[0],a[1]])
        return a
    else:
        return None
#
# def f(line,bc_id_v_map):
#     data = line.strip().split("\001")
#     id = data[0]
#     ids = data[1]
#     ids_list = ids.split(',')
#     if bc_id_v_map.has_key():
#

id_map = sc.textFile("/hive/warehouse/wl_usertag.db/" + table)\
    .map(lambda a:(a,"")).collectAsMap()
bc_id_map = sc.broadcast(id_map)
bc_id_v_map = bc_id_map.value
fir = sc.textFile("/hive/warehouse/wl_base.db/t_base_weibo_user_fri/ds=20161106/")
fir.map(lambda a:split(a))\
    .flatMapValues(lambda a:a.split(","))\
    .map(lambda (a,b):filter(a,b,bc_id_v_map)).filter(lambda a:a is not None).distinct()\
    .saveAsTextFile("/user/wrt/temp/dav_fans")

# schema = StructType([StructField("wbid", StringType(), True)])
# df = hiveContext.createDataFrame(fir, schema)
# hiveContext.registerDataFrameAsTable(df, 'dav_fans')
# hiveContext.sql("use wl_usertag")
# hiveContext.sql("drop table " + table_put)
# hiveContext.sql("create table  " + table_put + "as select * from dav_fans")

# hfs -rmr /user/wrt/temp/dav_fans
# spark2-submit --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 dav_fans.py t_wrt_weibo_invest_dav
# load inpath data '/user/wrt/temp/dav_fans' overwrite into table wl_usertag.t_wrt_weibo_wbid_isinvest;

