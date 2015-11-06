__author__ = 'wrt'
#coding:utf-8
from pyspark import SparkContext
import sys
from pyspark.sql import *
from pyspark.sql.types import *
import rapidjson as json

sc=SparkContext(appName="item_pinpai")
sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)


def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else :
        return content
def pinpai(line):
    ss = line.strip().split('\\001')
    return (ss[1], ss[3] + "\t" + ss[4])
def pinpai_en(line):
    ss = line.strip().split('\\001')
    return (ss[0], ss[3] + "\t" + ss[4])
def f(x,p_dict,pe_dict):
    if p_dict.has_key(x[0]):
        return x[1] + "\t" + x[0] + "\t" + str(x[2]) + "\t" + p_dict.get(x[0])
    if pe_dict.has_key(x[0]):
        return x[1] + "\t" + x[0] + "\t" + str(x[2]) + "\t" + pe_dict.get(x[0])
    '''
    for ky in p_dict.keys():
        if valid_jsontxt(x[0]) in ky:
            return x[1] + "\t" + x[0] + "\t" + ky.decode('utf-8') + "\t" + str(x[2])
    for ky in pe_dict.keys():
        if valid_jsontxt(x[0]) in ky:
            return x[1] + "\t" + x[0] + "\t" + ky.decode('utf-8') + "\t" + str(x[2])
    '''

hiveContext.sql('use wlbase_dev')
rdd = hiveContext.sql('select * from t_base_ec_brand')
rdd2 = sc.textFile('/user/wrt/pinpai.info')
p_dict = rdd2.map(lambda x: pinpai(x)).collectAsMap()
pe_dict = rdd2.map(lambda x: pinpai_en(x)).collectAsMap()
broadcastVar = sc.broadcast(p_dict)
broadcastVar2 = sc.broadcast(pe_dict)
place_dict = broadcastVar.value
place_en_dict = broadcastVar2.value
rdd.map(lambda x:[x.brand_name, x.brand_id, x.stars]).map(lambda x:f(x,place_dict,place_en_dict))\
		.filter(lambda x:x!=None)\
			.saveAsTextFile(sys.argv[1])
sc.stop()