#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf
import rapidjson as json
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.set("spark.kryoserializer.buffer.mb", "1024")
conf.set("spark.akka.frameSize", "100")
conf.set("spark.network.timeout", "1000s")
conf.set("spark.driver.maxResultSize", "8g")

sc = SparkContext(appName="user_cattags", conf=conf)

sqlContext = SQLContext(sc)
hiveContext = HiveContext(sc)

def valid_jsontxt(content):
    # res = content
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n',"").replace("\r","").replace('\001',"").replace("\u0001","")

def try_fun(x):
    ob=json.loads(x)
    if type(ob)!=type({}):return  None
    try:
        if type(ob['ids'])!=type([]):return None
        if ob['ids'] == []: return None
        return [str(ob['uid']),','.join(set([str(i) for i in ob['ids']]))]
    except:return None

sc.textFile('/commit/weibo/friendships/20161101/').map(lambda x:try_fun(x)).\
    filter(lambda x:x is not None  and x[0] is not None and x[1] is not None)\
    .groupByKey().map(lambda (x,y):valid_jsontxt(x) + "\001" + valid_jsontxt(list(y)[0]))\
    .saveAsTextFile('/user/wrt/temp/weibo_friendships_20161101')
    # .map(lambda x:'\001'.join(x)).saveAsTextFile('/user/zlj/tmp/weibo_friendships_20161101')
